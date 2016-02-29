package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/brimio"
	"github.com/gholt/brimtime"
	"github.com/gholt/flog"
	"github.com/gholt/store"
	"github.com/jessevdk/go-flags"
	"github.com/pandemicsyn/oort/api"
)

type optsStruct struct {
	Scale         float64 `long:"scale" description:"Sets the overall scale factor for many settings; default is 1, set lower (e.g. 0.5) to decrease memory usage."`
	API           string  `long:"api" description:"Connect to the address given, using Oort API instead of local store"`
	GroupStore    bool    `short:"g" long:"groupstore" description:"Use GroupStore instead of ValueStore."`
	Clients       int     `long:"clients" description:"The number of clients. Default: cores*cores"`
	Cores         int     `long:"cores" description:"The number of cores. Default: CPU core count"`
	Debug         bool    `long:"debug" description:"Turns on debug output."`
	ExtendedStats bool    `long:"extended-stats" description:"Extended statistics at exit."`
	Metrics       bool    `long:"metrics" description:"Displays metrics one per minute."`
	Length        int     `short:"l" long:"length" description:"Length of values. Default: 0"`
	Number        int     `short:"n" long:"number" description:"Number of keys. Default: 0"`
	Random        int     `long:"random" description:"Random number seed. Default: 0"`
	Replicate     bool    `long:"replicate" description:"Creates a second value store that will test replication."`
	Timestamp     int64   `long:"timestamp" description:"Timestamp value. Default: current time"`
	TombstoneAge  int     `long:"tombstone-age" description:"Seconds to keep tombstones. Default: 4 hours"`
	MaxGroupSize  int     `long:"max-group-size" description:"Maximum number of items per group for writegroup."`
	Positional    struct {
		Tests []string `name:"tests" description:"blockprof cpuprof memprof write lookup read delete writegroup lookupgroup readgroup run"`
	} `positional-args:"yes"`
	blockprofi int
	blockproff *os.File
	cpuprofi   int
	cpuproff   *os.File
	memprofi   int
	keyspace   []byte
	buffers    [][]byte
	st         runtime.MemStats
	store      store.Store
	repstore   store.Store
	ring       *ringPipe
	rring      *ringPipe
}

var opts optsStruct
var parser = flags.NewParser(&opts, flags.Default)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		args = append(args, "-h")
	}
	if _, err := parser.ParseArgs(args); err != nil {
		os.Exit(1)
	}
	var debugWriter io.Writer = &brimio.NullIO{}
	if opts.Debug {
		debugWriter = os.Stdout
	}
	flog.Default = flog.New(&flog.Config{
		CriticalWriter: os.Stderr,
		ErrorWriter:    os.Stderr,
		WarningWriter:  os.Stderr,
		InfoWriter:     os.Stdout,
		DebugWriter:    debugWriter,
	})
	flog.InfoPrintf("init:")
	for _, arg := range opts.Positional.Tests {
		switch arg {
		case "blockprof":
		case "cpuprof":
		case "memprof":
		case "delete":
		case "lookupgroup":
		case "readgroup":
		case "writegroup":
		case "lookup":
		case "read":
		case "run":
		case "write":
		default:
			flog.CriticalPrintf("unknown test named %#v", arg)
			os.Exit(1)
		}
	}
	if opts.Cores > 0 {
		runtime.GOMAXPROCS(opts.Cores)
	} else if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
	opts.Cores = runtime.GOMAXPROCS(0)
	if opts.Clients == 0 {
		opts.Clients = opts.Cores * opts.Cores
	}
	if opts.Timestamp == 0 {
		opts.Timestamp = brimtime.TimeToUnixMicro(time.Now())
	}
	opts.keyspace = make([]byte, opts.Number*16)
	brimio.NewSeededScrambled(int64(opts.Random)).Read(opts.keyspace)
	opts.buffers = make([][]byte, opts.Clients)
	for i := 0; i < opts.Clients; i++ {
		opts.buffers[i] = make([]byte, 4*1024*1024)
	}
	memstat()
	flog.InfoPrintf("start:")
	begin := time.Now()
	var vscfg *store.ValueStoreConfig
	var gscfg *store.GroupStoreConfig
	var logger flog.Flog
	if opts.GroupStore {
		logger = flog.Sub(&flog.Config{Name: "group-store"})
		gscfg = &store.GroupStoreConfig{
			Scale:       opts.Scale,
			Workers:     opts.Cores,
			LogCritical: logger.CriticalPrintf,
			LogError:    logger.ErrorPrintf,
		}
	} else {
		logger = flog.Sub(&flog.Config{Name: "value-store"})
		vscfg = &store.ValueStoreConfig{
			Scale:       opts.Scale,
			Workers:     opts.Cores,
			LogCritical: logger.CriticalPrintf,
			LogError:    logger.ErrorPrintf,
		}
	}
	if opts.TombstoneAge > 0 {
		if opts.GroupStore {
			gscfg.TombstoneAge = opts.TombstoneAge
		} else {
			vscfg.TombstoneAge = opts.TombstoneAge
		}
	}
	if opts.MaxGroupSize < 1 {
		opts.MaxGroupSize = 100
	}
	wg := &sync.WaitGroup{}
	if opts.Replicate {
		conn, rconn := net.Pipe()
		opts.ring = NewRingPipe("127.0.0.1:11111", conn)
		opts.rring = NewRingPipe("127.0.0.1:22222", rconn)
		var rvscfg *store.ValueStoreConfig
		var rgscfg *store.GroupStoreConfig
		var rlogger flog.Flog
		if opts.GroupStore {
			rgscfg = &store.GroupStoreConfig{}
			*rgscfg = *gscfg
			gscfg.MsgRing = opts.ring
			rgscfg.MsgRing = opts.rring
			rgscfg.Path = "replicated"
			rlogger = flog.Sub(&flog.Config{Name: "replicated-group-store"})
			rgscfg.LogCritical = rlogger.CriticalPrintf
			rgscfg.LogError = rlogger.ErrorPrintf
		} else {
			rvscfg = &store.ValueStoreConfig{}
			*rvscfg = *vscfg
			vscfg.MsgRing = opts.ring
			rvscfg.MsgRing = opts.rring
			rvscfg.Path = "replicated"
			rlogger = flog.Sub(&flog.Config{Name: "replicated-value-store"})
			rvscfg.LogCritical = rlogger.CriticalPrintf
			rvscfg.LogError = rlogger.ErrorPrintf
		}
		if opts.Debug {
			if opts.GroupStore {
				rgscfg.LogDebug = rlogger.DebugPrintf
			} else {
				rvscfg.LogDebug = rlogger.DebugPrintf
			}
		}
		wg.Add(1)
		go func() {
			var restartChan chan error
			if opts.GroupStore {
				opts.repstore, restartChan = store.NewGroupStore(rgscfg)
			} else {
				opts.repstore, restartChan = store.NewValueStore(rvscfg)
			}
			go func(restartChan chan error) {
				if err := <-restartChan; err != nil {
					panic(err)
				}
			}(restartChan)
			if opts.Metrics {
				go func() {
					for {
						time.Sleep(60 * time.Second)
						stats, err := opts.repstore.Stats(false)
						if err != nil {
							panic(err)
						}
						rlogger.DebugPrintf("%s", stats)
					}
				}()
			}
			wg.Done()
		}()
	}
	if opts.Debug {
		if opts.GroupStore {
			gscfg.LogDebug = logger.DebugPrintf
		} else {
			vscfg.LogDebug = logger.DebugPrintf
		}
	}
	var restartChan chan error
	if opts.GroupStore {
		if opts.API != "" {
			var err error
			opts.store, err = api.NewGroupStore(opts.API, opts.Cores*opts.Cores, true)
			if err != nil {
				panic(err)
			}
		} else {
			opts.store, restartChan = store.NewGroupStore(gscfg)
		}
	} else {
		if opts.API != "" {
			var err error
			opts.store, err = api.NewValueStore(opts.API, opts.Cores*opts.Cores, true)
			if err != nil {
				panic(err)
			}
		} else {
			opts.store, restartChan = store.NewValueStore(vscfg)
		}
	}
	if restartChan != nil {
		go func(restartChan chan error) {
			if err := <-restartChan; err != nil {
				panic(err)
			}
		}(restartChan)
	}
	if opts.Metrics {
		go func() {
			for {
				time.Sleep(60 * time.Second)
				stats, err := opts.store.Stats(false)
				if err != nil {
					panic(err)
				}
				logger.DebugPrintf("%s", stats)
			}
		}()
	}
	wg.Wait()
	if opts.repstore != nil {
		opts.ring.Start()
		opts.rring.Start()
		wg.Add(1)
		go func() {
			if err := opts.repstore.Startup(); err != nil {
				panic(err)
			}
			wg.Done()
		}()
	}
	wg.Add(1)
	go func() {
		if err := opts.store.Startup(); err != nil {
			panic(err)
		}
		wg.Done()
	}()
	wg.Wait()
	dur := time.Now().Sub(begin)
	flog.InfoPrintf("%s to start", dur)
	memstat()
	for _, arg := range opts.Positional.Tests {
		switch arg {
		case "blockprof":
			if opts.blockproff != nil {
				flog.InfoPrintf("blockprof: off")
				runtime.SetBlockProfileRate(0)
				pprof.Lookup("block").WriteTo(opts.blockproff, 1)
				opts.blockproff.Close()
				opts.blockproff = nil
			} else {
				flog.InfoPrintf("blockprof: on")
				var err error
				opts.blockproff, err = os.Create(fmt.Sprintf("blockprof%d", opts.blockprofi))
				opts.blockprofi++
				if err != nil {
					flog.CriticalPrintf("%s", err)
					os.Exit(1)
				}
				runtime.SetBlockProfileRate(1)
			}
		case "cpuprof":
			if opts.cpuproff != nil {
				flog.InfoPrintf("cpuprof: off")
				pprof.StopCPUProfile()
				opts.cpuproff.Close()
				opts.cpuproff = nil
			} else {
				flog.InfoPrintf("cpuprof: on")
				var err error
				opts.cpuproff, err = os.Create(fmt.Sprintf("cpuprof%d", opts.cpuprofi))
				opts.cpuprofi++
				if err != nil {
					flog.CriticalPrintf("%s", err)
					os.Exit(1)
				}
				pprof.StartCPUProfile(opts.cpuproff)
			}
		case "memprof":
			runtime.GC()
			f, err := os.Create(fmt.Sprintf("memprof%d", opts.memprofi))
			if err != nil {
				flog.CriticalPrintln(err)
				os.Exit(1)
			}
			opts.memprofi++
			pprof.WriteHeapProfile(f)
			f.Close()
		case "delete":
			delete()
		case "lookupgroup":
			lookupgroup()
		case "readgroup":
			readgroup()
		case "writegroup":
			writegroup()
		case "lookup":
			lookup()
		case "read":
			read()
		case "run":
			run()
		case "write":
			write()
		}
		memstat()
	}
	if opts.blockproff != nil {
		runtime.SetBlockProfileRate(0)
		pprof.Lookup("block").WriteTo(opts.blockproff, 0)
		opts.blockproff.Close()
		opts.blockproff = nil
	}
	if opts.cpuproff != nil {
		pprof.StopCPUProfile()
		opts.cpuproff.Close()
		opts.cpuproff = nil
	}
	flog.InfoPrintf("flush:")
	begin = time.Now()
	if opts.repstore != nil {
		wg.Add(1)
		go func() {
			opts.repstore.Flush()
			wg.Done()
		}()
	}
	opts.store.Flush()
	wg.Wait()
	dur = time.Now().Sub(begin)
	flog.InfoPrintf("%s to flush", dur)
	memstat()
	flog.InfoPrintf("stats:")
	begin = time.Now()
	var rvsStringerStats fmt.Stringer
	var rgsStringerStats fmt.Stringer
	var rvsStats *store.ValueStoreStats
	var rgsStats *store.GroupStoreStats
	var err error
	if opts.repstore != nil {
		wg.Add(1)
		go func() {
			if opts.GroupStore {
				rgsStringerStats, err = opts.repstore.Stats(opts.ExtendedStats)
				if err != nil {
					panic(err)
				}
				rgsStats, _ = rgsStringerStats.(*store.GroupStoreStats)
			} else {
				rvsStringerStats, err = opts.repstore.Stats(opts.ExtendedStats)
				if err != nil {
					panic(err)
				}
				rvsStats, _ = rvsStringerStats.(*store.ValueStoreStats)
			}
			wg.Done()
		}()
	}
	var vsStringerStats fmt.Stringer
	var gsStringerStats fmt.Stringer
	var vsStats *store.ValueStoreStats
	var gsStats *store.GroupStoreStats
	if opts.GroupStore {
		gsStringerStats, err = opts.store.Stats(opts.ExtendedStats)
		if err != nil {
			panic(err)
		}
		gsStats, _ = gsStringerStats.(*store.GroupStoreStats)
	} else {
		vsStringerStats, err = opts.store.Stats(opts.ExtendedStats)
		if err != nil {
			panic(err)
		}
		vsStats, _ = vsStringerStats.(*store.ValueStoreStats)
	}
	wg.Wait()
	dur = time.Now().Sub(begin)
	flog.InfoPrintf("%s to obtain stats", dur)
	statsOutput := false
	if opts.GroupStore {
		if gsStats == nil {
			flog.InfoPrintf("GroupStore: stats:\n%s", gsStringerStats)
			statsOutput = true
		}
	} else {
		if vsStats == nil {
			flog.InfoPrintf("ValueStore: stats:\n%s", vsStringerStats)
			statsOutput = true
		}
	}
	if !statsOutput {
		if opts.ExtendedStats {
			if opts.GroupStore {
				flog.InfoPrintf("GroupStore: stats:\n%s", gsStats.String())
			} else {
				flog.InfoPrintf("ValueStore: stats:\n%s", vsStats.String())
			}
		} else {
			if opts.GroupStore {
				flog.InfoPrintf("GroupStore: Values: %d", gsStats.Values)
				flog.InfoPrintf("GroupStore: ValueBytes: %d", gsStats.ValueBytes)
			} else {
				flog.InfoPrintf("ValueStore: Values: %d", vsStats.Values)
				flog.InfoPrintf("ValueStore: ValueBytes: %d", vsStats.ValueBytes)
			}
		}
	}
	if opts.repstore != nil {
		statsOutput = false
		if opts.GroupStore {
			if rgsStats == nil {
				flog.InfoPrintf("ReplicatedGroupStore: stats:\n%s", rgsStringerStats)
				statsOutput = true
			}
		} else {
			if rvsStats == nil {
				flog.InfoPrintf("ReplicatedValueStore: stats:\n%s", rvsStringerStats)
				statsOutput = true
			}
		}
		if !statsOutput {
			if opts.ExtendedStats {
				if opts.GroupStore {
					flog.InfoPrintf("ReplicatedGroupStore: stats:\n%s", rgsStats.String())
				} else {
					flog.InfoPrintf("ReplicatedValueStore: stats:\n%s", rvsStats.String())
				}
			} else {
				if opts.GroupStore {
					flog.InfoPrintf("ReplicatedGroupStore: Values: %d", rgsStats.Values)
					flog.InfoPrintf("ReplicatedGroupStore: ValueBytes: %d", rgsStats.ValueBytes)
				} else {
					flog.InfoPrintf("ReplicatedValueStore: Values: %d", rvsStats.Values)
					flog.InfoPrintf("ReplicatedValueStore: ValueBytes: %d", rvsStats.ValueBytes)
				}
			}
		}
	}
	memstat()
	if opts.repstore != nil {
		flog.InfoPrintf("drops %d %d", opts.ring.sendDrops, opts.rring.sendDrops)
	}
	flog.InfoPrintf("shutdown:")
	begin = time.Now()
	if opts.repstore != nil {
		wg.Add(1)
		go func() {
			opts.repstore.Shutdown()
			wg.Done()
		}()
	}
	opts.store.Shutdown()
	wg.Wait()
	dur = time.Now().Sub(begin)
	flog.InfoPrintf("%s to shutdown", dur)
}

func memstat() {
	lastAlloc := opts.st.TotalAlloc
	runtime.ReadMemStats(&opts.st)
	deltaAlloc := opts.st.TotalAlloc - lastAlloc
	lastAlloc = opts.st.TotalAlloc
	flog.InfoPrintf("%0.2fG total alloc, %0.2fG delta", float64(opts.st.TotalAlloc)/1024/1024/1024, float64(deltaAlloc)/1024/1024/1024)
}

func delete() {
	flog.InfoPrintf("delete:")
	var superseded uint64
	timestamp := opts.Timestamp | 1
	begin := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(opts.Clients)
	for i := 0; i < opts.Clients; i++ {
		go func(client int) {
			var s uint64
			number := len(opts.keyspace) / 16
			numberPer := number / opts.Clients
			var keys []byte
			if client == opts.Clients-1 {
				keys = opts.keyspace[numberPer*client*16:]
			} else {
				keys = opts.keyspace[numberPer*client*16 : numberPer*(client+1)*16]
			}
			if opts.GroupStore {
				gs := opts.store.(store.GroupStore)
				for o := 0; o < len(keys); o += 16 {
					if oldTimestamp, err := gs.Delete(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), timestamp); err != nil {
						panic(err)
					} else if oldTimestamp > timestamp {
						s++
					}
				}
			} else {
				vs := opts.store.(store.ValueStore)
				for o := 0; o < len(keys); o += 16 {
					if oldTimestamp, err := vs.Delete(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), timestamp); err != nil {
						panic(err)
					} else if oldTimestamp > timestamp {
						s++
					}
				}
			}
			if s > 0 {
				atomic.AddUint64(&superseded, s)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	opts.store.Flush()
	dur := time.Now().Sub(begin)
	flog.InfoPrintf("%s %.0f/s to delete %d values (timestamp %d)", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), opts.Number, timestamp)
	if superseded > 0 {
		flog.InfoPrintf("%d SUPERCEDED!", superseded)
	}
}

func lookupgroup() {
	flog.InfoPrintf("lookupgroup:")
	if !opts.GroupStore {
		flog.CriticalPrintf("not valid for ValueStore")
		return
	}
	var itemCount uint64
	var mismatch uint64
	begin := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(opts.Clients)
	for i := 0; i < opts.Clients; i++ {
		go func(client int) {
			number := len(opts.keyspace) / 16
			numberPer := number / opts.Clients
			var keys []byte
			if client == opts.Clients-1 {
				keys = opts.keyspace[numberPer*client*16:]
			} else {
				keys = opts.keyspace[numberPer*client*16 : numberPer*(client+1)*16]
			}
			gs := opts.store.(store.GroupStore)
			for o := 0; o < len(keys); o += 16 {
				groupSize := 1 + (binary.BigEndian.Uint64(keys[o:]) % uint64(opts.MaxGroupSize))
				list, err := gs.LookupGroup(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]))
				if err != nil {
					panic(err)
				}
				atomic.AddUint64(&itemCount, uint64(len(list)))
				if uint64(len(list)) != groupSize {
					atomic.AddUint64(&mismatch, 1)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	dur := time.Now().Sub(begin)
	flog.InfoPrintf("%s %.0f/s to lookup %d groups (%d items)", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), opts.Number, itemCount)
	if mismatch > 0 {
		flog.ErrorPrintf("%d MISMATCHES! (groups without the correct number of items)", mismatch)
	}
}

func readgroup() {
	flog.InfoPrintf("readgroup:")
	if !opts.GroupStore {
		flog.ErrorPrintf("not valid for ValueStore")
		return
	}
	var itemCount uint64
	var mismatch uint64
	begin := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(opts.Clients)
	for i := 0; i < opts.Clients; i++ {
		go func(client int) {
			number := len(opts.keyspace) / 16
			numberPer := number / opts.Clients
			var keys []byte
			if client == opts.Clients-1 {
				keys = opts.keyspace[numberPer*client*16:]
			} else {
				keys = opts.keyspace[numberPer*client*16 : numberPer*(client+1)*16]
			}
			gs := opts.store.(store.GroupStore)
			for o := 0; o < len(keys); o += 16 {
				groupSize := 1 + (binary.BigEndian.Uint64(keys[o:]) % uint64(opts.MaxGroupSize))
				if ags, ok := gs.(store.GroupStore); ok {
					itemList, err := ags.ReadGroup(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]))
					if err != nil {
						panic(err)
					}
					// TODO: Should probably verify all the values seem proper
					// like read does.
					items := uint64(len(itemList))
					atomic.AddUint64(&itemCount, items)
					if items != groupSize {
						flog.ErrorPrintf("%d, %d", groupSize, items)
						atomic.AddUint64(&mismatch, 1)
					}
				} else {
					// TODO: Local store needs to do LookupGroup and then Read
					// each item.
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	dur := time.Now().Sub(begin)
	flog.InfoPrintf("%s %.0f/s to read %d groups (%d items)", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), opts.Number, itemCount)
	if mismatch > 0 {
		flog.ErrorPrintf("%d MISMATCHES! (groups without the correct number of items)", mismatch)
	}
}

func writegroup() {
	flog.InfoPrintf("writegroup:")
	if !opts.GroupStore {
		flog.ErrorPrintf("not valid for ValueStore")
		return
	}
	var itemCount uint64
	var superseded uint64
	timestamp := opts.Timestamp
	if timestamp == 0 {
		timestamp = 2
	}
	begin := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(opts.Clients)
	for i := 0; i < opts.Clients; i++ {
		go func(client int) {
			value := make([]byte, opts.Length)
			randomness := value
			if len(value) > 10 {
				copy(value, []byte("START67890"))
				randomness = value[10:]
				if len(value) > 20 {
					copy(value[len(value)-10:], []byte("123456STOP"))
					randomness = value[10 : len(value)-10]
				}
			}
			scr := brimio.NewScrambled()
			var s uint64
			number := len(opts.keyspace) / 16
			numberPer := number / opts.Clients
			var keys []byte
			if client == opts.Clients-1 {
				keys = opts.keyspace[numberPer*client*16:]
			} else {
				keys = opts.keyspace[numberPer*client*16 : numberPer*(client+1)*16]
			}
			gs := opts.store.(store.GroupStore)
			for o := 0; o < len(keys); o += 16 {
				groupSize := 1 + (binary.BigEndian.Uint64(keys[o:]) % uint64(opts.MaxGroupSize))
				atomic.AddUint64(&itemCount, groupSize)
				for p := uint64(0); p < groupSize; p++ {
					scr.Read(randomness)
					if oldTimestamp, err := gs.Write(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), p, p, timestamp, value); err != nil {
						panic(err)
					} else if oldTimestamp > timestamp {
						s++
					}
				}
			}
			if s > 0 {
				atomic.AddUint64(&superseded, s)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	opts.store.Flush()
	dur := time.Now().Sub(begin)
	flog.InfoPrintf("%s %.0f/s %0.2fG/s to write %d items (%d groups) (timestamp %d)", dur, float64(itemCount)/(float64(dur)/float64(time.Second)), float64(itemCount)/(float64(dur)/float64(time.Second))/1024/1024/1024, itemCount, opts.Number, timestamp)
	if superseded > 0 {
		flog.ErrorPrintf("%d SUPERCEDED!", superseded)
	}
}

func lookup() {
	flog.InfoPrintf("lookup:")
	var missing uint64
	var deleted uint64
	begin := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(opts.Clients)
	for i := 0; i < opts.Clients; i++ {
		go func(client int) {
			number := len(opts.keyspace) / 16
			numberPer := number / opts.Clients
			var keys []byte
			if client == opts.Clients-1 {
				keys = opts.keyspace[numberPer*client*16:]
			} else {
				keys = opts.keyspace[numberPer*client*16 : numberPer*(client+1)*16]
			}
			var m uint64
			var d uint64
			if opts.GroupStore {
				gs := opts.store.(store.GroupStore)
				for o := 0; o < len(keys); o += 16 {
					timestamp, _, err := gs.Lookup(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]))
					if err == store.ErrNotFound {
						if timestamp == 0 {
							m++
						} else {
							d++
						}
					} else if err != nil {
						panic(err)
					}
				}
			} else {
				vs := opts.store.(store.ValueStore)
				for o := 0; o < len(keys); o += 16 {
					timestamp, _, err := vs.Lookup(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]))
					if err == store.ErrNotFound {
						if timestamp == 0 {
							m++
						} else {
							d++
						}
					} else if err != nil {
						panic(err)
					}
				}
			}
			if m > 0 {
				atomic.AddUint64(&missing, m)
			}
			if d > 0 {
				atomic.AddUint64(&deleted, d)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	dur := time.Now().Sub(begin)
	flog.InfoPrintf("%s %.0f/s to lookup %d values", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), opts.Number)
	if missing > 0 {
		flog.ErrorPrintf("%d MISSING!", missing)
	}
	if deleted > 0 {
		flog.ErrorPrintf("%d DELETED!", deleted)
	}
}

func read() {
	flog.InfoPrintf("read:")
	var valuesLength uint64
	var missing uint64
	var deleted uint64
	start := []byte("START67890")
	stop := []byte("123456STOP")
	wg := &sync.WaitGroup{}
	wg.Add(opts.Clients)
	begin := time.Now()
	for i := 0; i < opts.Clients; i++ {
		go func(client int) {
			f := func(keys []byte) {
				var vl uint64
				var m uint64
				var d uint64
				if opts.GroupStore {
					gs := opts.store.(store.GroupStore)
					for o := 0; o < len(keys); o += 16 {
						timestamp, v, err := gs.Read(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), opts.buffers[client][:0])
						if err == store.ErrNotFound {
							if timestamp == 0 {
								m++
							} else {
								d++
							}
						} else if err != nil {
							panic(err)
						} else if len(v) > 10 && !bytes.Equal(v[:10], start) {
							panic("bad start to value")
						} else if len(v) > 20 && !bytes.Equal(v[len(v)-10:], stop) {
							panic("bad stop to value")
						} else {
							vl += uint64(len(v))
						}
					}
				} else {
					vs := opts.store.(store.ValueStore)
					for o := 0; o < len(keys); o += 16 {
						timestamp, v, err := vs.Read(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), opts.buffers[client][:0])
						if err == store.ErrNotFound {
							if timestamp == 0 {
								m++
							} else {
								d++
							}
						} else if err != nil {
							panic(err)
						} else if len(v) > 10 && !bytes.Equal(v[:10], start) {
							panic("bad start to value")
						} else if len(v) > 20 && !bytes.Equal(v[len(v)-10:], stop) {
							panic("bad stop to value")
						} else {
							vl += uint64(len(v))
						}
					}
				}
				if vl > 0 {
					atomic.AddUint64(&valuesLength, vl)
				}
				if m > 0 {
					atomic.AddUint64(&missing, m)
				}
				if d > 0 {
					atomic.AddUint64(&deleted, d)
				}
			}
			number := len(opts.keyspace) / 16
			numberPer := number / opts.Clients
			var keys []byte
			if client == opts.Clients-1 {
				keys = opts.keyspace[numberPer*client*16:]
			} else {
				keys = opts.keyspace[numberPer*client*16 : numberPer*(client+1)*16]
			}
			keysplit := len(keys) / 16 / opts.Clients * client * 16
			f(keys[:keysplit])
			f(keys[keysplit:])
			wg.Done()
		}(i)
	}
	wg.Wait()
	dur := time.Now().Sub(begin)
	flog.InfoPrintf("%s %.0f/s %0.2fG/s to read %d values", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), float64(valuesLength)/(float64(dur)/float64(time.Second))/1024/1024/1024, opts.Number)
	if missing > 0 {
		flog.ErrorPrintf("%d MISSING!", missing)
	}
	if deleted > 0 {
		flog.ErrorPrintf("%d DELETED!", deleted)
	}
}

func write() {
	flog.InfoPrintf("write:")
	var superseded uint64
	timestamp := opts.Timestamp
	if timestamp == 0 {
		timestamp = 2
	}
	begin := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(opts.Clients)
	for i := 0; i < opts.Clients; i++ {
		go func(client int) {
			value := make([]byte, opts.Length)
			randomness := value
			if len(value) > 10 {
				copy(value, []byte("START67890"))
				randomness = value[10:]
				if len(value) > 20 {
					copy(value[len(value)-10:], []byte("123456STOP"))
					randomness = value[10 : len(value)-10]
				}
			}
			scr := brimio.NewScrambled()
			var s uint64
			number := len(opts.keyspace) / 16
			numberPer := number / opts.Clients
			var keys []byte
			if client == opts.Clients-1 {
				keys = opts.keyspace[numberPer*client*16:]
			} else {
				keys = opts.keyspace[numberPer*client*16 : numberPer*(client+1)*16]
			}
			if opts.GroupStore {
				errorCount := 0
				gs := opts.store.(store.GroupStore)
				for o := 0; o < len(keys); o += 16 {
					scr.Read(randomness)
					if oldTimestamp, err := gs.Write(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), timestamp, value); err != nil {
						//panic(err)
						errorCount++
					} else if oldTimestamp > timestamp {
						s++
					}
				}
			} else {
				vs := opts.store.(store.ValueStore)
				for o := 0; o < len(keys); o += 16 {
					scr.Read(randomness)
					if oldTimestamp, err := vs.Write(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), timestamp, value); err != nil {
						panic(err)
					} else if oldTimestamp > timestamp {
						s++
					}
				}
			}
			if s > 0 {
				atomic.AddUint64(&superseded, s)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	opts.store.Flush()
	dur := time.Now().Sub(begin)
	flog.InfoPrintf("%s %.0f/s %0.2fG/s to write %d values (timestamp %d)", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), float64(opts.Number*opts.Length)/(float64(dur)/float64(time.Second))/1024/1024/1024, opts.Number, timestamp)
	if superseded > 0 {
		flog.ErrorPrintf("%d SUPERCEDED!", superseded)
	}
}

func run() {
	flog.InfoPrintf("run:")
	begin := time.Now()
	<-time.After(1 * time.Minute)
	dur := time.Now().Sub(begin)
	flog.InfoPrintf("%s to run", dur)
}
