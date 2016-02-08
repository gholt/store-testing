package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/store"
	"github.com/jessevdk/go-flags"
	"gopkg.in/gholt/brimtime.v1"
	"gopkg.in/gholt/brimutil.v1"
)

type optsStruct struct {
	GroupStore    bool  `short:"g" long:"groupstore" description:"Use GroupStore instead of ValueStore."`
	Clients       int   `long:"clients" description:"The number of clients. Default: cores*cores"`
	Cores         int   `long:"cores" description:"The number of cores. Default: CPU core count"`
	Debug         bool  `long:"debug" description:"Turns on debug output."`
	ExtendedStats bool  `long:"extended-stats" description:"Extended statistics at exit."`
	Metrics       bool  `long:"metrics" description:"Displays metrics one per minute."`
	Length        int   `short:"l" long:"length" description:"Length of values. Default: 0"`
	Number        int   `short:"n" long:"number" description:"Number of keys. Default: 0"`
	Random        int   `long:"random" description:"Random number seed. Default: 0"`
	Replicate     bool  `long:"replicate" description:"Creates a second value store that will test replication."`
	Timestamp     int64 `long:"timestamp" description:"Timestamp value. Default: current time"`
	TombstoneAge  int   `long:"tombstone-age" description:"Seconds to keep tombstones. Default: 4 hours"`
	MaxGroupSize  int   `long:"max-group-size" description:"Maximum number of items per group for groupwrite."`
	Positional    struct {
		Tests []string `name:"tests" description:"blockprof cpuprof delete lookup read run write"`
	} `positional-args:"yes"`
	blockprofi int
	blockproff *os.File
	cpuprofi   int
	cpuproff   *os.File
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
	log.Print("init:")
	args := os.Args[1:]
	if len(args) == 0 {
		args = append(args, "-h")
	}
	if _, err := parser.ParseArgs(args); err != nil {
		os.Exit(1)
	}
	for _, arg := range opts.Positional.Tests {
		switch arg {
		case "blockprof":
		case "cpuprof":
		case "delete":
		case "grouplookup":
		case "groupread":
		case "groupwrite":
		case "lookup":
		case "read":
		case "run":
		case "write":
		default:
			log.Printf("unknown test named %#v", arg)
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
	brimutil.NewSeededScrambled(int64(opts.Random)).Read(opts.keyspace)
	opts.buffers = make([][]byte, opts.Clients)
	for i := 0; i < opts.Clients; i++ {
		opts.buffers[i] = make([]byte, 4*1024*1024)
	}
	memstat()
	log.Print("start:")
	begin := time.Now()
	var vscfg *store.ValueStoreConfig
	var gscfg *store.GroupStoreConfig
	if opts.GroupStore {
		gscfg = &store.GroupStoreConfig{Workers: opts.Cores}
	} else {
		vscfg = &store.ValueStoreConfig{Workers: opts.Cores}
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
		if opts.GroupStore {
			rgscfg = &store.GroupStoreConfig{}
			*rgscfg = *gscfg
			gscfg.MsgRing = opts.ring
			rgscfg.MsgRing = opts.rring
			rgscfg.Path = "replicated"
			rgscfg.LogCritical = log.New(os.Stderr, "ReplicatedGroupStore ", log.LstdFlags).Printf
			rgscfg.LogError = log.New(os.Stderr, "ReplicatedGroupStore ", log.LstdFlags).Printf
		} else {
			rvscfg = &store.ValueStoreConfig{}
			*rvscfg = *vscfg
			vscfg.MsgRing = opts.ring
			rvscfg.MsgRing = opts.rring
			rvscfg.Path = "replicated"
			rvscfg.LogCritical = log.New(os.Stderr, "ReplicatedValueStore ", log.LstdFlags).Printf
			rvscfg.LogError = log.New(os.Stderr, "ReplicatedValueStore ", log.LstdFlags).Printf
		}
		if opts.Debug {
			if opts.GroupStore {
				rgscfg.LogDebug = log.New(os.Stderr, "ReplicatedGroupStore ", log.LstdFlags).Printf
			} else {
				rvscfg.LogDebug = log.New(os.Stderr, "ReplicatedValueStore ", log.LstdFlags).Printf
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
						if opts.GroupStore {
							log.Println("ReplicatedGroupStore:\n" + opts.repstore.Stats(false).String())
						} else {
							log.Println("ReplicatedValueStore:\n" + opts.repstore.Stats(false).String())
						}
					}
				}()
			}
			wg.Done()
		}()
	}
	if opts.Debug {
		if opts.GroupStore {
			gscfg.LogDebug = log.New(os.Stderr, "GroupStore ", log.LstdFlags).Printf
		} else {
			vscfg.LogDebug = log.New(os.Stderr, "ValueStore ", log.LstdFlags).Printf
		}
	}
	var restartChan chan error
	if opts.GroupStore {
		opts.store, restartChan = store.NewGroupStore(gscfg)
	} else {
		opts.store, restartChan = store.NewValueStore(vscfg)
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
				if opts.GroupStore {
					log.Println("GroupStore:\n" + opts.store.Stats(false).String())
				} else {
					log.Println("ValueStore:\n" + opts.store.Stats(false).String())
				}
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
	log.Println(dur, "to start")
	memstat()
	for _, arg := range opts.Positional.Tests {
		switch arg {
		case "blockprof":
			if opts.blockproff != nil {
				log.Print("blockprof: off")
				runtime.SetBlockProfileRate(0)
				pprof.Lookup("block").WriteTo(opts.blockproff, 1)
				opts.blockproff.Close()
				opts.blockproff = nil
			} else {
				log.Print("blockprof: on")
				var err error
				opts.blockproff, err = os.Create(fmt.Sprintf("blockprof%d", opts.blockprofi))
				opts.blockprofi++
				if err != nil {
					log.Print(err)
					os.Exit(1)
				}
				runtime.SetBlockProfileRate(1)
			}
		case "cpuprof":
			if opts.cpuproff != nil {
				log.Print("cpuprof: off")
				pprof.StopCPUProfile()
				opts.cpuproff.Close()
				opts.cpuproff = nil
			} else {
				log.Print("cpuprof: on")
				var err error
				opts.cpuproff, err = os.Create(fmt.Sprintf("cpuprof%d", opts.cpuprofi))
				opts.cpuprofi++
				if err != nil {
					log.Print(err)
					os.Exit(1)
				}
				pprof.StartCPUProfile(opts.cpuproff)
			}
		case "delete":
			delete()
		case "grouplookup":
			grouplookup()
		case "groupread":
			groupread()
		case "groupwrite":
			groupwrite()
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
	log.Print("flush:")
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
	log.Println(dur, "to flush")
	memstat()
	log.Print("stats:")
	begin = time.Now()
	var rvsStats *store.ValueStoreStats
	var rgsStats *store.GroupStoreStats
	if opts.repstore != nil {
		wg.Add(1)
		go func() {
			if opts.GroupStore {
				rgsStats = opts.repstore.Stats(opts.ExtendedStats).(*store.GroupStoreStats)
			} else {
				rvsStats = opts.repstore.Stats(opts.ExtendedStats).(*store.ValueStoreStats)
			}
			wg.Done()
		}()
	}
	var vsStats *store.ValueStoreStats
	var gsStats *store.GroupStoreStats
	if opts.GroupStore {
		gsStats = opts.store.Stats(opts.ExtendedStats).(*store.GroupStoreStats)
	} else {
		vsStats = opts.store.Stats(opts.ExtendedStats).(*store.ValueStoreStats)
	}
	wg.Wait()
	dur = time.Now().Sub(begin)
	log.Println(dur, "to obtain stats")
	if opts.ExtendedStats {
		if opts.GroupStore {
			log.Print("GroupStore: stats:\n", gsStats.String())
		} else {
			log.Print("ValueStore: stats:\n", vsStats.String())
		}
	} else {
		if opts.GroupStore {
			log.Println("GroupStore: Values", gsStats.Values)
			log.Println("GroupStore: ValueBytes", gsStats.ValueBytes)
		} else {
			log.Println("ValueStore: Values", vsStats.Values)
			log.Println("ValueStore: ValueBytes", vsStats.ValueBytes)
		}
	}
	if opts.repstore != nil {
		if opts.ExtendedStats {
			if opts.GroupStore {
				log.Print("ReplicatedGroupStore: stats:\n", rgsStats.String())
			} else {
				log.Print("ReplicatedValueStore: stats:\n", rvsStats.String())
			}
		} else {
			if opts.GroupStore {
				log.Println("ReplicatedGroupStore: Values", rgsStats.Values)
				log.Println("ReplicatedGroupStore: ValueBytes", rgsStats.ValueBytes)
			} else {
				log.Println("ReplicatedValueStore: Values", rvsStats.Values)
				log.Println("ReplicatedValueStore: ValueBytes", rvsStats.ValueBytes)
			}
		}
	}
	memstat()
	if opts.repstore != nil {
		log.Print("drops", opts.ring.sendDrops, opts.rring.sendDrops)
	}
	log.Print("shutdown:")
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
	log.Println(dur, "to shutdown")
}

func memstat() {
	lastAlloc := opts.st.TotalAlloc
	runtime.ReadMemStats(&opts.st)
	deltaAlloc := opts.st.TotalAlloc - lastAlloc
	lastAlloc = opts.st.TotalAlloc
	log.Printf("%0.2fG total alloc, %0.2fG delta", float64(opts.st.TotalAlloc)/1024/1024/1024, float64(deltaAlloc)/1024/1024/1024)
}

func delete() {
	log.Print("delete:")
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
	log.Printf("%s %.0f/s to delete %d values (timestamp %d)", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), opts.Number, timestamp)
	if superseded > 0 {
		log.Println(superseded, "SUPERCEDED!")
	}
}

func grouplookup() {
	log.Print("grouplookup:")
	if !opts.GroupStore {
		log.Println("not valid for ValueStore")
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
				list := gs.LookupGroup(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]))
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
	log.Printf("%s %.0f/s to lookup %d groups (%d items)", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), opts.Number, itemCount)
	if mismatch > 0 {
		log.Println(mismatch, "MISMATCHES! (groups without the correct number of items)")
	}
}

func groupread() {
	log.Print("groupread:")
	if !opts.GroupStore {
		log.Println("not valid for ValueStore")
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
				items := uint64(len(gs.LookupGroup(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]))))
				atomic.AddUint64(&itemCount, items)
				if items != groupSize {
					log.Println(groupSize, items)
					atomic.AddUint64(&mismatch, 1)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	dur := time.Now().Sub(begin)
	log.Printf("%s %.0f/s to read %d groups (%d items)", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), opts.Number, itemCount)
	if mismatch > 0 {
		log.Println(mismatch, "MISMATCHES! (groups without the correct number of items)")
	}
}

func groupwrite() {
	log.Print("groupwrite:")
	if !opts.GroupStore {
		log.Println("not valid for ValueStore")
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
			scr := brimutil.NewScrambled()
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
	log.Printf("%s %.0f/s %0.2fG/s to write %d items (%d groups) (timestamp %d)", dur, float64(itemCount)/(float64(dur)/float64(time.Second)), float64(itemCount)/(float64(dur)/float64(time.Second))/1024/1024/1024, itemCount, opts.Number, timestamp)
	if superseded > 0 {
		log.Println(superseded, "SUPERCEDED!")
	}
}

func lookup() {
	log.Print("lookup:")
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
	log.Printf("%s %.0f/s to lookup %d values", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), opts.Number)
	if missing > 0 {
		log.Println(missing, "MISSING!")
	}
	if deleted > 0 {
		log.Println(deleted, "DELETED!")
	}
}

func read() {
	log.Print("read:")
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
	log.Printf("%s %.0f/s %0.2fG/s to read %d values", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), float64(valuesLength)/(float64(dur)/float64(time.Second))/1024/1024/1024, opts.Number)
	if missing > 0 {
		log.Println(missing, "MISSING!")
	}
	if deleted > 0 {
		log.Println(deleted, "DELETED!")
	}
}

func write() {
	log.Print("write:")
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
			scr := brimutil.NewScrambled()
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
					scr.Read(randomness)
					if oldTimestamp, err := gs.Write(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), timestamp, value); err != nil {
						panic(err)
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
	log.Printf("%s %.0f/s %0.2fG/s to write %d values (timestamp %d)", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), float64(opts.Number*opts.Length)/(float64(dur)/float64(time.Second))/1024/1024/1024, opts.Number, timestamp)
	if superseded > 0 {
		log.Println(superseded, "SUPERCEDED!")
	}
}

func run() {
	log.Print("run:")
	begin := time.Now()
	<-time.After(1 * time.Minute)
	dur := time.Now().Sub(begin)
	log.Println(dur, "to run")
}
