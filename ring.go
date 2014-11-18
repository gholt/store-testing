package main

import (
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/experimental-ring"
)

type msgMap struct {
	lock    sync.RWMutex
	mapping map[ring.MsgType]ring.MsgUnmarshaller
}

func newMsgMap() *msgMap {
	return &msgMap{mapping: make(map[ring.MsgType]ring.MsgUnmarshaller)}
}

func (mm *msgMap) set(t ring.MsgType, f ring.MsgUnmarshaller) ring.MsgUnmarshaller {
	mm.lock.Lock()
	p := mm.mapping[t]
	mm.mapping[t] = f
	mm.lock.Unlock()
	return p
}

func (mm *msgMap) get(t ring.MsgType) ring.MsgUnmarshaller {
	mm.lock.RLock()
	f := mm.mapping[t]
	mm.lock.RUnlock()
	return f
}

type ringPipe struct {
	nodeID          uint64
	conn            net.Conn
	lock            sync.RWMutex
	msgMap          *msgMap
	logError        *log.Logger
	logWarning      *log.Logger
	typeBytes       int
	lengthBytes     int
	writeChan       chan ring.Msg
	writingDoneChan chan struct{}
	sendDrops       uint32
}

func NewRingPipe(nodeID uint64, c net.Conn) *ringPipe {
	rp := &ringPipe{
		nodeID:          nodeID,
		conn:            c,
		msgMap:          newMsgMap(),
		logError:        log.New(os.Stderr, "", log.LstdFlags),
		logWarning:      log.New(os.Stderr, "", log.LstdFlags),
		typeBytes:       1,
		lengthBytes:     3,
		writeChan:       make(chan ring.Msg, 40),
		writingDoneChan: make(chan struct{}, 1),
	}
	return rp
}

func (rp *ringPipe) ID() uint64 {
	return 1
}

func (rp *ringPipe) PartitionPower() uint16 {
	return 8
}

func (rp *ringPipe) NodeID() uint64 {
	return rp.nodeID
}

func (rp *ringPipe) Responsible(partition uint32) bool {
	return partition&1 == uint32(rp.nodeID)&1
}

func (rp *ringPipe) Start() {
	go rp.reading()
	go rp.writing()
}

const _GLH_SEND_MSG_TIMEOUT = 1

func (rp *ringPipe) SetMsgHandler(t ring.MsgType, h ring.MsgUnmarshaller) {
	rp.msgMap.set(t, h)
}

func (rp *ringPipe) MsgToNode(nodeID uint64, m ring.Msg) bool {
	select {
	case rp.writeChan <- m:
		return true
	case <-time.After(_GLH_SEND_MSG_TIMEOUT * time.Second):
		atomic.AddUint32(&rp.sendDrops, 1)
		return false
	}
}

func (rp *ringPipe) MsgToOtherReplicas(ringID uint64, partition uint32, m ring.Msg) bool {
	// TODO: If ringID has changed, partition invalid, etc. return false
	select {
	case rp.writeChan <- m:
		return true
	case <-time.After(_GLH_SEND_MSG_TIMEOUT * time.Second):
		atomic.AddUint32(&rp.sendDrops, 1)
		return false
	}
}

func (rp *ringPipe) reading() {
	b := make([]byte, rp.typeBytes+rp.lengthBytes)
	d := make([]byte, 65536)
	for {
		var n int
		var sn int
		var err error
		for n != len(b) {
			if err != nil {
				if n != 0 || err != io.EOF {
					rp.logError.Print("error reading msg", err)
				}
				return
			}
			sn, err = rp.conn.Read(b[n:])
			n += sn
		}
		if err != nil {
			rp.logError.Print("error reading msg content", err)
			return
		}
		var t uint64
		for i := 0; i < rp.typeBytes; i++ {
			t = (t << 8) | uint64(b[i])
		}
		var l uint64
		for i := 0; i < rp.lengthBytes; i++ {
			l = (l << 8) | uint64(b[rp.typeBytes+i])
		}
		f := rp.msgMap.get(ring.MsgType(t))
		if f != nil {
			_, err = f(rp.conn, l)
			if err != nil {
				rp.logError.Print("error reading msg content", err)
				return
			}
		} else {
			rp.logWarning.Printf("unknown msg type %d", t)
			for l > 0 {
				if err != nil {
					rp.logError.Print("err reading msg content", err)
					return
				}
				if l >= uint64(len(d)) {
					sn, err = rp.conn.Read(d)
				} else {
					sn, err = rp.conn.Read(d[:l])
				}
				l -= uint64(sn)
			}
		}
	}
}

func (rp *ringPipe) writing() {
	b := make([]byte, rp.typeBytes+rp.lengthBytes)
	for {
		m := <-rp.writeChan
		if m == nil {
			break
		}
		t := m.MsgType()
		for i := rp.typeBytes - 1; i >= 0; i-- {
			b[i] = byte(t)
			t >>= 8
		}
		l := m.MsgLength()
		for i := rp.lengthBytes - 1; i >= 0; i-- {
			b[rp.typeBytes+i] = byte(l)
			l >>= 8
		}
		_, err := rp.conn.Write(b)
		if err != nil {
			rp.logError.Print("err writing msg", err)
			break
		}
		_, err = m.WriteContent(rp.conn)
		if err != nil {
			rp.logError.Print("err writing msg content", err)
			break
		}
		m.Done()
	}
	rp.writingDoneChan <- struct{}{}
}
