package main

import (
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/ring"
)

type msgMap struct {
	lock    sync.RWMutex
	mapping map[uint64]ring.MsgUnmarshaller
}

func newMsgMap() *msgMap {
	return &msgMap{mapping: make(map[uint64]ring.MsgUnmarshaller)}
}

func (mm *msgMap) set(t uint64, f ring.MsgUnmarshaller) ring.MsgUnmarshaller {
	mm.lock.Lock()
	p := mm.mapping[t]
	mm.mapping[t] = f
	mm.lock.Unlock()
	return p
}

func (mm *msgMap) get(t uint64) ring.MsgUnmarshaller {
	mm.lock.RLock()
	f := mm.mapping[t]
	mm.lock.RUnlock()
	return f
}

type node struct {
	id uint64
}

func (n *node) NodeID() uint64 {
	return n.id
}

func (n *node) Active() bool {
	return true
}

func (n *node) Capacity() uint32 {
	return 1
}

func (n *node) TierValues() []int {
	return nil
}

func (n *node) Address() string {
	return ""
}

type ringPipe struct {
	ring            ring.Ring
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

func NewRingPipe(localNodeAddress string, c net.Conn) *ringPipe {
	b := ring.NewBuilder()
	b.SetReplicaCount(2)
	var localNodeID uint64
	n := b.AddNode(true, 1, nil, []string{"127.0.0.1:11111"}, "")
	if localNodeAddress == "127.0.0.1:11111" {
		localNodeID = n.ID()
	}
	n = b.AddNode(true, 1, nil, []string{"127.0.0.1:22222"}, "")
	if localNodeAddress == "127.0.0.1:22222" {
		localNodeID = n.ID()
	}
	n = b.AddNode(true, 1, nil, []string{"127.0.0.1:33333"}, "")
	if localNodeAddress == "127.0.0.1:33333" {
		localNodeID = n.ID()
	}
	r := b.Ring()
	r.SetLocalNode(localNodeID)
	rp := &ringPipe{
		ring:            r,
		conn:            c,
		msgMap:          newMsgMap(),
		logError:        log.New(os.Stderr, "", log.LstdFlags),
		logWarning:      log.New(os.Stderr, "", log.LstdFlags),
		typeBytes:       8,
		lengthBytes:     8,
		writeChan:       make(chan ring.Msg, 40),
		writingDoneChan: make(chan struct{}, 1),
	}
	return rp
}

func (rp *ringPipe) Ring() ring.Ring {
	return rp.ring
}

func (rp *ringPipe) Start() {
	go rp.reading()
	go rp.writing()
}

const _GLH_SEND_MSG_TIMEOUT = 1

func (rp *ringPipe) MaxMsgLength() uint64 {
	return 16 * 1024 * 1024
}

func (rp *ringPipe) SetMsgHandler(t uint64, h ring.MsgUnmarshaller) {
	rp.msgMap.set(t, h)
}

func (rp *ringPipe) MsgToNode(localNodeID uint64, m ring.Msg) {
	select {
	case rp.writeChan <- m:
	case <-time.After(_GLH_SEND_MSG_TIMEOUT * time.Second):
		atomic.AddUint32(&rp.sendDrops, 1)
	}
	m.Done()
}

func (rp *ringPipe) MsgToOtherReplicas(ringVersion int64, partition uint32, m ring.Msg) {
	// TODO: If ringVersion has changed, partition invalid, etc. return false
	select {
	case rp.writeChan <- m:
	case <-time.After(_GLH_SEND_MSG_TIMEOUT * time.Second):
		atomic.AddUint32(&rp.sendDrops, 1)
	}
	m.Done()
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
			rp.logError.Print("error reading msg start", err)
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
		f := rp.msgMap.get(t)
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
					rp.logError.Print("err reading unknown msg content", err)
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
	}
	rp.writingDoneChan <- struct{}{}
}
