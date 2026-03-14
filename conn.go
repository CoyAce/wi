package wi

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type connManager struct {
	reliableWriter
	remoteAddr net.Addr
}
type blockManager struct {
	single
	multiple
}

type single struct {
	sync.RWMutex
	ackFunc map[CacheKey]context.CancelFunc // CacheKey -> context.CancelFunc
	retry   map[CacheKey]chan struct{}      // CacheKey -> chan struct{}
}

type multiple struct {
	sync.RWMutex
	rck map[CacheKey]chan uint32      // rck cache : CacheKey -> chan rck block
	fin map[CacheKey]chan uint32      // fin cache : CacheKey -> chan fin block
	req map[CacheKey]chan ReliableReq // req cache : CacheKey -> chan ReliableReq
	ret map[CacheKey]chan ReliableReq // ret cache : CacheKey -> chan ReliableReq
}

func (s *single) storeACK(cacheKey CacheKey, ackFunc context.CancelFunc) {
	s.Lock()
	s.ackFunc[cacheKey] = ackFunc
	s.Unlock()
}

func (s *single) deleteACK(cacheKey CacheKey) {
	s.Lock()
	delete(s.ackFunc, cacheKey)
	s.Unlock()
}

func (s *single) storeACKAndRetry(cacheKey CacheKey, ack context.CancelFunc, retryCh chan struct{}) {
	s.Lock()
	s.ackFunc[cacheKey] = ack
	s.retry[cacheKey] = retryCh
	s.Unlock()
}

func (s *single) deleteACKAndRetry(cacheKey CacheKey) {
	s.Lock()
	delete(s.ackFunc, cacheKey)
	delete(s.retry, cacheKey)
	s.Unlock()
}

func (s *single) complete(cacheKey CacheKey) {
	s.RLock()
	defer s.RUnlock()
	if ackFunc, ok := s.ackFunc[cacheKey]; ok {
		ackFunc()
	}
}

func (s *single) retryNow() {
	s.RLock()
	defer s.RUnlock()
	for _, v := range s.retry {
		v <- struct{}{}
	}
}

func (m *multiple) storeRCK(cacheKey CacheKey, rck chan uint32) {
	m.Lock()
	m.rck[cacheKey] = rck
	m.Unlock()
}

func (m *multiple) deleteRCK(cacheKey CacheKey) {
	m.Lock()
	delete(m.rck, cacheKey)
	m.Unlock()
}

func (m *multiple) storeFIN(cacheKey CacheKey, fin chan uint32) {
	m.Lock()
	m.fin[cacheKey] = fin
	m.Unlock()
}

func (m *multiple) loadOrStoreFIN(cacheKey CacheKey) chan uint32 {
	m.Lock()
	defer m.Unlock()
	if c, ok := m.fin[cacheKey]; ok {
		return c
	}
	c := make(chan uint32, 1)
	m.fin[cacheKey] = c
	return c
}

func (m *multiple) deleteFIN(cacheKey CacheKey) {
	m.Lock()
	delete(m.fin, cacheKey)
	m.Unlock()
}

func (m *multiple) storeREQ(cacheKey CacheKey, req chan ReliableReq) {
	m.Lock()
	m.req[cacheKey] = req
	m.Unlock()
}

func (m *multiple) deleteREQ(cacheKey CacheKey) {
	m.Lock()
	delete(m.req, cacheKey)
	m.Unlock()
}

func (m *multiple) storeRET(cacheKey CacheKey, ret chan ReliableReq) {
	m.Lock()
	m.ret[cacheKey] = ret
	m.Unlock()
}

func (m *multiple) deleteRET(cacheKey CacheKey) {
	m.Lock()
	delete(m.ret, cacheKey)
	m.Unlock()
}

func (m *multiple) loadRET(cacheKey CacheKey) <-chan ReliableReq {
	m.RLock()
	defer m.RUnlock()
	return m.ret[cacheKey]
}

func (m *multiple) loadOrStoreRET(cacheKey CacheKey) chan ReliableReq {
	m.Lock()
	defer m.Unlock()
	if c, ok := m.ret[cacheKey]; ok {
		return c
	}
	c := make(chan ReliableReq, 200)
	m.ret[cacheKey] = c
	return c
}

func (m *multiple) patch(cacheKey CacheKey, block uint32) {
	m.RLock()
	defer m.RUnlock()
	if c, ok := m.rck[cacheKey]; ok {
		c <- block
	}
}

func (m *multiple) finish(cacheKey CacheKey, block uint32) {
	m.loadOrStoreFIN(cacheKey) <- block
}

func newBlockManager() *blockManager {
	return &blockManager{
		single{
			ackFunc: make(map[CacheKey]context.CancelFunc),
			retry:   make(map[CacheKey]chan struct{}),
		},
		multiple{
			rck: make(map[CacheKey]chan uint32),
			fin: make(map[CacheKey]chan uint32),
			req: make(map[CacheKey]chan ReliableReq),
			ret: make(map[CacheKey]chan ReliableReq),
		},
	}
}

func (c *connManager) writeToServer(pkt []byte) error {
	return c.writeTo(c.remoteAddr, pkt)
}

func (c *connManager) reliableWrite(data []byte, block uint32) error {
	return c.reliableWriteTo(c.remoteAddr, data, block)
}

func (c *connManager) reliableWriteTo(addr net.Addr, data []byte, block uint32) error {
	ctx, ack := context.WithCancel(context.Background())
	retryCh := make(chan struct{}, 1)

	cacheKey := newBlockKey(block)
	c.storeACKAndRetry(cacheKey, ack, retryCh)
	defer c.deleteACKAndRetry(cacheKey)

	return c.reliableWriter.reliableWrite(ctx, retryCh, addr, data, block)
}

type RTO struct {
	minRTT     time.Duration // 观察到的最小RTT
	rttVar     time.Duration // RTT变化
	rto        atomic.Int64
	lastUpdate time.Time
}

func (r *RTO) Update(start time.Time) {
	rtt := time.Since(start)

	// 更新最小RTT (10分钟窗口)
	if rtt < r.minRTT || time.Now().Sub(r.lastUpdate) > 10*time.Minute {
		r.minRTT = rtt
		r.lastUpdate = time.Now()
	}

	r.rttVar = r.rttVar*3/4 + (rtt-r.minRTT)/4

	// RTO = minRTT + 4*rttVar
	r.rto.Store(int64(r.minRTT + 4*r.rttVar))
}

func (r *RTO) Increase() {
	const (
		maxRTO            = int64(3 * time.Second)
		rtoIncreaseFactor = 108
		rtoIncreaseBase   = int64(10 * time.Millisecond)
	)
	r.rto.Store(min(maxRTO, r.rto.Load()*rtoIncreaseFactor/100+rtoIncreaseBase))
	r.rttVar += r.rttVar / 4
}

func (r *RTO) Get() time.Duration {
	return time.Duration(r.rto.Load())
}

type reliableWriter struct {
	*blockManager
	RTO             // retransmission timeout
	retries   uint8 // the number of times to retry a failed transmission
	localAddr string
	connFlag  int32 // 0: connected, 1: relistening
	conn      net.PacketConn
}

// reliableWrite write with retry until receiving ack.
// if timeout, try sending check packet to see if ack lost.
func (w *reliableWriter) reliableWrite(ack context.Context, retry <-chan struct{}, addr net.Addr, data []byte, block uint32) error {
	var (
		start time.Time
		code  = new(OpCode(binary.BigEndian.Uint16(data[:2]))).String()
		err   error
	)

	for attempt := uint8(0); attempt < w.retries; attempt++ {
		if attempt%2 == 0 {
			log.Printf("[%v] send packet: %v", code, block)
			start = time.Now()
			err = w.writeTo(addr, data)
		} else {
			log.Printf("[%v] send check: %v", code, block)
			check := Check{Block: block}
			pkt, _ := check.Marshal()
			err = w.writeTo(addr, pkt)
		}
		if err != nil {
			log.Printf("[%v] write failed: %v", code, err)
			if errors.Is(err, syscall.EPIPE) {
				w.relisten()
			}
			continue
		}
	WAIT:
		timer := time.After(w.Get())
		log.Printf("current timeout: %dms", w.Get()/time.Millisecond)
		select {
		case <-ack.Done():
			w.Update(start)
			return nil
		case <-timer:
			log.Printf("[%v] packet: %v ack timeout", code, block)
			w.Increase()
		case <-retry:
			log.Printf("[%v] retry", code)
			if err = w.writeTo(addr, data); err != nil {
				log.Printf("[%v] write failed: %v", code, err)
			}
			goto WAIT
		}
	}
	return errors.New(fmt.Sprintf("[%v] exhausted retries", code))
}

// writeTo write with no retry
func (w *reliableWriter) writeTo(addr net.Addr, pkt []byte) error {
	if err := w.conn.SetWriteDeadline(time.Now().Add(_TIMEOUT)); err != nil {
		return err
	}
	if _, err := w.conn.WriteTo(pkt, addr); err != nil {
		return err
	}
	return nil
}

// relisten close old conn, reestablish a new conn
func (w *reliableWriter) relisten() {
	if w.localAddr == "" {
		return
	}
	if !atomic.CompareAndSwapInt32(&w.connFlag, 0, 1) {
		log.Printf("relisten already in progress, skipping")
		return
	}
	defer atomic.StoreInt32(&w.connFlag, 0)
	if w.conn != nil {
		w.conn.Close()
	}
	if err := w.listen(); err != nil {
		panic(err)
	}
	log.Printf("relisten success")
}

// listen on localAddr
func (w *reliableWriter) listen() error {
	var err error
	if w.conn, err = net.ListenPacket("udp", w.localAddr); err != nil {
		log.Printf("[%s] listen failed: %v", w.localAddr, err)
		return err
	}
	return nil
}

func (w *reliableWriter) reliableMultiWrite(addr net.Addr, cacheKey CacheKey, packets []ReliableReq) {
	var (
		rck       = make(chan uint32)
		last      = uint32(len(packets))
		finPkt, _ = new(Fin{ReqHeader{UUID: cacheKey.UUID, ReqID: cacheKey.Block, Block: last}}).Marshal()
		send      = func(packet ReliableReq) {
			if data, err := packet.Marshal(); err != nil {
				log.Printf("[%v] marshal failed: %v", new(OpReq).String(), err)
			} else if err = w.writeTo(addr, data); err != nil {
				bodyOffset := 2 + 4 + 4 + len(packet.UUID) + 1
				code := new(OpCode(binary.BigEndian.Uint16(data[bodyOffset : bodyOffset+2]))).String()
				log.Printf("[%v] write [%v] to [%v] failed: %v", code, cacheKey.Block, addr.String(), err)
			}
		}
	)
	w.storeRCK(cacheKey, rck)
	defer w.deleteRCK(cacheKey)
	for i := 0; i < len(packets); i++ {
		send(packets[i])
	}
	for attempt := byte(0); attempt < w.retries; attempt++ {
		if err := w.writeTo(addr, finPkt); err != nil {
			log.Printf("write fin [%v] to [%v] failed: %v", cacheKey.Block, addr.String(), err)
		}
		timer := time.After(time.Duration(w.rto.Load()))
		select {
		case r := <-rck:
			if r > last {
				return
			}
			packets = slices.DeleteFunc(packets, func(packet ReliableReq) bool {
				return packet.Block < r
			})
			if len(packets) == 0 {
				return
			}
			if packets[0].Block == r {
				log.Printf("resend packet %v", r)
				send(packets[0])
				attempt = 0
			}
		case <-timer:
			log.Printf("fin %v timeout", cacheKey.Block)
		}
	}
}

func (w *reliableWriter) rck(addr net.Addr, UUID string, block uint32, reqID uint32) {
	if pkt, err := new(Rck{ReqHeader{Block: block, ReqID: reqID, UUID: UUID}}).Marshal(); err != nil {
		log.Printf("rck marshal failed: %v", err)
	} else if err = w.writeTo(addr, pkt); err != nil {
		log.Printf("[%s] rck failed: %v", addr, err)
	}
}

func (w *reliableWriter) receive(addr net.Addr, req ReliableReq) {
	cacheKey := newCacheKey(req.UUID, req.ReqID)
	w.multiple.Lock()
	c, ok := w.req[cacheKey]
	if !ok {
		c = make(chan ReliableReq, 200)
		go w.processReq(addr, cacheKey, c)
		w.req[cacheKey] = c
	}
	w.multiple.Unlock()
	c <- req
}

func (w *reliableWriter) processReq(addr net.Addr, cacheKey CacheKey, req chan ReliableReq) {
	fin := w.loadOrStoreFIN(cacheKey)
	ret := w.loadOrStoreRET(cacheKey)
	defer w.deleteREQ(cacheKey)
	defer w.deleteFIN(cacheKey)
	defer func() {
		w.deleteRET(cacheKey)
		close(ret)
	}()
	rt := new(RangeTracker)
LOOP:
	for {
		timer := time.After(10 * time.Second)
		select {
		case d := <-req:
			r := MonoRange(d.Block)
			if rt.Contains(r) {
				continue
			}
			rt.Track(r)
			ret <- d
		case finBlock := <-fin:
			rt.Track(MonoRange(finBlock + 1))
			var next uint32
			if rt.isCompleted() {
				next = finBlock + 1
			} else {
				next = rt.Get()[0].start
			}
			w.rck(addr, cacheKey.UUID, next, cacheKey.Block)
			if rt.isCompleted() {
				break LOOP
			}
		case <-timer:
			return
		}
	}
	log.Printf("[%v] %v completed", new(OpReq).String(), cacheKey)
}
