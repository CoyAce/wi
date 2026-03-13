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
	*blockManager
	remoteAddr net.Addr
}
type blockManager struct {
	sync.RWMutex
	ackFunc map[uint32]context.CancelFunc // block -> context.CancelFunc
	retry   map[uint32]chan struct{}      // block -> chan struct{}
}

func newBlockManager() *blockManager {
	return &blockManager{
		ackFunc: make(map[uint32]context.CancelFunc),
		retry:   make(map[uint32]chan struct{}),
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

	c.Lock()
	c.ackFunc[block] = ack
	c.retry[block] = retryCh
	c.Unlock()

	defer func() {
		c.Lock()
		delete(c.ackFunc, block)
		delete(c.retry, block)
		c.Unlock()
	}()

	return c.reliableWriter.reliableWrite(ctx, retryCh, addr, data, block)
}

func (c *connManager) complete(block uint32) {
	c.RLock()
	defer c.RUnlock()
	if ackFunc, ok := c.ackFunc[block]; ok {
		ackFunc()
	}
}

func (c *connManager) retryNow() {
	c.RLock()
	defer c.RUnlock()
	for _, v := range c.retry {
		v <- struct{}{}
	}
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

func (w *reliableWriter) reliableMultiWrite(rck chan uint32, addr net.Addr, packets []Packet, sender string, block uint32) {
	var (
		finPkt, _ = new(Fin{ReqHeader{UUID: sender, ReqID: block, Block: uint32(len(packets))}}).Marshal()
		last      = packets[len(packets)-1].Block
		send      = func(packet Packet) {
			if err := w.writeTo(addr, packet.Data); err != nil {
				code := new(OpCode(binary.BigEndian.Uint16(packet.Data[:2]))).String()
				log.Printf("[%v] write [%v] to [%v] failed: %v", code, block, addr.String(), err)
			}
		}
	)
	for i := 0; i < len(packets); i++ {
		send(packets[i])
	}
	for attempt := byte(0); attempt < w.retries; attempt++ {
		if err := w.writeTo(addr, finPkt); err != nil {
			log.Printf("write fin [%v] to [%v] failed: %v", block, addr.String(), err)
		}
		timer := time.After(time.Duration(w.rto.Load()))
		select {
		case r := <-rck:
			if r > last {
				return
			}
			slices.DeleteFunc(packets, func(packet Packet) bool {
				return packet.Block < r
			})
			if len(packets) == 0 {
				return
			}
			if packets[0].Block == r {
				send(packets[0])
				attempt = 0
			}
		case <-timer:
			log.Printf("fin %v timeout", block)
		}
	}
}
