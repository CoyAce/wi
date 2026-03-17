package wi

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ============================================================================
// Constants
// ============================================================================

const opReqString = "REQ"

// ============================================================================
// Core Data Structures
// ============================================================================

// connManager manages connection state and remote address.
type connManager struct {
	reliableWriter
	remoteAddr net.Addr
}

// blockManager tracks acknowledgment state for individual blocks,
// organizing single and multi-block operations.
type blockManager struct {
	single
	multiple
}

// single manages ACK callbacks and retry channels for single block operations
// using concurrent-safe maps.
type single struct {
	sync.RWMutex
	ackFunc map[CacheKey]context.CancelFunc // CacheKey -> context.CancelFunc
	retry   map[CacheKey]chan struct{}      // CacheKey -> chan struct{}
}

// multiple manages channels for multi-block reliable transfers (SACK/FIN/REQ/RET).
type multiple struct {
	sync.RWMutex
	sack map[CacheKey]chan uint32      // SACK acknowledgment channels
	fin  map[CacheKey]chan uint32      // FIN completion channels
	req  map[CacheKey]chan ReliableReq // Incoming out-of-order request channels
	ret  map[CacheKey]chan ReliableReq // Outgoing in order response channels
}

// RTO implements adaptive RTO calculation using RTT variance with EWMA algorithm.
type RTO struct {
	minRTT     time.Duration // Minimum observed RTT
	rttVar     time.Duration // RTT variance (EWMA)
	rto        atomic.Int64  // Current RTO value (atomic)
	lastUpdate time.Time     // Last RTT update timestamp
}

// reliableWriter is the core reliable transmission engine combining block tracking,
// RTO calculation, and connection management.
type reliableWriter struct {
	*blockManager                // Embedded block tracking
	RTO                          // Embedded RTO calculator
	retries       uint8          // Maximum retry attempts
	localAddr     string         // Local binding address
	relistenState int32          // Connection state (0=connected, 1=relistening)
	conn          net.PacketConn // Underlying UDP connection
}

// reorderBuffer buffers out-of-order packets for resequencing.
// It maintains sorted order by block number using binary search insertion
// and supports flush up to specific block number.
type reorderBuffer struct {
	items []ReliableReq // Buffered requests in sorted order
}

// ============================================================================
// Constructor Functions
// ============================================================================

// newBlockManager initializes block manager with pre-allocated maps.
// Uses initialCapacity = 64 for all map pre-allocation for performance.
func newBlockManager() *blockManager {
	const initialCapacity = 64
	return &blockManager{
		single: single{
			ackFunc: make(map[CacheKey]context.CancelFunc, initialCapacity),
			retry:   make(map[CacheKey]chan struct{}, initialCapacity),
		},
		multiple: multiple{
			sack: make(map[CacheKey]chan uint32, initialCapacity),
			fin:  make(map[CacheKey]chan uint32, initialCapacity),
			req:  make(map[CacheKey]chan ReliableReq, initialCapacity),
			ret:  make(map[CacheKey]chan ReliableReq, initialCapacity),
		},
	}
}

// newReorderBuffer creates a new reorder buffer with pre-allocated capacity.
// Uses pendingBufCapacity = 16 for initial buffer size.
func newReorderBuffer() *reorderBuffer {
	const pendingBufCapacity = 16
	return &reorderBuffer{
		items: make([]ReliableReq, 0, pendingBufCapacity),
	}
}

// ============================================================================
// Single Operations (ACK/Retry Management)
// ============================================================================

// storeACK stores ACK callback for reliable transmission confirmation.
func (s *single) storeACK(key CacheKey, cancel context.CancelFunc) {
	s.Lock()
	defer s.Unlock()
	s.ackFunc[key] = cancel
}

// deleteACK removes ACK callback after successful acknowledgment.
func (s *single) deleteACK(key CacheKey) {
	s.Lock()
	defer s.Unlock()
	delete(s.ackFunc, key)
}

// storeACKAndRetry stores both ACK callback and retry channel for reliable transmission.
func (s *single) storeACKAndRetry(key CacheKey, cancel context.CancelFunc, retryCh chan struct{}) {
	s.Lock()
	defer s.Unlock()
	s.ackFunc[key] = cancel
	s.retry[key] = retryCh
}

// deleteACKAndRetry removes both ACK callback and retry channel after completion.
func (s *single) deleteACKAndRetry(key CacheKey) {
	s.Lock()
	defer s.Unlock()
	delete(s.ackFunc, key)
	delete(s.retry, key)
}

// complete triggers ACK cancellation to stop retransmission timer.
func (s *single) complete(key CacheKey) {
	s.RLock()
	defer s.RUnlock()
	if cancel, ok := s.ackFunc[key]; ok && cancel != nil {
		cancel()
	}
}

// triggerAllRetries triggers manual retry for all pending transmissions.
// Used when client reconnects to server after connection failure.
func (s *single) triggerAllRetries() {
	s.RLock()
	defer s.RUnlock()
	for _, ch := range s.retry {
		nonBlockingSend(ch, struct{}{})
	}
}

// ============================================================================
// Multiple Operations (SACK/FIN/REQ/RET Management)
// ============================================================================

// storeSACK stores SACK channel for multi-packet acknowledgment.
func (m *multiple) storeSACK(key CacheKey, ch chan uint32) {
	m.Lock()
	defer m.Unlock()
	m.sack[key] = ch
}

// deleteSACK removes SACK channel after multi-packet transfer completion.
func (m *multiple) deleteSACK(key CacheKey) {
	m.Lock()
	defer m.Unlock()
	delete(m.sack, key)
}

// storeFIN stores FIN channel for transfer completion notification.
func (m *multiple) storeFIN(key CacheKey, ch chan uint32) {
	m.Lock()
	defer m.Unlock()
	m.fin[key] = ch
}

// loadOrStoreFIN loads existing FIN channel or creates new one.
// Uses finChanSize = 1 for channel buffer capacity.
func (m *multiple) loadOrStoreFIN(key CacheKey) chan uint32 {
	const finChanSize uint8 = 1
	m.Lock()
	defer m.Unlock()
	if ch, ok := m.fin[key]; ok {
		return ch
	}
	ch := make(chan uint32, finChanSize)
	m.fin[key] = ch
	return ch
}

// deleteFIN removes FIN channel after transfer completion.
func (m *multiple) deleteFIN(key CacheKey) {
	m.Lock()
	defer m.Unlock()
	delete(m.fin, key)
}

// storeREQ stores REQ channel for incoming out-of-order requests.
func (m *multiple) storeREQ(key CacheKey, ch chan ReliableReq) {
	m.Lock()
	defer m.Unlock()
	m.req[key] = ch
}

// deleteREQ removes REQ channel after request processing completion.
func (m *multiple) deleteREQ(key CacheKey) {
	m.Lock()
	defer m.Unlock()
	delete(m.req, key)
}

// storeRET stores RET channel for outgoing in-order responses.
func (m *multiple) storeRET(key CacheKey, ch chan ReliableReq) {
	m.Lock()
	defer m.Unlock()
	m.ret[key] = ch
}

// deleteRET removes RET channel after response delivery completion.
func (m *multiple) deleteRET(key CacheKey) {
	m.Lock()
	defer m.Unlock()
	delete(m.ret, key)
}

// loadRET loads RET channel for reading outgoing responses.
func (m *multiple) loadRET(key CacheKey) <-chan ReliableReq {
	m.RLock()
	defer m.RUnlock()
	return m.ret[key]
}

// loadOrStoreRET loads existing RET channel or creates new one.
// Uses retChanSize = 200 for channel buffer capacity.
func (m *multiple) loadOrStoreRET(key CacheKey) chan ReliableReq {
	const retChanSize = 200
	m.Lock()
	defer m.Unlock()
	if ch, ok := m.ret[key]; ok {
		return ch
	}
	ch := make(chan ReliableReq, retChanSize)
	m.ret[key] = ch
	return ch
}

// notifySACK sends SACK notification for received packet.
// Uses non-blocking send to avoid blocking on full channel.
func (m *multiple) notifySACK(key CacheKey, block uint32) {
	m.RLock()
	defer m.RUnlock()
	if ch, ok := m.sack[key]; ok {
		nonBlockingSend(ch, block)
	}
}

// ============================================================================
// RTO Implementation (Adaptive Timeout Calculation)
// ============================================================================

// Update updates RTO based on measured RTT using EWMA algorithm.
// Uses rttWindow = 10 * time.Minute for minimum RTT update window.
func (r *RTO) Update(startTime time.Time) {
	rtt := time.Since(startTime)

	// Update minimum RTT if needed
	const (
		rttWindow = 10 * time.Minute
		maxRTO    = 3 * time.Second
	)
	if rtt < r.minRTT || time.Since(r.lastUpdate) > rttWindow {
		r.minRTT = rtt
		r.lastUpdate = time.Now()
	}

	// EWMA calculation for RTT variance
	r.rttVar = min(r.rttVar*3/4+(rtt-r.minRTT)/4, r.minRTT)

	// Calculate RTO: minRTT + 4*rttVar
	newRTO := r.minRTT + 4*r.rttVar
	r.rto.Store(int64(min(newRTO, maxRTO)))

	log.Printf("minRTT: %dms, rttVar: %dms, RTO: %dms", r.minRTT/time.Millisecond, r.rttVar/time.Millisecond, r.Get()/time.Millisecond)
}

// Get returns current RTO value for timeout scheduling.
func (r *RTO) Get() time.Duration {
	return time.Duration(r.rto.Load())
}

// ============================================================================
// Reorder Buffer Operations
// ============================================================================

// sendTo sends all buffered items to channel and clears the buffer.
func (b *reorderBuffer) sendTo(ch chan ReliableReq) {
	for _, item := range b.items {
		nonBlockingSend(ch, item)
	}
	b.clear()
}

// flushUpTo flushes buffered items up to maxBlock number.
// Optimized with clear() when all items are sent for memory efficiency.
func (b *reorderBuffer) flushUpTo(ch chan ReliableReq, maxBlock uint32) {
	if len(b.items) == 0 {
		return
	}

	splitIdx := 0
	for i, item := range b.items {
		if item.Block <= maxBlock {
			nonBlockingSend(ch, item)
			splitIdx = i + 1
		} else {
			break
		}
	}

	if splitIdx == len(b.items) {
		b.clear()
	} else {
		b.items = b.items[splitIdx:]
	}
}

// insert inserts request into buffer maintaining sorted order by block number.
// Uses binary search for O(log n) insertion performance.
func (b *reorderBuffer) insert(req ReliableReq) {
	idx := sort.Search(len(b.items), func(i int) bool {
		return b.items[i].Block >= req.Block
	})

	if idx == len(b.items) {
		b.items = append(b.items, req)
	} else {
		b.items = slices.Insert(b.items, idx, req)
	}
}

// clear clears buffer by resetting length to zero.
// Preserves capacity for reuse instead of reallocation.
func (b *reorderBuffer) clear() {
	b.items = b.items[:0]
}

// ============================================================================
// Helper Functions
// ============================================================================

// nonBlockingSend sends value to channel without blocking.
// Drops message if channel is full or closed.
// Used for SACK/FIN/REQ notifications to avoid blocking sender.
func nonBlockingSend[T any](ch chan T, value T) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("send to ret channel failed: %v", r)
		}
	}()
	select {
	case ch <- value:
	default:
		log.Printf("channel full, dropping value")
	}
}

// safeReceive receives from channel with panic recovery.
// Handles closed channel gracefully by returning ok=false.
// Used for reading from potentially closed channels.
func safeReceive[T any](ch chan T) (value T, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("receive from closed channel: %v", r)
			ok = false
		}
	}()
	value, ok = <-ch
	return
}

// ============================================================================
// Core Transmission Logic
// ============================================================================

// writeTo writes packet to address with timeout enforcement using _TIMEOUT constant.
// Implements deadline control to prevent blocking on slow connections.
func (w *reliableWriter) writeTo(addr net.Addr, pkt []byte) error {
	if err := w.conn.SetWriteDeadline(time.Now().Add(_TIMEOUT)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	if _, err := w.conn.WriteTo(pkt, addr); err != nil {
		return fmt.Errorf("write packet: %w", err)
	}

	return nil
}

// relisten recovers from connection failure by recreating listener.
// Only triggered on EPIPE errors during reliableWrite.
func (w *reliableWriter) relisten() {
	if w.localAddr == "" {
		return
	}

	if !atomic.CompareAndSwapInt32(&w.relistenState, 0, 1) {
		log.Printf("relisten already in progress, skipping")
		return
	}
	defer atomic.StoreInt32(&w.relistenState, 0)

	if w.conn != nil {
		w.conn.Close()
	}

	if err := w.listen(); err != nil {
		log.Printf("relisten failed: %v", err)
	} else {
		log.Printf("relisten success")
	}
}

// listen creates UDP listener on localAddr.
// Called during initial connection setup and relisten recovery.
func (w *reliableWriter) listen() error {
	var err error
	w.conn, err = net.ListenPacket("udp", w.localAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", w.localAddr, err)
	}
	return nil
}

// reliableWrite reliably writes single block with ACK/retry mechanism.
// Implements reliable transmission with retry mechanism.
// Key Logic:
//   - Send DATA packet on even attempts, CHECK packet on odd attempts
//   - Wait for ACK with timeout based on RTO
//   - On timeout: increase RTO exponentially and retry
//   - On write error: log and continue to next attempt
//   - Trigger relisten on EPIPE errors
//   - On manual retry (retryCh): resend data packet immediately and jump back to wait for ACK (not send CHECK)
//
// If timeout, try sending check packet to see if ack lost.
func (w *reliableWriter) reliableWrite(
	ctx context.Context,
	retryCh <-chan struct{},
	addr net.Addr,
	data []byte,
	block uint32,
) error {
	var (
		code      = new(toOpCode(data[:2])).String()
		target    = addr.String()
		lastErr   error
		startTime = time.Now() // Declared outside loop to preserve timestamp across retries
	)

	for attempt := uint8(0); attempt < w.retries; attempt++ {
		// Even attempts: send DATA, Odd attempts: send CHECK
		if attempt%2 == 0 {
			log.Printf("[%v] write data [%v] to [%v]", code, block, target)
			if lastErr = w.writeTo(addr, data); lastErr != nil {
				log.Printf("[%s] write failed: %v", code, lastErr)

				if errors.Is(lastErr, syscall.EPIPE) {
					go w.relisten()
				}
				continue
			}
		} else {
			check := Check{Block: block}
			log.Printf("[%v] check [%v] ack for [%v]", code, block, target)
			if pkt, err := check.Marshal(); err != nil {
				log.Printf("[%s] check marshal failed: %v", code, err)
				continue
			} else if lastErr = w.writeTo(addr, pkt); err != nil {
				log.Printf("[%s] check failed: %v", code, lastErr)
				continue
			}
		}

		// Wait for ACK, timeout, or manual retry
	WAIT:
		timer := time.After(w.RTO.Get())
		select {
		case <-ctx.Done():
			w.RTO.Update(startTime)
			return nil

		case <-timer:
			log.Printf("[%s] timeout, current RTO: %dms", code, w.RTO.Get()/time.Millisecond)
			lastErr = errors.New("timeout waiting for ACK")

		case <-retryCh:
			log.Printf("[%s] manual retry triggered", code)
			// Resend data packet (not CHECK) on manual retry
			// This is needed when client reconnects to server, previous packets were rejected
			if lastErr = w.writeTo(addr, data); lastErr != nil {
				log.Printf("[%s] retry write failed: %v", code, lastErr)
			}
			goto WAIT
		}
	}

	if lastErr == nil {
		lastErr = errors.New("exhausted retries")
	}
	return fmt.Errorf("[%s] %w", code, lastErr)
}

// reliableMultiWrite reliably writes multiple packets in sequence with SACK-based acknowledgment.
// Transfer multiple packets reliably in order with SACK-based acknowledgment.
// Key Logic:
//  1. Send all packets sequentially, last one with IsFinal=true
//  2. Wait for SACK from receiver
//  3. On timeout: resend FIN packet (not immediately after data)
//  4. Resend unacknowledged packets based on SACK feedback
//  5. Continue until all packets acknowledged or max retries exceeded
//
// Note: The last packet should set IsFinal=true to piggyback FIN signal.
func (w *reliableWriter) reliableMultiWrite(
	addr net.Addr,
	cacheKey CacheKey,
	packets [][]byte,
) error {
	const sackChanSize = 1
	sackCh := make(chan uint32, sackChanSize)
	w.storeSACK(cacheKey, sackCh)
	defer w.deleteSACK(cacheKey)

	// Prepare FIN packet
	finBlock := uint32(len(packets))
	fin := Fin{
		ReqHeader{
			Block: finBlock,
			ReqID: cacheKey.Block,
			UUID:  cacheKey.UUID,
		},
	}
	finPkt, err := fin.Marshal()
	if err != nil {
		return fmt.Errorf("marshal FIN: %w", err)
	}

	log.Printf("[%v] multiple write [%v] to [%v]", opReqString, cacheKey, addr)
	// Send all packets
	for _, pkt := range packets {
		if err = w.writeTo(addr, pkt); err != nil {
			log.Printf("packet write failed: %v", err)
		}
	}

	// Wait for SACK with retries
	for attempt := uint8(0); attempt < w.retries; attempt++ {
		timer := time.After(w.RTO.Get())
		select {
		case block := <-sackCh:
			if block > finBlock {
				return nil // All acknowledged
			}
			log.Printf("resend packet %v", block)
			if err = w.writeTo(addr, packets[block-1]); err != nil {
				log.Printf("packet write failed: %v", err)
			}

		case <-timer:
			log.Printf("SACK timeout, cache key: %v, final block: %v, retrying...", cacheKey, finBlock)
		}
		// Send FIN to signal completion
		if err := w.writeTo(addr, finPkt); err != nil {
			log.Printf("FIN write failed: %v", err)
		}
	}

	return errors.New("exhausted retries for FIN")
}

// ============================================================================
// Request Processing
// ============================================================================

// receive processes incoming reliable requests with channel multiplexing.
// Process incoming reliable requests with channel multiplexing.
// Key Logic:
//   - Generate CacheKey from request UUID and ReqID
//   - Load or create request channel
//   - Start processReq goroutine for new channels
//   - Forward request to channel
func (w *reliableWriter) receive(
	addr net.Addr,
	req ReliableReq,
	complete func(CacheKey),
) {
	const reqChanSize = 200
	cacheKey := newCacheKey(req.UUID, req.ReqID)

	w.multiple.Lock()
	reqCh, exists := w.req[cacheKey]
	if !exists {
		reqCh = make(chan ReliableReq, reqChanSize)
		w.req[cacheKey] = reqCh
		go w.handleRequestFlow(addr, cacheKey, reqCh, complete)
	}
	w.multiple.Unlock()

	nonBlockingSend(reqCh, req)
}

// handleRequestFlow processes incoming requests with reordering, acknowledgment, and result delivery.
// Process incoming requests for a specific cache key with timeout protection and reordering.
// Key Logic:
//  1. Initialize RangeTracker and reorderBuffer
//  2. Main loop with 10-second timeout (requestTimeout):
//     - Receive incoming request → track range, handle reordering
//     - If IsFinal flag is set: treat as FIN signal, check if all packets received
//     (nextMissing > incomingReq.Block)
//     - Receive FIN → send SACK, check if all blocks received
//     - Timeout → log and exit
//  3. Cleanup resources and call completion callback
func (w *reliableWriter) handleRequestFlow(
	addr net.Addr,
	cacheKey CacheKey,
	reqCh chan ReliableReq,
	complete func(CacheKey),
) {
	log.Printf("[%s] handle [%v] from [%v]", opReqString, cacheKey, addr)
	const requestTimeout = 10 * time.Second
	finCh := w.loadOrStoreFIN(cacheKey)
	retCh := w.loadOrStoreRET(cacheKey)

	tracker := new(RangeTracker)
	buffer := newReorderBuffer()
	timer := time.NewTimer(requestTimeout)

	defer func() {
		w.deleteREQ(cacheKey)
		w.deleteFIN(cacheKey)
		close(retCh)
		timer.Stop()

		if complete != nil && tracker.isCompleted() {
			complete(cacheKey)
		}
	}()

LOOP:
	for {
		timer.Reset(requestTimeout)
		select {
		case req, ok := <-reqCh:
			if !ok {
				return
			}
			w.handleIncomingRequest(req, tracker, buffer, retCh)

			// If IsFinal is set, treat it as FIN and forward to finCh
			if req.IsFinal {
				finCh <- req.Block
			}

		case finBlock, ok := <-finCh:
			if !ok {
				return
			}
			nextMissing := tracker.Next()
			buffer.flushUpTo(retCh, nextMissing-1)
			w.sack(addr, cacheKey.UUID, cacheKey.Block, nextMissing)

			if nextMissing > finBlock {
				break LOOP
			}

		case <-timer.C:
			log.Printf("[%s] %v timeout", opReqString, cacheKey)
			return
		}
	}

	log.Printf("[%s] %v completed", opReqString, cacheKey)
}

// handleIncomingRequest processes single incoming request with reordering logic based on sequence gaps.
// Process single incoming request with reordering logic based on sequence gaps.
// Key Logic:
//   - If block fills gap (nextMissing > incomingReq.Block):
//   - Send immediately via nonBlockingSend
//   - Flush buffer up to next missing
//   - If block ahead of sequence:
//   - Insert into buffer for later delivery
func (w *reliableWriter) handleIncomingRequest(
	req ReliableReq,
	tracker *RangeTracker,
	buffer *reorderBuffer,
	retCh chan ReliableReq,
) {
	blockRange := MonoRange(req.Block)

	// Skip duplicates
	if tracker.Contains(blockRange) {
		return
	}

	tracker.Track(blockRange)
	nextMissing := tracker.Next()

	if nextMissing > req.Block {
		// This block fills a gap or is the missing one
		nonBlockingSend(retCh, req)

		// Flush buffered blocks up to the gap
		if nextMissing != req.Block+1 {
			buffer.flushUpTo(retCh, nextMissing-1)
		}
	} else {
		// Block is ahead of sequence, buffer it
		buffer.insert(req)
	}
}

// notifyFIN sends FIN notification for transfer completion.
// Uses non-blocking send to avoid blocking on full channel.
// If no FIN channel exists and addr is provided, sends SACK for block 1 to request first frame (all REQ lost scenario).
func (w *reliableWriter) notifyFIN(addr net.Addr, key CacheKey, block uint32) {
	w.multiple.RLock()
	defer w.multiple.RUnlock()

	if ch, ok := w.fin[key]; ok {
		nonBlockingSend(ch, block)
	} else {
		// Three possibilities:
		// 1. FIN comes before Req
		// 2. SACK loss
		// 3. All Req loss - no handleRequestFlow goroutine started, need to request first frame
		log.Printf("[%s] notifyFIN: no FIN channel for %v", opReqString, key)

		// Scenario 3: Send SACK for block 1 to trigger retransmission of first frame
		w.sack(addr, key.UUID, key.Block, 1)
	}
}

// sack sends SACK acknowledgment to receiver.
// Used to notify sender about received packets during multi-packet transfer.
func (w *reliableWriter) sack(addr net.Addr, uuid string, reqID uint32, block uint32) {
	sack := Sack{
		ReqHeader{
			Block: block,
			ReqID: reqID,
			UUID:  uuid,
		},
	}

	if pkt, err := sack.Marshal(); err != nil {
		log.Printf("SACK marshal failed: %v", err)
	} else if err := w.writeTo(addr, pkt); err != nil {
		log.Printf("[%s] SACK failed: %v", addr, err)
	}
}

// ============================================================================
// Connection Manager Public API
// ============================================================================

func (c *connManager) writeToServer(pkt []byte) error {
	return c.writeTo(c.remoteAddr, pkt)
}

func (c *connManager) reliableWrite(data []byte, block uint32) error {
	return c.reliableWriteTo(c.remoteAddr, data, block)
}

func (c *connManager) reliableWriteTo(addr net.Addr, data []byte, block uint32) error {
	ctx, cancel := context.WithCancel(context.Background())
	retryCh := make(chan struct{}, 1)

	cacheKey := newBlockKey(block)
	c.storeACKAndRetry(cacheKey, cancel, retryCh)
	defer c.deleteACKAndRetry(cacheKey)

	return c.reliableWriter.reliableWrite(ctx, retryCh, addr, data, block)
}
