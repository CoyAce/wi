# conn.go Specification - Reliable UDP Transmission Layer

## Overview
This document specifies the reliable transmission layer for a self-developed UDP framework. The implementation provides ARQ (Automatic Repeat Request) mechanism with adaptive timeout control.

---

## 1. Design Goals

### Core Principles
- **Reliable UDP transmission** with support for out-of-order delivery and retransmission
- **Multi-packet transfer** with receiver-side reordering
- **Memory efficiency** through pre-allocation and buffer reuse
- **Concurrency-friendly** design using channels and contexts

### AI Implementation Guidelines
The following aspects encourage AI initiative:
1. **Data structure choices** - Can use channel/map/slice as appropriate
2. **Concurrency model** - Goroutine creation strategy can be optimized
3. **Memory management** - Buffer sizes and pre-allocation can be tuned
4. **Error recovery** - Specific retry and recovery logic can be innovative

The following aspects must be preserved:
1. **Protocol format** - OpCode and packet header structures
2. **Public interfaces** - Function signatures and return values

---

## 2. Public Interfaces

Core API for reliable UDP transmission:

```go
type ReliableWriter interface {
    // Write packet to address with timeout enforcement
    writeTo(addr net.Addr, pkt []byte) error
    
    // Reliably write single block with ACK/retry mechanism
    reliableWrite(addr net.Addr, data []byte, block uint32) error
    
    // Reliably write multiple packets in sequence with SACK-based acknowledgment
    reliableMultiWrite(addr net.Addr, packets []ReliableReq) error
}
```

**Note**: The last packet in `reliableMultiWrite` should set `IsFinal=true` to piggyback FIN signal.

---

## 3. Data Structures

### 3.1 connManager
Manages connection state and remote address.
```go
type connManager struct {
    reliableWriter           // Embedded reliable writer
    remoteAddr net.Addr      // Remote peer address
}
```

### 3.2 blockManager
Tracks acknowledgment state for individual blocks, organizing single and multi-block operations.
```go
type blockManager struct {
    single    // Single-block operations (ACK/retry)
    multiple  // Multi-block operations (SACK/FIN/REQ/RET)
}
```

### 3.3 single
Manages ACK callbacks and retry channels for single block operations using concurrent-safe maps.
```go
type single struct {
    sync.RWMutex
    ackFunc map[CacheKey]context.CancelFunc  // Cancellation functions for ACKs
    retry   map[CacheKey]chan struct{}       // Retry trigger channels
}
```

### 3.4 multiple
Manages channels for multi-block reliable transfers (SACK/FIN/REQ/RET).
```go
type multiple struct {
    sync.RWMutex
    sack map[CacheKey]chan uint32      // SACK acknowledgment channels
    fin  map[CacheKey]chan uint32      // FIN completion channels
    req  map[CacheKey]chan ReliableReq // Incoming out-of-order request channels
    ret  map[CacheKey]chan ReliableReq // Outgoing in order response channels
}
```

### 3.5 RTO (Retransmission Timeout)
Implements adaptive RTO calculation using RTT variance with EWMA algorithm.
```go
type RTO struct {
    minRTT     time.Duration  // Minimum observed RTT
    rttVar     time.Duration  // RTT variance (EWMA)
    rto        atomic.Int64   // Current RTO value (atomic)
    lastUpdate time.Time      // Last RTT update timestamp
}
```

**Key Methods**:
- `Update(start time.Time)`: Update RTO based on measured RTT
- `Increase()`: Exponentially increase RTO on timeout
- `Get() time.Duration`: Get current RTO value

### 3.6 reliableWriter
Core reliable transmission engine combining block tracking, RTO calculation, and connection management.
```go
type reliableWriter struct {
    *blockManager            // Embedded block tracking
    RTO                      // Embedded RTO calculator
    retries   uint8          // Maximum retry attempts
    localAddr string         // Local binding address
    relistenState  int32     // Connection state (0=connected, 1=relistening)
    conn      net.PacketConn // Underlying UDP connection
}
```

### 3.7 reorderBuffer
Buffers out-of-order packets for resequencing, maintaining sorted order by block number.

**Key Operations**: Buffer maintains sorted order using binary search insertion, supports flush up to specific block number.
```go
type reorderBuffer struct {
    items []ReliableReq  // Buffered requests in sorted order
}
```
---

### 3.8 Reliable Transmission

#### reliableWrite (Core Method)
Implement reliable transmission with retry mechanism.

**Key Logic**:
- Send DATA packet on even attempts, CHECK packet on odd attempts
- Wait for ACK with timeout based on RTO
- On timeout: increase RTO exponentially and retry
- On write error: log and continue to next attempt
- Trigger relisten on EPIPE errors
- On manual retry (retryCh): resend data packet immediately and jump back to wait for ACK (not send CHECK)

#### writeTo
Write packet with deadline enforcement using _TIMEOUT constant.

#### relisten
Recover from connection failure by recreating listener.

**Key Logic**:
- Use atomic CAS operation to prevent concurrent relisten
- Close existing connection
- Create new UDP listener
- Reset state after completion

#### listen
Create UDP listener on localAddr.

### 3.9 Multi-Packet Reliable Transfer

#### reliableMultiWrite
Transfer multiple packets reliably in order with SACK-based acknowledgment.

**Key Logic**:
1. Send all packets sequentially, last one with IsFinal=true
2. Wait for SACK from receiver
3. On timeout: resend FIN packet (not immediately after data)
4. Resend unacknowledged packets based on SACK feedback
5. Continue until all packets acknowledged or max retries exceeded

### 3.10 Request Processing

#### receive
Process incoming reliable requests with channel multiplexing.

**Key Logic**:
- Generate CacheKey from request UUID and ReqID
- Load or create request channel
- Start handleRequestFlow goroutine for new channels
- Forward request to channel

#### handleRequestFlow
Process incoming requests for a specific cache key with timeout protection and reordering.

**Key Logic**:
1. Initialize RangeTracker and reorderBuffer
2. Main loop with 10-second timeout:
   - Receive incoming request → track range, handle reordering
   - If IsFinal flag is set: forward block number to finCh channel for unified completion handling
   - Receive FIN from finCh → send SACK, check if all blocks received (nextMissing > finBlock)
   - Timeout → log and exit
3. Cleanup resources and call completion callback

#### handleIncomingRequest
Process single incoming request with reordering logic based on sequence gaps.

**Key Logic**:
- If block fills gap (nextMissing > incomingReq.Block):
  - Send immediately via trySend
  - Flush buffer up to next missing
- If block ahead of sequence:
  - Insert into buffer for later delivery

#### trySend
Non-blocking send with discard policy for closed/full channels.

**Features**:
- Panic recovery for closed channels
- Drop message if channel full
- Log dropped messages

---

## 4. Protocol Flow

### 4.1 Single Packet Reliable Transfer
```
Sender                          Receiver
  |                                |
  |-- [DATA] block N ------------> |
  |                                |-- ACK block N
  |<-- [ACK] block N ------------- |
  |                                |
  (On timeout)                    |
  |-- [CHECK] block N -----------> |
  |                                |-- ACK block N
  |<-- [ACK] block N ------------- |
  |                                |
  (On manual retry via retryCh)   |
  |-- [DATA] block N ------------> |  (resend data, not CHECK)
  |                                |-- ACK block N
  |<-- [ACK] block N ------------- |
```

**Note**: Manual retry (retryCh) resends DATA packet directly and jumps back to wait for ACK, avoiding redundant CHECK packet. This is needed when client reconnects to server.

### 4.2 Multi-Packet Reliable Transfer
```
Sender                          Receiver
  |                                |
  |-- [DATA] block 1 ------------> |
  |-- [DATA] block 2 ------------> |
  |-- [DATA] block 3 [FIN]-------> |  (IsFinal=true piggybacks FIN)
  |                                |-- [SACK] block 2
  |<-- [SACK] block 2 -------------| (missing block 2)
  |                                |
  |-- [DATA] block 2 ------------> |
  |-- [FIN] ---------------------> |  (resend FIN on timeout to prompt completion)
  |                                |-- [SACK] block 3
  |<-- [SACK] block 3 -------------| (all received, completion triggered)
```

**Note**: FIN packet is sent after timeout in the select loop, not immediately after data packets.

### 4.3 Out-of-Order Handling
```
handleRequestFlow receives: [block 1], [block 3], [block 2]
  
1. Receive block 1:
   - nextMissing = 2
   - trySend(retChan, block 1) - send immediately to output

2. Receive block 3:
   - nextMissing = 2 (waiting for block 2)
   - Insert block 3 into buffer (buffer: [block 3])
   - Still waiting for block 2...

3. Receive block 2:
   - nextMissing = 4
   - trySend(retChan, block 2) - send immediately
   - buffer.flushUpTo(3): sends block 3 from buffer
   - Buffer now empty
   - All blocks [1,2,3] sent in order!
```

**Corrected Logic Explanation**:
- **if nextMissing > incomingReq.Block**: Received block IS the missing one or fills a gap
  - Send it immediately via trySend (it's needed now)
  - If there's a gap between this block and nextMissing, flush buffered blocks
  - Example: nextMissing=5, received block=3 → send block 3, flush buffer up to 4
  
- **else** (nextMissing <= incomingReq.Block): Received block is AHEAD of sequence
  - This block is too far ahead, we're still waiting for earlier blocks
  - Insert current block into buffer for later delivery when gaps are filled

---

## 5. Error Handling

### 5.1 Network Errors
- **EPIPE (syscall.EPIPE)**: Triggers relisten procedure
- **Write errors**: Logged and retried
- **Marshal errors**: Logged, operation skipped

### 5.2 Channel Errors
- Closed channel: Panic recovered with log
- Full channel: Message dropped
- Send failure in finish(): Panic recovery

### 5.3 Timeout Handling
- **Per-attempt timeout**: Uses RTO value
- **Process timeout**: 10 seconds fixed
- **Timeout action**: Exponential backoff (Increase())

---

## 6. Concurrency Model

### 6.1 Lock Usage
- **single**: RWMutex for ACK/retry maps
- **multiple**: RWMutex for SACK/FIN/REQ/RET maps
- **RangeTracker**: Internal RWMutex (in util.go)

### 6.2 Goroutine Creation
- **handleRequestFlow**: One goroutine per unique CacheKey
- **reliableWrite**: Caller's goroutine
- **reliableMultiWrite**: Caller's goroutine

### 6.3 Synchronization Primitives
- **context.Context**: ACK cancellation
- **Channels**: Event notification (retry, SACK, FIN)
- **atomic.Int64**: Lock-free RTO storage

---

## 7. Performance Optimizations

### 7.1 Memory Management
- Pre-allocated maps (capacity 64)
- Buffer reuse with clear() instead of reallocation
- Slice truncation (buf[:0]) preserves capacity

### 7.2 Algorithm Efficiency
- Binary search for sorted insertion: O(log n)
- Range tracking with compression: O(ranges)
- Selective acknowledgment: O(1) for contiguous blocks

### 7.3 Resource Cleanup
- Deferred channel closure
- Timer.Stop() to prevent leaks
- Map entry deletion on completion

### 7.4 Constant Scoping
**Best Practice**: Constants are scoped to their usage location:
- **Global constant**: Only `opReqString` (used in multiple places)
- **Function-scoped constants**: All other constants defined where used
  - `initialCapacity` → `newBlockManager()`
  - `pendingBufCapacity` → `newReorderBuffer()`
  - `finChanSize` → `loadOrStoreFIN()`
  - `retChanSize` → `loadOrStoreRET()`
  - `rttWindow` → `RTO.Update()`
  - `maxRTO`, `rtoIncreaseFactor`, `rtoIncreaseBase` → `RTO.Increase()`
  - `sackChanSize` → `reliableMultiWrite()`
  - `reqChanSize` → `receive()`
  - `requestTimeout` → `handleRequestFlow()`

**Benefits**:
- Better encapsulation and code cohesion
- Avoids global namespace pollution
- Easier maintenance (changes localized)
- Prevents naming conflicts across packages

---

## 8. Configuration Notes

Key runtime parameters (all defined in function scope where used):
- `requestTimeout`: 10 seconds for request processing (in `handleRequestFlow`)
- `retries`: Maximum retry attempts before failure  
- `_TIMEOUT`: Write deadline for individual packets
- `pendingBufCapacity`: Initial capacity for reorder buffer, 16 (in `newReorderBuffer`)
- `retChanSize` / `reqChanSize`: Channel buffer sizes, 200 (in `receive`/`loadOrStoreRET`)
- `initialCapacity`: Map pre-allocation size, 64 (in `newBlockManager`)
- RTO constants: `maxRTO`, `rtoIncreaseFactor`, `rtoIncreaseBase` (in `RTO.Increase`)

These values can be tuned based on network conditions. See section 10 for guidelines.

## 9. Dependencies
- **types.go**: CacheKey, ReliableReq, OpCode, Check, Fin, Sack types, MonoRange function
- **util.go**: RangeTracker type and methods, newCacheKey/newBlockKey helpers

**Note**: Uses only Go standard library with no third-party dependencies.

---

## 10. Testing Guidelines

### Unit Test Targets
Focus on core algorithms and data structures:
1. **RTO calculations**: Verify EWMA algorithm correctness
2. **reorderBuffer**: Test insert/flush/clear operations
3. **trySend**: Test non-blocking behavior and panic recovery
4. **Channel management**: Test loadOrStore/delete pairs

### Integration Scenarios
Real-world failure scenarios:
1. Packet loss simulation and recovery
2. Out-of-order delivery handling
3. Duplicate packet detection
4. Timeout and exponential backoff
5. Connection recovery (relisten)

---

## 11. Future Enhancements

### Potential Improvements
1. **Congestion control**: Add window-based flow control
2. **Fast retransmit**: Trigger on 3 duplicate ACKs
3. **Selective ACK**: Support SACK for better loss recovery
4. **Connection pooling**: Reuse connections for multiple peers
5. **Metrics**: Export RTT, loss rate, throughput statistics

---

**Document Version**: 2.0 (Optimized)  
**Philosophy**: High-level design principles with implementation flexibility  
**Status**: Ready for AI-powered code generation ✨