# Project Context - UDP Framework

## Project Overview
This is a **self-developed UDP-based reliable transmission framework** built from scratch with zero external dependencies.

## Key Characteristics

### 🎯 Core Architecture
- **Protocol**: Custom UDP-based reliable transmission protocol
- **Dependencies**: None - uses only Go standard library
- **Purpose**: Provides reliable data transmission over unreliable UDP connections

### 🔧 Technical Stack
- **Language**: Pure Go (Golang)
- **Standard Library Only**: 
  - `net` - UDP socket operations
  - `context` - cancellation and timeouts
  - `sync` - concurrency primitives
  - `atomic` - lock-free operations
  - `binary` - packet serialization
  - `time` - timeout and RTT management

### 📦 Main Components

1. **Reliable Transmission Layer** (`conn.go`)
   - Automatic repeat request (ARQ) mechanism
   - Adaptive retransmission timeout (RTO) calculation
   - Packet reordering and duplicate detection
   - Range tracking for missing packets

2. **Connection Management**
   - `reliableWriter` - handles reliable packet delivery
   - `connManager` - manages connection state
   - `blockManager` - tracks individual packet acknowledgments

3. **Protocol Operations** (`types.go`)
   - OpCode-based message typing
   - Request/Response patterns
   - Control messages (ACK, NCK, FIN, SACK)
   - Data transfer operations

4. **Advanced Features**
   - RTT estimation with exponential weighted moving average
   - Automatic retransmission with exponential backoff
   - Connection recovery (relisten on EPIPE)
   - Multi-packet reliable transfers
   - Publish/Subscribe pattern support

### 🏗️ Design Principles
- **Zero Dependencies**: Complete independence from third-party libraries
- **Performance Optimized**: Minimal allocations, efficient buffering
- **Fault Tolerant**: Handles packet loss, duplication, and reordering
- **Self-Healing**: Automatic connection recovery and timeout adaptation

### 📁 Project Structure
```
wi/
├── conn.go          # Core reliable transmission logic
├── types.go         # Protocol types and message definitions
├── util.go          # Utility functions and helpers
├── server.go        # Server implementation
├── client.go        # Client implementation
└── ...              # Additional components
```

### 🔐 Key Innovations
1. **Custom RTO Algorithm**: Adaptive timeout calculation based on RTT variance
2. **Range Tracking**: Efficient missing packet detection using range compression
3. **Dual-Mode Transmission**: Alternates between data packets and check packets
4. **Channel-Based Concurrency**: Leverages Go's CSP model for clean async code

### 🎯 Use Cases
- Real-time messaging with delivery guarantees
- File transfer over UDP
- Audio/video streaming with reliability layers
- Distributed system communication

---

**Note**: This framework demonstrates that reliable transmission can be achieved over UDP without relying on external libraries like QUIC or gRPC, providing full control over performance characteristics and behavior.
