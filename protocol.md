## SendFile

### Scenario 1: Direct Send (OpContent)

```
Sender (Client)              Server              Receiver (Peer)
    |                          |                      |
    |=== Phase 1: Send File ==========================|
    |-- [WRQ] OpContent ------>|                      | init fileContent
    |   (fileId, size, name)   |                      |
    |                          |                      |
    |=== Phase 2: Initial NCK ========================|
    |<-- [Forward NCK] ---------|<-- [NCK] -----------| "Need blocks [1-finalBlock]"
    |   ranges: [1-finalBlock] |                      | (shuffle k ranges)
    |                          |                      |
    |=== Phase 3: Data Stream ========================|
    |                          |                      |
    |-- [DATA] block 1 ------->|------> [DATA] ------>| buffer + track
    |-- [DATA] block 2 ------->|------> [DATA] ------>| buffer + track
    |-- [DATA] block 3 ------->|------> [DATA] ------>| LOST! (silent)
    |-- [DATA] block 4 ------->|------> [DATA] ------>| buffer + track
    |                          |                      |
    |   ... (stream all blocks)                       |
    |                          |                      |
    |-- [DATA] block N ------->|------> [DATA] ------>| final block
    |                          |                      |
    |=== Phase 4: Completion Check ===================|
    |                          |                      | isCompleted()?
    |                          |                      | NO -->
    |                          |<-- [NCK] ------------| "Still need [3, 7]"
    |<-- [Forward NCK] ---------|                     |
    |                          |                      |
    |=== Phase 5: Retransmission =====================|
    |                          |                      |
    |-- [DATA] block 3 ------->|------> [DATA] ------>| fill gap
    |-- [DATA] block 7 ------->|------> [DATA] ------>| fill gap
    |                          |                      |
    |                          |                      | isCompleted()?
    |                          |                      | YES -->
    |=== Phase 6: Complete ===========================|
    |                          |                      |
    |-- [OpReady] ------------>|------> [OpReady] --->| flush to disk + notify
    |   (fileId)               |                      | cleanup
    |                          |                      |
```

### Scenario 2: Publish & Subscribe (OpPublish)

```
Sender (Client)              Server              Receiver (Peer)
    |                          |                      |
    |=== Phase 1: Publish Metadata ===================|
    |-- [WRQ] OpPublish ------>|                      | cache metadata
    |   (fileId, size, name)   |                      |
    |                          |                      |
    |=== Phase 2: Peer Subscribe =====================|
    |                          |<-- [RRQ] OpSubscribe-| "I want this file"
    |                          |   (fileId, sender)   |
    |<-- [Forward RRQ] --------|                      | trigger NCK
    |                          |                      |
    |=== Phase 3: NCK Loop ===========================|
    |<-- [NCK] ================|<-- [NCK] ============|
    |                          |                      |
    |=== Phase 4: Data Stream ========================|
    |-- [DATA] stream ================================|
    |                          |                      |
    |=== Phase 5: Retransmission (if needed) =========|
    |<-- [NCK] ================|<-- [NCK] ============| retransmit gaps
    |                          |                      |
    |=== Phase 6: Complete ===========================|
    |                          |                      |
    |-- [OpReady] ------------>|------> [OpReady] --->| flush + notify
    |                          |                      |
    |=== Optional: Unsubscribe =======================|
    |<-- [Unsub] ==============|<----- [Unsub] -------| cleanup
```

### Key Differences

**OpContent (Direct Send)**:
- One-step transfer: SendFile → NCK → Data → Complete
- No subscription required
- Used for direct push to known recipients

**OpPublish (Publish-Subscribe)**:
- Two-phase: Publish metadata first, then wait for subscriptions
- Triggers NCK only after receiving OpSubscribe
- Supports multi-peer distribution
- Optional unsubscribe for cleanup
