package wi

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Peer struct {
	*SignBody     // identity
	*RangeTracker // block tracker
	*reliableWriter
}

type history struct {
	*RangeTracker
	*sync.Map // block -> packet
}

func (h *history) add(block uint32, packet []byte) {
	h.Track(MonoRange(block))
	h.Store(block, packet)
}

type addrStore struct {
	addrToPeer sync.Map // addr -> Peer
	uuidToAddr sync.Map // uuid -> addr
	mu         sync.Mutex
}

func (s *addrStore) Set(sign *SignBody, addr net.Addr, conn net.PacketConn) {
	p := s.updatePeer(sign, addr, conn)
	// addr change, remove invalid addr
	if a, ok := s.uuidToAddr.Load(sign.UUID); ok && a != addr.String() {
		s.removeByUUID(sign.UUID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uuidToAddr.Store(sign.UUID, addr.String())
	s.addrToPeer.Store(addr.String(), p)
}

func (s *addrStore) removeByUUID(UUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	addr, ok := s.uuidToAddr.Load(UUID)
	if ok {
		s.addrToPeer.Delete(addr)
		s.uuidToAddr.Delete(UUID)
	}
}

func (s *addrStore) removeByAddr(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.addrToPeer.Load(addr)
	if ok {
		s.addrToPeer.Delete(addr)
		s.uuidToAddr.Delete(p.(Peer).UUID)
	}
}

func (s *addrStore) updatePeer(sign *SignBody, addr net.Addr, conn net.PacketConn) Peer {
	if v, ok := s.addrToPeer.Load(addr.String()); ok {
		p := v.(Peer)
		p.SignBody = sign
		return p
	}
	w := new(reliableWriter{RTO: RTO{minRTT: 3 * time.Second}, retries: 18, conn: conn, blockManager: newBlockManager()})
	w.rto.Store(int64(500 * time.Millisecond))
	return Peer{SignBody: sign, RangeTracker: new(RangeTracker), reliableWriter: w}
}

type Server struct {
	Status    chan struct{} `json:"-"` // initialization status
	idToFiles sync.Map      // fileId -> wrq
	idToSubs  sync.Map      // published files, FilePair -> *sync.Map { subscriber -> ReadReq }
	history   sync.Map      // sign -> *sync.Map { uuid -> *history }
	addrStore
	*audioManager
	counter uint32
	conn    net.PacketConn
}

func (s *Server) Ready() {
	if s.Status != nil {
		<-s.Status
	}
}

func (s *Server) nextID() uint32 {
	return atomic.AddUint32(&s.counter, 1)
}

func (s *Server) ListenAndServe(addr string) error {
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	s.conn = conn
	return s.Serve()
}

func (s *Server) Serve() error {
	if s.conn == nil {
		return errors.New("nil connection")
	}
	log.Printf("Listening on %s ...\n", s.conn.LocalAddr())

	s.init()
	if s.Status != nil {
		close(s.Status)
	}

	for {
		buf := make([]byte, DatagramSize)
		n, addr, err := s.conn.ReadFrom(buf)
		if err != nil {
			return err
		}

		pkt := buf[:n]
		go s.relay(pkt, addr)
	}
}

func (s *Server) relay(pkt []byte, addr net.Addr) {
	var (
		b    = bytes.NewBuffer(pkt)
		code OpCode
	)
	if binary.Read(b, binary.BigEndian, &code) != nil {
		return
	}
	switch code {
	case OpNck, OpSignedMSG, OpPull, OpDiscovery:
		if _, ok := s.addrToPeer.Load(addr.String()); !ok {
			s.reject(addr)
			return
		}
	default:
	}
	switch code {
	case OpSign:
		sign := new(SignReq)
		if sign.Unmarshal(pkt) != nil {
			return
		}
		s.ack(addr, sign.Block)
		if s.dup(addr.String(), sign.Block) {
			return
		}
		s.Set(&sign.SignBody, addr, s.conn)
		log.Printf("[%s] set sign: [%v]", addr.String(), sign)
	case OpSignOut:
		s.removeByAddr(addr.String())
	case OpAck:
		var ack Ack
		if ack.Unmarshal(pkt) != nil {
			return
		}
		log.Printf("[%v], ack received: %v", addr.String(), ack)
		if p, ok := s.addrToPeer.Load(addr.String()); ok {
			p.(Peer).complete(newCacheKey(ack.UUID, ack.Block))
		}
	case OpCheck:
		var block uint32
		if binary.Read(b, binary.BigEndian, &block) != nil {
			return
		}
		if s.check(addr.String(), block) {
			s.ack(addr, block)
		}
	case OpNck:
		var (
			block  uint32
			fileId uint32
		)
		if binary.Read(b, binary.BigEndian, &block) != nil {
			return
		}
		s.ack(addr, block)
		if s.dup(addr.String(), block) {
			return
		}
		if binary.Read(b, binary.BigEndian, &fileId) != nil {
			return
		}
		publisher, ok := s.findSignByFileId(fileId)
		if !ok {
			return
		}
		uuid, err := readString(b)
		if err != nil {
			return
		}
		if target, err := s.parseAddrByUUID(publisher.UUID); err == nil {
			s.dispatch(target, pkt, uuid, block)
		}
	case OpSignedMSG:
		msg := new(SignedMessage)
		if msg.Unmarshal(pkt) != nil {
			return
		}
		s.ack(addr, msg.Block)
		if s.dup(addr.String(), msg.Block) {
			return
		}
		s.cache(&msg.SignBody, msg.Block, pkt)
		s.ackedRelay(&msg.SignBody, msg.Block, pkt)
		log.Printf("received msg [%s] from [%s]", string(msg.Payload), addr.String())
	case OpData:
		var fileId uint32
		if binary.Read(b, binary.BigEndian, &fileId) != nil {
			return
		}
		if s.isAudio(fileId) {
			s.relayAudioStream(fileId, pkt, addr)
			return
		}
		if w, ok := s.isContent(fileId); ok {
			s.relayToSubscribers(*w, pkt)
			return
		}
		if publisher, ok := s.findSignByFileId(fileId); ok {
			s.directRelay(publisher, pkt)
		}
	case OpPull:
		var pr PullReq
		if pr.Unmarshal(pkt) != nil {
			return
		}
		s.ack(addr, pr.Block)
		if s.dup(addr.String(), pr.Block) {
			return
		}
		s.push(pr, addr)
	case OpDiscovery:
		var drq DiscoveryReq
		if drq.Unmarshal(pkt) != nil {
			return
		}
		s.ack(addr, drq.Block)
		if s.dup(addr.String(), drq.Block) {
			return
		}
		s.sendUsers(drq, addr)
	case OpSack:
		var sack Sack
		if sack.Unmarshal(pkt) != nil {
			return
		}
		log.Printf("SACK received: %v", sack)
		if p, ok := s.addrToPeer.Load(addr.String()); ok {
			p.(Peer).notifySACK(newCacheKey(sack.UUID, sack.ReqID), sack.Block)
		}
	case OpReq:
		req := ReliableReq{ReqBody: new(LongTextMessage)}
		if req.Unmarshal(pkt) != nil {
			return
		}
		if p, ok := s.addrToPeer.Load(addr.String()); ok {
			p.(Peer).receive(addr, req, s.newReqHandler(p.(Peer), addr, req))
		}
	case OpFin:
		fin := new(Fin)
		if fin.Unmarshal(pkt) != nil {
			return
		}
		if p, ok := s.addrToPeer.Load(addr.String()); ok {
			p.(Peer).notifyFIN(addr, newCacheKey(fin.UUID, fin.ReqID), fin.Block)
		}
	default:
		var (
			rrq  ReadReq
			wrq  WriteReq
			ctrl CtrlReq
		)
		switch {
		case wrq.Unmarshal(pkt) == nil:
			if _, ok := s.addrToPeer.Load(addr.String()); !ok {
				s.reject(addr)
				return
			}
			s.ack(addr, wrq.Block)
			if s.dup(addr.String(), wrq.Block) {
				return
			}
			audioId := s.decodeAudioId(wrq.FileId)
			switch wrq.Code {
			case OpAudioCall:
				s.addAudioStream(wrq)
				fallthrough
			case OpAcceptAudioCall:
				s.addAudioReceiver(audioId, wrq)
			case OpEndAudioCall:
				_, ok := s.deleteAudioReceiver(audioId, wrq.UUID)
				if ok && s.noReceiverLeft(audioId) {
					s.cleanup(audioId)
				}
			case OpPublish:
				pair := FilePair{FileId: wrq.FileId, UUID: wrq.UUID}
				s.idToSubs.Store(pair, &sync.Map{})
			case OpContent:
				s.addFile(wrq)
				s.dispatchToSubscribers(wrq, pkt)
				return
			default:
				s.addFile(wrq)
			}
			sign, ok := s.findSignByUUID(wrq.UUID)
			if !ok {
				return
			}
			switch wrq.Code {
			case OpSendImage, OpSendGif, OpSendVoice, OpPublish, OpSyncIcon:
				s.cache(sign, wrq.Block, pkt)
			default:
			}
			s.ackedRelay(sign, wrq.Block, pkt)
		case rrq.Unmarshal(pkt) == nil:
			if _, ok := s.addrToPeer.Load(addr.String()); !ok {
				s.reject(addr)
				return
			}
			s.ack(addr, rrq.Block)
			if s.dup(addr.String(), rrq.Block) {
				return
			}
			switch rrq.Code {
			case OpSubscribe, OpReadIcon:
				s.dispatchToPublisher(pkt, rrq)
			case OpUnsubscribe:
				s.deleteSub(rrq)
			default:
			}
		case ctrl.Unmarshal(pkt) == nil:
			if _, ok := s.addrToPeer.Load(addr.String()); !ok {
				s.reject(addr)
				return
			}
			s.ack(addr, ctrl.Block)
			if s.dup(addr.String(), ctrl.Block) {
				return
			}
			sign, ok := s.findSignByUUID(ctrl.UUID)
			if !ok {
				return
			}
			switch ctrl.Code {
			case OpSyncName:
				s.cache(sign, ctrl.Block, pkt)
				s.ackedRelay(sign, ctrl.Block, pkt)
			default:
			}
		}
	}
}

func (s *Server) newReqHandler(p Peer, addr net.Addr, req ReliableReq) func(cacheKey CacheKey) {
	return func(cacheKey CacheKey) {
		defer p.deleteRET(cacheKey)
		retChan := p.loadOrStoreRET(cacheKey)

		// Pre-allocate with reasonable capacity
		reqs := make([]ReliableReq, 0, 16)

		// Create timer once and reuse
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()

		for {
			// Reset timer for this iteration
			timer.Reset(10 * time.Second)

			select {
			case r, ok := <-retChan:
				if !ok {
					if s.dup(addr.String(), cacheKey.Block) {
						log.Printf("[Req] %v detected as duplicate", cacheKey)
						return
					}

					// Safe type assertion
					longText, ok := req.ReqBody.(*LongTextMessage)
					if !ok {
						log.Printf("[Req] invalid ReqBody type: expected *LongTextMessage, got %T", req.ReqBody)
						return
					}

					signBody := &SignBody{Sign: longText.Sign, UUID: req.UUID}
					reqSet := new(ReqSet(reqs))
					if packets, err := reqSet.ToPackets(); err != nil {
						log.Printf("req set to packets failed: %v", err)
					} else {
						s.ackedMultiRelay(signBody, packets, cacheKey)
					}

					if buf, err := reqSet.Marshal(); err != nil {
						log.Printf("req set marshal failed: %v", err)
					} else {
						s.cache(signBody, cacheKey.Block, buf)
					}
					return
				}
				reqs = append(reqs, r)

			case <-timer.C:
				log.Printf("[Req] %v timeout, discarding collected requests", cacheKey)
				return
			}
		}
	}
}

func (s *Server) sendUsers(drq DiscoveryReq, addr net.Addr) {
	users := s.collectUsers(drq.Sign, drq.DiscoveryFlag)
	if v, ok := s.addrToPeer.Load(addr.String()); ok {
		headerSize := 12 + len(v.(Peer).UUID)
		resp := s.toDiscoveryResp(users, headerSize)
		reqs := make([]ReliableReq, 0, len(resp))
		for i, d := range resp {
			reqs = append(reqs, ReliableReq{ReqHeader: ReqHeader{Block: uint32(i + 1), ReqID: drq.Block, UUID: v.(Peer).UUID}, ReqBody: &d})
		}
		reqs[len(reqs)-1].IsFinal = true
		if packets, err := new(ReqSet(reqs)).ToPackets(); err != nil {
			log.Printf("user reqs marshal failed: %v", err)
		} else {
			s.dispatchInOrder(addr, packets, newCacheKey(v.(Peer).UUID, drq.Block))
		}
	}
}

func (s *Server) toDiscoveryResp(users []string, baseSize int) []DiscoveryResp {
	maxSize := DatagramSize - baseSize - 3 // DatagramSize - OpCode - len(UUIDS)
	ret := make([]DiscoveryResp, 0, 20)
	prev, size := 0, 0
	for i := 0; i < len(users); i++ {
		n := 1 + len(users[i])
		if size+n > maxSize {
			ret = append(ret, DiscoveryResp{UUIDS: users[prev:i]})
			prev, size = i, 0
		}
		size += n
	}
	return append(ret, DiscoveryResp{UUIDS: users[prev:]})
}

func (s *Server) collectUsers(sign string, flag DiscoveryFlag) []string {
	switch flag {
	case Online:
		return s.collectOnlineUsers(sign)
	case Active:
		fallthrough
	default:
		return s.collectActiveUsers(sign)
	}
}

func (s *Server) collectOnlineUsers(sign string) []string {
	uuids := make([]string, 0, 16)
	s.addrToPeer.Range(func(k, v interface{}) bool {
		p := v.(Peer)
		if p.Sign == sign {
			uuids = append(uuids, p.UUID)
		}
		return true
	})
	return uuids
}

func (s *Server) collectActiveUsers(sign string) []string {
	uuids := make([]string, 0, 16)
	hs := s.loadHistorySet(sign)
	hs.Range(func(k, v interface{}) bool {
		uuids = append(uuids, k.(string))
		return true
	})
	return uuids
}

func (s *Server) push(pr PullReq, addr net.Addr) {
	h := s.loadHistory(&pr.SignBody)
	if pr.end == math.MaxUint32 {
		s.reply(pr, addr, h.Get())
		s.pushRange(pr, addr, h, Range{pr.start, h.nextBlock()})
		return
	}
	ranges := h.Select(pr.Range)
	s.reply(pr, addr, ranges)
	t := RangeTracker{ranges: pr.ranges, latestBlock: pr.end}
	t.Exclude(ranges)
	for _, r := range t.ranges {
		s.pushRange(pr, addr, h, r)
	}
}

func (s *Server) reply(pr PullReq, addr net.Addr, ranges []Range) {
	var (
		p        []byte
		err      error
		baseSize = 9 + len(pr.UUID) + len(pr.Sign)
		maxLen   = (DatagramSize - baseSize) / 8
		req      ReplyReq
	)
	for _, r := range partition(ranges, maxLen) {
		req = ReplyReq{Block: s.nextID(), SignBody: pr.SignBody, ranges: r}
		if p, err = req.Marshal(); err != nil {
			log.Printf("pull req [%s]: %v", pr.UUID, err)
		}
		s.dispatch(addr, p, _SERVER, req.Block)
	}
}

func (s *Server) pushRange(pr PullReq, addr net.Addr, h *history, r Range) {
	for i := r.start; i <= r.end; i++ {
		p, ok := h.Load(i)
		if !ok {
			continue
		}
		data := p.([]byte)
		if toOpCode(data[:2]) == OpReq {
			s.dispatchInOrder(addr, ToPackets(data), newCacheKey(pr.UUID, i))
		} else {
			s.dispatch(addr, data, pr.UUID, i)
		}
	}
}

func (s *Server) cache(sign *SignBody, block uint32, pkt []byte) {
	s.loadHistory(sign).add(block, pkt)
}

func (s *Server) loadHistorySet(sign string) *sync.Map {
	if h, ok := s.history.Load(sign); ok {
		return h.(*sync.Map)
	}
	h, _ := s.history.LoadOrStore(sign, new(sync.Map))
	return h.(*sync.Map)
}

func (s *Server) loadHistory(sign *SignBody) *history {
	hs := s.loadHistorySet(sign.Sign)
	if h, ok := hs.Load(sign.UUID); ok {
		return h.(*history)
	}
	h, _ := hs.LoadOrStore(sign.UUID, &history{RangeTracker: new(RangeTracker), Map: new(sync.Map)})
	return h.(*history)
}

func (s *Server) isContent(fileId uint32) (*WriteReq, bool) {
	wrq, ok := s.idToFiles.Load(fileId)
	if !ok {
		return nil, false
	}
	w := wrq.(WriteReq)
	return &w, w.Code == OpContent
}

func (s *Server) relayToSubscribers(wrq WriteReq, pkt []byte) {
	subs, ok := s.idToSubs.Load(FilePair{FileId: wrq.FileId, UUID: wrq.UUID})
	if !ok {
		return
	}
	subs.(*sync.Map).Range(func(k, v interface{}) bool {
		if addr, err := s.parseAddrByUUID(k.(string)); err != nil {
			return true
		} else if err = s.writePacket(addr, pkt); err != nil {
			log.Printf("relay to subscriber [%v] failed,%v", k.(string), err)
		}
		return true
	})
}

func (s *Server) dispatchToSubscribers(wrq WriteReq, pkt []byte) {
	subs, ok := s.idToSubs.Load(FilePair{FileId: wrq.FileId, UUID: wrq.UUID})
	if !ok {
		return
	}
	subs.(*sync.Map).Range(func(k, v interface{}) bool {
		addr, err := s.parseAddrByUUID(k.(string))
		if err != nil {
			return true
		}
		go s.dispatch(addr, pkt, wrq.UUID, wrq.Block)
		return true
	})
}

func (s *Server) dispatchToPublisher(pkt []byte, rrq ReadReq) {
	pair := FilePair{FileId: rrq.FileId, UUID: rrq.Publisher}
	subs, ok := s.idToSubs.Load(pair)
	if !ok {
		subs = &sync.Map{}
		s.idToSubs.Store(pair, subs)
	}
	subs.(*sync.Map).Store(rrq.Subscriber, rrq)
	addr, err := s.parseAddrByUUID(rrq.Publisher)
	if err != nil {
		return
	}
	go s.dispatch(addr, pkt, rrq.Subscriber, rrq.Block)
}

func (s *Server) deleteSub(rrq ReadReq) {
	subs, ok := s.idToSubs.Load(FilePair{FileId: rrq.FileId, UUID: rrq.Publisher})
	if !ok {
		return
	}
	subs.(*sync.Map).Delete(rrq.Subscriber)
}

func (s *Server) reject(addr net.Addr) {
	if p, err := new(ErrUnknownUser).Marshal(); err != nil {
		log.Printf("reject [%v] failed, %v", addr.String(), err)
	} else if err = s.writePacket(addr, p); err != nil {
		log.Printf("reject [%v] failed, %v", addr.String(), err)
	}
}

func (s *Server) relayAudioStream(fileId uint32, pkt []byte, sender net.Addr) {
	p, ok := s.addrToPeer.Load(sender.String())
	if !ok {
		return
	}
	UUID := p.(Peer).UUID
	receivers, ok := s.audioReceiver.Load(s.decodeAudioId(fileId))
	if !ok {
		return
	}
	receivers.(*sync.Map).Range(func(key, value interface{}) bool {
		if key != UUID {
			if addr, err := s.parseAddrByUUID(key.(string)); err != nil {
				log.Printf("relay audio stream to receiver [%v] failed,%v", key.(string), err)
			} else if err = s.writePacket(addr, pkt); err != nil {
				log.Printf("relay audio stream to receiver [%v] failed,%v", key.(string), err)
			}
		}
		return true
	})
}

func (s *Server) directRelay(sign *SignBody, pkt []byte) {
	s.addrToPeer.Range(func(key, value interface{}) bool {
		p := value.(Peer)
		if p.Sign == sign.Sign && p.UUID != sign.UUID {
			if addr, err := net.ResolveUDPAddr("udp", key.(string)); err != nil {
				log.Printf("direct relay to [%v] failed,%v", p.UUID, err)
			} else if err = s.writePacket(addr, pkt); err != nil {
				log.Printf("direct relay to [%v] failed,%v", p.UUID, err)
			}
		}
		return true
	})
}

func (s *Server) addFile(wrq WriteReq) {
	s.idToFiles.Store(wrq.FileId, wrq)
}

func (s *Server) init() {
	s.audioManager = &audioManager{}
}

func (s *Server) findSignByUUID(uuid string) (*SignBody, bool) {
	addr, ok := s.uuidToAddr.Load(uuid)
	if !ok {
		return nil, false
	}
	p, ok := s.addrToPeer.Load(addr)
	if !ok {
		return nil, false
	}
	return p.(Peer).SignBody, true
}

func (s *Server) parseAddrByUUID(uuid string) (net.Addr, error) {
	a, ok := s.uuidToAddr.Load(uuid)
	if !ok {
		return nil, errors.New("no such uuid")
	}
	return net.ResolveUDPAddr("udp", a.(string))
}

func (s *Server) findSignByFileId(fileId uint32) (*SignBody, bool) {
	wrq, ok := s.idToFiles.Load(fileId)
	if !ok {
		return nil, false
	}
	UUID := wrq.(WriteReq).UUID
	return s.findSignByUUID(UUID)
}

func (s *Server) ack(addr net.Addr, block uint32) {
	if pkt, err := new(Ack{Block: block}).Marshal(); err != nil {
		log.Printf("ack [%v] failed, %v", addr.String(), err)
	} else if err = s.writePacket(addr, pkt); err != nil {
		log.Printf("[%s] ack failed: %v", addr, err)
	}
}

// dup return true if block is duplicated
func (s *Server) dup(addr string, block uint32) bool {
	v, ok := s.addrToPeer.Load(addr)
	if !ok {
		return false
	}
	p := v.(Peer)
	r := MonoRange(block)
	dup := p.Contains(r)
	if !dup {
		p.Track(r)
	}
	return dup
}

func (s *Server) check(addr string, block uint32) bool {
	v, ok := s.addrToPeer.Load(addr)
	if !ok {
		return false
	}
	return v.(Peer).Contains(MonoRange(block))
}

func (s *Server) ackedRelay(sign *SignBody, block uint32, bytes []byte) {
	s.addrToPeer.Range(func(key, value interface{}) bool {
		p := value.(Peer)
		if p.Sign == sign.Sign && p.UUID != sign.UUID {
			// use goroutine to avoid blocking by slow connection
			addr, err := net.ResolveUDPAddr("udp", key.(string))
			if err != nil {
				return true
			}
			go s.dispatch(addr, bytes, sign.UUID, block)
		}
		return true
	})
}

func (s *Server) ackedMultiRelay(sign *SignBody, packets [][]byte, cacheKey CacheKey) {
	s.addrToPeer.Range(func(key, value interface{}) bool {
		p := value.(Peer)
		if p.Sign == sign.Sign && p.UUID != sign.UUID {
			// use goroutine to avoid blocking by slow connection
			addr, err := net.ResolveUDPAddr("udp", key.(string))
			if err != nil {
				return true
			}
			go s.dispatchInOrder(addr, packets, cacheKey)
		}
		return true
	})
}

func (s *Server) dispatchInOrder(addr net.Addr, packets [][]byte, cacheKey CacheKey) {
	if v, ok := s.addrToPeer.Load(addr.String()); ok {
		if err := v.(Peer).reliableMultiWrite(addr, cacheKey, packets); err != nil {
			log.Printf("[%v] dispatch in order failed: %v", addr.String(), err)
		}

	}
}

func (s *Server) writePacket(addr net.Addr, pkt []byte) error {
	if err := s.conn.SetWriteDeadline(time.Now().Add(_TIMEOUT)); err != nil {
		return err
	}
	if _, err := s.conn.WriteTo(pkt, addr); err != nil {
		return err
	}
	return nil
}

// dispatch may block by slow connection
func (s *Server) dispatch(addr net.Addr, data []byte, sender string, block uint32) {
	var (
		target = addr.String()
		v, ok  = s.addrToPeer.Load(target)
	)
	if !ok {
		return
	}

	p, code, key := v.(Peer), toOpCode(data[:2]), newCacheKey(sender, block)
	ctx, ack := context.WithCancel(context.Background())
	defer ack()
	p.storeACK(key, ack)
	defer p.deleteACK(key)

	log.Printf("[%v] dispatch packet %v to [%v]-[%v]", code.String(), block, p.UUID, target)
	if err := p.reliableWrite(ctx, nil, addr, data, block); err != nil {
		log.Printf("[%v]-[%v]-[%v] dispatch failed, %v", code.String(), p.UUID, target, err)
	}
}
