package wi

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log"
	"math"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Peer struct {
	*SignBody     // identity
	*sync.Map     // block -> CancelFunc
	*RangeTracker // block tracker
	Timeout       *time.Duration
}

func (p Peer) ack(cacheKey string) {
	cancel, ok := p.Load(cacheKey)
	if !ok {
		return
	}
	cancel.(context.CancelFunc)()
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

func (s *addrStore) Set(sign *SignBody, addr net.Addr) {
	p := s.updatePeer(sign, addr)
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

func (s *addrStore) updatePeer(sign *SignBody, addr net.Addr) Peer {
	timeout := 500 * time.Millisecond
	p := Peer{SignBody: sign, Map: new(sync.Map), RangeTracker: new(RangeTracker), Timeout: &timeout}
	if v, ok := s.addrToPeer.Load(addr.String()); ok {
		p = v.(Peer)
		p.SignBody = sign
	}
	return p
}

type Server struct {
	Status    chan struct{} `json:"-"` // initialization status
	Retries   uint8         // the number of times to retry a failed transmission
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
		s.Set(&sign.SignBody, addr)
		log.Printf("[%s] set sign: [%v]", addr.String(), sign)
	case OpSignOut:
		s.removeByAddr(addr.String())
	case OpAck:
		var ack Ack
		if ack.Unmarshal(pkt) != nil {
			return
		}
		log.Printf("ack received: %v", ack.Block)
		if p, ok := s.addrToPeer.Load(addr.String()); ok {
			p.(Peer).ack(s.cacheKey(ack.UUID, ack.Block))
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
		if target, err := s.parseAddrByUUID(publisher.UUID); err == nil {
			s.dispatch(target, pkt, _NCK, block)
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

func (s *Server) sendUsers(drq DiscoveryReq, addr net.Addr) {
	users := s.collectUsers(drq.Sign, drq.DiscoveryFlag)
	for _, d := range s.toDiscoveryResp(users, drq.Block) {
		v, err := d.Marshal()
		if err != nil {
			log.Printf("discovery marshal failed: %v", err)
		}
		s.dispatch(addr, v, _SERVER, d.Block)
	}
}

func (s *Server) toDiscoveryResp(users []string, reqID uint32) []DiscoveryResp {
	const maxSize = DatagramSize - 12
	ret := make([]DiscoveryResp, 0, 20)
	prev, size := 0, 0
	for i := 0; i < len(users); i++ {
		n := 1 + len(users[i])
		if size+n > maxSize {
			ret = append(ret, DiscoveryResp{Block: s.nextID(), ReqID: reqID, Final: 0, UUIDS: users[prev:i]})
			prev = i
			size = 0
		}
		size += n
	}
	return append(ret, DiscoveryResp{Block: s.nextID(), ReqID: reqID, Final: 1, UUIDS: users[prev:]})
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
	uuids := make([]string, 0)
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
	uuids := make([]string, 0)
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
		s.dispatch(addr, p.([]byte), pr.UUID, i)
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
		addr, err := s.parseAddrByUUID(k.(string))
		if err != nil {
			return true
		}
		_, _ = s.conn.WriteTo(pkt, addr)
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
	err := ErrUnknownUser
	p, _ := err.Marshal()
	_, _ = s.conn.WriteTo(p, addr)
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
			addr, err := s.parseAddrByUUID(key.(string))
			if err != nil {
				return true
			}
			_, _ = s.conn.WriteTo(pkt, addr)
		}
		return true
	})
}

func (s *Server) directRelay(sign *SignBody, pkt []byte) {
	s.addrToPeer.Range(func(key, value interface{}) bool {
		p := value.(Peer)
		if p.Sign == sign.Sign && p.UUID != sign.UUID {
			addr, _ := net.ResolveUDPAddr("udp", key.(string))
			_, _ = s.conn.WriteTo(pkt, addr)
		}
		return true
	})
}

func (s *Server) addFile(wrq WriteReq) {
	s.idToFiles.Store(wrq.FileId, wrq)
}

func (s *Server) init() {
	if s.Retries == 0 {
		s.Retries = 18
	}

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
	ack := Ack{Block: block}
	pkt, err := ack.Marshal()
	_, err = s.conn.WriteTo(pkt, addr)
	if err != nil {
		log.Printf("[%s] write failed: %v", addr, err)
		return
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

func (s *Server) cacheKey(UUID string, block uint32) string {
	return UUID + "#" + strconv.FormatUint(uint64(block), 16)
}

// dispatch may block by slow connection
func (s *Server) dispatch(addr net.Addr, bytes []byte, sender string, block uint32) {
	var (
		target = addr.String()
		v      any
		ok     bool
	)
	if v, ok = s.addrToPeer.Load(target); !ok {
		return
	}
	var (
		p           = v.(Peer)
		start       time.Time
		code        = OpCode(binary.BigEndian.Uint16(bytes[:2]))
		ctx, cancel = context.WithCancel(context.Background())
		cacheKey    = s.cacheKey(sender, block)
	)

	p.Store(cacheKey, cancel)
	defer p.Delete(cacheKey)
	defer cancel()

	for i := uint8(0); i < s.Retries; i++ {
		if _, ok = s.addrToPeer.Load(target); !ok {
			return
		}
		if i%2 == 0 {
			log.Printf("[%v] send packet: %v to [%v]-[%v]", code.String(), block, p.UUID, target)
			start = time.Now()
			_, _ = s.conn.WriteTo(bytes, addr)
		} else {
			log.Printf("[%v] send check: %v to [%v]-[%v]", code.String(), block, p.UUID, target)
			check := Check{UUID: p.UUID, Block: block}
			pkt, _ := check.Marshal()
			_, _ = s.conn.WriteTo(pkt, addr)
		}
		timer := time.After(*p.Timeout)
		log.Printf("[%v]-[%v] current timeout: %dms", p.UUID, target, *p.Timeout/time.Millisecond)
		select {
		case <-ctx.Done():
			roundTrip := time.Since(start)
			*p.Timeout = *p.Timeout*8/10 + roundTrip*2/10
			return
		case <-timer:
			log.Printf("[%v]-[%v] packet: %v timeout", code.String(), p.UUID, block)
			*p.Timeout += *p.Timeout * 8 / 100
		}
	}
	log.Printf("[%v]-[%v]-[%v] write timeout after %d retries", code.String(), p.UUID, target, s.Retries)
}
