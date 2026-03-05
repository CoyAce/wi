package wi

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

type Peer struct {
	*SignBody     // identity
	*sync.Map     // block -> CancelFunc
	*RangeTracker // block tracker
	Timeout       *time.Duration
}

func (p Peer) ack(block uint32) {
	cancel, ok := p.Load(block)
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
	h.Add(MonoRange(block))
	h.Store(block, packet)
}

type Server struct {
	Status  chan struct{} `json:"-"` // initialization status
	Retries uint8         // the number of times to retry a failed transmission
	fileMap sync.Map      // fileId -> wrq
	pubMap  sync.Map      // published files, FilePair -> *sync.Map { subscriber -> ReadReq }
	addrMap sync.Map      // addr -> Peer
	uuidMap sync.Map      // uuid -> addr
	history sync.Map      // sign -> *sync.Map { uuid -> *history }
	*audioManager
	conn net.PacketConn
}

func (s *Server) Ready() {
	if s.Status != nil {
		<-s.Status
	}
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
	case OpSign:
		sign := new(SignReq)
		if sign.Unmarshal(pkt) != nil {
			return
		}
		s.ack(addr, sign.Block)
		if s.dup(addr.String(), sign.Block) {
			return
		}
		p := s.updatePeer(&sign.SignBody, addr)
		// addr change, remove invalid addr
		if a, ok := s.uuidMap.Load(sign.UUID); ok && a != addr.String() {
			s.removeByUUID(sign.UUID)
		}
		s.uuidMap.Store(sign.UUID, addr.String())
		s.addrMap.Store(addr.String(), p)
		log.Printf("[%s] set sign: [%v]", addr.String(), sign)
	case OpSignOut:
		s.removeByAddr(addr.String())
	case OpAck:
		var block uint32
		if binary.Read(b, binary.BigEndian, &block) != nil {
			return
		}
		log.Printf("ack received: %v", block)
		if p, ok := s.addrMap.Load(addr.String()); ok {
			p.(Peer).ack(block)
		} else {
			log.Printf("unknown ack: %v, addr: %v", block, addr.String())
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
		if binary.Read(b, binary.BigEndian, &fileId) != nil {
			return
		}
		sn, ok := s.findSignByFileId(fileId)
		if !ok {
			return
		}
		if s.userNotExist(sn.UUID) {
			s.reject(addr)
			return
		}
		s.ack(addr, block)
		if s.dup(addr.String(), block) {
			return
		}
		go s.dispatch(s.findAddrByUUID(sn.UUID), pkt, block)
	case OpSignedMSG:
		msg := new(SignedMessage)
		if msg.Unmarshal(pkt) != nil {
			return
		}
		if s.userNotExist(msg.UUID) {
			s.reject(addr)
			return
		}
		s.ack(addr, msg.Block)
		if s.dup(addr.String(), msg.Block) {
			return
		}
		s.track(&msg.SignBody, msg.Block, pkt)
		s.handle(&msg.SignBody, msg.Block, pkt)
		log.Printf("received msg [%s] from [%s]", string(msg.Payload), addr.String())
	case OpData:
		var fileId uint32
		if binary.Read(b, binary.BigEndian, &fileId) != nil {
			return
		}
		if s.isAudio(fileId) {
			s.handleStreamData(fileId, pkt, addr)
			return
		}
		if w, ok := s.isContent(fileId); ok {
			s.relayToSubscribers(*w, pkt)
			return
		}
		sign, ok := s.findSignByFileId(fileId)
		if !ok {
			return
		}
		s.directRelay(sign, pkt)
	default:
		var (
			rrq  ReadReq
			wrq  WriteReq
			ctrl CtrlReq
		)
		switch {
		case wrq.Unmarshal(pkt) == nil:
			if s.userNotExist(wrq.UUID) {
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
				s.pubMap.Store(pair, &sync.Map{})
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
				s.track(sign, wrq.Block, pkt)
			default:
			}
			s.handle(sign, wrq.Block, pkt)
		case rrq.Unmarshal(pkt) == nil:
			if s.userNotExist(rrq.Subscriber) {
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
			if s.userNotExist(ctrl.UUID) {
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
				s.track(sign, ctrl.Block, pkt)
				s.handle(sign, ctrl.Block, pkt)
			default:
			}
		}
	}
}

func (s *Server) track(sign *SignBody, block uint32, pkt []byte) {
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

func (s *Server) updatePeer(sign *SignBody, addr net.Addr) Peer {
	timeout := 500 * time.Millisecond
	p := Peer{SignBody: sign, Map: new(sync.Map), RangeTracker: new(RangeTracker), Timeout: &timeout}
	if v, ok := s.addrMap.Load(addr.String()); ok {
		p = v.(Peer)
		p.SignBody = sign
	}
	return p
}

func (s *Server) isContent(fileId uint32) (*WriteReq, bool) {
	wrq, ok := s.fileMap.Load(fileId)
	if !ok {
		return nil, false
	}
	w := wrq.(WriteReq)
	return &w, w.Code == OpContent
}

func (s *Server) relayToSubscribers(wrq WriteReq, pkt []byte) {
	subs, ok := s.pubMap.Load(FilePair{FileId: wrq.FileId, UUID: wrq.UUID})
	if !ok {
		return
	}
	subs.(*sync.Map).Range(func(k, v interface{}) bool {
		go func() {
			_, addr := s.findTarget(k.(string))
			_, _ = s.conn.WriteTo(pkt, addr)
		}()
		return true
	})
}

func (s *Server) dispatchToSubscribers(wrq WriteReq, pkt []byte) {
	subs, ok := s.pubMap.Load(FilePair{FileId: wrq.FileId, UUID: wrq.UUID})
	if !ok {
		return
	}
	subs.(*sync.Map).Range(func(k, v interface{}) bool {
		go s.dispatch(s.findAddrByUUID(k.(string)), pkt, wrq.Block)
		return true
	})
}

func (s *Server) dispatchToPublisher(pkt []byte, rrq ReadReq) {
	pair := FilePair{FileId: rrq.FileId, UUID: rrq.Publisher}
	subs, ok := s.pubMap.Load(pair)
	if !ok {
		subs = &sync.Map{}
		s.pubMap.Store(pair, subs)
	}
	subs.(*sync.Map).Store(rrq.Subscriber, rrq)
	go s.dispatch(s.findAddrByUUID(rrq.Publisher), pkt, rrq.Block)
}

func (s *Server) deleteSub(rrq ReadReq) {
	subs, ok := s.pubMap.Load(FilePair{FileId: rrq.FileId, UUID: rrq.Publisher})
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

func (s *Server) findTarget(UUID string) (string, *net.UDPAddr) {
	target := s.findAddrByUUID(UUID)
	addr, _ := net.ResolveUDPAddr("udp", target)
	return target, addr
}

func (s *Server) removeByUUID(UUID string) {
	addr, ok := s.uuidMap.Load(UUID)
	if ok {
		s.addrMap.Delete(addr)
		s.uuidMap.Delete(UUID)
	}
}

func (s *Server) removeByAddr(addr string) {
	p, ok := s.addrMap.Load(addr)
	if ok {
		s.addrMap.Delete(addr)
		s.uuidMap.Delete(p.(Peer).UUID)
	}
}

func (s *Server) handleStreamData(fileId uint32, pkt []byte, sender net.Addr) {
	p, ok := s.addrMap.Load(sender.String())
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
			go func() {
				_, addr := s.findTarget(key.(string))
				_, _ = s.conn.WriteTo(pkt, addr)
			}()
		}
		return true
	})
}

func (s *Server) directRelay(sign *SignBody, pkt []byte) {
	s.addrMap.Range(func(key, value interface{}) bool {
		p := value.(Peer)
		if p.Sign == sign.Sign && p.UUID != sign.UUID {
			go func() {
				addr, _ := net.ResolveUDPAddr("udp", key.(string))
				_, _ = s.conn.WriteTo(pkt, addr)
			}()
		}
		return true
	})
}

func (s *Server) addFile(wrq WriteReq) {
	s.fileMap.Store(wrq.FileId, wrq)
}

func (s *Server) init() {
	if s.Retries == 0 {
		s.Retries = 18
	}

	s.audioManager = &audioManager{}
}

func (s *Server) findSignByUUID(uuid string) (*SignBody, bool) {
	addr, ok := s.uuidMap.Load(uuid)
	if !ok {
		return nil, false
	}
	p, ok := s.addrMap.Load(addr)
	if !ok {
		return nil, false
	}
	return p.(Peer).SignBody, true
}

func (s *Server) findAddrByUUID(uuid string) string {
	addr, ok := s.uuidMap.Load(uuid)
	if ok {
		return addr.(string)
	}
	return ""
}

func (s *Server) findSignByFileId(fileId uint32) (*SignBody, bool) {
	wrq, ok := s.fileMap.Load(fileId)
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
	v, ok := s.addrMap.Load(addr)
	if !ok {
		return false
	}
	p := v.(Peer)
	r := MonoRange(block)
	dup := p.Contains(r)
	if !dup {
		p.Add(r)
	}
	return dup
}

func (s *Server) check(addr string, block uint32) bool {
	v, ok := s.addrMap.Load(addr)
	if !ok {
		return false
	}
	return v.(Peer).Contains(MonoRange(block))
}

func (s *Server) handle(sign *SignBody, block uint32, bytes []byte) {
	s.addrMap.Range(func(key, value interface{}) bool {
		p := value.(Peer)
		if p.Sign == sign.Sign && p.UUID != sign.UUID {
			// use goroutine to avoid blocking by slow connection
			go s.dispatch(key.(string), bytes, block)
		}
		return true
	})
}

func (s *Server) userNotExist(UUID string) bool {
	_, ok := s.uuidMap.Load(UUID)
	return !ok
}

// dispatch may block by slow connection
func (s *Server) dispatch(addr string, bytes []byte, block uint32) {
	v, ok := s.addrMap.Load(addr)
	if !ok {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := v.(Peer)
	p.Store(block, cancel)
	defer p.Delete(block)
	defer cancel()

	udpAddr, _ := net.ResolveUDPAddr("udp", addr)
	var start time.Time
	for i := uint8(0); i < s.Retries; i++ {
		if i%2 == 0 {
			log.Printf("send packet: %v to %v", block, addr)
			start = time.Now()
			_, _ = s.conn.WriteTo(bytes, udpAddr)
		} else {
			log.Printf("send check: %v to %v", block, addr)
			check := Check{UUID: p.UUID, Block: block}
			pkt, _ := check.Marshal()
			_, _ = s.conn.WriteTo(pkt, udpAddr)
		}
		timer := time.After(*p.Timeout)
		log.Printf("[%v] current timeout: %v", addr, p.Timeout)
		select {
		case <-ctx.Done():
			roundTrip := time.Since(start)
			*p.Timeout = *p.Timeout*8/10 + roundTrip*2/10
			return
		case <-timer:
			*p.Timeout += *p.Timeout * 8 / 100
		}
	}
	s.removeByAddr(addr)
	log.Printf("[%s] write timeout after %d retries", addr, s.Retries)
}
