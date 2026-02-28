package wi

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

type Peer struct {
	*Sign         // identity
	*sync.Map     // block -> CancelFunc
	*RangeTracker // block tracker
}

type Server struct {
	Status  chan struct{} `json:"-"` // initialization status
	Retries uint8         // the number of times to retry a failed transmission
	Timeout time.Duration // the duration to wait for an acknowledgement
	fileMap sync.Map      // fileId -> wrq
	pubMap  sync.Map      // published files, FilePair -> *sync.Map { subscriber -> ReadReq }
	addrMap sync.Map      // addr -> Peer
	uuidMap sync.Map      // uuid -> addr
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
		sign   Sign
		msg    SignedMessage
		rrq    ReadReq
		wrq    WriteReq
		data   = Data{}
		ack    = Ack{}
		nck    = Nck{}
		check  = Check{}
		unsign = OpSignOut
	)
	switch {
	case ack.Unmarshal(pkt) == nil:
		log.Printf("ack received: %v", ack.Block)
		p, ok := s.addrMap.Load(addr.String())
		if !ok {
			log.Printf("unknown ack: %v, addr: %v", ack.Block, addr.String())
			return
		}
		cancel, ok := p.(Peer).Load(ack.Block)
		if !ok {
			return
		}
		cancel.(context.CancelFunc)()
	case check.Unmarshal(pkt) == nil:
		if s.check(addr.String(), check.Block) {
			s.ack(addr, msg.Block)
		}
	case msg.Unmarshal(pkt) == nil:
		if s.userNotExist(msg.Sign.UUID) {
			s.reject(addr)
			return
		}
		s.ack(addr, msg.Block)
		if s.dup(addr.String(), msg.Block) {
			return
		}
		s.handle(&msg.Sign, pkt, msg.Block)
		log.Printf("received msg [%s] from [%s]", string(msg.Payload), addr.String())
	case data.Unmarshal(pkt) == nil:
		if s.isAudio(data.FileId) {
			s.handleStreamData(data, pkt, addr)
			return
		}
		if w, ok := s.isContent(data.FileId); ok {
			s.relayToSubscribers(*w, pkt)
			return
		}
		s.handleFileData(data, pkt)
	case sign.Unmarshal(pkt) == nil:
		s.ack(addr, sign.Block)
		if s.dup(addr.String(), msg.Block) || s.sameUser(sign.UUID, addr.String()) {
			return
		}
		// addr change, remove invalid addr
		s.removeByUUID(sign.UUID)
		s.uuidMap.Store(sign.UUID, addr.String())
		s.addrMap.Store(addr.String(), Peer{Sign: &sign, Map: new(sync.Map), RangeTracker: new(RangeTracker)})
		log.Printf("[%s] set sign: [%v]", addr.String(), sign)
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
		s.handle(s.findSignByUUID(wrq.UUID), pkt, wrq.Block)
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
		case OpSubscribe:
			s.dispatchToPublisher(pkt, rrq)
		case OpUnsubscribe:
			s.deleteSub(rrq)
		default:
		}
	case nck.Unmarshal(pkt) == nil:
		sn := s.findSignByFileId(nck.FileId)
		if s.userNotExist(sn.UUID) {
			s.reject(addr)
			return
		}
		s.ack(addr, nck.Block)
		if s.dup(addr.String(), nck.Block) {
			return
		}
		go s.dispatch(s.findAddrByUUID(sn.UUID), pkt, nck.Block)
	case unsign.Unmarshal(pkt) == nil:
		s.removeByAddr(addr.String())
	}
}

func (s *Server) sameUser(UUID string, addr string) bool {
	return addr == s.findAddrByUUID(UUID)
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

func (s *Server) handleStreamData(data Data, pkt []byte, sender net.Addr) {
	p, ok := s.addrMap.Load(sender.String())
	if !ok {
		return
	}
	UUID := p.(Peer).UUID
	receivers, ok := s.audioReceiver.Load(s.decodeAudioId(data.FileId))
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

func (s *Server) handleFileData(data Data, pkt []byte) {
	sign := s.findSignByFileId(data.FileId)
	s.directRelay(sign, pkt)
}

func (s *Server) isFinalPacket(n int) bool {
	return n < DatagramSize
}

func (s *Server) directRelay(sign *Sign, pkt []byte) {
	s.addrMap.Range(func(key, value interface{}) bool {
		p := value.(Peer)
		if p.Sign.Sign == sign.Sign && p.UUID != sign.UUID {
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

	if s.Timeout == 0 {
		s.Timeout = 500 * time.Millisecond
	}

	s.audioManager = &audioManager{}
}

func (s *Server) findSignByUUID(uuid string) *Sign {
	addr, ok := s.uuidMap.Load(uuid)
	if !ok {
		return &Sign{UUID: uuid}
	}
	p, ok := s.addrMap.Load(addr)
	if !ok {
		return &Sign{UUID: uuid}
	}
	return p.(Peer).Sign
}

func (s *Server) findAddrByUUID(uuid string) string {
	addr, ok := s.uuidMap.Load(uuid)
	if ok {
		return addr.(string)
	}
	return ""
}

func (s *Server) findSignByFileId(fileId uint32) *Sign {
	wrq, ok := s.fileMap.Load(fileId)
	if !ok {
		return &Sign{}
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

func (s *Server) handle(sign *Sign, bytes []byte, block uint32) {
	s.addrMap.Range(func(key, value interface{}) bool {
		p := value.(Peer)
		if p.Sign.Sign == sign.Sign && p.UUID != sign.UUID {
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
		timer := time.After(s.Timeout)
		log.Printf("timeout: %v", s.Timeout)
		select {
		case <-ctx.Done():
			elapsed := time.Since(start)
			s.Timeout = s.Timeout*8/10 + elapsed*2/10
			return
		case <-timer:
			s.Timeout += s.Timeout * 8 / 100
		}
	}
	log.Printf("[%s] write timeout after %d retries", addr, s.Retries)
}
