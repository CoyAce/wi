package wi

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

type Server struct {
	Retries uint8         // the number of times to retry a failed transmission
	Timeout time.Duration // the duration to wait for an acknowledgement
	fileMap sync.Map      // fileId -> wrq
	pubMap  sync.Map      // published files, FilePair -> *sync.Map { subscriber -> ReadReq }
	addrMap sync.Map      // addr -> Sign
	uuidMap sync.Map      // uuid -> addr
	ackMap  sync.Map      // addr -> *sync.Map { block -> CancelFunc }
	*audioManager
	conn net.PacketConn
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
		unsign = OpSignOut
	)
	switch {
	case ack.Unmarshal(pkt) == nil:
		log.Printf("ack received, block: %v", ack.Block)
		m, ok := s.ackMap.Load(addr.String())
		if !ok {
			return
		}
		cancel, ok := m.(*sync.Map).Load(ack.Block)
		if !ok {
			return
		}
		cancel.(context.CancelFunc)()
	case msg.Unmarshal(pkt) == nil:
		if s.userNotExist(msg.Sign.UUID) {
			s.reject(addr)
			return
		}
		s.ack(addr, msg.Block)
		s.handle(msg.Sign, pkt, msg.Block)
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
		s.ack(addr, 0)
		if s.sameUser(sign.UUID, addr.String()) {
			return
		}
		s.uuidMap.Store(sign.UUID, addr.String())
		s.addrMap.Store(addr.String(), sign)
		s.ackMap.Store(addr.String(), &sync.Map{})
		log.Printf("[%s] set sign: [%s]", addr.String(), sign)
	case wrq.Unmarshal(pkt) == nil:
		if s.userNotExist(wrq.UUID) {
			s.reject(addr)
			return
		}
		s.ack(addr, wrq.Block)
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
			_, ok := s.pubMap.Load(pair)
			if !ok {
				s.pubMap.Store(pair, &sync.Map{})
			}
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
	log.Printf("reject %v", addr.String())
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
		s.ackMap.Delete(addr)
	}
}

func (s *Server) removeByAddr(addr string) {
	sign, ok := s.addrMap.Load(addr)
	if ok {
		s.addrMap.Delete(addr)
		s.uuidMap.Delete(sign.(Sign).UUID)
	}
}

func (s *Server) handleStreamData(data Data, pkt []byte, sender net.Addr) {
	senderSign, ok := s.addrMap.Load(sender.String())
	if !ok {
		return
	}
	UUID := senderSign.(Sign).UUID
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

func (s *Server) directRelay(sign Sign, pkt []byte) {
	s.addrMap.Range(func(key, value interface{}) bool {
		if value.(Sign).Sign == sign.Sign && value.(Sign).UUID != sign.UUID {
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
		s.Retries = 9
	}

	if s.Timeout == 0 {
		s.Timeout = 6 * time.Second
	}

	s.audioManager = &audioManager{}
}

func (s *Server) findSignByUUID(uuid string) Sign {
	addr, ok := s.uuidMap.Load(uuid)
	if !ok {
		return Sign{UUID: uuid}
	}
	sign, ok := s.addrMap.Load(addr)
	if !ok {
		return Sign{UUID: uuid}
	}
	return sign.(Sign)
}

func (s *Server) findAddrByUUID(uuid string) string {
	addr, ok := s.uuidMap.Load(uuid)
	if ok {
		return addr.(string)
	}
	return ""
}

func (s *Server) findSignByFileId(fileId uint32) Sign {
	wrq, ok := s.fileMap.Load(fileId)
	if !ok {
		return Sign{}
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

func (s *Server) handle(sign Sign, bytes []byte, block uint32) {
	s.addrMap.Range(func(key, value interface{}) bool {
		if value.(Sign).Sign == sign.Sign && value.(Sign).UUID != sign.UUID {
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
	ack, ok := s.ackMap.Load(addr)
	if !ok {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	ack.(*sync.Map).Store(block, cancel)
	defer ack.(*sync.Map).Delete(block)
	defer cancel()

	udpAddr, _ := net.ResolveUDPAddr("udp", addr)
	for i := uint8(0); i < s.Retries; i++ {
		_, _ = s.conn.WriteTo(bytes, udpAddr)
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.Timeout):
		}
	}
	sign, _ := s.addrMap.Load(addr)
	log.Printf("[%s] write timeout after %d retries, type %v, block %v sign %v", addr, s.Retries, bytes[:2], block, sign)
}
