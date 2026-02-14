package wi

import (
	"errors"
	"log"
	"net"
	"sync"
	"syscall"
	"time"
)

type Server struct {
	Retries uint8         // the number of times to retry a failed transmission
	Timeout time.Duration // the duration to wait for an acknowledgement
	fileMap sync.Map      // fileId -> wrq
	pubMap  sync.Map      // published files, FilePair -> *sync.Map { subscriber -> ReadReq }
	addrMap sync.Map      // addr -> sign
	uuidMap sync.Map      // uuid -> addr
	*audioManager
}

func (s *Server) ListenAndServe(addr string) error {
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	log.Printf("Listening on %s ...\n", conn.LocalAddr())
	return s.Serve(conn)
}

func (s *Server) Serve(conn net.PacketConn) error {
	if conn == nil {
		return errors.New("nil connection")
	}

	s.init()

	for {
		buf := make([]byte, DatagramSize)
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			return err
		}

		pkt := buf[:n]
		go s.relay(conn, pkt, addr, n)
	}
}

func (s *Server) relay(conn net.PacketConn, pkt []byte, addr net.Addr, n int) {
	var (
		sign Sign
		msg  SignedMessage
		rrq  ReadReq
		wrq  WriteReq
		data = Data{}
		nck  = Nck{}
		oso  = OpSignOut
	)
	switch {
	case msg.Unmarshal(pkt) == nil:
		exist := s.checkUser(msg.Sign)
		if !exist {
			s.reject(conn, addr)
			return
		}
		s.ack(conn, addr, 0)
		s.handle(msg.Sign, pkt)
		log.Printf("received msg [%s] from [%s]", string(msg.Payload), addr.String())
	case data.Unmarshal(pkt) == nil:
		if s.isAudio(data.FileId) {
			s.handleStreamData(conn, data, pkt, addr)
			return
		}
		if w, ok := s.isContent(data.FileId); ok {
			s.relayToSubscribers(conn, data, *w, pkt, addr, n)
			return
		}
		s.handleFileData(conn, data, pkt, addr, n)
	case sign.Unmarshal(pkt) == nil:
		s.ack(conn, addr, 0)
		s.removeByUUID(sign.UUID)
		s.uuidMap.Store(sign.UUID, addr.String())
		s.addrMap.Store(addr.String(), sign)
		log.Printf("[%s] set sign: [%s]", addr.String(), sign)
	case wrq.Unmarshal(pkt) == nil:
		s.ack(conn, addr, 0)
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
		s.handle(s.findSignByUUID(wrq.UUID), pkt)
	case rrq.Unmarshal(pkt) == nil:
		s.ack(conn, addr, 0)
		switch rrq.Code {
		case OpSubscribe:
			s.dispatchToPublisher(pkt, rrq)
		case OpUnsubscribe:
			s.deleteSub(rrq)
		default:
		}
	case nck.Unmarshal(pkt) == nil:
		s.ack(conn, addr, 0)
		s.handleNck(pkt, nck)
	case oso.Unmarshal(pkt) == nil:
		s.removeByAddr(addr.String())
	}
}

func (s *Server) isContent(fileId uint32) (*WriteReq, bool) {
	wrq, ok := s.fileMap.Load(fileId)
	if !ok {
		return nil, false
	}
	w := wrq.(WriteReq)
	return &w, w.Code == OpContent
}

func (s *Server) relayToSubscribers(conn net.PacketConn, data Data, wrq WriteReq, pkt []byte, sender net.Addr, n int) {
	if s.isFinalPacket(n) {
		s.ack(conn, sender, data.Block)
	}
	subs, ok := s.pubMap.Load(FilePair{FileId: wrq.FileId, UUID: wrq.UUID})
	if !ok {
		return
	}
	subs.(*sync.Map).Range(func(k, v interface{}) bool {
		go func() {
			_, addr := s.findTarget(k.(string))
			_, _ = conn.WriteTo(pkt, addr)
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
		go s.connectAndDispatch(s.findAddrByUUID(k.(string)), pkt)
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
	go s.connectAndDispatch(s.findAddrByUUID(rrq.Publisher), pkt)
}

func (s *Server) deleteSub(rrq ReadReq) {
	subs, ok := s.pubMap.Load(FilePair{FileId: rrq.FileId, UUID: rrq.Publisher})
	if !ok {
		return
	}
	subs.(*sync.Map).Delete(rrq.Subscriber)
}

func (s *Server) reject(conn net.PacketConn, addr net.Addr) {
	err := ErrUnknownUser
	p, _ := err.Marshal()
	_, _ = conn.WriteTo(p, addr)
}

func (s *Server) handleNck(pkt []byte, nck Nck) {
	sign := s.findSignByFileId(nck.FileId)
	go s.connectAndDispatch(s.findAddrByUUID(sign.UUID), pkt)
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
	sign, ok := s.addrMap.Load(addr)
	if ok {
		s.addrMap.Delete(addr)
		s.uuidMap.Delete(sign.(Sign).UUID)
	}
}

func (s *Server) handleStreamData(conn net.PacketConn, data Data, pkt []byte, sender net.Addr) {
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
				_, _ = conn.WriteTo(pkt, addr)
			}()
		}
		return true
	})
}

func (s *Server) handleFileData(conn net.PacketConn, data Data, pkt []byte, sender net.Addr, n int) {
	sign := s.findSignByFileId(data.FileId)
	if s.isFinalPacket(n) {
		s.ack(conn, sender, data.Block)
		s.handle(sign, pkt)
		time.AfterFunc(5*time.Minute, func() {
			s.fileMap.Delete(data.FileId)
		})
	} else {
		s.directRelay(conn, sign, pkt)
	}
}

func (s *Server) isFinalPacket(n int) bool {
	return n < DatagramSize
}

func (s *Server) directRelay(conn net.PacketConn, sign Sign, pkt []byte) {
	s.addrMap.Range(func(key, value interface{}) bool {
		if value.(Sign).Sign == sign.Sign && value.(Sign).UUID != sign.UUID {
			go func() {
				addr, _ := net.ResolveUDPAddr("udp", key.(string))
				_, _ = conn.WriteTo(pkt, addr)
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

func (s *Server) ack(conn net.PacketConn, clientAddr net.Addr, block uint32) {
	ack := Ack{Block: block}
	pkt, err := ack.Marshal()
	_, err = conn.WriteTo(pkt, clientAddr)
	if err != nil {
		log.Printf("[%s] write failed: %v", clientAddr, err)
		return
	}
}

func (s *Server) handle(sign Sign, bytes []byte) {
	s.addrMap.Range(func(key, value interface{}) bool {
		if value.(Sign).Sign == sign.Sign && value.(Sign).UUID != sign.UUID {
			// use goroutine to avoid blocking by slow connection
			go s.connectAndDispatch(key.(string), bytes)
		}
		return true
	})
}

func (s *Server) checkUser(sign Sign) bool {
	_, ok := s.uuidMap.Load(sign.UUID)
	return ok
}

// connectAndDispatch may block by slow connection
func (s *Server) connectAndDispatch(addr string, bytes []byte) {
	udpAddr, _ := net.ResolveUDPAddr("udp", addr)
	conn, err := net.Dial("udp", addr)
	if err != nil {
		log.Printf("[%s] dial failed: %v", addr, err)
		if errors.Is(err, syscall.ECONNREFUSED) {
			s.removeByAddr(addr)
		}
		return
	}
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()

	s.dispatch(udpAddr, conn, bytes)
}

func (s *Server) dispatch(clientAddr net.Addr, conn net.Conn, bytes []byte) {
	var (
		ackPkt Ack
	)
	buf := make([]byte, DatagramSize)
RETRY:
	for i := s.Retries; i > 0; i-- {
		_, err := conn.Write(bytes)
		if err != nil {
			log.Printf("[%s] write failed: %v", clientAddr, err)
			return
		}

		// wait for the client's ACK packet
		_ = conn.SetReadDeadline(time.Now().Add(s.Timeout))
		_, err = conn.Read(buf)

		if err != nil {
			var nErr net.Error
			if errors.As(err, &nErr) && nErr.Timeout() {
				continue RETRY
			}
			if errors.Is(err, syscall.ECONNREFUSED) {
				s.removeByAddr(clientAddr.String())
				log.Printf("[%s] connection refused", clientAddr)
			}
			log.Printf("[%s] waiting for ACK: %v", clientAddr, err)
			return
		}

		switch {
		case ackPkt.Unmarshal(buf) == nil:
			return
		default:
			log.Printf("[%s] bad packet", clientAddr)
		}
	}
	log.Printf("[%s] exhausted retries", clientAddr)
	return
}
