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
	)
	switch {
	case msg.Unmarshal(pkt) == nil:
		exist := s.checkUser(msg.Sign)
		if !exist {
			s.reject(conn, addr)
			return
		}
		go s.handle(msg.Sign, pkt)
		log.Printf("received msg [%s] from [%s]", string(msg.Payload), addr.String())
		s.ack(conn, addr, 0)
	case data.Unmarshal(pkt) == nil:
		if s.isAudio(data.FileId) {
			go s.handleStreamData(conn, data, pkt, addr)
			return
		}
		s.handleFileData(conn, data, pkt, addr, n)
	case sign.Unmarshal(pkt) == nil:
		s.removeByUUID(sign.UUID)
		s.uuidMap.Store(sign.UUID, addr.String())
		s.addrMap.Store(addr.String(), sign)
		s.ack(conn, addr, 0)
		log.Printf("[%s] set sign: [%s]", addr.String(), sign)
	case wrq.Unmarshal(pkt) == nil:
		go s.handle(s.findSignByUUID(wrq.UUID), pkt)
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
			s.pubMap.Store(FilePair{FileId: wrq.FileId, UUID: wrq.UUID}, &sync.Map{})
		default:
			s.addFile(wrq)
		}
		s.ack(conn, addr, 0)
	case rrq.Unmarshal(pkt) == nil:
		switch rrq.Code {
		case OpSubscribe:
			go s.handleSub(conn, pkt, rrq)
		case OpUnsubscribe:
			go s.deleteSub(rrq)
		default:
		}
		s.ack(conn, addr, 0)
	case nck.Unmarshal(pkt) == nil:
		go s.handleNck(conn, pkt, nck)
	}
}

func (s *Server) reject(conn net.PacketConn, addr net.Addr) {
	err := ErrUnknownUser
	p, _ := err.Marshal()
	_, _ = conn.WriteTo(p, addr)
}

func (s *Server) handleSub(conn net.PacketConn, pkt []byte, rrq ReadReq) {
	pair := FilePair{FileId: rrq.FileId, UUID: rrq.Publisher}
	subs, ok := s.pubMap.Load(pair)
	if ok {
		_, target := s.findTarget(rrq.Publisher)
		_, err := conn.WriteTo(pkt, target)
		if err != nil {
			log.Printf("Send rrq to [%s} failed: %v", rrq.Publisher, err)
		}
		subs.(*sync.Map).Store(rrq.Subscriber, rrq)
	}
}

func (s *Server) deleteSub(rrq ReadReq) {
	subs, ok := s.pubMap.Load(FilePair{FileId: rrq.FileId, UUID: rrq.Publisher})
	if !ok {
		return
	}
	subs.(*sync.Map).Delete(rrq.Subscriber)
}

func (s *Server) handleNck(conn net.PacketConn, pkt []byte, nck Nck) {
	sign := s.findSignByFileId(nck.FileId)
	target, addr := s.findTarget(sign.UUID)
	_, err := conn.WriteTo(pkt, addr)
	if err != nil {
		log.Printf("Send ack to [%s] failed: %v", target, err)
	}
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
	isLastPacket := n < DatagramSize
	sign := s.findSignByFileId(data.FileId)
	if isLastPacket {
		go s.handle(sign, pkt)
		time.AfterFunc(5*time.Minute, func() {
			s.fileMap.Delete(data.FileId)
		})
	} else {
		go s.directRelay(conn, sign, pkt)
	}
	s.ack(conn, sender, data.Block)
}

func (s *Server) directRelay(conn net.PacketConn, sign Sign, pkt []byte) {
	s.addrMap.Range(func(key, value interface{}) bool {
		if value.(Sign).Sign == sign.Sign && value.(Sign).UUID != sign.UUID {
			// use goroutine to avoid blocking by slow connection
			go func() {
				udpAddr, _ := net.ResolveUDPAddr("udp", key.(string))
				_, _ = conn.WriteTo(pkt, udpAddr)
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
		s.Retries = 3
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

func (s *Server) connectAndDispatch(addr string, bytes []byte) {
	udpAddr, _ := net.ResolveUDPAddr("udp", addr)
	conn, err := net.Dial("udp", addr)
	if err != nil {
		log.Printf("[%s] dial failed: %v", addr, err)
		s.removeByAddr(addr)
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
