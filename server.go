package whily

import (
	"errors"
	"log"
	"maps"
	"net"
	"sync"
	"syscall"
	"time"
)

type Server struct {
	SignMap  map[string]Sign
	Retries  uint8         // the number of times to retry a failed transmission
	Timeout  time.Duration // the duration to wait for an acknowledgement
	fileMap  map[uint32]WriteReq
	signLock sync.RWMutex
	wrqLock  sync.RWMutex
	audioManager
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
		wrq  WriteReq
		data = Data{}
		nck  = Nck{}
	)
	switch {
	case sign.Unmarshal(pkt) == nil:
		s.signLock.Lock()
		s.remove(sign)
		s.SignMap[addr.String()] = sign
		s.signLock.Unlock()
		s.ack(conn, addr, OpSign, 0)
		log.Printf("[%s] set sign: [%s]", addr.String(), sign)
	case msg.Unmarshal(pkt) == nil:
		go s.handle(msg.Sign, pkt)
		log.Printf("received msg [%s] from [%s]", string(msg.Payload), addr.String())
		s.ack(conn, addr, OpSignedMSG, 0)
	case wrq.Unmarshal(pkt) == nil:
		go s.handle(s.findSignByUUID(wrq.UUID), pkt)
		audioId := s.decodeAudioId(wrq.FileId)
		s.wrqLock.Lock()
		switch wrq.Code {
		case OpAudioCall:
			s.addAudioStream(wrq)
			fallthrough
		case OpAcceptAudioCall:
			s.addAudioReceiver(audioId, wrq)
		case OpEndAudioCall:
			s.deleteAudioReceiver(audioId, wrq.UUID)
			s.cleanupAudioResource(audioId)
		default:
			s.addFile(wrq)
		}
		s.wrqLock.Unlock()
		s.ack(conn, addr, wrq.Code, 0)
	case data.Unmarshal(pkt) == nil:
		if s.isAudio(data.FileId) {
			go s.handleStreamData(conn, data, pkt, addr)
			return
		}
		s.handleFileData(conn, data, pkt, addr, n)
	case nck.Unmarshal(pkt) == nil:
		go s.handleNck(conn, pkt, nck)
	}
}

func (s *Server) handleNck(conn net.PacketConn, pkt []byte, nck Nck) {
	sign := s.findSignByFileId(nck.FileId)
	target := s.findAddrByUUID(sign.UUID)
	targetAddr, _ := net.ResolveUDPAddr("udp", target)
	_, err := conn.WriteTo(pkt, targetAddr)
	if err != nil {
		log.Printf("send ack to [%s] failed: %v", target, err)
	}
}

func (s *Server) remove(sign Sign) {
	maps.DeleteFunc(s.SignMap, func(s string, sn Sign) bool {
		return sn.UUID == sign.UUID
	})
}

func (s *Server) handleStreamData(conn net.PacketConn, data Data, pkt []byte, sender net.Addr) {
	s.signLock.RLock()
	senderSign := s.SignMap[sender.String()]
	s.signLock.RUnlock()
	receivers := s.audioReceiver[s.decodeAudioId(data.FileId)]
	for _, wrq := range receivers {
		if wrq.UUID != senderSign.UUID {
			receiverAddr := s.findAddrByUUID(wrq.UUID)
			go func() {
				udpAddr, _ := net.ResolveUDPAddr("udp", receiverAddr)
				_, _ = conn.WriteTo(pkt, udpAddr)
			}()
		}
	}
}

func (s *Server) handleFileData(conn net.PacketConn, data Data, pkt []byte, sender net.Addr, n int) {
	isLastPacket := n < DatagramSize
	sign := s.findSignByFileId(data.FileId)
	if isLastPacket {
		go s.handle(sign, pkt)
		time.AfterFunc(5*time.Minute, func() {
			s.wrqLock.Lock()
			delete(s.fileMap, data.FileId)
			s.wrqLock.Unlock()
		})
	} else {
		go s.directRelay(conn, sign, pkt)
	}
	s.ack(conn, sender, OpData, data.Block)
}

func (s *Server) directRelay(conn net.PacketConn, sign Sign, pkt []byte) {
	s.signLock.RLock()
	defer s.signLock.RUnlock()
	for addrStr, v := range s.SignMap {
		if v.Sign == sign.Sign && v.UUID != sign.UUID {
			// use goroutine to avoid blocking by slow connection
			go func() {
				udpAddr, _ := net.ResolveUDPAddr("udp", addrStr)
				_, _ = conn.WriteTo(pkt, udpAddr)
			}()
		}
	}
}

func (s *Server) addFile(wrq WriteReq) {
	s.fileMap[wrq.FileId] = wrq
}

func (s *Server) init() {
	s.SignMap = make(map[string]Sign)
	s.fileMap = make(map[uint32]WriteReq)
	s.audioMap = make(map[uint16]WriteReq)
	s.audioReceiver = make(map[uint16][]WriteReq)

	if s.Retries == 0 {
		s.Retries = 3
	}

	if s.Timeout == 0 {
		s.Timeout = 6 * time.Second
	}
}

func (s *Server) findSignByUUID(uuid string) Sign {
	s.signLock.RLock()
	defer s.signLock.RUnlock()
	for _, sign := range s.SignMap {
		if sign.UUID == uuid {
			return sign
		}
	}
	return Sign{UUID: uuid}
}

func (s *Server) findAddrByUUID(uuid string) string {
	s.signLock.RLock()
	defer s.signLock.RUnlock()
	for addr, v := range s.SignMap {
		if v.UUID == uuid {
			return addr
		}
	}
	return ""
}

func (s *Server) findSignByFileId(fileId uint32) Sign {
	s.wrqLock.RLock()
	wrq := s.fileMap[fileId]
	s.wrqLock.RUnlock()
	return s.findSignByUUID(wrq.UUID)
}

func (s *Server) ack(conn net.PacketConn, clientAddr net.Addr, code OpCode, block uint32) {
	ack := Ack{SrcOp: code, Block: block}
	pkt, err := ack.Marshal()
	_, err = conn.WriteTo(pkt, clientAddr)
	if err != nil {
		log.Printf("[%s] write failed: %v", clientAddr, err)
		return
	}
}

func (s *Server) handle(sign Sign, bytes []byte) {
	s.signLock.RLock()
	defer s.signLock.RUnlock()
	for addr, v := range s.SignMap {
		if v.Sign == sign.Sign && v.UUID != sign.UUID {
			// use goroutine to avoid blocking by slow connection
			go s.connectAndDispatch(addr, bytes)
		}
	}
}

func (s *Server) connectAndDispatch(addr string, bytes []byte) {
	udpAddr, _ := net.ResolveUDPAddr("udp", addr)
	conn, err := net.Dial("udp", addr)
	if err != nil {
		log.Printf("[%s] dial failed: %v", addr, err)
		s.signLock.Lock()
		delete(s.SignMap, addr)
		s.signLock.Unlock()
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
				s.signLock.Lock()
				delete(s.SignMap, clientAddr.String())
				s.signLock.Unlock()
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
