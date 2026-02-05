package wi

import (
	"bytes"
	"errors"
	"image"
	"image/gif"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

type file struct {
	req  WriteReq
	data []Data
	rt   *RangeTracker
}

type FileWriter struct {
	FileId       chan uint32   `json:"-"` // finished file id
	Wrq          chan WriteReq `json:"-"` // file request
	FileData     chan Data     `json:"-"` // file data
	dataDir      string
	fileMessages chan<- WriteReq  // notify file complete, receiver could refresh icon or update status
	files        map[uint32]*file // internal file info
	nck          func(f file)     // request lost packet
}

func (f *FileWriter) Loop() {
	for {
		select {
		// last block received, complete file transfer
		case id := <-f.FileId:
			f.tryComplete(id)
		case req := <-f.Wrq:
			f.init(req)
		case data := <-f.FileData:
			if !f.isFile(data.FileId) {
				continue
			}
			f.tryWrite(data)
		}
	}
}

func (f *FileWriter) tryWrite(data Data) {
	fd := f.files[data.FileId]
	req := fd.req
	fd.data = append(fd.data, data)
	if f.received100kb(fd) {
		f.flush(fd, f.getPath(req.UUID, req.Filename))
		f.tryNck(*fd)
	}
	if f.receivedNckRequestedPackets(data, *fd) {
		f.flush(fd, f.getPath(req.UUID, req.Filename))
	}
}

func (f *FileWriter) tryNck(fd file) {
	if fd.rt.isCompleted() {
		return
	}
	f.nck(fd)
}

func (f *FileWriter) receivedNckRequestedPackets(data Data, fd file) bool {
	return data.Block < fd.rt.nextBlock()
}

func (f *FileWriter) received100kb(fd *file) bool {
	return len(fd.data) >= 100*1024/BlockSize
}

func (f *FileWriter) init(req WriteReq) {
	f.files[req.FileId] = &file{req: req, rt: &RangeTracker{}}
	// remove before append
	RemoveFile(f.getPath(req.UUID, req.Filename))
}

func (f *FileWriter) getDir(uuid string) string {
	if uuid == "" {
		return f.dataDir + "/default"
	}
	return f.dataDir + "/" + strings.Replace(uuid, "#", "_", -1)
}

func (f *FileWriter) getPath(uuid string, filename string) string {
	return f.getDir(uuid) + "/" + filename
}

func (f *FileWriter) tryComplete(id uint32) {
	fd := f.files[id]
	if fd == nil {
		return
	}
	req := fd.req
	filePath := f.getPath(req.UUID, req.Filename)
	f.flush(fd, filePath)
	if fd.rt.isCompleted() {
		f.clean(id)
		f.fileMessages <- req
	} else {
		f.nck(*fd)
		f.tryCompleteIn1Second(id)
	}
}

func (f *FileWriter) tryCompleteIn1Second(id uint32) {
	time.AfterFunc(1*time.Second, func() {
		f.tryComplete(id)
	})
}

func (f *FileWriter) flush(fd *file, filePath string) {
	d := fd.data
	r := Range{}
	for d != nil {
		d, r = write(filePath, d)
		fd.rt.Add(r)
	}
	fd.data = fd.data[:0]
}

func (f *FileWriter) clean(id uint32) {
	delete(f.files, id)
}

func (f *FileWriter) isFile(fileId uint32) bool {
	fd := f.files[fileId]
	return fd != nil && fd.req.FileId == fileId
}

func writeTo(filePath string, data []Data) {
	// os.O_CREATE: 如果文件不存在，则创建文件
	// os.O_WRONLY: 以只写模式打开文件
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("error opening file: %v", err)
	}
	defer file.Close()

	readers := make([]io.Reader, 0, len(data))
	for _, d := range data {
		//log.Printf("block: %v", d.Block)
		readers = append(readers, d.Payload)
	}
	multiReader := io.MultiReader(readers...)
	_, err = file.Seek(int64((data[0].Block-1)*BlockSize), 0)
	if err != nil {
		log.Printf("seeking to block %d failed: %v", data[0].Block, err)
	}
	// 使用io.Copy将multiReader的内容写入文件
	if _, err := io.Copy(file, multiReader); err != nil {
		log.Printf("error writing to file: %v", err)
	}
}

type Client struct {
	UUID       string
	Nickname   string
	Sign       string
	Status     chan struct{} `json:"-"` // initialization status
	SyncFunc   func()        `json:"-"` // e.t. sync icon
	ServerAddr string
	DataDir    string
	ConfigName string `json:"-"`
	messages
	connManager
	fileManager
	audioManager
}

type messages struct {
	SignedMessages chan SignedMessage `json:"-"`
	FileMessages   chan WriteReq      `json:"-"`
	MessageCounter uint32
	Retries        uint8         // the number of times to retry a failed transmission
	Timeout        time.Duration // the duration to wait for an acknowledgement
}

type connManager struct {
	SAddr     net.Addr `json:"-"`
	Connected bool     `json:"-"`
	conn      net.PacketConn
}

type fileManager struct {
	*FileWriter
	fileCache map[uint32]*CircularBuffer
	lock      sync.Mutex
}

func (f *fileManager) cleanCacheIn5Min(id uint32) *time.Timer {
	return time.AfterFunc(5*time.Minute, func() {
		f.lock.Lock()
		delete(f.fileCache, id)
		f.lock.Unlock()
	})
}

func (f *fileManager) initCache(id uint32) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.fileCache[id] = NewCircularBuffer(block512kb)
}

func (f *fileManager) loadCache(id uint32) *CircularBuffer {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.fileCache[id]
}

func newFileMetaInfo(dataDir string, nck func(f file), fileMessages chan<- WriteReq) fileManager {
	return fileManager{
		FileWriter: &FileWriter{
			Wrq:          make(chan WriteReq),
			FileData:     make(chan Data),
			FileId:       make(chan uint32),
			dataDir:      dataDir,
			fileMessages: fileMessages,
			files:        make(map[uint32]*file),
			nck:          nck,
		},
		fileCache: make(map[uint32]*CircularBuffer),
	}
}

type audioManager struct {
	audioMap      map[uint16]WriteReq
	audioReceiver map[uint16][]WriteReq
	AudioData     chan Data `json:"-"`
	lock          sync.Mutex
}

func newAudioMetaInfo() audioManager {
	return audioManager{
		audioMap:      make(map[uint16]WriteReq),
		audioReceiver: make(map[uint16][]WriteReq),
		AudioData:     make(chan Data, 100),
	}
}

func (a *audioManager) addAudioStream(wrq WriteReq) {
	a.audioMap[GetHigh16(wrq.FileId)] = wrq
}

func (a *audioManager) isAudio(fileId uint32) bool {
	audioId := a.decodeAudioId(fileId)
	return a.decodeAudioId(a.audioMap[audioId].FileId) == audioId
}

func (a *audioManager) decodeAudioId(fileId uint32) uint16 {
	return GetHigh16(fileId)
}

func (a *audioManager) addAudioReceiver(fileId uint16, wrq WriteReq) {
	a.audioReceiver[fileId] = slices.DeleteFunc(a.audioReceiver[fileId], func(w WriteReq) bool {
		return w.UUID == wrq.UUID
	})
	a.audioReceiver[fileId] = append(a.audioReceiver[fileId], wrq)
}

func (a *audioManager) deleteAudioReceiver(fileId uint16, UUID string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.audioReceiver[fileId] = slices.DeleteFunc(a.audioReceiver[fileId], func(w WriteReq) bool {
		return w.UUID == UUID
	})
}

func (a *audioManager) cleanupAudioResource(fileId uint16) bool {
	if len(a.audioReceiver[fileId]) == 0 {
		delete(a.audioReceiver, fileId)
		delete(a.audioMap, fileId)
		return true
	}
	return false
}

func (c *Client) Ready() {
	if c.Status != nil {
		<-c.Status
	}
}

func (c *Client) SyncIcon(img image.Image) error {
	return c.sendImage(img, OpSyncIcon, "icon.png")
}

func (c *Client) SyncGif(gifImg *gif.GIF) error {
	return c.sendGif(gifImg, OpSyncIcon, "icon.gif")
}

func (c *Client) SendImage(img image.Image, filename string) error {
	if filepath.Ext(filename) == ".webp" {
		filename = strings.TrimSuffix(filepath.Base(filename), ".webp") + ".png"
	}
	return c.sendImage(img, OpSendImage, filename)
}

func (c *Client) sendImage(img image.Image, code OpCode, filename string) error {
	buf := new(bytes.Buffer)
	err := EncodeImg(buf, filename, img)
	if err != nil {
		return err
	}

	return c.SendFile(bytes.NewReader(buf.Bytes()), code, filename, 0, 0)
}

func (c *Client) SendGif(GIF *gif.GIF, filename string) error {
	return c.sendGif(GIF, OpSendGif, filename)
}

func (c *Client) sendGif(GIF *gif.GIF, code OpCode, filename string) error {
	buf := new(bytes.Buffer)
	err := gif.EncodeAll(buf, GIF)
	if err != nil {
		return err
	}
	return c.SendFile(bytes.NewReader(buf.Bytes()), code, filename, 0, 0)
}

func (c *Client) SendVoice(filename string, duration uint64) error {
	r, err := os.Open(c.getPath(c.FullID(), filename))
	if err != nil {
		return err
	}
	defer r.Close()
	return c.SendFile(r, OpSendVoice, filename, 0, duration)
}

func (c *Client) SendAudioPacket(fileId uint32, blockId uint32, packet []byte) error {
	data := Data{FileId: fileId, Block: blockId, Payload: bytes.NewReader(packet)}
	pkt, err := data.Marshal()
	if err != nil {
		return err
	}
	_, err = c.conn.WriteTo(pkt, c.SAddr)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) MakeAudioCall(fileId uint32) error {
	c.addAudioStream(WriteReq{Code: OpAudioCall, FileId: fileId, UUID: c.FullID()})
	return c.send(c.newAudioReq(OpAudioCall, fileId))
}

func (c *Client) EndAudioCall(fileId uint32) error {
	audioId := GetHigh16(fileId)
	c.deleteAudioReceiver(audioId, c.FullID())
	c.cleanupAudioResource(audioId)
	c.FileMessages <- WriteReq{Code: OpEndAudioCall, FileId: fileId, UUID: c.FullID()}
	return c.send(c.newAudioReq(OpEndAudioCall, fileId))
}

func (c *Client) AcceptAudioCall(fileId uint32) error {
	return c.send(c.newAudioReq(OpAcceptAudioCall, fileId))
}

func (c *Client) newAudioReq(code OpCode, fileId uint32) *WriteReq {
	return &WriteReq{Code: code, FileId: fileId, UUID: c.FullID()}
}

func (c *Client) send(wrq Req) error {
	conn, err := net.Dial("udp", c.ServerAddr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	pkt, err := wrq.Marshal()
	if err != nil {
		return err
	}
	pktBuf := make([]byte, DatagramSize)
	_, err = c.sendPacket(conn, pktBuf, pkt, 0)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) PublishFile(name string, size uint64, id uint32) error {
	return c.send(&WriteReq{Code: OpPublish, FileId: id, Filename: name, Size: size, UUID: c.FullID()})
}

func (c *Client) SubscribeFile(id uint32, sender string) error {
	return c.send(&ReadReq{Code: OpSubscribe, FileId: id, Publisher: sender, Subscriber: c.FullID()})
}

func (c *Client) SendFile(reader io.Reader, code OpCode,
	filename string, size uint64, duration uint64) error {
	conn, err := net.Dial("udp", c.ServerAddr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	hash := Hash(unsafe.Pointer(&reader))
	log.Printf("file id: %v", hash)

	pktBuf := make([]byte, DatagramSize)

	wrq := WriteReq{Code: code, FileId: hash, UUID: c.FullID(),
		Filename: filename, Size: size, Duration: duration}
	pkt, err := wrq.Marshal()
	if err != nil {
		return err
	}
	_, err = c.sendPacket(conn, pktBuf, pkt, 0)
	if err != nil {
		return err
	}
	c.initCache(wrq.FileId)

	data := Data{FileId: wrq.FileId, Payload: reader}
	err = c.sendData(conn, pktBuf, data)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) sendData(conn net.Conn, readBuf []byte, data Data) error {
	cb := c.loadCache(data.FileId)
	for n := DatagramSize; n == DatagramSize; {
		pkt, err := data.Marshal()
		if err != nil {
			log.Printf("[%s] marshal failed: %v", "icon", err)
		}
		cb.Write(Packet{Block: data.Block, Data: pkt})
		isLastPacket := len(pkt) == DatagramSize
		if isLastPacket {
			n, err = c.sendPacket(conn, readBuf, pkt, data.Block)
		} else {
			n, err = c.conn.WriteTo(pkt, c.SAddr)
		}
		if err != nil {
			return err
		}
	}
	c.cleanCacheIn5Min(data.FileId)
	return nil
}

func (c *Client) SendText(text string) error {
	conn, err := net.Dial("udp", c.ServerAddr)
	if err != nil {
		log.Printf("[%s] dial failed: %v", c.ServerAddr, err)
	}
	defer func() { _ = conn.Close() }()

	msg := SignedMessage{Sign: Sign{c.Sign, c.FullID()}, Payload: []byte(text)}
	pkt, err := msg.Marshal()
	if err != nil {
		log.Printf("[%s] marshal failed: %v", text, err)
	}

	c.MessageCounter++
	buf := make([]byte, DatagramSize)
	_, err = c.sendPacket(conn, buf, pkt, 0)
	return err
}

func (c *Client) FullID() string {
	return c.Nickname + c.UUID
}

func (c *Client) ListenAndServe(addr string) {
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		log.Printf("[%s] dial failed: %v", addr, err)
	}
	c.conn = conn
	defer func() { _ = conn.Close() }()

	// init
	c.init()

	go c.Loop()

	if c.Status != nil {
		close(c.Status)
	}

	c.SAddr, err = net.ResolveUDPAddr("udp", c.ServerAddr)
	go func() {
		// auto reconnect in case of server down
		var threshold uint32 = 5
		for {
			c.SendSign()
			sentEnoughMessages := c.MessageCounter > threshold
			if sentEnoughMessages && c.SyncFunc != nil {
				c.MessageCounter = 0
				threshold++
				c.SyncFunc()
			}
			time.Sleep(30 * time.Second)
		}
	}()

	log.Printf("Listening on %s ...\n", conn.LocalAddr())
	c.serve(conn)
}

func (c *Client) init() {
	if c.Retries == 0 {
		c.Retries = 3
	}

	if c.Timeout == 0 {
		c.Timeout = 6 * time.Second
	}

	c.SignedMessages = make(chan SignedMessage, 100)
	c.FileMessages = make(chan WriteReq, 100)
	c.audioManager = newAudioMetaInfo()
	c.fileManager = newFileMetaInfo(c.DataDir, c.nck, c.FileMessages)
	Mkdir(c.getDir(c.FullID()))
}

func (c *Client) SendSign() {
	sign := Sign{c.Sign, c.FullID()}
	pkt, err := sign.Marshal()
	if err != nil {
		log.Printf("[%s] marshal failed: %v", c.Sign, err)
	}
	_, err = c.conn.WriteTo(pkt, c.SAddr)
	if err != nil {
		log.Printf("[%s] write failed: %v", c.ServerAddr, err)
		return
	}
}

func (c *Client) serve(conn net.PacketConn) {

	for {
		buf := make([]byte, DatagramSize)
		_ = conn.SetReadDeadline(time.Now().Add(c.Timeout))
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			var nErr net.Error
			if errors.As(err, &nErr) && nErr.Timeout() {
				//log.Printf("receive text timeout")
			}
			if errors.Is(err, syscall.ECONNREFUSED) {
				c.Connected = false
				log.Printf("[%s] connection refused", c.ServerAddr)
			}
			//log.Printf("[%s] receive text: %v", c.ServerAddr, err)
			continue
		}
		go c.handle(buf[:n], conn, addr)
	}
}

func (c *Client) handle(buf []byte, conn net.PacketConn, addr net.Addr) {
	var (
		ack  Ack
		nck  Nck
		msg  SignedMessage
		data Data
		wrq  WriteReq
	)
	switch {
	case ack.Unmarshal(buf) == nil:
		if ack.SrcOp == OpSign {
			c.Connected = true
		}
	case msg.Unmarshal(buf) == nil:
		s := string(msg.Payload)
		log.Printf("received text [%s] from [%s]\n", s, msg.Sign.UUID)
		c.SignedMessages <- msg
		c.ack(conn, addr, OpSignedMSG, 0)
	case wrq.Unmarshal(buf) == nil:
		c.ack(conn, addr, wrq.Code, 0)
		audioId := c.decodeAudioId(wrq.FileId)
		switch wrq.Code {
		case OpAudioCall:
			c.addAudioStream(wrq)
			fallthrough
		case OpAcceptAudioCall:
			c.addAudioReceiver(audioId, wrq)
			c.FileMessages <- wrq
		case OpEndAudioCall:
			c.deleteAudioReceiver(audioId, wrq.UUID)
			cancel := c.audioMap[audioId].UUID == wrq.UUID
			cleanup := c.cleanupAudioResource(audioId)
			if cancel {
				wrq.FileId = 0
			}
			if cleanup {
				c.FileMessages <- wrq
			}
		case OpPublish:
			c.FileMessages <- wrq
		default:
			c.addFile(wrq)
		}
	case data.Unmarshal(buf) == nil:
		if c.isAudio(data.FileId) {
			c.AudioData <- data
			return
		}
		if c.isFile(data.FileId) {
			c.handleFileData(conn, addr, data, len(buf))
		}
	case nck.Unmarshal(buf) == nil:
		cb := c.loadCache(nck.FileId)
		if cb == nil {
			return
		}
		packets := cb.Read(nck.ranges)
		for _, pkt := range packets {
			_, _ = c.conn.WriteTo(pkt.Data, c.SAddr)
		}
	}
}

func (c *Client) handleFileData(conn net.PacketConn, addr net.Addr, data Data, n int) {
	c.FileData <- data
	if n < DatagramSize {
		c.ack(conn, addr, OpData, data.Block)
		c.FileId <- data.FileId
		log.Printf("file id: [%d] received", data.FileId)
	}
}

func (c *Client) addFile(wrq WriteReq) {
	c.Wrq <- wrq
}

func (c *Client) ack(conn net.PacketConn, addr net.Addr, code OpCode, block uint32) {
	ack := Ack{SrcOp: code, Block: block}
	pkt, err := ack.Marshal()
	_, err = conn.WriteTo(pkt, addr)
	if err != nil {
		log.Printf("[%s] write failed: %v", addr, err)
	}
}

func (c *Client) nck(f file) {
	nck := Nck{FileId: f.req.FileId, ranges: f.rt.GetRanges()}
	pkt, _ := nck.Marshal()
	_, err := c.conn.WriteTo(pkt, c.SAddr)
	if err != nil {
		log.Printf("[%s] write failed: %v", c.ServerAddr, err)
	}
	log.Printf("request missing packets %v", nck)
}

func (c *Client) SetNickName(nickname string) {
	c.Nickname = nickname
}

func (c *Client) SetSign(sign string) {
	c.Sign = sign
}

func (c *Client) SetServerAddr(addr string) {
	c.ServerAddr = addr
	c.SAddr, _ = net.ResolveUDPAddr("udp", addr)
}

func (c *Client) sendPacket(conn net.Conn, buf []byte, bytes []byte, block uint32) (int, error) {
	var ackPkt Ack
RETRY:
	for i := c.Retries; i > 0; i-- {
		n, err := conn.Write(bytes)
		if err != nil {
			log.Printf("[%s] write failed: %v", c.ServerAddr, err)
			return 0, err
		}

		// wait for the Server's ACK packet
		_ = conn.SetReadDeadline(time.Now().Add(c.Timeout))
		_, err = conn.Read(buf)

		if err != nil {
			if nErr, ok := err.(net.Error); ok && nErr.Timeout() {
				log.Printf("waiting for ACK timeout")
				continue RETRY
			}
			if errors.Is(err, syscall.ECONNREFUSED) {
				c.Connected = false
				log.Printf("[%s] connection refused", c.ServerAddr)
				return 0, err
			}
			log.Printf("[%s] waiting for ACK: %v", c.ServerAddr, err)
			continue
		}

		switch {
		case ackPkt.Unmarshal(buf[:n]) == nil:
			if block == 0 || ackPkt.Block == block {
				return n, nil
			}
		default:
			log.Printf("[%s] bad packet", c.ServerAddr)
		}
	}
	return 0, errors.New("exhausted retries")
}

var DefaultClient *Client
var block512kb = 512 * 1024 / BlockSize
