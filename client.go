package wi

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

type file struct {
	req      WriteReq // file id, name and size
	data     []Data   // file packets
	counter  int
	createAt time.Time
	updateAt time.Time
	*RangeTracker
	*updater
}

type updater struct {
	update func(p int, s int) // progress and speed updater
}

type fileWriter struct {
	wrq          chan WriteReq       // file request
	fileData     chan Data           // file data
	dataDir      string              // files save in dataDir
	fileMessages chan<- WriteReq     // notify file complete, receiver could refresh icon or update status
	files        map[uint32]*file    // internal file info
	updaters     map[uint32]*updater // metrics updater
	nck          func(f file)        // request lost packet
}

func (f *fileWriter) loop() {
	for {
		select {
		case req := <-f.wrq:
			if req.Code == OpReady {
				log.Printf("OpReady received, try complete")
				if !f.isFile(req.FileId) {
					continue
				}
				f.tryComplete(req.FileId)
			} else {
				f.init(req)
			}
		case data := <-f.fileData:
			if !f.isFile(data.FileId) || f.dup(data.FileId, data.Block) {
				continue
			}
			f.tryWrite(data)
		}
	}
}

func (f *fileWriter) tryWrite(data Data) {
	fd := f.files[data.FileId]
	fd.counter++
	req := fd.req
	fd.data = append(fd.data, data)
	fd.Track(MonoRange(data.Block))
	if f.received256kb(fd) || fd.elapsed1Second() {
		f.flush(fd, f.getPath(req.UUID, req.Filename))
		fd.updateAndReset()
	}
}

func (f *file) elapsed1Second() bool {
	return time.Since(f.updateAt) >= 1*time.Second
}

func (f *file) noDataReceived() bool {
	return f.counter == 0
}

func (f *file) updateAndReset() {
	if f.updater != nil {
		f.update()
	}
	f.reset()
}

func (f *file) reset() {
	f.updateAt = time.Now()
	f.counter = 0
}

func (f *file) update() {
	f.updater.update(f.GetProgress(), f.getSpeed())
}

func (f *file) getSpeed() int {
	var (
		elapsed         time.Duration
		packetsReceived int
		speed           float32
	)
	if f.isCompleted() {
		elapsed = time.Since(f.createAt) / time.Millisecond
		packetsReceived = int(f.nextBlock() - 1)
	} else {
		elapsed = time.Since(f.updateAt) / time.Millisecond
		packetsReceived = f.counter
	}
	speed = float32(packetsReceived*BlockSize) * 1000 / float32(elapsed)
	return int(speed)
}

func (f *fileWriter) received256kb(fd *file) bool {
	const size = 256 * 1024 / BlockSize
	return len(fd.data) >= size
}

func (f *fileWriter) init(req WriteReq) {
	if f.isProcessing(req) {
		return
	}
	fd := &file{
		req:          req,
		RangeTracker: f.newRangeTracker(req.Size),
		createAt:     time.Now(),
		updateAt:     time.Now(),
		updater:      f.updaters[req.FileId],
	}
	f.files[req.FileId] = fd
	go f.nck(*fd)
}

func (f *fileWriter) newRangeTracker(size uint64) *RangeTracker {
	rt := &RangeTracker{}
	if size == 0 {
		return rt
	}
	finalBlock := (size + BlockSize - 1) / BlockSize
	rt.Track(MonoRange(uint32(finalBlock + 1)))
	return rt
}

func (f *fileWriter) isProcessing(req WriteReq) bool {
	return f.files[req.FileId] != nil
}

func (f *fileWriter) getDir(uuid string) string {
	if uuid == "" {
		return f.dataDir + "/default"
	}
	return f.dataDir + "/" + strings.Replace(uuid, "#", "_", -1)
}

func (f *fileWriter) getPath(uuid string, filename string) string {
	return f.getDir(uuid) + "/" + filename
}

func (f *fileWriter) tryComplete(id uint32) {
	fd := f.files[id]
	req := fd.req
	f.flush(fd, f.getPath(req.UUID, req.Filename))
	if fd.isCompleted() {
		fd.updateAndReset()
		f.clean(id)
		f.fileMessages <- req
	} else {
		go f.nck(*fd)
		if fd.elapsed1Second() {
			fd.updateAndReset()
		}
	}
}

func (f *fileWriter) flush(fd *file, filePath string) {
	d := fd.data
	for d != nil {
		d = write(filePath, d)
	}
	fd.data = fd.data[:0]
}

func (f *fileWriter) clean(id uint32) {
	delete(f.files, id)
}

func (f *fileWriter) isFile(fileId uint32) bool {
	fd := f.files[fileId]
	return fd != nil
}

func (f *fileWriter) dup(fileId uint32, block uint32) bool {
	fd := f.files[fileId]
	return fd != nil && fd.Contains(MonoRange(block))
}

func writeTo(filePath string, data []Data) {
	// os.O_CREATE: 如果文件不存在，则创建文件
	// os.O_WRONLY: 以只写模式打开文件
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Printf("open file failed: %v", err)
	}
	defer f.Close()

	readers := make([]io.Reader, 0, len(data))
	for _, d := range data {
		readers = append(readers, d.Payload)
	}
	multiReader := io.MultiReader(readers...)
	_, err = f.Seek(int64((data[0].Block-1)*BlockSize), 0)
	if err != nil {
		log.Printf("Seeking to block %d failed: %v", data[0].Block, err)
	}
	// 使用io.Copy将multiReader的内容写入文件
	if _, err := io.Copy(f, multiReader); err != nil {
		log.Printf("Write to file failed: %v", err)
	}
}

type fileContent struct {
	fileId     uint32
	processing bool
	content    func() (io.ReadSeekCloser, error)
	reading    *RangeTracker
	pending    *RangeTracker
	lock       sync.Mutex
}

func (f *fileContent) add(ranges []Range) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.reading.isCompleted() {
		f.reading.Set(ranges)
	} else {
		tracker := &RangeTracker{ranges: ranges}
		tracker.Exclude(f.reading.ranges)
		f.pending.Merge(tracker)
	}
}

func (f *fileContent) remove(rg Range) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.reading.Track(rg)
}

func (f *fileContent) swap() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.reading = f.pending
	f.pending = &RangeTracker{}
}

func (f *fileContent) isProcessing() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.processing
}

func (f *fileContent) setProcessing() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.processing = true
}

func (f *fileContent) unsetProcessing() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.processing = false
}

type fileReader struct {
	req       chan Nck // packets request
	opReady   func(id uint32) error
	contents  map[uint32]*fileContent // fileId -> *fileContent
	writeOnce func(data Data) error   // send single data packet
}

func (f *fileReader) process() {
	for n := range f.req {
		c := f.contents[n.FileId]
		if c == nil {
			continue
		}
		c.add(n.ranges)
		if !c.isProcessing() {
			go f.read(n.FileId)
		}
	}
}

func (f *fileReader) read(id uint32) {
	c := f.contents[id]
	c.setProcessing()
	content, err := c.content()
	if err != nil {
		log.Printf("read file failed: %v", err)
		return
	}
	defer content.Close()
LOOP:
	reading := make([]Range, len(c.reading.ranges))
	copy(reading, c.reading.ranges)
	for _, rg := range reading {
		pos := (rg.start - 1) * BlockSize
		if _, err = content.Seek(int64(pos), 0); err != nil {
			log.Printf("Seek failed: %v", err)
		}
		for i := rg.start; i <= rg.end; i++ {
			d := Data{FileId: c.fileId, Block: i - 1, Payload: content}
			if err = f.writeOnce(d); err != nil {
				log.Printf("Send failed: %v", err)
			}
			if i%50 == 0 {
				c.remove(Range{rg.start, i})
			}
		}
		c.remove(rg)
	}
	if !c.pending.isCompleted() {
		c.swap()
		goto LOOP
	}
	c.unsetProcessing()
	err = f.opReady(id)
	if err != nil {
		log.Printf("Ready failed: %v", err)
	}
}

func (f *fileReader) isPull(fileId uint32) bool {
	return f.contents[fileId] != nil
}

type Client struct {
	Status chan struct{} `json:"-"` // initialization status
	Identity
	Config
	messages
	connManager
	fileManager
	*audioManager
}

type Config struct {
	ServerAddr     string
	DataDir        string // save config
	ExternalDir    string // save files, e.t. on Android should be /Android/data/...
	ConfigName     string `json:"-"`
	MessageCounter uint32 // the number of sent messages
}

type Identity struct {
	Sign     string // chat sign
	UUID     string // generated 5 digit id
	Nickname string // user typed nickname
}

func (i *Identity) ID() string {
	return i.Nickname + i.UUID
}

type messages struct {
	SignedMessages chan SignedMessage `json:"-"` // text message
	FileMessages   chan WriteReq      `json:"-"` // audio and image
	SubMessages    chan ReadReq       `json:"-"` // subscribe requests
	CtrlMessages   chan CtrlReq       `json:"-"` // control requests
	Retries        uint8              `json:"-"` // the number of times to retry a failed transmission
	Timeout        time.Duration      `json:"-"` // the duration to wait for an acknowledgement
	history        sync.Map           // sign -> *sync.Map { uuid -> *RangeTracker }
	ackHandlers    sync.Map           // block -> context.CancelFunc
	retryHandlers  sync.Map           // block -> context.CancelFunc
	trackers       sync.Map           // uuid -> *RangeTracker
	discoveries    sync.Map           // reqID -> chan DiscoveryResp
}

func (c *Client) nextID() uint32 {
	return atomic.AddUint32(&c.MessageCounter, 1)
}

func newMessages() messages {
	return messages{
		Retries:        18,
		Timeout:        500 * time.Millisecond,
		SignedMessages: make(chan SignedMessage, 100),
		FileMessages:   make(chan WriteReq, 20),
		SubMessages:    make(chan ReadReq, 20),
		CtrlMessages:   make(chan CtrlReq, 10),
	}
}

type connManager struct {
	conn       net.PacketConn
	remoteAddr net.Addr
	localAddr  string
	connFlag   int32 // 0: connected, 1: reconnecting
}

type fileManager struct {
	*fileWriter
	*fileReader
}

func newFileMetaInfo(
	externalDir string,
	nck func(f file),
	opReady func(id uint32) error,
	writeOnce func(d Data) error,
	fileMessages chan<- WriteReq,
) fileManager {
	return fileManager{
		fileReader: &fileReader{
			req:       make(chan Nck, 10),
			opReady:   opReady,
			writeOnce: writeOnce,
			contents:  make(map[uint32]*fileContent),
		},
		fileWriter: &fileWriter{
			wrq:          make(chan WriteReq, 10),
			fileData:     make(chan Data, 200),
			dataDir:      externalDir,
			fileMessages: fileMessages,
			files:        make(map[uint32]*file),
			updaters:     make(map[uint32]*updater),
			nck:          nck,
		},
	}
}

type audioManager struct {
	audioMap      sync.Map  // audio maker, fileId -> WriteReq
	audioReceiver sync.Map  // audio receiver, fileId -> *sync.Map { UUID -> WriteReq }
	AudioData     chan Data `json:"-"`
}

func newAudioMetaInfo() *audioManager {
	return &audioManager{
		AudioData: make(chan Data, 100),
	}
}

func (a *audioManager) addAudioStream(wrq WriteReq) {
	audioId := a.decodeAudioId(wrq.FileId)
	a.audioMap.Store(audioId, wrq)
	receivers := &sync.Map{}
	receivers.Store(wrq.UUID, wrq)
	a.audioReceiver.Store(audioId, receivers)
}

func (a *audioManager) isAudio(fileId uint32) bool {
	audioId := a.decodeAudioId(fileId)
	wrq, ok := a.audioMap.Load(audioId)
	if !ok {
		return false
	}
	return a.decodeAudioId(wrq.(WriteReq).FileId) == audioId
}

func (a *audioManager) isAudioMaker(fileId uint16, UUID string) bool {
	wrq, ok := a.audioMap.Load(fileId)
	if !ok {
		return false
	}
	return wrq.(WriteReq).UUID == UUID
}

func (a *audioManager) decodeAudioId(fileId uint32) uint16 {
	return GetHigh16(fileId)
}

func (a *audioManager) addAudioReceiver(fileId uint16, wrq WriteReq) {
	receivers, ok := a.audioReceiver.Load(fileId)
	if !ok {
		return
	}
	receivers.(*sync.Map).Store(wrq.UUID, wrq)
}

func (a *audioManager) isAudioReceiver(fileId uint16, UUID string) bool {
	receivers, ok := a.audioReceiver.Load(fileId)
	if !ok {
		return false
	}
	_, ok = receivers.(*sync.Map).Load(UUID)
	return ok
}

func (a *audioManager) deleteAudioReceiver(fileId uint16, UUID string) (value any, loaded bool) {
	receivers, ok := a.audioReceiver.Load(fileId)
	if !ok {
		return
	}
	return receivers.(*sync.Map).LoadAndDelete(UUID)
}

func (a *audioManager) noReceiverLeft(fileId uint16) bool {
	return a.countReceiver(fileId, 1)
}

func (a *audioManager) atMostOneReceiver(fileId uint16) bool {
	return a.countReceiver(fileId, 2)
}

func (a *audioManager) countReceiver(fileId uint16, num int) bool {
	receivers, ok := a.audioReceiver.Load(fileId)
	if !ok {
		return true
	}
	cnt := 0
	receivers.(*sync.Map).Range(func(k, v interface{}) bool {
		cnt++
		return cnt < num
	})
	return cnt < num
}

func (a *audioManager) cleanup(fileId uint16) {
	a.audioReceiver.Delete(fileId)
	a.audioMap.Delete(fileId)
}

func (c *Client) Ready() {
	if c.Status != nil {
		<-c.Status
	}
}

func (c *Client) SendVoice(filename string, duration uint32) error {
	i, err := os.Stat(filename)
	if err != nil {
		return err
	}
	return c.SendFile(func() (io.ReadSeekCloser, error) {
		return os.Open(filename)
	}, OpSendVoice, Hash(unsafe.Pointer(&filename)), filepath.Base(filename), uint64(i.Size()), duration)
}

func (c *Client) SendAudioPacket(fileId uint32, blockId uint32, packet []byte) error {
	data := Data{FileId: fileId, Block: blockId, Payload: bytes.NewReader(packet)}
	pkt, err := data.Marshal()
	if err != nil {
		return err
	}
	if err = c.conn.SetWriteDeadline(time.Now().Add(_TIMEOUT)); err != nil {
		return err
	}
	if _, err = c.conn.WriteTo(pkt, c.remoteAddr); err != nil {
		return err
	}
	return nil
}

func (c *Client) MakeAudioCall(fileId uint32) error {
	req := c.newAudioReq(OpAudioCall, fileId)
	c.addAudioStream(*req)
	return c.send(req)
}

func (c *Client) EndAudioCall(fileId uint32) error {
	audioId := GetHigh16(fileId)
	c.deleteAudioReceiver(audioId, c.ID())
	c.cleanup(audioId)
	req := c.newAudioReq(OpEndAudioCall, fileId)
	c.FileMessages <- *req
	return c.send(req)
}

func (c *Client) AcceptAudioCall(fileId uint32) error {
	return c.send(c.newAudioReq(OpAcceptAudioCall, fileId))
}

func (c *Client) newAudioReq(code OpCode, fileId uint32) *WriteReq {
	return &WriteReq{Code: code, Block: c.nextID(), FileId: fileId, UUID: c.ID()}
}

func (c *Client) send(req Req) error {
	pkt, err := req.Marshal()
	if err != nil {
		return err
	}
	return c.write(pkt, req.ID(), true)
}

func (c *Client) PublishFile(name string, size uint64, id uint32) error {
	return c.send(&WriteReq{Code: OpPublish, Block: c.nextID(), FileId: id, Filename: name, Size: size, UUID: c.ID(), CreatedAt: time.Now().UnixMilli()})
}

func (c *Client) PublishContent(content func() (io.ReadSeekCloser, error), name string, size uint64, id uint32) error {
	return c.SendFile(content, OpContent, id, name, size, 0)
}

func (c *Client) SubscribeFile(id uint32, sender string, update func(p int, s int)) error {
	c.updaters[id] = &updater{update: update}
	return c.send(&ReadReq{Code: OpSubscribe, Block: c.nextID(), FileId: id, Publisher: sender, Subscriber: c.ID()})
}

func (c *Client) UnsubscribeFile(id uint32, sender string) error {
	// clean OpContent wrq after finished download
	c.clean(id)
	return c.send(&ReadReq{Code: OpUnsubscribe, Block: c.nextID(), FileId: id, Publisher: sender, Subscriber: c.ID()})
}

func (c *Client) SendFile(content func() (io.ReadSeekCloser, error), code OpCode, fileId uint32, filename string, size uint64, duration uint32) error {
	log.Printf("Send file: %v", fileId)

	if c.contents[fileId] == nil {
		c.contents[fileId] = &fileContent{fileId: fileId, content: content, reading: new(RangeTracker), pending: new(RangeTracker)}
	}

	return c.send(&WriteReq{
		Code:      code,
		Block:     c.nextID(),
		FileId:    fileId,
		UUID:      c.ID(),
		Filename:  filename,
		Size:      size,
		Duration:  duration,
		CreatedAt: time.Now().UnixMilli(),
	})
}

func (c *Client) opReady(id uint32) error {
	log.Printf("send OpReady")
	return c.send(&WriteReq{Code: OpReady, Block: c.nextID(), FileId: id, UUID: c.ID()})
}

func (c *Client) writeOnce(data Data) error {
	pkt, err := data.Marshal()
	if err != nil {
		return err
	}
	if err = c.conn.SetWriteDeadline(time.Now().Add(_TIMEOUT)); err != nil {
		return err
	}
	if _, err = c.conn.WriteTo(pkt, c.remoteAddr); err != nil {
		return err
	}
	return nil
}

func (c *Client) SendText(text string) error {
	msg := SignedMessage{SignReq: SignReq{Block: c.nextID(), SignBody: SignBody{Sign: c.Sign, UUID: c.ID()}}, CreatedAt: time.Now().UnixMilli(), Payload: []byte(text)}
	pkt, err := msg.Marshal()
	if err != nil {
		return err
	}
	return c.write(pkt, msg.SignReq.Block, true)
}

func (c *Client) SyncName(oldUUID string) {
	nameReq := CtrlReq{Code: OpSyncName, Block: c.nextID(), Target: oldUUID, UUID: c.ID()}
	pkt, err := nameReq.Marshal()
	if err != nil {
		panic(err)
	}
	err = c.write(pkt, nameReq.Block, true)
	if err != nil {
		log.Printf("sync name: %v", err)
	}
}

func (c *Client) ReadIcon(target string) error {
	// icon file id defaults to 0
	return c.send(&ReadReq{Code: OpReadIcon, Block: c.nextID(), FileId: 0, Publisher: target, Subscriber: c.ID()})
}

func (c *Client) Track(sign *SignBody, block uint32) {
	c.loadRangeTracker(sign).Track(MonoRange(block))
}

func (c *Client) Discover(flag DiscoveryFlag) ([]string, error) {
	reqID := c.nextID()
	resp := make(chan DiscoveryResp, 150)
	c.discoveries.Store(reqID, resp)
	defer c.discoveries.Delete(reqID)
	err := c.send(&DiscoveryReq{Block: reqID, Sign: c.Sign, DiscoveryFlag: flag})
	if err != nil {
		log.Printf("discovery failed: %v", err)
	}
	ret := make([]string, 0)
LOOP:
	for {
		timer := time.After(10 * time.Second)
		select {
		case d := <-resp:
			ret = append(ret, d.UUIDS...)
			if d.Final == 1 {
				break LOOP
			}
		case <-timer:
			return nil, errors.New("discovery timed out")
		}
	}
	return ret, nil
}

func (c *Client) Pull() {
	hs := c.loadHistorySet(c.Sign)
	knownUsers := c.getUsers(hs)
	go c.pullMessagesOfUnknownUsers(hs)
	go c.pullTimeoutFiles()
	c.pullMessagesOf(knownUsers, hs)
}

func (c *Client) pullMessagesOfUnknownUsers(hs *sync.Map) {
	var (
		unknown []string
		err     error
	)
	if unknown, err = c.getUnknownUsers(hs); err != nil {
		log.Printf("pull messages of unknown users failed: %v", err)
		return
	}
	for _, u := range unknown {
		c.Track(&SignBody{Sign: c.Sign, UUID: u}, 0)
	}
	c.pullMessagesOf(unknown, hs)
}

func (c *Client) pullMessagesOf(users []string, hs *sync.Map) {
	for _, user := range users {
		if t, ok := hs.Load(user); ok {
			go c.pullMessages(user, t.(*RangeTracker))
		}
	}
}

func (c *Client) getUnknownUsers(hs *sync.Map) ([]string, error) {
	users, err := c.Discover(Active)
	if err != nil {
		return nil, err
	}
	unknown := make([]string, 0)
	for _, u := range users {
		if _, ok := hs.Load(u); !ok && u != c.ID() {
			unknown = append(unknown, u)
		}
	}
	return unknown, nil
}

func (c *Client) getUsers(hs *sync.Map) []string {
	users := make([]string, 0)
	hs.Range(func(uuid, value interface{}) bool {
		if uuid != c.ID() {
			users = append(users, uuid.(string))
		}
		return true
	})
	return users
}

func (c *Client) pullMessages(UUID string, tracker *RangeTracker) {
	var (
		ranges   = tracker.Get()
		baseSize = 17 + len(UUID) + len(c.Sign)
		maxLen   = (DatagramSize - baseSize) / 8
		pr       *PullReq
		signBody = SignBody{Sign: c.Sign, UUID: UUID}
	)
	for _, r := range partition(ranges, maxLen) {
		rg := Range{r[0].start, r[len(r)-1].end}
		pr = &PullReq{Block: c.nextID(), SignBody: signBody, Range: rg, ranges: r}
		if err := c.send(pr); err != nil {
			log.Printf("pull failed: %v", err)
		}
	}
	pr = &PullReq{Block: c.nextID(), SignBody: signBody, Range: Range{start: tracker.nextBlock(), end: math.MaxUint32}}
	if err := c.send(pr); err != nil {
		log.Printf("pull failed: %v", err)
	}
}

func (c *Client) loadHistorySet(sign string) *sync.Map {
	if h, ok := c.history.Load(sign); ok {
		return h.(*sync.Map)
	}
	h, _ := c.history.LoadOrStore(sign, new(sync.Map))
	return h.(*sync.Map)
}

func (c *Client) loadRangeTracker(sign *SignBody) *RangeTracker {
	hs := c.loadHistorySet(sign.Sign)
	if h, ok := hs.Load(sign.UUID); ok {
		return h.(*RangeTracker)
	}
	h, _ := hs.LoadOrStore(sign.UUID, new(RangeTracker))
	return h.(*RangeTracker)
}

func (c *Client) ListenAndServe(addr string) {
	c.localAddr = addr
	if err := c.listen(); err != nil {
		panic(err)
	}
	defer func() { _ = c.conn.Close() }()

	// init
	c.init()

	go c.loop()
	go c.process()

	c.remoteAddr, _ = net.ResolveUDPAddr("udp", c.ServerAddr)
	go func() {
		// auto reconnect in case of server restart
		c.SendSign()
		if c.Status != nil {
			close(c.Status)
		}
		for {
			time.Sleep(30 * time.Second)
			c.SendSign()
			c.pullTimeoutFiles()
		}
	}()

	c.serve()
}

func (c *Client) pullTimeoutFiles() {
	for _, v := range c.files {
		if time.Since(v.updateAt) > 3*time.Second {
			c.tryComplete(v.req.FileId)
		}
	}
}

func (c *Client) init() {
	c.messages = newMessages()
	c.audioManager = newAudioMetaInfo()
	c.fileManager = newFileMetaInfo(c.ExternalDir, c.nck, c.opReady, c.writeOnce, c.FileMessages)
}

// SendSign try to write sign to server
func (c *Client) SendSign() {
	sign := SignReq{0, SignBody{c.Sign, c.ID()}}
	pkt, err := sign.Marshal()
	if err != nil {
		log.Printf("[%s] marshal failed: %v", c.Sign, err)
	}
	if err = c.conn.SetWriteDeadline(time.Now().Add(_TIMEOUT)); err != nil {
		log.Printf("[%s] write failed: %v", c.ServerAddr, err)
	}
	if _, err = c.conn.WriteTo(pkt, c.remoteAddr); err != nil {
		log.Printf("[%s] write failed: %v", c.ServerAddr, err)
	}
}

func (c *Client) SignIn() {
	sign := SignReq{c.nextID(), SignBody{c.Sign, c.ID()}}
	pkt, err := sign.Marshal()
	if err != nil {
		panic(err)
	}
	err = c.write(pkt, sign.Block, false)
	if err != nil {
		log.Printf("[%s] send failed: %v", c.Sign, err)
	} else {
		c.retryHandlers.Range(func(k, v interface{}) bool {
			v.(context.CancelFunc)()
			return true
		})
	}
}

func (c *Client) SignOut() {
	op := OpSignOut
	pkt, err := op.Marshal()
	if err != nil {
		log.Printf("marshal failed: %v", err)
	}
	if err = c.conn.SetWriteDeadline(time.Now().Add(_TIMEOUT)); err != nil {
		log.Printf("[%s] write failed: %v", c.ServerAddr, err)
	}
	if _, err = c.conn.WriteTo(pkt, c.remoteAddr); err != nil {
		log.Printf("[%s] write failed: %v", c.ServerAddr, err)
	}
}

func (c *Client) reconnect() {
	if !atomic.CompareAndSwapInt32(&c.connFlag, 0, 1) {
		log.Printf("reconnect already in progress, skipping")
		return
	}
	defer atomic.StoreInt32(&c.connFlag, 0)
	if c.conn != nil {
		c.conn.Close()
	}
	if err := c.listen(); err != nil {
		panic(err)
	}
	log.Printf("reconnected")
}

func (c *Client) listen() error {
	var err error
	if c.conn, err = net.ListenPacket("udp", c.localAddr); err != nil {
		log.Printf("[%s] listen failed: %v", c.localAddr, err)
		return err
	}
	return nil
}

func (c *Client) serve() {
	log.Printf("Listening on %s ...\n", c.conn.LocalAddr())
	for {
		buf := make([]byte, DatagramSize)
		_ = c.conn.SetReadDeadline(time.Now().Add(_TIMEOUT))
		n, addr, err := c.conn.ReadFrom(buf)
		if err != nil {
			if nErr, ok := errors.AsType[net.Error](err); ok && nErr.Timeout() {
				//log.Printf("receive text timeout")
			}
			if errors.Is(err, syscall.ECONNREFUSED) {
				_ = c.conn.SetWriteDeadline(time.Now().Add(_TIMEOUT))
				log.Printf("[%s] connection refused", c.ServerAddr)
			}
			//log.Printf("[%s] receive text: %v", c.ServerAddr, err)
			continue
		}
		go c.handle(buf[:n], addr)
	}
}

func (c *Client) handle(buf []byte, addr net.Addr) {
	var (
		ack       Ack
		nck       Nck
		msg       SignedMessage
		data      Data
		ec        ErrCode
		rrq       ReadReq
		wrq       WriteReq
		check     Check
		ctrl      CtrlReq
		reply     ReplyReq
		discovery DiscoveryResp
	)
	switch {
	case ack.Unmarshal(buf) == nil:
		log.Printf("ack received: %v", ack.Block)
		cancel, ok := c.ackHandlers.Load(ack.Block)
		if !ok {
			return
		}
		cancel.(context.CancelFunc)()
	case check.Unmarshal(buf) == nil:
		if c.check(check.UUID, check.Block) {
			c.ack(addr, check.UUID, msg.Block)
		}
	case msg.Unmarshal(buf) == nil:
		c.ack(addr, msg.UUID, msg.Block)
		if c.dup(msg.UUID, msg.Block) {
			return
		}
		s := string(msg.Payload)
		log.Printf("Receiving text [%s] from [%s]\n", s, msg.UUID)
		c.SignedMessages <- msg
	case rrq.Unmarshal(buf) == nil:
		c.ack(addr, rrq.Subscriber, rrq.Block)
		if c.dup(rrq.Subscriber, rrq.Block) {
			return
		}
		c.SubMessages <- rrq
	case wrq.Unmarshal(buf) == nil:
		c.ack(addr, wrq.UUID, wrq.Block)
		if c.dup(wrq.UUID, wrq.Block) {
			return
		}
		audioId := c.decodeAudioId(wrq.FileId)
		switch wrq.Code {
		case OpAudioCall:
			c.addAudioStream(wrq)
			c.addAudioReceiver(audioId, wrq)
			c.FileMessages <- wrq
		case OpAcceptAudioCall:
			c.addAudioReceiver(audioId, wrq)
			if c.isAudioMaker(audioId, c.ID()) {
				c.FileMessages <- wrq
			}
		case OpEndAudioCall:
			_, ok := c.deleteAudioReceiver(audioId, wrq.UUID)
			if !ok {
				return
			}
			isReceiver := c.isAudioReceiver(audioId, c.ID())
			if !isReceiver {
				return
			}
			noOtherReceiver := c.atMostOneReceiver(audioId)
			if noOtherReceiver {
				c.cleanup(audioId)
				c.FileMessages <- wrq
			}
		case OpPublish:
			c.FileMessages <- wrq
		case OpContent:
			// content ready
			fallthrough
		default:
			c.wrq <- wrq
		}
	case data.Unmarshal(buf) == nil:
		if c.isAudio(data.FileId) {
			select {
			case c.AudioData <- data:
			default:
			}
		} else if c.isFile(data.FileId) {
			c.fileData <- data
		}
	case nck.Unmarshal(buf) == nil:
		c.ack(addr, nck.UUID, nck.Block)
		if c.dup(nck.UUID, nck.Block) {
			return
		}
		log.Printf("nck received, [%v]-[%v]", nck.UUID, nck.Block)
		c.req <- nck
	case ec.Unmarshal(buf) == nil:
		if ec == ErrUnknownUser {
			c.SignIn()
		}
	case ctrl.Unmarshal(buf) == nil:
		c.ack(addr, ctrl.UUID, ctrl.Block)
		c.CtrlMessages <- ctrl
	case reply.Unmarshal(buf) == nil:
		c.ack(addr, _SERVER, reply.Block)
		tracker := c.loadRangeTracker(&reply.SignBody)
		for _, r := range reply.ranges {
			tracker.Track(r)
		}
	case discovery.Unmarshal(buf) == nil:
		c.ack(addr, _SERVER, discovery.Block)
		dc, ok := c.discoveries.Load(discovery.ReqID)
		if !ok {
			return
		}
		if c.dup(_SERVER, discovery.Block) {
			return
		}
		dc.(chan DiscoveryResp) <- discovery
	default:
		code := OpCode(binary.BigEndian.Uint16(buf[:2]))
		log.Printf("unknown pkt, %v, %v", code.String(), buf)
	}
}

func (c *Client) dup(UUID string, block uint32) bool {
	t := c.loadTracker(UUID)
	r := MonoRange(block)
	dup := t.Contains(r)
	if !dup {
		t.Track(r)
		if UUID != _SERVER {
			c.Track(&SignBody{Sign: c.Sign, UUID: UUID}, block)
		}
	}
	return dup
}

func (c *Client) loadTracker(UUID string) *RangeTracker {
	if v, ok := c.trackers.Load(UUID); ok {
		return v.(*RangeTracker)
	}
	v, _ := c.trackers.LoadOrStore(UUID, new(RangeTracker))
	return v.(*RangeTracker)
}

func (c *Client) check(UUID string, block uint32) bool {
	t := c.loadTracker(UUID)
	return t.Contains(MonoRange(block))
}

func (c *Client) ack(addr net.Addr, UUID string, block uint32) {
	ack := Ack{Block: block, UUID: UUID}
	pkt, err := ack.Marshal()
	if err = c.conn.SetWriteDeadline(time.Now().Add(_TIMEOUT)); err != nil {
		log.Printf("[%s] ack failed: %v", addr, err)
	}
	if _, err = c.conn.WriteTo(pkt, addr); err != nil {
		log.Printf("[%s] ack failed: %v", addr, err)
	}
}

func (c *Client) nck(f file) {
	nck := Nck{Block: c.nextID(), FileId: f.req.FileId, UUID: c.ID(), ranges: f.Get()}
	if err := c.send(&nck); err != nil {
		log.Printf("nck failed: %v", err)
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
	c.remoteAddr, _ = net.ResolveUDPAddr("udp", addr)
}

func (c *Client) write(bytes []byte, block uint32, retryable bool) error {
	var (
		ackCtx, ack     = context.WithCancel(context.Background())
		retryCtx, retry = context.WithCancel(context.Background())
		start           time.Time
		code            = OpCode(binary.BigEndian.Uint16(bytes[:2]))
		err             error
	)
	defer ack()
	defer retry()

	c.ackHandlers.Store(block, ack)
	defer c.ackHandlers.Delete(block)

	if retryable {
		c.retryHandlers.Store(block, retry)
		defer c.retryHandlers.Delete(block)
	}

	for i := uint8(0); i < c.Retries; i++ {
		if err = c.conn.SetWriteDeadline(time.Now().Add(_TIMEOUT)); err != nil {
			log.Printf("[%v] write failed: %v", code.String(), err)
		}
		if i%2 == 0 {
			log.Printf("[%v] send packet: %v", code.String(), block)
			start = time.Now()
			_, err = c.conn.WriteTo(bytes, c.remoteAddr)
		} else {
			log.Printf("[%v] send check: %v", code.String(), block)
			check := Check{Block: block}
			pkt, _ := check.Marshal()
			_, err = c.conn.WriteTo(pkt, c.remoteAddr)
		}
		if err != nil {
			log.Printf("[%v] write failed: %v", code.String(), err)
			if errors.Is(err, syscall.EPIPE) {
				c.reconnect()
			}
			continue
		}
	WAIT:
		timer := time.After(c.Timeout)
		log.Printf("current timeout: %dms", c.Timeout/time.Millisecond)
		select {
		case <-ackCtx.Done():
			elapsed := time.Since(start)
			c.Timeout = c.Timeout*8/10 + elapsed*2/10
			return nil
		case <-timer:
			log.Printf("[%v] packet: %v timeout", code.String(), block)
			c.Timeout = min(3*time.Second, c.Timeout*108/100+10*time.Millisecond)
		case <-retryCtx.Done():
			log.Printf("retry")
			retryCtx, retry = context.WithCancel(context.Background())
			c.retryHandlers.Store(block, retry)
			if err = c.conn.SetWriteDeadline(time.Now().Add(_TIMEOUT)); err != nil {
				log.Printf("[%v] write failed: %v", code.String(), err)
			}
			if _, err = c.conn.WriteTo(bytes, c.remoteAddr); err != nil {
				log.Printf("[%v] write failed: %v", code.String(), err)
			}
			goto WAIT
		}
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(_TIMEOUT))
	return errors.New(fmt.Sprintf("[%v] exhausted retries", code.String()))
}

var DefaultClient *Client

const (
	_SERVER  = ""
	_TIMEOUT = 3 * time.Second
)
