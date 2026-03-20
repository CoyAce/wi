package wi

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

const (
	DatagramSize = 1460
	BlockSize    = DatagramSize - 2 - 4 - 4 // DataGramSize - OpCode - FileId - Block
)

type OpCode uint16

const (
	OpRRQ OpCode = iota + 1
	OpWRQ
	OpData
	OpSign
	OpSignOut
	OpSignedMSG
	OpAck
	OpNck
	OpErr
	OpSyncIcon
	OpSyncName
	OpSendImage
	OpSendGif
	OpSendVoice
	OpAudioCall
	OpAcceptAudioCall
	OpEndAudioCall
	OpPublish
	OpSubscribe
	OpUnsubscribe
	OpContent
	OpReady
	OpCheck
	OpReadIcon
	OpPull
	OpReply
	OpDiscovery
	OpDiscoveryResp
	OpSack
	OpFin
	OpReq
	OpLongText
)

var wrqSet = map[OpCode]bool{
	OpWRQ:             true,
	OpSyncIcon:        true,
	OpSendImage:       true,
	OpSendGif:         true,
	OpSendVoice:       true,
	OpAudioCall:       true,
	OpAcceptAudioCall: true,
	OpEndAudioCall:    true,
	OpPublish:         true,
	OpContent:         true,
	OpReady:           true,
}

var rrqSet = map[OpCode]bool{
	OpRRQ:         true,
	OpSubscribe:   true,
	OpUnsubscribe: true,
	OpReadIcon:    true,
}

func (op *OpCode) String() string {
	switch *op {
	case OpRRQ:
		return "OpRRQ"
	case OpWRQ:
		return "OpWRQ"
	case OpData:
		return "OpData"
	case OpSign:
		return "OpSign"
	case OpSignOut:
		return "SignOut"
	case OpSignedMSG:
		return "SignedMSG"
	case OpAck:
		return "ACK"
	case OpNck:
		return "NCK"
	case OpErr:
		return "Err"
	case OpSyncIcon:
		return "SyncIcon"
	case OpSyncName:
		return "SyncName"
	case OpSendImage:
		return "SendImage"
	case OpSendGif:
		return "SendGif"
	case OpSendVoice:
		return "SendVoice"
	case OpAudioCall:
		return "AudioCall"
	case OpAcceptAudioCall:
		return "AcceptAudioCall"
	case OpEndAudioCall:
		return "EndAudioCall"
	case OpPublish:
		return "Publish"
	case OpSubscribe:
		return "Subscribe"
	case OpUnsubscribe:
		return "Unsubscribe"
	case OpContent:
		return "Content"
	case OpReady:
		return "Ready"
	case OpCheck:
		return "Check"
	case OpReadIcon:
		return "ReadIcon"
	case OpPull:
		return "Pull"
	case OpReply:
		return "Reply"
	case OpDiscovery:
		return "Discovery"
	case OpDiscoveryResp:
		return "DiscoveryResp"
	case OpSack:
		return "SACK"
	case OpFin:
		return "FIN"
	case OpReq:
		return "REQ"
	case OpLongText:
		return "LongText"
	}
	return "unknown"
}

func (op *OpCode) Marshal() ([]byte, error) {
	b := new(bytes.Buffer)
	b.Grow(2)

	err := binary.Write(b, binary.BigEndian, op) // write operation code
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (op *OpCode) Unmarshal(p []byte) error {
	b := bytes.NewBuffer(p)
	var code OpCode

	err := binary.Read(b, binary.BigEndian, &code) // read operation code
	if err != nil || code != *op {
		return InvalidData
	}
	return nil
}

type Req interface {
	ID() uint32
	ReqBody
}

type ReqBody interface {
	Marshal() ([]byte, error)
	Unmarshal(p []byte) error
}

type ReadReq struct {
	Code       OpCode
	Block      uint32
	FileId     uint32
	Publisher  string
	Subscriber string
}

func (r *ReadReq) ID() uint32 {
	return r.Block
}

func (r *ReadReq) Marshal() ([]byte, error) {
	size := 2 + 4 + 4 + len(r.Publisher) + 1 + len(r.Subscriber) + 1
	b := new(bytes.Buffer)
	b.Grow(size)

	if !rrqSet[r.Code] {
		return nil, InvalidRRQ
	}

	err := binary.Write(b, binary.BigEndian, r.Code) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, r.Block) // write block number
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, r.FileId) // write file id
	if err != nil {
		return nil, err
	}

	err = writeString(b, r.Publisher) // write publisher
	if err != nil {
		return nil, err
	}

	err = writeString(b, r.Subscriber) // write subscriber
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (r *ReadReq) Unmarshal(p []byte) error {
	b := bytes.NewBuffer(p)

	err := binary.Read(b, binary.BigEndian, &r.Code) // read operation code
	if err != nil {
		return err
	}

	if !rrqSet[r.Code] {
		return InvalidRRQ
	}

	err = binary.Read(b, binary.BigEndian, &r.Block) // read block number
	if err != nil {
		return InvalidRRQ
	}

	err = binary.Read(b, binary.BigEndian, &r.FileId) // read file id
	if err != nil {
		return InvalidRRQ
	}

	r.Publisher, err = readString(b)
	if err != nil {
		return InvalidRRQ
	}

	r.Subscriber, err = readString(b)
	if err != nil {
		return InvalidRRQ
	}

	return nil
}

type FilePair struct {
	FileId uint32
	UUID   string
}

type WriteReq struct {
	Code      OpCode
	Block     uint32
	FileId    uint32
	UUID      string
	Filename  string
	Size      uint64
	Duration  uint32
	CreatedAt int64
}

func (q *WriteReq) ID() uint32 {
	return q.Block
}

func (q *WriteReq) Marshal() ([]byte, error) {
	size := 2 + 4 + 4 + len(q.UUID) + 1 + len(q.Filename) + 1 + 8 + 4
	b := new(bytes.Buffer)
	b.Grow(size)

	if !wrqSet[q.Code] {
		return nil, InvalidWRQ
	}

	err := binary.Write(b, binary.BigEndian, q.Code) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, q.Block) // write block number
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, q.FileId) // write file id
	if err != nil {
		return nil, err
	}

	err = writeString(b, q.UUID) // write UUID
	if err != nil {
		return nil, err
	}

	err = writeString(b, q.Filename) // write filename
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, q.Size) // write size
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, q.Duration) // write duration
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, q.CreatedAt) // write create time
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (q *WriteReq) Unmarshal(p []byte) error {
	r := bytes.NewBuffer(p)

	err := binary.Read(r, binary.BigEndian, &q.Code) // read operation code
	if err != nil {
		return err
	}

	if !wrqSet[q.Code] {
		return InvalidWRQ
	}

	err = binary.Read(r, binary.BigEndian, &q.Block) // read block number
	if err != nil {
		return InvalidWRQ
	}

	err = binary.Read(r, binary.BigEndian, &q.FileId) // read file id
	if err != nil {
		return InvalidWRQ
	}

	q.UUID, err = readString(r)
	if err != nil {
		return InvalidWRQ
	}

	q.Filename, err = readString(r)
	if err != nil {
		return InvalidWRQ
	}

	err = binary.Read(r, binary.BigEndian, &q.Size) // read size
	if err != nil {
		return InvalidWRQ
	}

	err = binary.Read(r, binary.BigEndian, &q.Duration) // read duration
	if err != nil {
		return InvalidWRQ
	}

	err = binary.Read(r, binary.BigEndian, &q.CreatedAt) // read create time
	if err != nil {
		return InvalidWRQ
	}

	return nil
}

type Data struct {
	FileId  uint32
	Block   uint32
	Payload io.Reader
}

func (d *Data) Marshal() ([]byte, error) {
	b := new(bytes.Buffer)
	b.Grow(DatagramSize)

	d.Block++ // block numbers increment from 1

	err := binary.Write(b, binary.BigEndian, OpData) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, d.FileId) // write file id
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, d.Block) // write block number
	if err != nil {
		return nil, err
	}

	// write up to BlockSize worth of bytes
	_, err = io.CopyN(b, d.Payload, BlockSize)
	if err != nil && err != io.EOF {
		return nil, err
	}

	return b.Bytes(), nil
}

func (d *Data) Unmarshal(p []byte) error {
	if l := len(p); l < 10 || l > DatagramSize {
		return InvalidData
	}

	var code OpCode

	err := binary.Read(bytes.NewReader(p[:2]), binary.BigEndian, &code) // read operation code
	if err != nil || code != OpData {
		return InvalidData
	}

	err = binary.Read(bytes.NewReader(p[2:6]), binary.BigEndian, &d.FileId) // read file id
	if err != nil {
		return InvalidData
	}

	err = binary.Read(bytes.NewReader(p[6:10]), binary.BigEndian, &d.Block) // read block number

	d.Payload = bytes.NewBuffer(p[10:])

	return nil
}

type SignBody struct {
	Sign string
	UUID string
}

func (s *SignBody) Marshal(b *bytes.Buffer) error {
	err := writeString(b, s.Sign) // write Sign
	if err != nil {
		return err
	}

	err = writeString(b, s.UUID) // write UUID
	if err != nil {
		return err
	}

	return nil
}

func (s *SignBody) Unmarshal(r *bytes.Buffer) (err error) {
	s.Sign, err = readString(r) // read sign
	if err != nil {
		return InvalidData
	}

	s.UUID, err = readString(r) // read UUID
	if err != nil {
		return InvalidData
	}
	return nil
}

type SignReq struct {
	Block uint32
	SignBody
}

func (s *SignReq) Marshal() ([]byte, error) {
	b := new(bytes.Buffer)
	b.Grow(2 + 4 + len(s.Sign) + 1 + len(s.UUID) + 1)

	err := binary.Write(b, binary.BigEndian, OpSign) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, s.Block) // write block number
	if err != nil {
		return nil, err
	}

	err = s.SignBody.Marshal(b) // write sign body
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (s *SignReq) Unmarshal(p []byte) error {
	r := bytes.NewBuffer(p)
	var opcode OpCode
	err := binary.Read(r, binary.BigEndian, &opcode) // read operation code
	if err != nil || opcode != OpSign {
		return InvalidData
	}

	err = binary.Read(r, binary.BigEndian, &s.Block) // read block number
	if err != nil {
		return InvalidData
	}

	return s.SignBody.Unmarshal(r) // read sign body
}

type SignedMessage struct {
	SignReq
	CreatedAt int64
	Payload   []byte
}

func (m *SignedMessage) Marshal() ([]byte, error) {
	size := 2 + len(m.SignReq.Sign) + 1 + len(m.SignReq.UUID) + 1 + len(m.Payload)
	if size > DatagramSize {
		return nil, errors.New("packet is greater than DatagramSize")
	}
	b := new(bytes.Buffer)
	b.Grow(size)

	err := binary.Write(b, binary.BigEndian, OpSignedMSG) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, m.Block) // write block number
	if err != nil {
		return nil, err
	}

	err = m.SignBody.Marshal(b) // write sign body
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, m.CreatedAt) // write create time
	if err != nil {
		return nil, err
	}

	b.Write(m.Payload)
	return b.Bytes(), nil
}

func (m *SignedMessage) Unmarshal(p []byte) error {
	if l := len(p); l < 8 || l > DatagramSize {
		return InvalidData
	}
	r := bytes.NewBuffer(p)
	var opcode OpCode
	err := binary.Read(r, binary.BigEndian, &opcode)
	if err != nil || opcode != OpSignedMSG {
		return InvalidData
	}

	err = binary.Read(r, binary.BigEndian, &m.Block)
	if err != nil {
		return InvalidData
	}

	err = m.SignBody.Unmarshal(r) // read sign body
	if err != nil {
		return InvalidData
	}

	err = binary.Read(r, binary.BigEndian, &m.CreatedAt) // read create time
	if err != nil {
		return InvalidData
	}

	m.Payload = r.Bytes()
	return nil
}

type CtrlReq struct {
	Code   OpCode
	Block  uint32
	Target string
	UUID   string
}

func (n *CtrlReq) Marshal() ([]byte, error) {
	size := 2 + 4 + len(n.Target) + 1 + len(n.UUID) + 1 // operation code  + block number + target UUID + UUID

	b := new(bytes.Buffer)
	b.Grow(size)

	err := binary.Write(b, binary.BigEndian, n.Code) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, n.Block) // write block number
	if err != nil {
		return nil, err
	}

	err = writeString(b, n.Target) //  write Target
	if err != nil {
		return nil, err
	}

	err = writeString(b, n.UUID) //  write UUID
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (n *CtrlReq) Unmarshal(p []byte) error {
	r := bytes.NewBuffer(p)

	err := binary.Read(r, binary.BigEndian, &n.Code) // read operation code
	if err != nil {
		return InvalidData
	}

	if n.Code != OpSyncName {
		return InvalidData
	}

	err = binary.Read(r, binary.BigEndian, &n.Block) // read block number
	if err != nil {
		return InvalidData
	}

	n.Target, err = readString(r) // read target uuid
	if err != nil {
		return InvalidData
	}

	n.UUID, err = readString(r) // read uuid
	if err != nil {
		return InvalidData
	}

	return nil
}

type ReqHeader struct {
	Block uint32
	ReqID uint32
	UUID  string
}

func (s *ReqHeader) Marshal(b *bytes.Buffer) error {
	err := binary.Write(b, binary.BigEndian, s.Block) // write block number
	if err != nil {
		return err
	}

	err = binary.Write(b, binary.BigEndian, s.ReqID) // write req id
	if err != nil {
		return err
	}

	err = writeString(b, s.UUID) // write uuid
	if err != nil {
		return err
	}

	return nil
}

func (s *ReqHeader) Unmarshal(b *bytes.Buffer) error {
	err := binary.Read(b, binary.BigEndian, &s.Block) // read block number
	if err != nil {
		return InvalidData
	}

	err = binary.Read(b, binary.BigEndian, &s.ReqID) // read req id
	if err != nil {
		return InvalidData
	}

	s.UUID, err = readString(b) // read uuid
	if err != nil {
		return InvalidData
	}

	return nil
}

type Fin struct {
	ReqHeader
}

func (s *Fin) Marshal() ([]byte, error) {
	size := 2 + 4 + 4 + len(s.UUID) + 1
	b := new(bytes.Buffer)
	b.Grow(size)

	err := binary.Write(b, binary.BigEndian, uint16(OpFin)) // write operation code
	if err != nil {
		return nil, err
	}

	err = s.ReqHeader.Marshal(b) // write header
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (s *Fin) Unmarshal(p []byte) error {
	var code OpCode
	b := bytes.NewBuffer(p)

	err := binary.Read(b, binary.BigEndian, &code) // read operation code
	if err != nil {
		return InvalidData
	}

	if code != OpFin {
		return InvalidData
	}

	return s.ReqHeader.Unmarshal(b)
}

type Check struct {
	Block uint32
	UUID  string
}

func (c *Check) Marshal() ([]byte, error) {
	size := 2 + 4 + len(c.UUID) + 1 // operation code  + block number + uuid

	b := new(bytes.Buffer)
	b.Grow(size)

	err := binary.Write(b, binary.BigEndian, uint16(OpCheck)) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, c.Block) // write block number
	if err != nil {
		return nil, err
	}

	err = writeString(b, c.UUID) //  write UUID
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (c *Check) Unmarshal(p []byte) error {
	var code OpCode
	r := bytes.NewBuffer(p)

	err := binary.Read(r, binary.BigEndian, &code) // read operation code
	if err != nil {
		return InvalidData
	}

	if code != OpCheck {
		return InvalidData
	}

	err = binary.Read(r, binary.BigEndian, &c.Block) // read block number
	if err != nil {
		return InvalidData
	}

	c.UUID, err = readString(r)
	if err != nil {
		return InvalidData
	}

	return nil
}

type Sack struct {
	ReqHeader
}

func (s *Sack) ID() uint32 {
	return s.Block
}

func (s *Sack) Marshal() ([]byte, error) {
	size := 2 + 4 + 4 + len(s.UUID) + 1 // operation code  + block number + req id + uuid

	b := new(bytes.Buffer)
	b.Grow(size)

	err := binary.Write(b, binary.BigEndian, uint16(OpSack)) // write operation code
	if err != nil {
		return nil, err
	}

	err = s.ReqHeader.Marshal(b) // write header
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (s *Sack) Unmarshal(p []byte) error {
	var code OpCode
	b := bytes.NewBuffer(p)

	err := binary.Read(b, binary.BigEndian, &code) // read operation code
	if err != nil {
		return InvalidData
	}

	if code != OpSack {
		return InvalidData
	}

	return s.ReqHeader.Unmarshal(b)
}

type Ack struct {
	Block uint32
	UUID  string
}

func (a *Ack) Marshal() ([]byte, error) {
	size := 2 + 4 + len(a.UUID) + 1 // operation code  + block number + uuid

	b := new(bytes.Buffer)
	b.Grow(size)

	err := binary.Write(b, binary.BigEndian, uint16(OpAck)) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, a.Block) // write block number
	if err != nil {
		return nil, err
	}

	err = writeString(b, a.UUID) // write uuid
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (a *Ack) Unmarshal(p []byte) error {
	var code OpCode
	r := bytes.NewBuffer(p)

	err := binary.Read(r, binary.BigEndian, &code) // read operation code
	if err != nil {
		return InvalidData
	}

	if code != OpAck {
		return InvalidData
	}

	err = binary.Read(r, binary.BigEndian, &a.Block) // read block number
	if err != nil {
		return InvalidData
	}

	a.UUID, err = readString(r) // read uuid
	if err != nil {
		return InvalidData
	}

	return nil
}

type ErrCode uint16

const (
	ErrUnknown ErrCode = iota
	ErrIllegalOp
	ErrUnknownUser
)

func (e *ErrCode) Marshal() ([]byte, error) {
	const size = 2 + 2
	b := new(bytes.Buffer)
	b.Grow(size)

	err := binary.Write(b, binary.BigEndian, uint16(OpErr)) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, e) // write error code
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (e *ErrCode) Unmarshal(p []byte) error {
	var (
		code OpCode
		r    = bytes.NewBuffer(p)
	)
	err := binary.Read(r, binary.BigEndian, &code) // read operation code
	if err != nil {
		return err
	}

	if code != OpErr {
		return InvalidData
	}

	err = binary.Read(r, binary.BigEndian, e) // read error code
	if err != nil {
		return err
	}

	return nil
}

type Nck struct {
	Block  uint32
	FileId uint32
	UUID   string
	ranges []Range
}

func (n *Nck) ID() uint32 {
	return n.Block
}

func (n *Nck) Marshal() ([]byte, error) {
	// operation code + Block + fileId  + ranges count + len(ranges) * 4
	baseSize := 2 + 4 + 4 + 1 + len(n.UUID) + 1
	size := baseSize + len(n.ranges)*8
	b := new(bytes.Buffer)
	if size > DatagramSize {
		m := (DatagramSize - baseSize) / 8
		n.ranges = n.ranges[:m]
		size = DatagramSize
	}
	b.Grow(size)

	err := binary.Write(b, binary.BigEndian, uint16(OpNck)) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, n.Block) // write block number
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, n.FileId) // write file id
	if err != nil {
		return nil, err
	}

	err = writeString(b, n.UUID) // write uuid
	if err != nil {
		return nil, err
	}

	err = b.WriteByte(byte(len(n.ranges))) // write ranges count
	if err != nil {
		return nil, err
	}

	for _, r := range n.ranges {
		b.Write(r.Marshal())
	}

	return b.Bytes(), nil
}

func (n *Nck) Unmarshal(p []byte) error {
	var (
		code OpCode
		l    byte
		r    = bytes.NewBuffer(p)
		rg   = Range{}
	)

	err := binary.Read(r, binary.BigEndian, &code) // read operation code
	if err != nil {
		return InvalidData
	}

	if code != OpNck {
		return InvalidData
	}

	err = binary.Read(r, binary.BigEndian, &n.Block) // read block id
	if err != nil {
		return InvalidData
	}

	err = binary.Read(r, binary.BigEndian, &n.FileId) // read file id
	if err != nil {
		return InvalidData
	}

	n.UUID, err = readString(r) // read uuid
	if err != nil {
		return InvalidData
	}

	l, err = r.ReadByte() // read ranges count
	if err != nil {
		return InvalidData
	}

	n.ranges = make([]Range, 0, l)
	for i := 0; i < int(l); i++ {
		err = rg.Unmarshal(r)
		if err != nil {
			return InvalidData
		}
		n.ranges = append(n.ranges, rg)
	}
	return nil
}

type PullReq struct {
	Block uint32
	SignBody
	Range          // pull range
	ranges []Range // missing packets
}

func (pr *PullReq) ID() uint32 {
	return pr.Block
}

func (pr *PullReq) Marshal() ([]byte, error) {
	baseSize := 2 + 4 + len(pr.Sign) + 1 + len(pr.UUID) + 1 + 8 + 1
	size := baseSize + len(pr.ranges)*8
	b := new(bytes.Buffer)
	b.Grow(size)
	if size > DatagramSize {
		return nil, InvalidData
	}

	err := binary.Write(b, binary.BigEndian, uint16(OpPull)) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, pr.Block) // write block number
	if err != nil {
		return nil, err
	}

	err = pr.SignBody.Marshal(b) // write sign body
	if err != nil {
		return nil, err
	}

	b.Write(pr.Range.Marshal()) // write pull range

	err = binary.Write(b, binary.BigEndian, byte(len(pr.ranges))) // write ranges count
	if err != nil {
		return nil, err
	}

	for _, r := range pr.ranges { // write ranges
		b.Write(r.Marshal())
	}

	return b.Bytes(), nil
}

func (pr *PullReq) Unmarshal(p []byte) error {
	var (
		code OpCode
		l    byte
		r    = bytes.NewBuffer(p)
		rg   = Range{}
	)

	err := binary.Read(r, binary.BigEndian, &code) // read operation code
	if err != nil {
		return InvalidData
	}

	if code != OpPull {
		return InvalidData
	}

	err = binary.Read(r, binary.BigEndian, &pr.Block) // read block id
	if err != nil {
		return InvalidData
	}

	err = pr.SignBody.Unmarshal(r) //  read sign body
	if err != nil {
		return InvalidData
	}

	err = pr.Range.Unmarshal(r)
	if err != nil {
		return InvalidData
	}

	l, err = r.ReadByte() // read ranges count
	if err != nil {
		return InvalidData
	}

	pr.ranges = make([]Range, 0, l)
	for i := 0; i < int(l); i++ {
		err = rg.Unmarshal(r)
		if err != nil {
			return InvalidData
		}
		pr.ranges = append(pr.ranges, rg)
	}
	return nil
}

type ReplyReq struct {
	Block uint32
	SignBody
	ranges []Range
}

func (r *ReplyReq) Marshal() ([]byte, error) {
	baseSize := 2 + 4 + len(r.Sign) + 1 + len(r.UUID) + 1 + 1
	size := baseSize + len(r.ranges)*8
	b := new(bytes.Buffer)
	b.Grow(size)
	if size > DatagramSize {
		return nil, errors.New("too many ranges")
	}

	err := binary.Write(b, binary.BigEndian, uint16(OpReply)) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, r.Block) // write block number
	if err != nil {
		return nil, err
	}

	err = r.SignBody.Marshal(b) // write sign body
	if err != nil {
		return nil, err
	}

	err = b.WriteByte(byte(len(r.ranges))) // write ranges count
	if err != nil {
		return nil, err
	}

	for _, r := range r.ranges { // write ranges
		b.Write(r.Marshal())
	}

	return b.Bytes(), nil
}

func (r *ReplyReq) Unmarshal(p []byte) error {
	var (
		code OpCode
		l    byte
		b    = bytes.NewBuffer(p)
		rg   = Range{}
	)

	err := binary.Read(b, binary.BigEndian, &code) // read operation code
	if err != nil {
		return InvalidData
	}

	if code != OpReply {
		return InvalidData
	}

	err = binary.Read(b, binary.BigEndian, &r.Block) // read block number
	if err != nil {
		return InvalidData
	}

	err = r.SignBody.Unmarshal(b) // read sign body
	if err != nil {
		return InvalidData
	}

	l, err = b.ReadByte() // read ranges count
	if err != nil {
		return InvalidData
	}

	r.ranges = make([]Range, 0, l)
	for i := 0; i < int(l); i++ {
		err = rg.Unmarshal(b)
		if err != nil {
			return InvalidData
		}
		r.ranges = append(r.ranges, rg)
	}
	return nil
}

type ReqData []byte

func (c *ReqData) Code() OpCode {
	return toOpCode((*c)[:2])
}

func toOpCode(buf []byte) OpCode {
	return OpCode(binary.BigEndian.Uint16(buf[:2]))
}

func (c *ReqData) Marshal() ([]byte, error) {
	return *c, nil
}

func (c *ReqData) Unmarshal(p []byte) error {
	*c = p
	return nil
}

type ReqSet []ReliableReq

func (r *ReqSet) ToPackets() ([][]byte, error) {
	packets := make([][]byte, 0, len(*r))
	for _, req := range *r {
		p, err := req.Marshal()
		if err != nil {
			return nil, err
		}
		packets = append(packets, p)
	}
	return packets, nil
}

func ToPackets(p []byte) [][]byte {
	var (
		n   = len(p)
		ret = make([][]byte, 0, n/DatagramSize+1)
	)
	for i := 0; i < n; i += DatagramSize {
		if i+DatagramSize > n {
			ret = append(ret, p[i:])
		} else {
			ret = append(ret, p[i:i+DatagramSize])
		}
	}
	return ret
}

func (r *ReqSet) Marshal() ([]byte, error) {
	size := len(*r) * DatagramSize
	b := new(bytes.Buffer)
	b.Grow(size)
	for _, req := range *r {
		p, err := req.Marshal()
		if err != nil {
			return nil, err
		}
		b.Write(p)
	}
	return b.Bytes(), nil
}

func (r *ReqSet) Unmarshal(p []byte) error {
	var (
		err error
		n   = len(p)
		ret = make([]ReliableReq, 0, n/DatagramSize+1)
	)
	for i := 0; i < n; i += DatagramSize {
		req := ReliableReq{ReqBody: new(ReqData)}
		if i+DatagramSize > n {
			if err = req.Unmarshal(p[i:]); err != nil {
				return err
			}
		} else {
			if err = req.Unmarshal(p[i : i+DatagramSize]); err != nil {
				return err
			}
		}
		ret = append(ret, req)
	}
	*r = ret
	return nil
}

type ReliableReq struct {
	ReqHeader
	ReqBody
	IsFinal bool // true if this is the last packet (piggybacks FIN)
}

func (r *ReliableReq) HeaderSize() int {
	return 2 + 4 + 4 + len(r.UUID) + 1 + 1 // +1 for IsFinal flag
}

func (r *ReliableReq) Marshal() ([]byte, error) {
	body, err := r.ReqBody.Marshal()
	if err != nil {
		return nil, err
	}
	size := r.HeaderSize() + len(body)
	b := new(bytes.Buffer)
	b.Grow(size)
	err = binary.Write(b, binary.BigEndian, uint16(OpReq)) // write operation code
	if err != nil {
		return nil, err
	}
	err = r.ReqHeader.Marshal(b) // write header
	if err != nil {
		return nil, err
	}
	err = b.WriteByte(boolToUint(r.IsFinal)) // Write IsFinal flag
	if err != nil {
		return nil, err
	}
	_, err = b.Write(body) // write body
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (r *ReliableReq) Unmarshal(p []byte) error {
	var (
		code OpCode
		b    = bytes.NewBuffer(p)
	)
	err := binary.Read(b, binary.BigEndian, &code) // read operation code
	if err != nil {
		return InvalidData
	}

	if code != OpReq {
		return InvalidData
	}

	err = r.ReqHeader.Unmarshal(b) // read header
	if err != nil {
		return InvalidData
	}

	isFinalByte, err := b.ReadByte() // Read IsFinal flag
	if err != nil {
		return InvalidData
	}
	r.IsFinal = isFinalByte != 0

	err = r.ReqBody.Unmarshal(b.Bytes()) // read body
	if err != nil {
		return InvalidData
	}

	return nil
}

type DiscoveryFlag byte

const (
	Active DiscoveryFlag = iota
	Online
)

type DiscoveryReq struct {
	Block uint32
	Sign  string
	DiscoveryFlag
}

func (d *DiscoveryReq) ID() uint32 {
	return d.Block
}

func (d *DiscoveryReq) Marshal() ([]byte, error) {
	size := 2 + 4 + len(d.Sign) + 1
	b := new(bytes.Buffer)
	b.Grow(size)

	err := binary.Write(b, binary.BigEndian, uint16(OpDiscovery)) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, d.Block) // write block number
	if err != nil {
		return nil, err
	}

	err = writeString(b, d.Sign) // write sign
	if err != nil {
		return nil, err
	}

	err = b.WriteByte(byte(d.DiscoveryFlag)) // write flag
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (d *DiscoveryReq) Unmarshal(p []byte) error {
	var (
		code OpCode
		r    = bytes.NewBuffer(p)
	)

	err := binary.Read(r, binary.BigEndian, &code) // read operation code
	if err != nil {
		return InvalidData
	}

	if code != OpDiscovery {
		return InvalidData
	}

	err = binary.Read(r, binary.BigEndian, &d.Block) // read block number
	if err != nil {
		return InvalidData
	}

	d.Sign, err = readString(r)
	if err != nil {
		return InvalidData
	}

	f, err := r.ReadByte()
	if err != nil {
		return InvalidData
	}
	d.DiscoveryFlag = DiscoveryFlag(f)

	return nil
}

type LongTextMessage struct {
	CreatedAt int64
	Sign      string
	Text      string
}

func (l *LongTextMessage) Marshal() ([]byte, error) {
	size := 2 + 8 + len(l.Sign) + 1 + len(l.Text) + 1
	if size > DatagramSize {
		return nil, errors.New("text is too long")
	}
	b := new(bytes.Buffer)
	b.Grow(size)
	err := binary.Write(b, binary.BigEndian, uint16(OpLongText)) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, l.CreatedAt) // write created at
	if err != nil {
		return nil, err
	}

	err = writeString(b, l.Sign) // write sign
	if err != nil {
		return nil, err
	}

	err = writeString(b, l.Text) // write text
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (l *LongTextMessage) Unmarshal(p []byte) error {
	var (
		code OpCode
		r    = bytes.NewBuffer(p)
	)
	err := binary.Read(r, binary.BigEndian, &code) // read operation code
	if err != nil {
		return InvalidData
	}

	if code != OpLongText {
		return InvalidData
	}

	err = binary.Read(r, binary.BigEndian, &l.CreatedAt) // read created at
	if err != nil {
		return InvalidData
	}

	l.Sign, err = readString(r) // read sign
	if err != nil {
		return InvalidData
	}

	l.Text, err = readString(r) // read text
	if err != nil {
		return InvalidData
	}

	return nil
}

type DiscoveryResp struct {
	UUIDS []string
}

func (d *DiscoveryResp) Marshal() ([]byte, error) {
	size := 2 + 1 // operation code + len(UUIDS)
	for _, u := range d.UUIDS {
		size += 1 + len(u)
	}
	if size > DatagramSize {
		return nil, errors.New("too many UUIDs")
	}
	b := new(bytes.Buffer)
	b.Grow(size)

	err := binary.Write(b, binary.BigEndian, uint16(OpDiscoveryResp)) //  write operation code
	if err != nil {
		return nil, err
	}

	err = b.WriteByte(byte(len(d.UUIDS))) // write uuid count
	if err != nil {
		return nil, err
	}

	for _, u := range d.UUIDS { // write uuids
		err = writeString(b, u)
		if err != nil {
			return nil, err
		}
	}

	return b.Bytes(), nil
}

func (d *DiscoveryResp) Unmarshal(p []byte) error {
	var (
		code OpCode
		r    = bytes.NewBuffer(p)
		l    byte
		s    string
	)

	err := binary.Read(r, binary.BigEndian, &code) // read operation code
	if err != nil {
		return InvalidData
	}

	if code != OpDiscoveryResp {
		return InvalidData
	}

	l, err = r.ReadByte() // read uuid count
	if err != nil {
		return InvalidData
	}

	d.UUIDS = make([]string, 0, l) // read uuids
	for i := byte(0); i < l; i++ {
		s, err = readString(r)
		if err != nil {
			return InvalidData
		}
		d.UUIDS = append(d.UUIDS, s)
	}
	return nil
}

type Range struct {
	start, end uint32
}

func MonoRange(v uint32) Range {
	return Range{start: v, end: v}
}

func RangeFrom(start, end uint32) Range {
	return Range{start: start, end: end}
}

func (r *Range) Marshal() []byte {
	b := new(bytes.Buffer)
	b.Grow(8)
	_ = binary.Write(b, binary.BigEndian, r.start) // write range start
	_ = binary.Write(b, binary.BigEndian, r.end)   // write range end
	return b.Bytes()
}

func (r *Range) Unmarshal(rd io.Reader) error {
	err := binary.Read(rd, binary.BigEndian, &r.start)
	if err != nil {
		return err
	}
	err = binary.Read(rd, binary.BigEndian, &r.end)
	if err != nil {
		return err
	}
	return nil
}

// contains return r contains v
func (r *Range) contains(v Range) bool {
	return r.start <= v.start && r.end >= v.end
}

func (r *Range) overlaps(v Range) bool {
	return !(r.before(v) || r.after(v))
}

func (r *Range) intersects(v Range) (*Range, bool) {
	start := max(r.start, v.start)
	end := min(r.end, v.end)

	if start > end {
		return nil, false
	}

	return &Range{start: start, end: end}, true
}

func (r *Range) before(v Range) bool {
	return r.end < v.start
}

func (r *Range) after(v Range) bool {
	return r.start > v.end
}

func (r *Range) endWithin(v Range) bool {
	return r.end < v.end && r.end >= v.start
}

func (r *Range) startWithin(v Range) bool {
	return r.start > v.start && r.start <= v.end
}

func (r *Range) Within(v uint32) bool {
	return v >= r.start && v <= r.end
}

var (
	InvalidData = errors.New("invalid DATA")
	InvalidRRQ  = errors.New("invalid RRQ")
	InvalidWRQ  = errors.New("invalid WRQ")
)

func boolToUint(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
