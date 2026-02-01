package main

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
	OpSignedMSG
	OpAck
	OpNck
	OpErr
	OpSyncIcon
	OpSendImage
	OpSendGif
	OpSendVoice
	OpAudioCall
	OpAcceptAudioCall
	OpEndAudioCall
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
}

type WriteReq struct {
	Code     OpCode
	FileId   uint32
	UUID     string
	Filename string
	Size     uint64
	Duration uint64
}

func (q *WriteReq) Marshal() ([]byte, error) {
	size := 2 + 4 + len(q.UUID) + 1 + len(q.Filename) + 1 + 8 + 8
	b := new(bytes.Buffer)
	b.Grow(size)

	if !wrqSet[q.Code] {
		return nil, errors.New("invalid WRQ")
	}

	err := binary.Write(b, binary.BigEndian, q.Code) // write operation code
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

	return b.Bytes(), nil
}

func (q *WriteReq) Unmarshal(p []byte) error {
	r := bytes.NewBuffer(p)

	err := binary.Read(r, binary.BigEndian, &q.Code) // read operation code
	if err != nil {
		return err
	}

	if !wrqSet[q.Code] {
		return errors.New("invalid WRQ")
	}

	err = binary.Read(r, binary.BigEndian, &q.FileId) // read file id
	if err != nil {
		return errors.New("invalid WRQ")
	}

	q.UUID, err = readString(r)
	if err != nil {
		return errors.New("invalid WRQ")
	}

	q.Filename, err = readString(r)
	if err != nil {
		return errors.New("invalid WRQ")
	}

	err = binary.Read(r, binary.BigEndian, &q.Size) // read size
	if err != nil {
		return errors.New("invalid WRQ")
	}

	err = binary.Read(r, binary.BigEndian, &q.Duration) // read duration
	if err != nil {
		return errors.New("invalid WRQ")
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
		return errors.New("invalid DATA")
	}

	var code OpCode

	err := binary.Read(bytes.NewReader(p[:2]), binary.BigEndian, &code) // read operation code
	if err != nil || code != OpData {
		return errors.New("invalid DATA")
	}

	err = binary.Read(bytes.NewReader(p[2:6]), binary.BigEndian, &d.FileId) // read file id
	if err != nil {
		return errors.New("invalid DATA")
	}

	err = binary.Read(bytes.NewReader(p[6:10]), binary.BigEndian, &d.Block) // read block number

	d.Payload = bytes.NewBuffer(p[10:])

	return nil
}

type Sign struct {
	Sign string
	UUID string
}

func (sign *Sign) Marshal() ([]byte, error) {
	b := new(bytes.Buffer)
	b.Grow(2 + len(sign.Sign) + 1 + len(sign.UUID) + 1)

	err := binary.Write(b, binary.BigEndian, OpSign) // write operation code
	if err != nil {
		return nil, err
	}

	err = writeString(b, sign.Sign) // write Sign
	if err != nil {
		return nil, err
	}

	err = writeString(b, sign.UUID) // write UUID
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (sign *Sign) Unmarshal(p []byte) error {
	r := bytes.NewBuffer(p)
	var opcode OpCode
	err := binary.Read(r, binary.BigEndian, &opcode)
	if err != nil || opcode != OpSign {
		return errors.New("invalid DATA")
	}

	sign.Sign, err = readString(r) // read sign
	if err != nil {
		return errors.New("invalid DATA")
	}

	sign.UUID, err = readString(r) // read UUID
	if err != nil {
		return errors.New("invalid DATA")
	}
	return nil
}

type SignedMessage struct {
	Sign    Sign
	Payload []byte
}

func (m *SignedMessage) Marshal() ([]byte, error) {
	size := 2 + len(m.Sign.Sign) + 1 + len(m.Sign.UUID) + 1 + len(m.Payload)
	if size > DatagramSize {
		return nil, errors.New("packet is greater than DatagramSize")
	}
	b := new(bytes.Buffer)
	b.Grow(size)

	err := binary.Write(b, binary.BigEndian, OpSignedMSG) // write operation code
	if err != nil {
		return nil, err
	}

	sign, err := m.Sign.Marshal()
	if err != nil {
		return nil, err
	}
	b.Write(sign[2:])

	b.Write(m.Payload)
	return b.Bytes(), nil
}

func (m *SignedMessage) Unmarshal(p []byte) error {
	if l := len(p); l < 4 || l > DatagramSize {
		return errors.New("invalid DATA")
	}
	r := bytes.NewBuffer(p)
	var opcode OpCode
	err := binary.Read(r, binary.BigEndian, &opcode)
	if err != nil || opcode != OpSignedMSG {
		return errors.New("invalid DATA")
	}

	m.Sign.Sign, err = readString(r)
	if err != nil {
		return errors.New("invalid DATA")
	}

	m.Sign.UUID, err = readString(r)
	if err != nil {
		return errors.New("invalid DATA")
	}

	m.Payload = r.Bytes()
	return nil
}

type Ack struct {
	SrcOp OpCode
	Block uint32
}

func (a *Ack) Marshal() ([]byte, error) {
	size := 2 + 2 + 4 // operation code + source operation code + block number

	b := new(bytes.Buffer)
	b.Grow(size)

	err := binary.Write(b, binary.BigEndian, uint16(OpAck)) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, uint16(a.SrcOp)) // write source operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, a.Block) // write block number
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (a *Ack) Unmarshal(p []byte) error {
	var code OpCode
	r := bytes.NewReader(p)

	err := binary.Read(r, binary.BigEndian, &code) // read operation code
	if err != nil {
		return errors.New("invalid DATA")
	}

	if code != OpAck {
		return errors.New("invalid DATA")
	}

	err = binary.Read(r, binary.BigEndian, &a.SrcOp) // read source operation code
	if err != nil {
		return errors.New("invalid DATA")
	}

	return binary.Read(r, binary.BigEndian, &a.Block) // read block number
}

type ErrCode uint16

const (
	ErrUnknown ErrCode = iota
	ErrIllegalOp
)

type Nck struct {
	FileId uint32
	ranges []Range
}

func (n *Nck) Marshal() ([]byte, error) {
	// operation code + fileId  + ranges count + len(ranges) * 4
	size := 2 + 4 + 1 + len(n.ranges)*4
	b := new(bytes.Buffer)
	b.Grow(size)

	err := binary.Write(b, binary.BigEndian, uint16(OpNck)) // write operation code
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, n.FileId) // write file id
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, byte(len(n.ranges))) // write ranges count
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
		return err
	}

	if code != OpNck {
		return errors.New("invalid DATA")
	}

	err = binary.Read(r, binary.BigEndian, &n.FileId) // read file id
	if err != nil {
		return errors.New("invalid DATA")
	}

	err = binary.Read(r, binary.BigEndian, &l) // read ranges count
	if err != nil {
		return errors.New("invalid DATA")
	}

	n.ranges = make([]Range, 0, l)
	for i := 0; i < int(l); i++ {
		err = rg.Unmarshal(r)
		if err != nil {
			return errors.New("invalid DATA")
		}
		n.ranges = append(n.ranges, rg)
	}
	return nil
}

type Range struct {
	start, end uint32
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

func (r *Range) contains(v Range) bool {
	return v.start >= r.start && v.end <= r.end
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
