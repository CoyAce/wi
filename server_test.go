package wi

import (
	"bytes"
	"encoding/hex"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestSignMarshal(t *testing.T) {
	sign := "default"
	uuid := "mock#00001"
	s := Sign{Sign: sign, UUID: uuid}
	pkt, _ := s.Marshal()
	t.Logf("pkt: [%v]", hex.EncodeToString(pkt))
}

func TestMsgMarshal(t *testing.T) {
	sign := "default"
	uuid := "mock#00001"
	s := Sign{Sign: sign, UUID: uuid}
	msg := SignedMessage{s, []byte("hello")}
	pkt, _ := msg.Marshal()
	t.Logf("pkt: [%v]", hex.EncodeToString(pkt))
}

func TestListenPacketUDP(t *testing.T) {
	// init data
	signAck := Ack{Block: 0}
	signAckBytes, err := signAck.Marshal()
	msgAck := Ack{Block: 0}
	msgAckBytes, err := msgAck.Marshal()

	sign := "test"

	uuid := "#00001"
	clientSign := Sign{0, sign, uuid}
	clientSignPkt, _ := clientSign.Marshal()
	t.Logf("sign pkt: [%v]", hex.EncodeToString(clientSignPkt))

	uuidA := "#00002"

	text := "hello beautiful world"
	clientMsg := SignedMessage{Sign: clientSign, Payload: []byte(text)}
	clientMsgPkt, err := clientMsg.Marshal()

	textA := "beautiful world"

	s, serverAddr := setUpServer(t)
	client, err := setUpClient(t)
	defer func() { _ = client.Close() }()

	// test send sign, server should ack
	buf := make([]byte, DatagramSize)
	sAddr, _ := net.ResolveUDPAddr("udp", serverAddr)

	// send sign
	_, err = client.WriteTo(clientSignPkt, sAddr)
	if err != nil {
		t.Fatal(err)
	}

	// read ack
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err := client.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(signAckBytes, buf[:n]) {
		t.Errorf("expected reply %q; actual reply %q", signAckBytes, buf[:n])
	}

	// clientA send text "beautiful world"
	clientA := Client{
		Config:   Config{ServerAddr: serverAddr},
		Status:   make(chan struct{}),
		Identity: Identity{UUID: uuidA, Sign: sign},
	}
	go func() {
		clientA.ListenAndServe("127.0.0.1:")
	}()
	clientA.Ready()
	clientA.SendText(textA)

	// client read text. client should receive "beautiful world" from clientA
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err = client.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	var m SignedMessage
	err = m.Unmarshal(buf[:n])
	if err != nil {
		t.Fatal(err)
	}
	if string(m.Payload) != textA {
		t.Errorf("expected %q; actual reply %q", textA, string(m.Payload))
	}
	if m.UUID != uuidA {
		t.Errorf("expected %q; actual reply %q", uuidA, string(m.Payload))
	}
	// send ack
	client.WriteTo(msgAckBytes, sAddr)

	// client send text, server should reply ack
	_, err = client.WriteTo(clientMsgPkt, sAddr)
	if err != nil {
		t.Fatal(err)
	}
	// read ack
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err = client.ReadFrom(buf)
	if !bytes.Equal(msgAckBytes, buf[:n]) {
		t.Errorf("expected reply %q; actual reply %q", signAckBytes, buf[:n])
	}

	s.addFile(WriteReq{FileId: 1, UUID: uuidA})
	// client send nck, server should reply ack
	nck := Nck{FileId: 1, ranges: []Range{{1, 1}}}
	nckPkt, _ := nck.Marshal()
	_, err = client.WriteTo(nckPkt, sAddr)
	// read ack
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err = client.ReadFrom(buf)
	if !bytes.Equal(msgAckBytes, buf[:n]) {
		t.Errorf("expected reply %q; actual reply %q", signAckBytes, buf[:n])
	}
}

type byteReadSeekCloser struct {
	*bytes.Reader
}

func (b *byteReadSeekCloser) Close() error {
	b.Reader = nil
	return nil
}

func BytesToReadSeekCloser(data []byte) io.ReadSeekCloser {
	return &byteReadSeekCloser{
		Reader: bytes.NewReader(data),
	}
}

func TestPubSub(t *testing.T) {
	s, serverAddr := setUpServer(t)
	pub := newClient(serverAddr, "pub")
	sub := newClient(serverAddr, "sub")
	other := newClient(serverAddr, "other")
	_ = pub.PublishFile("test.txt", 5, 1)
	select {
	case wrq := <-sub.FileMessages:
		if wrq.FileId != 1 {
			t.Errorf("expected file id 1; actual file id %d", wrq.FileId)
		}
		if wrq.Filename != "test.txt" {
			t.Errorf("expected filename \"test.txt\"; actual filename %q", wrq.Filename)
		}
		q := <-other.FileMessages
		if !reflect.DeepEqual(wrq, q) {
			t.Errorf("expected file message %v; actual file message %v", wrq, q)
		}
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timeout")
	}
	_ = sub.SubscribeFile(1, "pub", func(p int, s int) {
		log.Printf("progress %d, speed %d", p, s)
	})
	select {
	case rrq := <-pub.SubMessages:
		if rrq.FileId != 1 {
			t.Errorf("expected file id 1; actual file id %d", rrq.FileId)
		}
		if rrq.Subscriber != "sub" {
			t.Errorf("expected subscriber \"sub\"; actual subscriber %q", rrq.Subscriber)
		}
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timeout")
	}

	_ = pub.PublishContent("test.txt", 5, 1, BytesToReadSeekCloser([]byte("hello")))
	select {
	case wrq := <-sub.FileMessages:
		if wrq.FileId != 1 {
			t.Errorf("expected file id 1; actual file id %d", wrq.FileId)
		}
		if wrq.Code != OpContent {
			t.Errorf("expected op content %d; actual op content %d", OpContent, wrq.Code)
		}
		f, err := os.Open("./test/pub/test.txt")
		if err != nil {
			t.Fatal(err)
		}
		text, err := io.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}
		if string(text) != "hello" {
			t.Errorf("expected \"hello\"; actual %q", string(text))
		}
		_ = sub.UnsubscribeFile(1, "pub")
		subs, ok := s.pubMap.Load(FilePair{FileId: wrq.FileId, UUID: wrq.UUID})
		if !ok {
			t.Errorf("expected pub to have file id %d", wrq.FileId)
		}
		found := false
		subs.(*sync.Map).Range(func(k, v interface{}) bool {
			if k.(string) == "sub" {
				found = true
			}
			return true
		})
		if found {
			t.Errorf("sub should be removed")
		}
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timeout")
	}
}

func TestSendFile(t *testing.T) {
	_, serverAddr := setUpServer(t)
	sender := newClient(serverAddr, "sender")
	receiver1 := newClient(serverAddr, "receiver1")
	receiver2 := newClient(serverAddr, "receiver2")
	_ = sender.SendText("sync")
	data := []byte("This is a file")
	content := BytesToReadSeekCloser(data)
	err := sender.SendFile(content, OpSyncIcon, 1, "test.txt", uint64(len(data)), 0)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case wrq := <-receiver1.FileMessages:
		if wrq.FileId != 1 {
			t.Errorf("expected file id 1; actual file id %d", wrq.FileId)
		}
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timeout")
	}
	select {
	case wrq := <-receiver2.FileMessages:
		if wrq.FileId != 1 {
			t.Errorf("expected file id 1; actual file id %d", wrq.FileId)
		}
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timeout")
	}
}

func TestUnknownUser(t *testing.T) {
	server, serverAddr := setUpServer(t)
	receiver := newClient(serverAddr, "receiver")
	sender := newClient(serverAddr, "sender")
	server.removeByUUID("sender")
	err := sender.SendText("hello")
	if err != nil {
		t.Fatal(err)
	}
	msg := <-receiver.SignedMessages
	text := string(msg.Payload)
	if text != "hello" {
		t.Errorf("expected \"hello\"; actual %q", text)
	}
	err = receiver.SendText("hello")
	if err != nil {
		t.Fatal(err)
	}
	msg = <-sender.SignedMessages
	text = string(msg.Payload)
	if text != "hello" {
		t.Errorf("expected \"hello\"; actual %q", text)
	}
}

func TestSignedMessage(t *testing.T) {
	msg := SignedMessage{Sign: Sign{Block: 1, Sign: "default", UUID: "test"}, Payload: []byte("hello")}
	pkt, err := msg.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var m SignedMessage
	err = m.Unmarshal(pkt)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(msg, m) {
		t.Errorf("expected %v; actual %v", msg, m)
	}
}

func TestSignOut(t *testing.T) {
	server, serverAddr := setUpServer(t)
	sender := newClient(serverAddr, "sender")
	time.Sleep(1 * time.Millisecond)
	if server.findAddrByUUID("sender") == "" {
		t.Errorf("sender not found by UUID")
	}
	sender.SignOut()
	time.Sleep(1 * time.Millisecond)
	if server.findAddrByUUID("sender") != "" {
		t.Errorf("sign out failed")
	}
	_ = sender.SendText("hello")
	if server.findAddrByUUID("sender") == "" {
		t.Errorf("should retry sign")
	}
}

func TestSyncMap(t *testing.T) {
	rrq := ReadReq{Code: OpSubscribe, Subscriber: "sub"}
	m := sync.Map{}
	m.Store(rrq.Subscriber, rrq)
	m.Range(func(k, v interface{}) bool {
		r := v.(ReadReq)
		r.Code = OpContent
		return true
	})
	sub, ok := m.Load(rrq.Subscriber)
	if !ok {
		t.Fatal("expected subscriber")
	}
	if sub.(ReadReq).Code != OpSubscribe {
		t.Errorf("expected OpSubscribe; actual %#v", sub)
	}
}

func TestOpCode(t *testing.T) {
	s := OpSign
	op := OpSignOut
	pkt, err := op.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if s.Unmarshal(pkt) == nil {
		t.Errorf("expected op sign out; actual %#v", s)
	}
	if op.Unmarshal(pkt) != nil {
		t.Errorf("expected op sign out; actual %#v", op)
	}
}

func newClient(serverAddr string, UUID string) *Client {
	client := Client{
		Config:   Config{ExternalDir: "./test", ServerAddr: serverAddr},
		Identity: Identity{UUID: UUID, Sign: "default"},
		Status:   make(chan struct{}),
	}
	go func() {
		client.ListenAndServe("127.0.0.1:")
	}()
	client.Ready()
	return &client
}

func setUpClient(t *testing.T) (net.PacketConn, error) {
	client, err := net.ListenPacket("udp", "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
	}
	return client, err
}

func setUpServer(t *testing.T) (*Server, string) {
	s := Server{}
	port := rand.Intn(40000) + 10000
	serverAddr := "127.0.0.1:" + strconv.Itoa(port)
	go func() {
		t.Error(s.ListenAndServe(serverAddr))
	}()
	return &s, serverAddr
}
