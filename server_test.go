package wi

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestListenPacketUDP(t *testing.T) {
	// init data
	signAck := Ack{Block: 0}
	signAckBytes, err := signAck.Marshal()
	msgAck := Ack{Block: 0}
	msgAckBytes, err := msgAck.Marshal()

	sign := "test"

	uuid := "#00001"
	clientSign := SignReq{0, SignBody{sign, uuid}}
	clientSignPkt, _ := clientSign.Marshal()
	t.Logf("sign pkt: [%v]", hex.EncodeToString(clientSignPkt))

	uuidA := "#00002"

	text := "hello beautiful world"
	clientMsg := SignedMessage{SignReq: clientSign, Payload: []byte(text)}
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
	_ = sub.SendText("ping")
	_ = other.SendText("pong")
	_ = pub.PublishFile("test.txt", 5, 1)
	select {
	case wrq := <-sub.FileMessages:
		if wrq.FileId != 1 {
			t.Errorf("expected file id 1; actual file id %d", wrq.FileId)
		}
		if wrq.Filename != "test.txt" {
			t.Errorf("expected filename \"test.txt\"; actual filename %q", wrq.Filename)
		}
		select {
		case q := <-other.FileMessages:
			if !reflect.DeepEqual(wrq, q) {
				t.Errorf("expected file message %v; actual file message %v", wrq, q)
			}
		case <-time.After(10 * time.Millisecond):
			t.Errorf("timeout")
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

	_ = pub.PublishContent(func() (io.ReadSeekCloser, error) {
		return BytesToReadSeekCloser([]byte("hello")), nil
	}, "test.txt", 5, 1)
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
		defer f.Close()
		text, err := io.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}
		if string(text) != "hello" {
			t.Errorf("expected \"hello\"; actual %q", string(text))
		}
		_ = sub.UnsubscribeFile(1, "pub")
		subs, ok := s.idToSubs.Load(FilePair{FileId: wrq.FileId, UUID: wrq.UUID})
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
	err := sender.SendFile(func() (io.ReadSeekCloser, error) {
		return BytesToReadSeekCloser(data), nil
	}, OpSyncIcon, 1, "test.txt", uint64(len(data)), 0)
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

func TestClientDupMessage(t *testing.T) {
	_, serverAddr := setUpServer(t)
	receiver := newClient(serverAddr, "receiver")
	client, err := setUpClient(t)
	defer func() { _ = client.Close() }()

	// send sign
	sign := SignReq{1, SignBody{"default", "sender"}}
	signPkt, _ := sign.Marshal()

	sAddr, _ := net.ResolveUDPAddr("udp", serverAddr)
	_, err = client.WriteTo(signPkt, sAddr)
	if err != nil {
		t.Fatal(err)
	}
	// read ack
	buf := make([]byte, DatagramSize)
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	_, _, _ = client.ReadFrom(buf)

	msg := SignedMessage{SignReq: sign, Payload: []byte("hello")}
	msgPkt, _ := msg.Marshal()
	_, _ = client.WriteTo(msgPkt, sAddr)
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	_, _, _ = client.ReadFrom(buf)
	select {
	case received := <-receiver.SignedMessages:
		if !reflect.DeepEqual(received, msg) {
			t.Errorf("expected message %v; actual message %v", msg, received)
		}
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timeout")
	}
	_, _ = client.WriteTo(msgPkt, sAddr)
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	_, _, _ = client.ReadFrom(buf)
	select {
	case received := <-receiver.SignedMessages:
		t.Errorf("duplicated message %v", received)
	default:
	}
}

func TestServerDupMessage(t *testing.T) {
	s, serverAddr := setUpServer(t)
	receiver := newClient(serverAddr, "receiver")
	_ = receiver.SendText("sync")
	sign := SignReq{1, SignBody{"default", "sender"}}
	msg := SignedMessage{SignReq: sign, Payload: []byte("hello")}
	msgPkt, _ := msg.Marshal()
	addr, err := s.parseAddrByUUID(receiver.ID())
	if err != nil {
		t.Fatal(err)
	}
	s.dispatch(addr, msgPkt, sign.UUID, sign.Block)
	select {
	case received := <-receiver.SignedMessages:
		if !reflect.DeepEqual(received, msg) {
			t.Errorf("expected message %v; actual message %v", msg, received)
		}
	default:
		t.Errorf("no message received")
	}
	s.dispatch(addr, msgPkt, sign.UUID, sign.Block)
	select {
	case received := <-receiver.SignedMessages:
		t.Errorf("duplicated message %v", received)
	default:
	}
}

func TestDupMessage(t *testing.T) {
	_, serverAddr := setUpServer(t)
	receiver := newClient(serverAddr, "receiver")
	_ = receiver.SendText("sync")
	sender1 := newClient(serverAddr, "sender1")
	sender2 := newClient(serverAddr, "sender2")
	go func() {
		_ = sender1.SendText("hello")
	}()
	time.Sleep(1 * time.Millisecond)
	go func() {
		_ = sender2.SendText("world")
	}()
	select {
	case received := <-receiver.SignedMessages:
		if string(received.Payload) != "hello" {
			t.Errorf("expected message %v; actual message %v", "hello", string(received.Payload))
		}
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timeout")
	}
	select {
	case received := <-receiver.SignedMessages:
		if string(received.Payload) != "world" {
			t.Errorf("expected message %v; actual message %v", "world", string(received.Payload))
		}
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timeout")
	}
	select {
	case received := <-receiver.SignedMessages:
		t.Errorf("duplicated message %v", received)
	default:
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
	select {
	case msg := <-receiver.SignedMessages:
		text := string(msg.Payload)
		if text != "hello" {
			t.Errorf("expected \"hello\"; actual %q", text)
		}
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timeout")
	}

	err = receiver.SendText("hello")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-sender.SignedMessages:
		text := string(msg.Payload)
		if text != "hello" {
			t.Errorf("expected \"hello\"; actual %q", text)
		}
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timeout")
	}
}

func TestSignOut(t *testing.T) {
	server, serverAddr := setUpServer(t)
	sender := newClient(serverAddr, "sender")
	sender.SignIn()
	_, err := server.parseAddrByUUID(sender.ID())
	if err != nil {
		t.Fatal(err)
	}
	sender.SignOut()
	time.Sleep(1 * time.Millisecond)
	_, err = server.parseAddrByUUID(sender.ID())
	if err == nil {
		t.Errorf("sign out failed")
	}
	_ = sender.SendText("hello")
	_, err = server.parseAddrByUUID(sender.ID())
	if err != nil {
		t.Errorf("should retry sign")
	}
}

func TestSyncName(t *testing.T) {
	_, serverAddr := setUpServer(t)
	receiver := newClient(serverAddr, "receiver")
	sender := newClient(serverAddr, "sender")
	receiver.SignIn()
	OldUUID := sender.ID()
	sender.SetNickName("#")
	sender.SignIn()
	sender.SyncName(OldUUID)
	time.Sleep(1 * time.Millisecond)
	select {
	case received := <-receiver.CtrlMessages:
		if received.Target != OldUUID || received.UUID != sender.ID() {
			t.Errorf("expected old uuid %v, uuid %v; actual old uuid %v, uuid %v", OldUUID, sender.ID(), received.Target, received.UUID)
		}
	default:
		t.Errorf("no message received")
	}
}

func TestReadIcon(t *testing.T) {
	_, serverAddr := setUpServer(t)
	pub := newClient(serverAddr, "pub")
	sub := newClient(serverAddr, "sub")
	other := newClient(serverAddr, "other")
	_ = sub.SendText("ping")
	_ = other.SendText("pong")
	_ = sub.ReadIcon(pub.ID())
	select {
	case rrq := <-pub.SubMessages:
		if rrq.Code != OpReadIcon {
			t.Errorf("expected OpReadIcon; actual opcode %v", rrq.Code)
		}
		s := "this is a icon"
		data := []byte(s)
		_ = pub.PublishContent(func() (io.ReadSeekCloser, error) {
			return BytesToReadSeekCloser(data), nil
		}, "icon.txt", uint64(len(data)), 0)
		select {
		case wrq := <-sub.FileMessages:
			if wrq.Code != OpContent || wrq.FileId != 0 {
				t.Errorf("expected OpContent; actual opcode %v", wrq.Code)
			}
			f, err := os.Open("./test/pub/icon.txt")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			text, err := io.ReadAll(f)
			if err != nil {
				t.Fatal(err)
			}
			if string(text) != s {
				t.Errorf("expected %q; actual %q", s, string(text))
			}
			_ = sub.UnsubscribeFile(0, pub.ID())
		case <-time.After(10 * time.Millisecond):
			t.Errorf("timeout")
		}
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timeout")
	}
}

func TestPull(t *testing.T) {
	_, serverAddr := setUpServer(t)
	sender := newClient(serverAddr, "sender")
	sender.SignIn()
	_ = sender.SendText("hello")
	_ = sender.SendText("world")
	_ = sender.PublishFile("test.txt", 5, 1)
	receiver := newClient(serverAddr, "receiver")
	time.Sleep(1 * time.Millisecond)
	receiver.SignIn()
	receiver.Pull()
	select {
	case msg := <-receiver.SignedMessages:
		if string(msg.Payload) != "hello" {
			t.Errorf("expected \"hello\"; actual %q", string(msg.Payload))
		}
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timeout")
	}
	select {
	case msg := <-receiver.SignedMessages:
		if string(msg.Payload) != "world" {
			t.Errorf("expected \"world\"; actual %q", string(msg.Payload))
		}
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timeout")
	}
	select {
	case wrq := <-receiver.FileMessages:
		if wrq.Filename != "test.txt" {
			t.Errorf("expected filename \"test.txt\"; actual filename %q", wrq.Filename)
		}
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timeout")
	}
	tracker := receiver.loadRangeTracker(&SignBody{Sign: receiver.Sign, UUID: sender.ID()})
	if !tracker.isCompleted() {
		t.Errorf("tracker not completed")
	}
	receiver.Pull()
	select {
	case _ = <-receiver.SignedMessages:
		t.Errorf("duplicated message")
	default:
	}
}

func TestCheck(t *testing.T) {
	msg := SignedMessage{SignReq: SignReq{1, SignBody{"default", "sender"}}, Payload: []byte(("hello beautiful world"))}
	pkt, err := msg.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	s, serverAddr := setUpServer(t)
	receiver := newClient(serverAddr, "receiver")
	receiver.SignIn()
	_, err = s.conn.WriteTo(pkt, receiver.conn.LocalAddr())
	if err != nil {
		t.Fatal(err)
	}
	<-receiver.SignedMessages
	v, ok := s.addrToPeer.Load(receiver.conn.LocalAddr().String())
	if !ok {
		t.Error("expected peer to load")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cacheKey := newCacheKey(msg.UUID, msg.Block)
	p := v.(Peer)
	p.putACK(cacheKey, cancel)
	defer p.deleteRCK(cacheKey)
	defer cancel()
	check := Check{msg.Block, msg.UUID}
	pkt, _ = check.Marshal()
	_, err = s.conn.WriteTo(pkt, receiver.conn.LocalAddr())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timeout")
	}
}

func TestDiscovery(t *testing.T) {
	s, serverAddr := setUpServer(t)
	wg := sync.WaitGroup{}
	for i := 0; i < 200; i++ {
		u := fmt.Sprintf("user_prefix_abcdefghij#000%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := newClient(serverAddr, u)
			c.SignIn()
		}()
	}
	wg.Wait()
	ret := s.collectUsers("default", Online)
	if len(ret) != 200 {
		t.Errorf("expected 200 users; actual %d", len(ret))
	}
	ret = s.collectUsers("default", Active)
	if len(ret) != 0 {
		t.Errorf("expected 0 users; actual %d", len(ret))
	}
	c := newClient(serverAddr, "c")
	ret, err := c.Discover(Online)
	if err != nil {
		t.Fatal(err)
	}
	if len(ret) != 201 {
		t.Errorf("expected 201 users; actual %d", len(ret))
	}
	users := s.collectUsers("default", Online)
	sort.Strings(users)
	sort.Strings(ret)
	if !reflect.DeepEqual(users, ret) {
		t.Errorf("expected %v; actual %v", len(users), len(ret))
	}
	ret, err = c.Discover(Active)
	if err != nil {
		t.Fatal(err)
	}
	if len(ret) != 0 {
		t.Errorf("expected 0 users; actual %d", len(ret))
	}
	_ = c.SendText("hello")
	ret, err = c.Discover(Active)
	if err != nil {
		t.Fatal(err)
	}
	if len(ret) != 1 {
		t.Errorf("expected  users; actual %d", len(ret))
	}
}

func TestReply(t *testing.T) {
	s, serverAddr := setUpServer(t)
	c := newClient(serverAddr, "c")
	sign := &SignBody{Sign: "default", UUID: "sender"}
	c.Track(sign, 2)
	c.SignIn()
	tracker := c.loadRangeTracker(sign)
	expected := []Range{{1, 1}}
	if !reflect.DeepEqual(tracker.ranges, expected) {
		t.Errorf("expected %v; actual %v", expected, tracker.ranges)
	}
	a, _ := s.parseAddrByUUID("c")
	r := Range{1, 1}
	s.reply(PullReq{Block: c.nextID(), SignBody: *sign, Range: r, ranges: tracker.ranges}, a, tracker.ranges)
	if len(tracker.ranges) != 0 {
		t.Errorf("expected 0; actual %d", len(tracker.ranges))
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
	s := Server{Status: make(chan struct{})}
	port := rand.Intn(40000) + 10000
	serverAddr := "127.0.0.1:" + strconv.Itoa(port)
	go func() {
		t.Error(s.ListenAndServe(serverAddr))
	}()
	s.Ready()
	return &s, serverAddr
}
