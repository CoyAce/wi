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
		State:    State{Status: make(chan struct{})},
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
		case <-time.After(100 * time.Millisecond):
			t.Errorf("timeout")
		}
	case <-time.After(100 * time.Millisecond):
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
	case <-time.After(100 * time.Millisecond):
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
	case <-time.After(100 * time.Millisecond):
		t.Errorf("timeout")
	}
}

func TestSendFile(t *testing.T) {
	_, serverAddr := setUpServer(t)
	sender := newClient(serverAddr, "sender")
	receiver1 := newClient(serverAddr, "receiver1")
	receiver1.SignIn()
	receiver2 := newClient(serverAddr, "receiver2")
	receiver2.SignIn()
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
	receiver.SignIn()
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
	case <-time.After(100 * time.Millisecond):
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
	time.Sleep(1 * time.Millisecond)
	select {
	case received := <-receiver.SignedMessages:
		if !reflect.DeepEqual(received, msg) {
			t.Errorf("expected message %v; actual message %v", msg, received)
		}
	default:
		t.Errorf("no message received")
	}
	s.dispatch(addr, msgPkt, sign.UUID, sign.Block)
	time.Sleep(1 * time.Millisecond)
	select {
	case received := <-receiver.SignedMessages:
		t.Errorf("duplicated message %v", received)
	default:
	}
}

func TestDupMessage(t *testing.T) {
	_, serverAddr := setUpServer(t)
	receiver := newClient(serverAddr, "receiver")
	receiver.SignIn()
	sender := newClient(serverAddr, "sender")
	go func() {
		_ = sender.SendText("hello")
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
	time.Sleep(1 * time.Millisecond)
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
	sender.SignIn()
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

func TestPullLongText(t *testing.T) {
	_, serverAddr := setUpServer(t)
	sender := newClient(serverAddr, "sender")
	sender.SignIn()
	_ = sender.SendText(LongText)
	receiver := newClient(serverAddr, "receiver")
	receiver.SignIn()
	time.Sleep(1 * time.Millisecond)
	receiver.Pull()
	select {
	case msg := <-receiver.SignedMessages:
		if string(msg.Payload) != LongText {
			t.Errorf("expected %v; actual %q", LongText, string(msg.Payload))
		}
	case <-time.After(100 * time.Millisecond):
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
	receiver.SignIn()
	time.Sleep(1 * time.Millisecond)
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
		t.Fatal("expected peer to load")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cacheKey := newCacheKey(msg.UUID, msg.Block)
	p := v.(Peer)
	p.storeACK(cacheKey, cancel)
	defer p.deleteACK(cacheKey)
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
	cnt := 200
	for i := 0; i < cnt; i++ {
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
	if len(ret) != cnt {
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
	if len(ret) != cnt+1 {
		t.Errorf("expected %v users; actual %d", cnt+1, len(ret))
	}
	users := s.collectUsers("default", Online)
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
		t.Errorf("expected 1 users; actual %d", len(ret))
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
	time.Sleep(1 * time.Millisecond)
	a, _ := s.parseAddrByUUID("c")
	s.reply(sign, a, tracker.ranges)
	if len(tracker.ranges) != 0 {
		t.Errorf("expected 0; actual %d", len(tracker.ranges))
	}
}

func TestMultiWrite(t *testing.T) {
	s, serverAddr := setUpServer(t)
	c := newClient(serverAddr, "c")
	c.SignIn()
	key := newCacheKey(c.ID(), 1)
	if v, ok := s.addrToPeer.Load(c.conn.LocalAddr().String()); ok {
		sack := make(chan uint32)
		v.(Peer).storeSACK(key, sack)
		header := ReqHeader{Block: 1, ReqID: key.Block, SignBody: SignBody{UUID: key.UUID}}
		data, err := new(ReliableReq{ReqHeader: header, ReqBody: new(ReqData("hello"))}).Marshal()
		if err != nil {
			t.Fatal(err)
		}
		_ = v.(Peer).writeTo(c.conn.LocalAddr(), data)
		time.Sleep(1 * time.Millisecond)
		finPkt, _ := new(Fin{header}).Marshal()
		_ = v.(Peer).writeTo(c.conn.LocalAddr(), finPkt)
		r := <-sack
		if r != 2 {
			t.Errorf("expected 2; actual %d", r)
		}
	}
}

func TestLongText(t *testing.T) {
	_, serverAddr := setUpServer(t)
	sender := newClient(serverAddr, "sender")
	sender.SignIn()
	receiver1 := newClient(serverAddr, "receiver1")
	receiver1.SignIn()
	receiver2 := newClient(serverAddr, "receiver2")
	receiver2.SignIn()
	err := sender.SendText(LongText)
	if err != nil {
		t.Error(err)
	}
	select {
	case msg := <-receiver1.SignedMessages:
		if string(msg.Payload) != LongText {
			t.Errorf("expected %s; actual %s", LongText, string(msg.Payload))
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout")
	}
	select {
	case msg := <-receiver2.SignedMessages:
		if string(msg.Payload) != LongText {
			t.Errorf("expected %s; actual %s", LongText, string(msg.Payload))
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout")
	}
}

func newClient(serverAddr string, UUID string) *Client {
	client := Client{
		Config:   Config{ExternalDir: "./test", ServerAddr: serverAddr},
		Identity: Identity{UUID: UUID, Sign: "default"},
		State:    State{Status: make(chan struct{})},
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

const LongText = "Love's Philosophy\nBy Percy Bysshe Shelley\n\nThe fountains mingle with the river\nAnd the rivers with the ocean,\nThe winds of heaven mix for ever\nWith a sweet emotion;\nNothing in the world is single;\nAll things by a law divine\nIn one spirit meet and mingle.\nWhy not I with thine?—\n\nSee the mountains kiss high heaven\nAnd the waves clasp one another;\nNo sister-flower would be forgiven\nIf it disdained its brother;\nAnd the sunlight clasps the earth\nAnd the moonbeams kiss the sea:\nWhat is all this sweet work worth\nIf thou kiss not me?**Ode to the West Wind**\n*By Percy Bysshe Shelley*\n\n---\n\n**I**\n\nO wild West Wind, thou breath of Autumn's being,\nThou, from whose unseen presence the leaves dead\nAre driven, like ghosts from an enchanter fleeing,\n\nYellow, and black, and pale, and hectic red,\nPestilence-stricken multitudes: O thou,\nWho chariotest to their dark wintry bed\n\nThe wingèd seeds, where they lie cold and low,\nEach like a corpse within its grave, until\nThine azure sister of the Spring shall blow\n\nHer clarion o'er the dreaming earth, and fill\n(Driving sweet buds like flocks to feed in air)\nWith living hues and odours plain and hill:\n\nWild Spirit, which art moving everywhere;\nDestroyer and preserver; hear, oh hear!\n\n---\n\n**II**\n\nThou on whose stream, mid the steep sky's commotion,\nLoose clouds like earth's decaying leaves are shed,\nShook from the tangled boughs of Heaven and Ocean,\n\nAngels of rain and lightning: there are spread\nOn the blue surface of thine aery surge,\nLike the bright hair uplifted from the head\n\nOf some fierce Maenad, even from the dim verge\nOf the horizon to the zenith's height,\nThe locks of the approaching storm. Thou dirge\n\nOf the dying year, to which this closing night\nWill be the dome of a vast sepulchre,\nVaulted with all thy congregated might\n\nOf vapours, from whose solid atmosphere\nBlack rain, and fire, and hail will burst: oh hear!\n\n---\n\n**III**\n\nThou who didst waken from his summer dreams\nThe blue Mediterranean, where he lay,\nLulled by the coil of his crystalline streams,\n\nBeside a pumice isle in Baiae's bay,\nAnd saw in sleep old palaces and towers\nQuivering within the wave's intenser day,\n\nAll overgrown with azure moss and flowers\nSo sweet, the sense faints picturing them! Thou\nFor whose path the Atlantic's level powers\n\nCleave themselves into chasms, while far below\nThe sea-blooms and the oozy woods which wear\nThe sapless foliage of the ocean, know\n\nThy voice, and suddenly grow gray with fear,\nAnd tremble and despoil themselves: oh hear!\n\n---\n\n**IV**\n\nIf I were a dead leaf thou mightest bear;\nIf I were a swift cloud to fly with thee;\nA wave to pant beneath thy power, and share\n\nThe impulse of thy strength, only less free\nThan thou, O uncontrollable! If even\nI were as in my boyhood, and could be\n\nThe comrade of thy wanderings over Heaven,\nAs then, when to outstrip thy skiey speed\nScarce seem'd a vision; I would ne'er have striven\n\nAs thus with thee in prayer in my sore need.\nOh, lift me as a wave, a leaf, a cloud!\nI fall upon the thorns of life! I bleed!\n\nA heavy weight of hours has chained and bowed\nOne too like thee: tameless, and swift, and proud.\n\n---\n\n**V**\n\nMake me thy lyre, even as the forest is:\nWhat if my leaves are falling like its own!\nThe tumult of thy mighty harmonies\n\nWill take from both a deep, autumnal tone,\nSweet though in sadness. Be thou, Spirit fierce,\nMy spirit! Be thou me, impetuous one!\n\nDrive my dead thoughts over the universe\nLike withered leaves to quicken a new birth!\nAnd, by the incantation of this verse,\n\nScatter, as from an unextinguished hearth\nAshes and sparks, my words among mankind!\nBe through my lips to unawakened earth\n\nThe trumpet of a prophecy! O Wind,\nIf Winter comes, can Spring be far behind?\n\n---\n\n*Written at Florence, 1819*"
