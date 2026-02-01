package whily

import (
	"bytes"
	"encoding/hex"
	"net"
	"testing"
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
	signAck := Ack{SrcOp: OpSign, Block: 0}
	signAckBytes, err := signAck.Marshal()
	msgAck := Ack{SrcOp: OpSignedMSG, Block: 0}
	msgAckBytes, err := msgAck.Marshal()

	sign := "test"

	uuid := "#00001"
	clientSign := Sign{sign, uuid}
	clientSignPkt, _ := clientSign.Marshal()
	t.Logf("sign pkt: [%v]", hex.EncodeToString(clientSignPkt))

	uuidA := "#00002"
	clientASign := Sign{sign, uuidA}

	text := "hello beautiful world"
	clientMsg := SignedMessage{Sign: clientSign, Payload: []byte(text)}
	clientMsgPkt, err := clientMsg.Marshal()

	textA := "beautiful world"
	clientAMsg := SignedMessage{Sign: clientASign, Payload: []byte(textA)}
	clientAMsgPkt, err := clientAMsg.Marshal()

	serverAddr := setUpServer(t)
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
	n, _, err := client.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(signAckBytes, buf[:n]) {
		t.Errorf("expected reply %q; actual reply %q", signAckBytes, buf[:n])
	}

	// clientA send text
	clientA := Client{ServerAddr: serverAddr, Status: make(chan struct{}), UUID: uuidA, Sign: sign}
	go func() {
		clientA.ListenAndServe("127.0.0.1:")
	}()
	clientA.Ready()
	clientA.SendText(textA)

	// client read text. client should receive "beautiful world" from clientA
	n, _, err = client.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(clientAMsgPkt, buf[:n]) {
		t.Errorf("expected reply %q; actual reply %q", clientAMsgPkt, buf[:n])
	}
	// send ack
	client.WriteTo(msgAckBytes, sAddr)

	// client send text, server should ack
	_, err = client.WriteTo(clientMsgPkt, sAddr)
	if err != nil {
		t.Fatal(err)
	}
	// read ack
	n, _, err = client.ReadFrom(buf)
	if !bytes.Equal(msgAckBytes, buf[:n]) {
		t.Errorf("expected reply %q; actual reply %q", signAckBytes, buf[:n])
	}
}

func setUpClient(t *testing.T) (net.PacketConn, error) {
	client, err := net.ListenPacket("udp", "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
	}
	return client, err
}

func setUpServer(t *testing.T) string {
	s := Server{}
	serverAddr := "127.0.0.1:52000"
	go func() {
		t.Error(s.ListenAndServe(serverAddr))
	}()
	return serverAddr
}
