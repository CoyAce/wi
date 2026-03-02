package wi

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

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

func TestTimeout(t *testing.T) {
	timeout := 5 * time.Second
	elapsed := 200 * time.Millisecond
	timeout = timeout*8/10 + elapsed*2/10
	if timeout != 4040*time.Millisecond {
		t.Errorf("timeout should be 4040ms")
	}
}

func TestCircularBuffer(t *testing.T) {
	cb := NewCircularBuffer(5)
	for i := 0; i < 5; i++ {
		cb.Write(Packet{Block: uint32(i + 1)})
	}
	ret := cb.Read([]Range{{1, 2}, {4, 4}})
	expected := []Packet{{Block: uint32(1)}, {Block: uint32(2)}, {Block: uint32(4)}}
	if !reflect.DeepEqual(ret, expected) {
		t.Errorf("Expected %v, got %v", expected, ret)
	}
}

func TestMap(t *testing.T) {
	var files map[uint32][]Data
	files = make(map[uint32][]Data)
	files[0] = append(files[0], Data{FileId: 0})
	if files[0][0].FileId != 0 {
		t.Fatal("file id should be 0")
	}
}

func TestConsecutive(t *testing.T) {
	d := []Data{{Block: 1}, {Block: 2}, {Block: 3}, {Block: 5}}
	i := findConsecutive(d)
	if i != 3 {
		t.Fatal("consecutive function error")
	}
}
