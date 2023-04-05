package raft_kvfsm

import (
	"testing"
)
import raft "github.com/fuyao-w/go-raft"

func TestFsm(t *testing.T) {
	fsm := NewKvFsm()
	fsm.Apply(&raft.LogEntry{
		Data: KvSchema{}.Encode("name", "abc"),
	})
	snapshot, _ := fsm.Snapshot()
	filess, err := raft.NewFileSnapshot("./test.bin", false, 3)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := filess.Create(1, 1, 1, raft.Configuration{}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(snapshot.Persist(sink))
	sink.Close()
	_, rc, err := filess.Open(sink.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	newFsm := NewKvFsm()
	newFsm.ReStore(rc)
	t.Log(newFsm.kv.Load("name"))

}
