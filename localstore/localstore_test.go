package localstore

import (
	"github.com/coreos/etcd/raft/raftpb"
	"testing"
)

func TestTruncate(t *testing.T) {
	defEntry := raftpb.Entry{
		Data: []byte("skskmcmska"),
	}

	lStore1 := localStore{
		ents: []raftpb.Entry{defEntry, defEntry, raftpb.Entry{Data: nil}},
	}

	lStore2 := localStore{
		ents: []raftpb.Entry{raftpb.Entry{Data: nil}, defEntry, defEntry},
	}

	lStore3 := localStore{
		ents: []raftpb.Entry{defEntry, raftpb.Entry{Data: nil}, defEntry},
	}

	lStore4 := localStore{
		ents: []raftpb.Entry{raftpb.Entry{Data: nil}, defEntry, raftpb.Entry{Data: nil}, defEntry, raftpb.Entry{Data: nil}},
	}

	if lStore1.TruncateEmpty(); len(lStore1.ents) != 2 {
		t.Errorf("Store1 should have 2 entries, has %d", len(lStore1.ents))
	}

	if lStore2.TruncateEmpty(); len(lStore2.ents) != 2 {
		t.Errorf("Store1 should have 2 entries, has %d", len(lStore2.ents))
	}

	if lStore3.TruncateEmpty(); len(lStore3.ents) != 2 {
		t.Errorf("Store1 should have 2 entries, has %d", len(lStore3.ents))
	}

	if lStore4.TruncateEmpty(); len(lStore4.ents) != 2 {
		t.Errorf("Store1 should have 2 entries, has %d", len(lStore4.ents))
	}
}
