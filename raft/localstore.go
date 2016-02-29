package raft

import (
	"sync"

	//serverpb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	pb "github.com/coreos/etcd/raft/raftpb"
)

type LocalStore interface {
	MaybeAdd(ent *pb.Entry) (bool, error)

	Clear()

	Merge(store LocalStore)

	Entries() []*pb.Entry

	//actually returns index of first entry not sent
	LastSent() uint64
}

//TODO: add mutex to protect
type localStore struct {
	mu     sync.Mutex
	ents   []*pb.Entry
	logger Logger
	lastIndexSent uint64
}

func NewLocalStore(log Logger) *localStore {
	return &localStore{
		ents:   []*pb.Entry{},
		logger: log,
	}
}

func (ls *localStore) MaybeAdd(ent *pb.Entry) (bool, error) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	for index, entry := range ls.ents {
		if entry.CompareMessage(ent) {
			//TODO: choose better (based on index and term)
			if entry.Term > ent.Term {
				//TODO: write to log that entry has not been added
				return false, nil
			}
			ls.ents = append(ls.ents[:index], ls.ents[index+1:]...)
			ls.ents = append(ls.ents,ent)
			if ls.lastIndexSent != 0 {
				ls.lastIndexSent--
			}
			return true, nil
		}
	}
	ls.ents = append(ls.ents, ent)
	ls.logger.Infof("Local log after MaybeAdd: %v", ls.ents)
	return true, nil
}

func (ls *localStore) Clear() {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.ents = []*pb.Entry{}
}

func (ls *localStore) Entries() []*pb.Entry { return ls.ents }

func (ls *localStore) Merge(otherStore LocalStore) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	for _, entryToMerge := range otherStore.Entries() {
		if _, err := ls.MaybeAdd(entryToMerge); err != nil {
			//TODO: write to log about error
		}
	}
}

func (ls *localStore) LastSent() {return ls.lastIndexSent}
