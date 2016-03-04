package raft

import (
	"bytes"
	"sync"
	"time"

	//serverpb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	pb "github.com/coreos/etcd/raft/raftpb"
)

const (
	pushLocalStoreDeadline time.Duration = 5 * time.Second
)

type LocalStore interface {
	MaybeAdd(ent pb.Entry) (bool, error)

	Clear()

	Merge(ents []pb.Entry)

	Entries() []pb.Entry

	//actually returns index of first entry not sent
	LastSent() uint64

	SetLastSent(index uint64)

	Context() (context.Context, context.CancelFunc)

	SetContext(context.Context, context.CancelFunc)

	//removes first LastSent() entries, resets LastSent to 0
	TrimWithLastSent()

	//removes all entries with nil Data attribute
	//should only be called on leader, when not waiting for MsgLocalStoreResp
	TruncateEmpty()

	RemoveFirst(count uint64)

	RemoveFromWaiting(timestamp int64) *pb.Entry
}

type localStore struct {
	entsMutex        sync.Mutex
	waitingMutex     sync.Mutex
	ents             []pb.Entry
	waitingForCommit []pb.Entry
	logger           Logger
	lastIndexSent    uint64
	context          context.Context
	cancel           context.CancelFunc
}

func NewLocalStore(log Logger) *localStore {
	return &localStore{
		ents:             []pb.Entry{},
		waitingForCommit: []pb.Entry{},
		logger:           log,
		entsMutex:        sync.Mutex{},
		waitingMutex:     sync.Mutex{},
	}
}

func (ls *localStore) MaybeAdd(ent pb.Entry) (bool, error) {
	ls.entsMutex.Lock()
	defer ls.entsMutex.Unlock()
	for index, entry := range ls.ents {
		if entry.CompareMessage(ent) {
			//TODO: choose better (based on index and term)
			if entry.Term > ent.Term {
				plog.Infof("Conflict found, localstore already has entry %s, but with higher term", ent.Print())
				return false, nil
			} else if entry.Term == ent.Term {
				if entry.Timestamp >= ent.Timestamp {
					plog.Infof("Conflict found, localstore already has entry %s, but with higher timestamp", ent.Print())
					return false, nil
				}
			}
			ls.ents[index].Data = nil
			break
			//ls.ents = append(ls.ents, ent)
			//return true, nil
		}
	}
	ls.ents = append(ls.ents, ent)
	ls.logger.Infof("Local log after MaybeAdd: %s", FormatEnts(ls.ents))
	return true, nil
}

func (ls *localStore) Clear() {
	ls.entsMutex.Lock()
	defer ls.entsMutex.Unlock()
	ls.ents = []pb.Entry{}
}

func (ls *localStore) Entries() []pb.Entry { return ls.ents }

func (ls *localStore) Merge(ents []pb.Entry) {
	for _, entryToMerge := range ents {
		if entryToMerge.Data == nil {
			continue
		}
		if _, err := ls.MaybeAdd(entryToMerge); err != nil {
			//TODO: write to log about error
		}
	}
}

func (ls *localStore) LastSent() uint64 { return ls.lastIndexSent }

func (ls *localStore) SetLastSent(index uint64) {
	ls.lastIndexSent = index
}

func (ls *localStore) Context() (context.Context, context.CancelFunc) { return ls.context, ls.cancel }

func (ls *localStore) SetContext(ctx context.Context, cancel context.CancelFunc) {
	ls.context = ctx
	ls.cancel = cancel
}

//TODO: write tests
func (ls *localStore) TrimWithLastSent() {
	ls.waitingForCommit = append(ls.waitingForCommit, ls.ents[:ls.LastSent()]...)
	ls.ents = ls.ents[ls.LastSent():]
	ls.SetLastSent(0)
}

func (ls *localStore) TruncateEmpty() {
	ls.entsMutex.Lock()
	defer ls.entsMutex.Unlock()
	for index := len(ls.ents) - 1; index >= 0; index-- {
		if ls.ents[index].Data == nil || len(ls.ents[index].Data) == 0 {
			if index == len(ls.ents)-1 {
				ls.ents = ls.ents[:index]
			} else {
				ls.ents = append(ls.ents[:index], ls.ents[index+1:]...)
			}
		}

	}
}

func (ls *localStore) RemoveFirst(count uint64) {
	ls.entsMutex.Lock()
	defer ls.entsMutex.Unlock()
	if len(ls.ents) == 0 {
		return
	}
	ls.ents = ls.ents[count:]
}

func (ls *localStore) RemoveFromWaiting(timestamp int64) *pb.Entry {
	ls.waitingMutex.Lock()
	defer ls.waitingMutex.Unlock()

	for index, entry := range ls.waitingForCommit {
		if entry.Timestamp == timestamp {
			ls.ents = append(ls.waitingForCommit[:index], ls.waitingForCommit[index+1:]...)
			return &entry
		}
	}
	return nil
}

func FormatEnts(ents []pb.Entry) string {
	var buffer bytes.Buffer
	for _, ent := range ents {
		buffer.WriteString(ent.Print())
	}
	return buffer.String()
}
