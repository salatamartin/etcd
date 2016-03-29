package raft

import (
	"bytes"
	"io/ioutil"
	"os"
	"sync"
	"time"

	//serverpb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	pb "github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/wal"
	//"github.com/coreos/etcd/wal/walpb"
	//"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"errors"
	"fmt"
	"github.com/coreos/etcd/store"
	"path"
)

const (
	pushLocalStoreDeadline = 10 * time.Second
	ClusterPrefix          = "/0"
	KeysPrefix             = "/1"
)

type LocalStore interface {
	MaybeAdd(ent pb.Entry) (*store.Event, error)

	Clear()

	Merge(ents []pb.Entry)

	Entries() []pb.Entry

	WaitingForCommitEntries() []pb.Entry

	//actually returns index of first entry not sent
	LastSent() uint64

	SetLastSent(index uint64)

	LastTimestampSent() int64

	SetLastTimestampSent(index int64)

	Context() (context.Context, context.CancelFunc)

	SetContext(context.Context, context.CancelFunc)

	//removes first LastSent() entries, resets LastSent to 0
	TrimWithLastSent()

	//removes all entries with empty Data attribute
	//should only be called on leader, when not waiting for MsgLocalStoreResp
	TruncateEmpty() int

	//removes all entries with empty Data attribute
	//should only be called on leader, when not waiting for MsgLocalStoreResp
	TruncateEmptyWaiting() int

	RemoveFirst(count uint64)

	//removes entry with defined timestamp
	RemoveFromWaiting(ent pb.Entry) *pb.Entry

	KVStore() store.Store

	ResetWaitingList()

	RemoveWaitingList()

	EntriesFilled() chan struct{}

	WaitingForCommitFilled() chan struct{}
}

type localStore struct {
	entsMutex         sync.Mutex
	waitingMutex      sync.Mutex
	walMutex          sync.Mutex
	ents              []pb.Entry
	waitingForCommit  []pb.Entry
	wal               *wal.WAL
	logger            Logger
	lastIndexSent     uint64
	lastTimestampSent int64
	lastInWal         uint64
	context           context.Context
	cancel            context.CancelFunc
	kvStore           store.Store
	entriesFilled	  chan struct{}
	waitingFilled	  chan struct{}
}

func NewLocalStore(log Logger, w *wal.WAL, wSize uint64) *localStore {
	return &localStore{
		ents:             []pb.Entry{},
		waitingForCommit: []pb.Entry{},
		wal:              w,
		logger:           log,
		entsMutex:        sync.Mutex{},
		waitingMutex:     sync.Mutex{},
		walMutex:         sync.Mutex{},
		lastInWal:        wSize,
		kvStore:          store.New(ClusterPrefix, KeysPrefix),
		entriesFilled: 	  make(chan struct{}),
		waitingFilled:	  make(chan struct{}),
	}
}

func (ls *localStore) MaybeAdd(ent pb.Entry) (*store.Event, error) {
	ls.entsMutex.Lock()
	defer ls.entsMutex.Unlock()
	for index, entry := range ls.ents {
		if entry.CompareMessage(ent) {
			if entry.Term > ent.Term {
				errStr := fmt.Sprintf("Conflict found, localstore already has entry %s, but with higher term", ent.Print())
				plog.Infof(errStr)
				return nil, errors.New(errStr)
			} else if entry.Term == ent.Term {
				if entry.Timestamp >= ent.Timestamp {
					errStr := fmt.Sprintf("Conflict found, localstore already has entry %s, but with higher timestamp", ent.Print())
					plog.Infof(errStr)
					return nil, errors.New(errStr)
				}
			}
			ls.ents[index].Data = nil
			break
		}
	}
	ls.ents = append(ls.ents, ent)
	// length was 0 before append, fill channel
	if len(ls.ents) == 1 {
		go AddToChan(ls.entriesFilled)
		/*TOREMOVE*/plog.Infof("entriesFilled + 1")
	}
	//ls.logger.Infof("Local log after MaybeAdd: %s", FormatEnts(ls.ents))
	//we have to wait until log is persisted on disk before continuing
	ls.walMutex.Lock()
	ls.lastInWal++
	ent.Index = ls.lastInWal
	ls.wal.Save(pb.HardState{}, []pb.Entry{ent})
	ls.walMutex.Unlock()

	//write to KVstore to get the Event
	r := ent.RetrieveMessage()
	event, err := ls.kvStore.Set(r.Path, r.Dir, r.Val, store.TTLOptionSet{ExpireTime: store.Permanent})
	if err != nil {
		plog.Infof("Could not write entry to local KV store")
		return nil, err
	}
	event.Action = fmt.Sprintf("noQuorum%s", event.Action)
	return event, nil
}

func (ls *localStore) Clear() {
	ls.entsMutex.Lock()
	defer ls.entsMutex.Unlock()
	ls.ents = []pb.Entry{}
}

func (ls *localStore) Entries() []pb.Entry { return ls.ents }

func (ls *localStore) WaitingForCommitEntries() []pb.Entry { return ls.waitingForCommit }

func (ls *localStore) Merge(ents []pb.Entry) {
	if len(ls.ents) == 0 {
		ls.entsMutex.Lock()
		defer ls.entsMutex.Unlock()
		ls.ents = ents
		go AddToChan(ls.entriesFilled)
		plog.Infof("entriesFilled + 1")
		return
	}
	for _, entryToMerge := range ents {
		if entryToMerge.Data == nil {
			continue
		}
		//errors produced by MaybeAdd are already handled inside function
		ls.MaybeAdd(entryToMerge)
	}
}

func (ls *localStore) LastSent() uint64 { return ls.lastIndexSent }

func (ls *localStore) SetLastSent(index uint64) {
	ls.lastIndexSent = index
}

func (ls *localStore) LastTimestampSent() int64 { return ls.lastTimestampSent }

func (ls *localStore) SetLastTimestampSent(ts int64) {
	ls.lastTimestampSent = ts
}

func (ls *localStore) Context() (context.Context, context.CancelFunc) { return ls.context, ls.cancel }

func (ls *localStore) SetContext(ctx context.Context, cancel context.CancelFunc) {
	ls.context = ctx
	ls.cancel = cancel
}

func (ls *localStore) TrimWithLastSent() {
	if ls.LastSent() == 0 {
		return
	}
	ls.waitingMutex.Lock()
	ls.waitingForCommit = append(ls.waitingForCommit, ls.ents[:ls.LastSent()]...)
	if len(ls.waitingForCommit) == len(ls.ents[:ls.LastSent()]){
		go AddToChan(ls.waitingFilled)
		plog.Infof("waitingFilled + 1")
	}
	ls.waitingMutex.Unlock()

	ls.entsMutex.Lock()
	if uint64(len(ls.ents)) <= ls.LastSent() {
		ls.ents = []pb.Entry{}
	} else {
		ls.ents = ls.ents[ls.LastSent():]
	}
	ls.entsMutex.Unlock()

	ls.SetLastSent(0)
}

func (ls *localStore) TruncateEmpty() int {
	ls.entsMutex.Lock()
	defer ls.entsMutex.Unlock()
	var count int
	for index := len(ls.ents) - 1; index >= 0; index-- {
		if ls.ents[index].Data == nil || len(ls.ents[index].Data) == 0 {
			count++
			if index == len(ls.ents)-1 {
				ls.ents = ls.ents[:index]
			} else {
				ls.ents = append(ls.ents[:index], ls.ents[index+1:]...)
			}
		}

	}
	if len(ls.ents) == 0 && len(ls.waitingForCommit) == 0 {
		go ls.resetLocalWal()
	}
	return count
}

func (ls *localStore) TruncateEmptyWaiting() int {
	ls.waitingMutex.Lock()
	defer ls.waitingMutex.Unlock()
	var count int
	for index := len(ls.waitingForCommit) - 1; index >= 0; index-- {
		if ls.waitingForCommit[index].Data == nil || len(ls.waitingForCommit[index].Data) == 0 {
			count++
			if index == len(ls.waitingForCommit)-1 {
				ls.waitingForCommit = ls.waitingForCommit[:index]
			} else {
				ls.waitingForCommit = append(ls.waitingForCommit[:index], ls.waitingForCommit[index+1:]...)
			}
		}

	}
	if len(ls.ents) == 0 && len(ls.waitingForCommit) == 0 {
		go ls.resetLocalWal()
	}
	return count
}

func (ls *localStore) RemoveFirst(count uint64) {
	ls.entsMutex.Lock()
	defer ls.entsMutex.Unlock()
	if len(ls.ents) == 0 {
		return
	}
	ls.ents = ls.ents[count:]
	if len(ls.ents) == 0 && len(ls.waitingForCommit) == 0 {
		go ls.resetLocalWal()
	}
}

func (ls *localStore) RemoveFromWaiting(ent pb.Entry) *pb.Entry {
	ls.waitingMutex.Lock()
	defer ls.waitingMutex.Unlock()

	for index, entry := range ls.waitingForCommit {
		if entry.CompareMessage(ent) && entry.Timestamp == ent.Timestamp{
			//ls.waitingForCommit = append(ls.waitingForCommit[:index], ls.waitingForCommit[index+1:]...)
			ls.waitingForCommit[index].Data = nil
			// if all logs are empty, clear persistent storage (not needed anymore)
			//plog.Infof("Entry removed from waitingList")
			//plog.Infof("Number of NQPUTs: not yet received by leader: %d, not yet committed: %d", len(ls.ents), len(ls.waitingForCommit))
			//if len(ls.ents) == 0 && len(ls.waitingForCommit) == 0 {
			//	go ls.resetLocalWal()
			//}

			//remove entry from KV store
			go func(entry pb.Entry) {
				r := entry.RetrieveMessage()
				//TODO: check other request types
				if r.Method == "PUT" {
					ls.kvStore.Delete(r.Path, r.Dir, r.Recursive)
				}
			}(entry)
			return &entry
		}
	}
	return nil
}

func FormatEnts(ents []pb.Entry) string {
	var buffer bytes.Buffer
	for _, ent := range ents {
		buffer.WriteString(fmt.Sprintf("%s\n", ent.Print()))
	}
	return buffer.String()
}

func (ls *localStore) KVStore() store.Store { return ls.kvStore }

func (ls *localStore) resetLocalWal() {
	ls.walMutex.Lock()
	defer ls.walMutex.Unlock()
	ls.wal.Close()
	files, err := ioutil.ReadDir(ls.wal.GetDir())
	if err != nil {
		plog.Infof("Error at ioutil: %v", err)
		return
	}
	for _, file := range files {
		if error := os.Remove(path.Join(ls.wal.GetDir(), file.Name())); error != nil {
			plog.Infof("Could not remove file %s, error:%v", file.Name(), error)
		}
	}
	//var walsnap walpb.Snapshot
	newWal, error := wal.Create(ls.wal.GetDir(), nil)
	if error != nil {
		plog.Infof("Could not open new wal at %s: %v", ls.wal.GetDir(), error)
		return
	}
	ls.wal = newWal
	ls.lastInWal = 0
	plog.Infof("Successfully removed all entries from persistent storage")
}

func (ls *localStore) ResetWaitingList() {
	ls.entsMutex.Lock()
	defer ls.entsMutex.Unlock()
	ls.waitingMutex.Lock()
	defer ls.waitingMutex.Unlock()
	ls.ents = append(ls.ents, ls.waitingForCommit...)
}

func (ls *localStore) RemoveWaitingList() {
	ls.waitingMutex.Lock()
	defer ls.waitingMutex.Unlock()
	ls.waitingForCommit = []pb.Entry{}
}

func (ls *localStore) EntriesFilled() chan struct{} {
	return ls.entriesFilled
}

func (ls *localStore) WaitingForCommitFilled() chan struct{} {
	return ls.waitingFilled
}

//should be called in separate goroutine
func AddToChan(c chan struct{}) {
	c <- struct{}{}
}