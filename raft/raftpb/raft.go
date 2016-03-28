package raftpb

import (
	"fmt"
	serverpb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/pkg/pbutil"
	"time"
)

func (e *Entry) RetrieveMessage() serverpb.Request {
	var request serverpb.Request
	var raftReq serverpb.InternalRaftRequest
	if !pbutil.MaybeUnmarshal(&raftReq, e.Data) { // backward compatible
		pbutil.MustUnmarshal(&request, e.Data)
	} else {
		switch {
		case raftReq.V2 != nil:
			request = *raftReq.V2
		}
	}
	return request
}

//returns true if messages have same method and same key
func (e *Entry) CompareMessage(e2 Entry) bool {
	req1 := e.RetrieveMessage()
	req2 := e2.RetrieveMessage()
	return req1.Method == req2.Method && req1.Path == req2.Path
}

func (e *Entry) AddTimestamp() {
	e.Timestamp = time.Now().UnixNano()
}

func (e *Entry) Print() string {
	msg := e.RetrieveMessage()
	msgStr := fmt.Sprintf("M:%s K:%s V:%s NQP:%t", msg.Method, msg.Path, msg.Val, msg.NoQuorumRequest)
	return fmt.Sprintf("{T:%d I:%d Ts:%v R:%x M:(%s)}", e.Term, e.Index, time.Unix(0, e.Timestamp), e.Receiver, msgStr)
}

func NewTimestamp() int64 {
	return time.Now().UnixNano()
}

func IsHardStateEqual(a, b HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st HardState) bool {
	return IsHardStateEqual(st, HardState{})
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp Snapshot) bool {
	return sp.Metadata.Index == 0
}

func CreateMessage(r ...serverpb.Request) Message {
	var ents []Entry
	for index, req := range r {
		reqData, _ := req.Marshal()
		ents[index] = Entry{Data: reqData}
	}
	return Message{Entries: ents}
}
