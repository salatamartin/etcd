package raftpb

import (
	serverpb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/pkg/pbutil"
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
//TODO: consider only PUT requests without prevValue
func (e *Entry) CompareMessage(e2 *Entry) bool {
	req1 := e.RetrieveMessage()
	req2 := e2.RetrieveMessage()
	return req1.Method == req2.Method && req1.Path == req2.Path
}
