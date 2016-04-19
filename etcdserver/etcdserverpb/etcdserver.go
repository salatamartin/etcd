package etcdserverpb

func (request *Request) Equals(request2 Request) bool {
	return request.Method == request2.Method && request.Path == request2.Path && request.Val == request2.Val
}
