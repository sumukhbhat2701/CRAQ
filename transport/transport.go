package transport

import (
	"log"
	"net/rpc"
)

type Err string

type PutArgs struct {
	Key     string
	Value   string
	Version string
}

type PutReply struct {
	Err     Err
	Next    string
	Prev    string
	Version int
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err     Err
	Value   string
	Version int
}

func Connect(port_no string) *rpc.Client {
	client, err := rpc.Dial("tcp", port_no)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}
