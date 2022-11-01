package node

import (
	"log"
	"net"
	"net/rpc"
	"sync"
)

type bookkeeping struct {
	Next             string
	Prev             string
	Coordinator_port string
	IsHead           bool
	IsTail           bool
	Id               string
	//Dirty            string
	DirtyMap map[string]string
	Version  map[string]int
}

type craq_coordinator struct {
	Port_number    string
	Nodes          []string
	Node_ports     []string
	Head_index     int
	Tail_index     int
	Latest_version map[string]int
}

type KV struct {
	mu   sync.Mutex
	data map[string]string
	Bk   bookkeeping
	Cc   craq_coordinator
}

func node(id string, port_no string, next string, prev string, coordinator string, isHead bool, isTail bool) {
	kv := new(KV)
	kv.Bk.Id = id
	kv.Bk.Next = next
	kv.Bk.Prev = prev
	kv.Bk.Coordinator_port = coordinator
	kv.Bk.IsHead = isHead
	kv.Bk.IsTail = isTail
	kv.data = map[string]string{}
	kv.Bk.DirtyMap = map[string]string{}
	kv.Bk.Version = map[string]int{}
	rpcs := rpc.NewServer()
	rpcs.Register(kv)
	l, e := net.Listen("tcp", port_no)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		first := true
		for {
			if first == true {
				first = false
				wg.Done()
			}
			conn, err := l.Accept()
			if err == nil {
				go rpcs.ServeConn(conn)
			}

		}
		l.Close()
	}()
}
