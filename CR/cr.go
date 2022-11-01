package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

var wg sync.WaitGroup

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type PutReply struct {
	Err     Err
	Next    string
	Prev    string
	Version int
}

type PutArgs struct {
	Key   string
	Value string
}

type GetArgs struct {
	Key  string
	Port string
}

type GetReply struct {
	Err     Err
	Value   string
	Version int
	Next    string
}

func connect(port_no string) *rpc.Client {
	client, err := rpc.Dial("tcp", port_no)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}

type bookkeeping struct {
	Next     string
	Prev     string
	HeadPort string
	TailPort string
	Id       string
	Version  map[string]int
}

type KV struct {
	mu      sync.Mutex
	storage map[string]string
	bk      bookkeeping
}

func node(id string, port_no string, next string, prev string, head string, tail string) {
	kv := new(KV)
	kv.storage = map[string]string{}
	kv.bk.Id = id
	kv.bk.Next = next
	kv.bk.Prev = prev
	kv.bk.HeadPort = head
	kv.bk.TailPort = tail
	kv.bk.Version = map[string]int{}

	rpcs := rpc.NewServer()
	rpcs.Register(kv)
	l, e := net.Listen("tcp", port_no)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		first := true
		for {
			if first {
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

func get(key string, port_no string) GetReply {
	wg.Add(1)
	defer wg.Done()
	reply := GetReply{}

	for port_no != "" {
		client := connect(port_no)
		args := GetArgs{key, port_no}
		reply = GetReply{}
		err := client.Call("KV.Get", &args, &reply)
		//fmt.Printf("connected to %s and isTail: %t\n", port_no, reply.isTail)
		//fmt.Println("next is ", reply.Next)
		if err != nil {
			log.Fatal("error:", err)
		}
		client.Close()
		port_no = reply.Next
		//fmt.Println("Next port:", port_no)
	}

	return reply
}

func put(key string, val string, head string) {
	wg.Add(1)
	defer wg.Done()

	//head := get("head", HeadPort).Value

	for head != "" {
		client := connect(head)
		args := PutArgs{key, val}
		reply := PutReply{}

		err := client.Call("KV.Put", &args, &reply)

		if err != nil {
			log.Fatal("error:", err)
		}
		client.Close()

		head = reply.Next
	}
}

func (kv *KV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, _ := kv.storage[args.Key]
	//fmt.Printf("port:%s tailport:%s\n", args.Port, kv.bk.TailPort)
	if args.Port == kv.bk.TailPort {
		reply.Value = val
		reply.Version = kv.bk.Version[args.Key]
		reply.Err = OK
		reply.Next = ""
		return nil
	} else {
		reply.Value = "dirty"
		reply.Err = ErrNoKey
		reply.Next = kv.bk.Next
	}
	return nil
}

func set_null(i int, n int, arr []string) string {
	if i >= n || i < 0 {
		return ""
	}
	return arr[i]
}

func (kv *KV) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.storage[args.Key] = args.Value
	kv.bk.Version[args.Key]++

	reply.Err = OK
	reply.Next = kv.bk.Next
	reply.Prev = kv.bk.Prev
	reply.Version = kv.bk.Version[args.Key]

	return nil
}

func main() {
	n := 5
	nodes := []string{"localhost:8000", "localhost:8001", "localhost:8002", "localhost:8003", "localhost:8004"}
	node_ports := make([]string, n)
	for i := 0; i < n; i++ {
		node_ports[i] = ":800" + string(i+48)
		//fmt.Println(nodes[i])
	}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go node(nodes[i], node_ports[i], set_null(i+1, n, node_ports), set_null(i-1, n, node_ports), node_ports[0], node_ports[n-1])
	}
	wg.Wait()

	go put("R1", "12.5", node_ports[0])

	time.Sleep(2 * time.Second)
	fmt.Println("R1 @8000: ", get("R1", node_ports[n-1]))

	go put("R1", "12.6", node_ports[0])
	time.Sleep(2 * time.Second)
	fmt.Println("R1 @8000: ", get("R1", node_ports[n-1]))

	go put("R1", "12.8", node_ports[0])
	time.Sleep(2 * time.Second)
	fmt.Println("R1 @8000: ", get("R1", node_ports[n-1]))

	wg.Wait()

}
