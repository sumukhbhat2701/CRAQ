package main

import (
	"CRAQ/transport"
	"fmt"
	"log"
	"math/rand"
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

func get(key string, port_no string) transport.GetReply {
	wg.Add(1)
	defer wg.Done()

	client := transport.Connect(port_no)
	args := transport.GetArgs{key}
	reply := transport.GetReply{}

	err := client.Call("KV.Get", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
	return reply
}

func get_latest(key string, port string) int {
	wg.Add(1)
	defer wg.Done()

	client := transport.Connect(port)
	args := transport.GetArgs{key}
	reply := transport.GetReply{}

	err := client.Call("KV.GetLatest", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
	return reply.Version
}

func updateCoordinator(key string, port string) {
	wg.Add(1)
	defer wg.Done()

	client := transport.Connect(port)
	args := transport.GetArgs{key}
	reply := transport.GetReply{}

	err := client.Call("KV.Update", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
	//return reply.Version
}

func put(key string, val string, cport_no string) {
	wg.Add(1)
	defer wg.Done()

	head := get("head", cport_no).Value
	tail := ""
	curr := ""

	//fmt.Printf("\nKEY:%s\n", key)
	//latest := get_latest(key, cport_no)
	//fmt.Println("Latest version:", latest)

	for head != "" {
		client := transport.Connect(head)
		version := "dirty"
		args := transport.PutArgs{key, val, version}
		reply := transport.PutReply{}
		//fmt.Println("Calling KV.Put for:", head, ", state:", version)
		err := client.Call("KV.Put", &args, &reply)
		//fmt.Println()
		if err != nil {
			log.Fatal("error:", err)
		}
		client.Close()

		curr = head
		head = reply.Next
		tail = reply.Prev
	}

	//fmt.Println("Here head is:", head, " and tail is:", tail)
	// Need to clean tail node which is next
	client := transport.Connect(curr)
	args := transport.PutArgs{key, val, "clean"}
	reply := transport.PutReply{}
	//fmt.Println("Calling KV.Put for:", curr, ", state:", "clean")
	err := client.Call("KV.Put", &args, &reply)
	//fmt.Println()
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()

	updateCoordinator(key, cport_no)

	//latest = get_latest("R1", cport_no)
	//fmt.Println("Latest version:", latest)

	for tail != "" {
		client := transport.Connect(tail)
		args := transport.PutArgs{key, val, "clean"}
		reply := transport.PutReply{}
		//fmt.Println("Calling KV.Put for:", tail, ", state:", "clean")
		err := client.Call("KV.Put", &args, &reply)
		//fmt.Println()
		if err != nil {
			log.Fatal("error:", err)
		}
		client.Close()

		//head = reply.Next
		tail = reply.Prev
	}

}

func (kv *KV) Update(args *transport.GetArgs, reply *transport.GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.Cc.Latest_version[args.Key]++
	reply.Version = kv.Cc.Latest_version[args.Key]
	return nil
}

func (kv *KV) GetLatest(args *transport.GetArgs, reply *transport.GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Version = kv.Cc.Latest_version[args.Key]
	return nil
}

func (kv *KV) Get(args *transport.GetArgs, reply *transport.GetReply) error {
	//fmt.Println("GET started for %s...", args.Key)
	random := (rand.Intn(5-1) + 1)
	time.Sleep(time.Duration(random) * time.Second)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Version = kv.Bk.Version[args.Key]
	if args.Key == "head" {
		reply.Err = ErrNoKey
		reply.Value = kv.Cc.Node_ports[kv.Cc.Head_index]

	} else if args.Key == "tail" {
		reply.Err = ErrNoKey
		reply.Value = kv.Cc.Node_ports[kv.Cc.Tail_index]
	} else if kv.Bk.DirtyMap[args.Key] == "dirty" {
		fmt.Println("Dirty data found for:", args.Key, "contacting tail... %s", kv.Bk.DirtyMap[args.Key])
		tail := get("tail", kv.Bk.Coordinator_port).Value
		reply.Value = get(args.Key, tail).Value
		reply.Err = OK
	} else {
		val, ok := kv.data[args.Key]
		if ok {
			reply.Err = OK
			reply.Value = val

		} else {
			reply.Err = ErrNoKey
			reply.Value = "Empty"
		}
	}
	//fmt.Println("GET ended for %s...", args.Key)
	return nil
}

func (kv *KV) Put(args *transport.PutArgs, reply *transport.PutReply) error {
	//fmt.Println("PUT started %s:%s...", args.Key, args.Value)
	random := (rand.Intn(3-1) + 1)
	time.Sleep(time.Duration(random) * time.Second)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//fmt.Println("isTail:", kv.Bk.IsTail)

	if args.Version == "dirty" {
		if kv.Bk.Next != "" {
			//kv.Bk.Dirty = args.Value
			kv.Bk.DirtyMap[args.Key] = "dirty"
		} else {
			kv.Bk.DirtyMap[args.Key] = "clean"
		}
		kv.Bk.DirtyMap[args.Key] = "clean"
		reply.Err = OK
		reply.Next = kv.Bk.Next
		reply.Prev = kv.Bk.Prev
		reply.Version = kv.Bk.Version[args.Key]
		//fmt.Printf("dirty PUT ended %s:%s and version no:%d...\n", args.Key, args.Value, reply.Version)
		return nil
	}
	//kv.Bk.Dirty = ""
	kv.Bk.Version[args.Key]++
	kv.Bk.DirtyMap[args.Key] = "clean"
	kv.data[args.Key] = args.Value
	reply.Err = OK
	reply.Next = kv.Bk.Next
	reply.Prev = kv.Bk.Prev
	reply.Version = kv.Bk.Version[args.Key]

	//var wg_temp sync.WaitGroup
	/*if kv.Bk.IsTail {
		fmt.Printf("In Here, Coordinator port:%s, Version no:%d\n", kv.Bk.Coordinator_port, reply.Version)
		v := get_latest("R1", kv.Bk.Coordinator_port)
		fmt.Println("Check $$$$$$$$$ ", v)
	}*/

	//fmt.Printf("PUT ended %s:%s and version no:%d...\n", args.Key, args.Value, reply.Version)
	return nil
}

func coordinator_node(port_no string, nodes []string, nodes_ports []string, hi int, ti int) {
	kv := new(KV)
	kv.Cc.Port_number = port_no
	kv.Cc.Nodes = nodes
	kv.Cc.Node_ports = nodes_ports
	kv.Cc.Head_index = hi
	kv.Cc.Tail_index = ti
	kv.Cc.Latest_version = map[string]int{}
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

func isHead(i int) bool {
	return i == 0
}

func isTail(i int, n int) bool {
	return i == n-1
}

func set_null(i int, n int, arr []string) string {
	if i >= n || i < 0 {
		return ""
	}
	return arr[i]
}

func main() {
	n := 5
	nodes := []string{"n1", "n2", "n3", "n4", "n5"}
	node_ports := []string{":8001", ":8002", ":8003", ":8004", ":8005"}
	coordinator_port := ":8000"

	wg.Add(1)
	go coordinator_node(coordinator_port, nodes, node_ports, 0, n-1)
	wg.Wait()

	for i := 0; i < n; i++ {
		wg.Add(1)
		go node(set_null(i, n, nodes), set_null(i, n, node_ports), set_null(i+1, n, node_ports), set_null(i-1, n, node_ports), coordinator_port, isHead(i), isTail(i, n))

	}
	wg.Wait()

	/* wg.Add(1) */
	go put("R1", "4.50", coordinator_port)
	fmt.Println("Here")
	/* wg.Done()
	wg.Wait() */

	latest := get_latest("R1", coordinator_port)
	fmt.Println("Latest version:", latest)

	go put("R2", "2.39", coordinator_port)

	fmt.Println("R1 @8001: ", get("R1", ":8001"))
	fmt.Println("R1 @8002: ", get("R1", ":8002"))
	fmt.Println("R1 @8003: ", get("R1", ":8003"))
	fmt.Println("R1 @8004: ", get("R1", ":8004"))
	fmt.Println("R1 @8005: ", get("R1", ":8005"))

	fmt.Println("R2 @8001: ", get("R2", ":8001"))
	fmt.Println("R2 @8002: ", get("R2", ":8002"))
	fmt.Println("R2 @8003: ", get("R2", ":8003"))
	fmt.Println("R2 @8004: ", get("R2", ":8004"))
	fmt.Println("R2 @8005: ", get("R2", ":8005"))

	go put("R1", "5.99", coordinator_port)

	fmt.Println("R1 @8001: ", get("R1", ":8001"))
	fmt.Println("R1 @8002: ", get("R1", ":8002"))
	fmt.Println("R1 @8003: ", get("R1", ":8003"))
	fmt.Println("R1 @8004: ", get("R1", ":8004"))
	fmt.Println("R1 @8005: ", get("R1", ":8005"))

	fmt.Println("R2 @8001: ", get("R2", ":8001"))
	fmt.Println("R2 @8002: ", get("R2", ":8002"))
	fmt.Println("R2 @8003: ", get("R2", ":8003"))
	fmt.Println("R2 @8004: ", get("R2", ":8004"))
	fmt.Println("R2 @8005: ", get("R2", ":8005"))

	fmt.Println("R1 @8001: ", get("R1", ":8001"))
	fmt.Println("R1 @8002: ", get("R1", ":8002"))
	fmt.Println("R1 @8003: ", get("R1", ":8003"))
	fmt.Println("R1 @8004: ", get("R1", ":8004"))
	fmt.Println("R1 @8005: ", get("R1", ":8005"))

	wg.Wait()

}
