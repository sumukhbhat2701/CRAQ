package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
	"math/rand"
)


var wg sync.WaitGroup


const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type PutArgs struct {
	Key   string
	Value string
	Version string
}

type PutReply struct {
	Err Err
	Next string
	Prev string

}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}


func connect(port_no string) *rpc.Client {
	client, err := rpc.Dial("tcp", port_no)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}

func get(key string, port_no string) string {
	wg.Add(1)
	defer wg.Done()

	client := connect(port_no)
	args := GetArgs{key}
	reply := GetReply{}
	err := client.Call("KV.Get", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
	return reply.Value
}

func put(key string, val string, cport_no string) {
	wg.Add(1)
	defer wg.Done()

	head := get("head", cport_no)
	tail := ""

	for head != "" {
		client := connect(head)
		version := "dirty"
		args := PutArgs{key, val, version}
		reply := PutReply{}
		err := client.Call("KV.Put", &args, &reply)
		if err != nil {
			log.Fatal("error:", err)
		}
		client.Close()

		head = reply.Next;
		tail = reply.Prev;
	}

	for tail != "" {
		client := connect(tail)
		args := PutArgs{key, val, "clean"}
		reply := PutReply{}
		err := client.Call("KV.Put", &args, &reply)
		if err != nil {
			log.Fatal("error:", err)
		}
		client.Close()

		head = reply.Next;
		tail = reply.Prev;
	}
}


type bookkeeping struct {
	Next string
	Prev string
	Coordinator_port string
	IsHead bool
	IsTail bool
	Id string
	Dirty string
}

type craq_coordinator struct {
	Port_number string
	Nodes []string
	Node_ports[] string
	Head_index int
	Tail_index int
}

type KV struct {
	mu   sync.Mutex
	data map[string]string
	Bk bookkeeping
	Cc craq_coordinator
}


func node(id string, port_no string, next string, prev string, coordinator string, isHead bool, isTail bool) {
	kv := new(KV)
	kv.Bk.Id = id
	kv.Bk.Next = next
	kv.Bk.Prev = prev
	kv.Bk.Coordinator_port = coordinator
	kv.Bk.IsHead = isHead;
	kv.Bk.IsTail = isTail;
 	kv.data = map[string]string{}
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

func (kv *KV) Get(args *GetArgs, reply *GetReply) error {
	//fmt.Println("GET started for %s...", args.Key);
	random := (rand.Intn(5 - 1) + 1) 
	time.Sleep(time.Duration(random) * time.Second);
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Key == "head" {
		reply.Err = ErrNoKey
		reply.Value = kv.Cc.Node_ports[kv.Cc.Head_index] 
		
	} else if args.Key == "tail" {
		reply.Err = ErrNoKey
		reply.Value = kv.Cc.Node_ports[kv.Cc.Tail_index]
	} else if kv.Bk.Dirty != "" {
		fmt.Println("Dirty data found, contacting tail...");
		tail := get("tail", kv.Bk.Coordinator_port);
		reply.Value = get(args.Key, tail);
		reply.Err = OK
	} else {
		val, ok := kv.data[args.Key]
		if ok {
			reply.Err = OK
			reply.Value = val
		
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
	}
	//fmt.Println("GET ended for %s...", args.Key);
	return nil
}

func (kv *KV) Put(args *PutArgs, reply *PutReply) error {
	//fmt.Println("PUT started %s:%s...", args.Key, args.Value);
	random := (rand.Intn(3 - 1) + 1) 
	time.Sleep(time.Duration(random) * time.Second);
	kv.mu.Lock()
	defer kv.mu.Unlock()
 	if(args.Version == "dirty") {
		if(kv.Bk.Next != "") {
			kv.Bk.Dirty = args.Value
		} else {
			kv.Bk.Dirty = ""
		}
		reply.Err = OK
		reply.Next = kv.Bk.Next
		reply.Prev = kv.Bk.Prev
		return nil
	}
	kv.Bk.Dirty = ""
	kv.data[args.Key] = args.Value
	reply.Err = OK
	reply.Next = kv.Bk.Next;
	reply.Prev = kv.Bk.Prev;
	//fmt.Println("PUT ended %s:%s...", args.Key, args.Value);
	return nil
}



func coordinator_node(port_no string, nodes []string, nodes_ports []string, hi int, ti int) {
	kv := new(KV)
	kv.Cc.Port_number = port_no
	kv.Cc.Nodes = nodes
	kv.Cc.Node_ports = nodes_ports
	kv.Cc.Head_index = hi
	kv.Cc.Tail_index = ti
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
	return i == 0;
}

func isTail(i int, n int) bool {
	return i == n-1;
}

func set_null(i int, n int, arr []string) string {
	if i >= n || i < 0 {
		return "";
	}
	return arr[i];
}


func main() {
	n := 5;
	nodes := []string{"n1", "n2", "n3", "n4", "n5"};
	node_ports := []string{":8001", ":8002", ":8003", ":8004", ":8005"};
	coordinator_port := ":8000"
	
	wg.Add(1)
	go coordinator_node(coordinator_port, nodes, node_ports, 0, n-1);
	wg.Wait()

	for i:=0; i<n; i++ {
		wg.Add(1)
		go node(set_null(i, n, nodes), set_null(i, n, node_ports), set_null(i+1, n, node_ports), set_null(i-1, n, node_ports), coordinator_port, isHead(i), isTail(i, n));

	}
	wg.Wait()

	go put("Without me", "4.50", coordinator_port);
	go put("Falling", "2.39", coordinator_port);
	

	fmt.Println("Without me @8001: ", get("Without me", ":8001"));
	fmt.Println("Without me @8002: ", get("Without me", ":8002"));
	fmt.Println("Without me @8003: ", get("Without me", ":8003"));
	fmt.Println("Falling @8004: ", get("Falling", ":8004"));
	
	go put("24K Magic", "3.45", coordinator_port);
	fmt.Println("24K Magic @8003: ", get("24K Magic", ":8003"));
	

	go put("Falling", "2.389", coordinator_port);
	go put("Without me", "4.51", coordinator_port);
	

	fmt.Println("Without me @8001: ", get("Without me", ":8001"));
	fmt.Println("Without me @8002: ", get("Without me", ":8002"));
	fmt.Println("Without me @8003: ", get("Without me", ":8003"));
	fmt.Println("Falling @8004: ", get("Falling", ":8004"));


	fmt.Println("Without me @8001: ", get("Without me", ":8001"));
	fmt.Println("Without me @8002: ", get("Without me", ":8002"));
	fmt.Println("Without me @8003: ", get("Without me", ":8003"));
	fmt.Println("Falling @8004: ", get("Falling", ":8004"));

	
	fmt.Println("Without me @8001: ", get("Without me", ":8001"));
	fmt.Println("Without me @8002: ", get("Without me", ":8002"));
	fmt.Println("Without me @8003: ", get("Without me", ":8003"));
	fmt.Println("Falling @8004: ", get("Falling", ":8004"));
	
	wg.Wait()

}

