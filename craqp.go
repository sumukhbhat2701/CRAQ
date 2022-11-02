
package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"github.com/pkg/profile"
	"math/rand"
	"strconv"
	"time"
)

var wg sync.WaitGroup

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
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

func connect(port_no string) *rpc.Client {
	client, err := rpc.Dial("tcp", port_no)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}

func get(key string, port_no string) GetReply {
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
	return reply
}

func get_latest(key string, port string) int {
	wg.Add(1)
	defer wg.Done()

	client := connect(port)
	args := GetArgs{key}
	reply := GetReply{}

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

	client := connect(port)
	args := GetArgs{key}
	reply := GetReply{}

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
		client := connect(head)
		version := "dirty"
		args := PutArgs{key, val, version}
		reply := PutReply{}
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
	client := connect(curr)
	args := PutArgs{key, val, "clean"}
	reply := PutReply{}
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
		client := connect(tail)
		args := PutArgs{key, val, "clean"}
		reply := PutReply{}
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

func (kv *KV) Update(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.Cc.Latest_version[args.Key]++
	reply.Version = kv.Cc.Latest_version[args.Key]
	return nil
}

func (kv *KV) GetLatest(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Version = kv.Cc.Latest_version[args.Key]
	return nil
}

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

func (kv *KV) Get(args *GetArgs, reply *GetReply) error {
	//fmt.Println("GET started for %s...", args.Key)
	//random := (rand.Intn(5-1) + 1)
	//time.Sleep(time.Duration(random) * time.Second)
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

func (kv *KV) Put(args *PutArgs, reply *PutReply) error {
	//fmt.Println("PUT started %s:%s...", args.Key, args.Value)
	// random := (rand.Intn(3-1) + 1)
	//time.Sleep(time.Duration(random) * time.Second)
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
	rand.Seed(time.Now().UnixNano())
	defer profile.Start(profile.ProfilePath(".")).Stop()

	n := 1000   // parameter !!
	nodes := make([]string, n)
	node_ports := make([]string, n)
	coordinator_port := ":6999"
	portNo := 7000
	
	for i := 0; i < n; i++ {
		node_ports[i] = ":" + strconv.Itoa(portNo)
		portNo++
		nodes[i] = "localhost" + node_ports[i] 
	}

	wg.Add(1)
	go coordinator_node(coordinator_port, nodes, node_ports, 0, n-1)
	wg.Wait()
	
	for i := 0; i < n; i++ {
		wg.Add(1)
		go node(set_null(i, n, nodes), set_null(i, n, node_ports), set_null(i+1, n, node_ports), set_null(i-1, n, node_ports), coordinator_port, isHead(i), isTail(i, n))
	}
	wg.Wait()
	
	//time.Sleep(30 * time.Second)
	
	reads := 400 // parameter !!
	writes := 200 // parameter !!
	readsLeft := reads
	writesLeft := writes
	ops := make([]string, reads + writes)
	max := reads+writes
	min := 1
	for i := 0; i<(reads+writes); i++ {
		rorw := rand.Intn(max-min) + min
		if rorw%2 == 0 {
			if(readsLeft > 0) {
				readsLeft--;
				ops[i] = "r"
			} else {
				writesLeft--;
				ops[i] = "w"
			}
		} else {
			if(writesLeft > 0) {
				writesLeft--;
				ops[i] = "w"
			} else {
				readsLeft--;
				ops[i] = "r"
			}
 			
		}
		//fmt.Println(ops[i]);
	}
	
	minKey := 1
	maxKey:= int((reads+writes)/2)
	
	RRLoadBalancer := 0
	portNo = portNo - n
	fmt.Println(portNo)
	for i:=0; i<(reads+writes); i++ {
		key := strconv.Itoa(rand.Intn(maxKey-minKey) + minKey)
		if(ops[i] == "r") {
			fmt.Println(i, ": get ", key);
			port_no := ":" + strconv.Itoa(portNo + RRLoadBalancer)
			RRLoadBalancer = (RRLoadBalancer + 1)%n;
			go get(key, port_no)
		} else {
			value := strconv.Itoa(rand.Intn(1000000))
			fmt.Println(i, ": put ", key, " : ", value);
			go put(key, value, coordinator_port)
		}
	}

	wg.Wait()

}
