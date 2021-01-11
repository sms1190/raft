package main

import (
	"flag"
	"fmt"
	"log"
	pb "megrec/raft/pb/proto"
	"megrec/raft/raftserver"
	"megrec/raft/types"
	"net"

	"google.golang.org/grpc"
)

func main(){
	// get port, id and peers links
	port:= flag.Int("port",0,"the server port")
	id:= flag.String("id","default","id of the node")
	var peers types.ArrayFlags
	flag.Var(&peers,"peers","list of peers")

	flag.Parse()
	fmt.Println(*port)
	fmt.Println(*id)
	fmt.Println(peers)
	
	log.Printf("start server on port %d", *port)

	
	// create a new server with new node
	raftServer := raftserver.NewRaftServer(*id, peers)

	go raftServer.Node.GetReadyForServing()
	// register server
	grpcServer := grpc.NewServer()
	pb.RegisterRaftServer(grpcServer, raftServer)

	address := fmt.Sprintf("0.0.0.0:%d", *port)
	listner, err := net.Listen("tcp",address)
	if err !=nil{
		log.Fatal("cannot start server:", err)
	}

	
	log.Println("server started")
	// wait for every peers to be ready
	//raftServer.GetReadyForServing()
	// start listening
	err = grpcServer.Serve(listner)
	if err !=nil{
		log.Fatal("cannot start server:", err)
	}

	
}
