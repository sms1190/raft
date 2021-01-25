package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"megrec/raft/handlers"

	pb "megrec/raft/pb/proto"
	"megrec/raft/raftserver"
	"megrec/raft/types"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	gh "github.com/gorilla/handlers"

	"github.com/gorilla/mux"
	"google.golang.org/grpc"
)

func main(){
	// get port, id and peers links
	port:= flag.Int("port",0,"the grpc server port")
	restport := flag.Int("restport",0,"the rest server port")
	id:= flag.String("id","default","id of the node")
	var peers types.ArrayFlags
	flag.Var(&peers,"peers","list of peers")

	flag.Parse()
	fmt.Println(*port)
	fmt.Println(*id)
	fmt.Println(peers)
	
	log.Printf("start server on port %d", *port)

	
	processC := make(chan []byte)
	commitC := make(chan []byte)
	// create a new server with new node
	raftServer := raftserver.NewRaftServer(*id, peers,processC, commitC)

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
	//err = grpcServer.Serve(listner)
	//if err !=nil{
	//	log.Fatal("cannot start server:", err)
	//}

	go grpcServer.Serve(listner)

	l:= log.New(os.Stdout,"key-value store API",log.LstdFlags)

	kvstoreH := handlers.NewStore(l, processC, commitC)
	sm:= mux.NewRouter()
	putRouter := sm.Methods("POST").Subrouter()
	putRouter.HandleFunc("/store",kvstoreH.Add)

	getRouter := sm.Methods("GET").Subrouter()
	getRouter.HandleFunc("/store/{key}",kvstoreH.Get)

	deleteRouter := sm.Methods("DELETE").Subrouter()
	deleteRouter.HandleFunc("/store/{key}", kvstoreH.Delete)


	//headersOk := gh.AllowedHeaders([]string{"X-Requested-With"})
	originsOk := gh.AllowedOrigins([]string{"*"})
	methodsOk := gh.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "OPTIONS"})
	server := &http.Server{
		Addr: fmt.Sprintf(":%d",*restport),
		Handler:  gh.CORS(originsOk, methodsOk)(sm),
		IdleTimeout: 120*time.Second,
		ReadTimeout: 1*time.Second,
		WriteTimeout: 1* time.Second,
	}
	
	// start the server
	//go func() {
		
	
	//}()
	err1 := server.ListenAndServe()
	log.Println("Server started.")
	if err1 != nil {
		
		os.Exit(1)
	}
	// create a channel
	c:=make(chan os.Signal,1)

	// send message to that channel to notify about os interruption or kill
	signal.Notify(c,os.Interrupt)
	signal.Notify(c, os.Kill)

	sig:=<-c
	l.Println("Received terminate, graceful shutdown", sig)
	tc, _ :=context.WithTimeout(context.Background(),30*time.Second)
	server.Shutdown(tc)
}
