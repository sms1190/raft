package raftserver

import (
	"context"
	"log"
	"megrec/raft/node"
	pb "megrec/raft/pb/proto"
	"megrec/raft/types"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RaftServer provides services for raft
type RaftServer struct {
	Node *node.Node
	pb.UnimplementedRaftServer
}

//NewRaftServer creates a new raft server
func NewRaftServer(id string, peers types.ArrayFlags, processC chan []byte, commitC chan []byte) *RaftServer{
	node := node.NewNode(id, peers, processC, commitC)
	return &RaftServer{
		Node: node,
	}
}

/*
//GetReadyForServing is a function to start a server and wait for all other nodes to be ready
func (server *RaftServer) GetReadyForServing(){
	// need to see if all
	
	server.node.GetReadyForServing() 
}*/

//RequestVote function to handle request vote coming from candidate nodes
func (server *RaftServer) RequestVote(ctx context.Context, vr *pb.VoteRequest) (*pb.VoteResponse, error){
	
	if vr==nil{
		return nil, status.Error(codes.NotFound,"Request is empty")
	}
	log.Printf("Vote Request came from %s", vr.CandidateId)
	return server.Node.HandleRequestVote(vr), nil
}

//AppendEntry function to handle append entry and heartbeat
func (server *RaftServer) AppendEntry(ctx context.Context, ae *pb.AppendEntryRequest) (*pb.AppendEntryResponse, error){
	if ae==nil{
		return nil, status.Error(codes.NotFound, "Request is epmpty")
	}
	
	log.Printf("Append Request came from %s", ae.LeaderId)
	return server.Node.HandleAppendEntry(ae), nil
}

//Ready function to let clients know that server is ready to accept other requests too
func (server *RaftServer) Ready(ctx context.Context, rr *pb.ReadyRequest) (*pb.ReadyResponse, error){
	if rr==nil {
		return nil, status.Error(codes.NotFound, "Request is empty")
	}
	log.Printf("Ready Request received.")
	return &pb.ReadyResponse{Ready:true, Id: server.Node.ID }, nil
}

//ForwardMessage function to handle forward message came from a follower
func (server *RaftServer) ForwardMessage(ctx context.Context, fr *pb.ForwardRequest) (*pb.ForwardResponse, error){
	if fr == nil {
		return nil, status.Error(codes.NotFound, "Request is empty")
	}
	log.Printf("Forward request received.")
	return server.Node.HandleForwardMessage(fr), nil
}