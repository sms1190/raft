package node

import (
	"context"
	"log"
	"math/rand"
	pb "megrec/raft/pb/proto"
	"megrec/raft/types"
	"time"

	"google.golang.org/grpc"
)

//Type to know wheter it's a leader,candidate or follower
type Type int32

const (
	//LEADER when this node is a leader
	LEADER Type = 1 
	//CANDIDATE when node becomes candidate
	CANDIDATE Type = 2
	//FOLLOWER when node is a follower 
	FOLLOWER Type = 3 
)

//Node type defined for initializing raft node
type Node struct{
	id string
	peers types.ArrayFlags
	peerClients map[string]*types.Peer // for initializing clients
	nodeType Type
	currentTerm int64
	voteCount int
	votedFor string // mostly to know whom this node voted

	lastLogIndex int64
	lastCommitIndex int64
	currentLeader string

	//timers
	electionTimer *time.Timer
	heartBeatTimer *time.Ticker
	//node channels
	voteC chan *pb.VoteResponse
	appendEntryC chan appendEntryInput
}

//appendEntryInput type for handling appendnetry request response
type appendEntryInput struct{
	appendEntry *pb.AppendEntryRequest
	responseC chan *pb.AppendEntryResponse
}
//NewNode to create a new node with id and its peers list
func NewNode(id string, peers []string) *Node{
	return &Node{
		id: id,
		peers: peers,
		nodeType: FOLLOWER,
		currentTerm: 0,
		peerClients: make(map[string]*types.Peer),
		voteC: make(chan *pb.VoteResponse),
		appendEntryC: make(chan appendEntryInput),
	}
}

//GetReadyForServing to wait for all cluster nodes to be available before starts request votes
func (node *Node) GetReadyForServing(){
	
	timer := time.NewTicker(200 * time.Millisecond)
	for range timer.C{
		log.Printf("Checking all peers whether they are online or not.")
		result:= node.checkPeers()
		log.Println(result)
		if result{
		
			break
		}
		log.Println("here")
	}

	log.Println("done")
	
	timer.Stop()
	
	log.Println("done")
	t:=randomDuration()
	log.Printf("Timeout : %d", t)
	node.electionTimer = time.NewTimer(t)
	node.heartBeatTimer = time.NewTicker(1800* time.Millisecond)
	currentTime := time.Now()
 
    log.Println("Current Time in String: ", currentTime.String())
	log.Println("All peers are ready.")
	// start go routine to accept different data on different channels
	go func(){
		for{
			select{
			//case <- node.signal:
				// received signal that node and peers are ready
				// start leader election timeout
			//	log.Println("Election timer started")
			//	node.electionTimer = time.NewTimer(randomDuration())
			case <- node.electionTimer.C:
				// election timeout happened
				// request vote
				log.Println("Election timeout received.")
				currentTime := time.Now()
    			log.Println("Election Time in String: ", currentTime.String())
				node.heartBeatTimer.Reset(1800 * time.Millisecond)
				if node.nodeType != LEADER{
					node.nodeType = CANDIDATE
					node.voteCount = 1 // own vote
					node.currentTerm = node.currentTerm + 1
					node.requestVote()
				}

				//stop electionTimer if got enough votes
			case <- node.heartBeatTimer.C:
				if node.nodeType == LEADER {
					log.Println("Heartbeat timeout received. send heartbeat")
					// if leader, there would be an heartbeat time working
					// send heartbeat to peers
					node.sendHeartBeat()
				}
			case vr := <- node.voteC:
				log.Println("Vote request received.")
				// when peers vote for you
				if node.nodeType == CANDIDATE{
					log.Printf("Vote came with %t for term %d and current Term %d", vr.Voted, vr.Term, node.currentTerm)
					if vr.Voted && node.currentTerm == vr.Term{
						node.voteCount++
						if wonElection(node.voteCount, len(node.peers)){
							
							// send append entry with new term and log index
							log.Printf("Won election by %d",node.voteCount)
							node.nodeType = LEADER
							node.currentLeader = node.id
							node.sendHeartBeat()
						}
					}		
				}
			case ae := <- node.appendEntryC:
				ae.responseC <- node.handleAppendEntryInput(ae)
				// heartbeat, append log entry
				// if heartbeat received and follower, reset heartbeat timer
				// if new term, make yourself follower and update leader and index, start heartbeat counter
			}
		}
	}()
}

// internal function to check whether peers are online or not
func (node *Node) checkPeers() (bool) {
	
	online := true 
	// whether all peers are now online or not
	log.Println(node.peers)
	for _, peer:= range node.peers{
		_, ok := node.peerClients[peer]
		if !ok{
			client, err := connectToPeer(peer)

			if err != nil{
				online = online && false
				log.Printf("Peer node with link %s still not online.", peer)
			}else{
				_, err1 := checkReady(client)
				if err1 != nil {
					online = online && false
					log.Printf("Peer node with link %s still not online.", peer)
				}else{
					node.peerClients[peer] = &types.Peer{
						Address: peer,
						ClientConn: client,
					}
					online = online && true
				}
			}
		}		
	}
	return online
}

// function to check whether service is up and running
func checkReady(client pb.RaftClient) (*pb.ReadyResponse, error){
	return client.Ready(context.Background(),&pb.ReadyRequest{IsReady: true})
}

// function for leader node to send heartbeat to peers
func (node *Node) sendHeartBeat(){
	// send heartbeat every peer
	for _, peerClient:= range node.peerClients {
		go func(clientConn pb.RaftClient){
			ret, err:= clientConn.AppendEntry(context.Background(), &pb.AppendEntryRequest{
				Term: node.currentTerm,
				LeaderId: node.currentLeader,
			})
			if err != nil {
				log.Printf("peer not online to receive heartbeat.")
			}else if ret.Success{
				log.Printf("Received successful response for the heartbeat.")
			}
		}(peerClient.ClientConn)
		
	}
}

func (node *Node) sendAppendEntry(){
	for _, peerClient:= range node.peerClients {
		go func(clientConn pb.RaftClient){
			ret, err:= clientConn.AppendEntry(context.Background(), &pb.AppendEntryRequest{
				Term: node.currentTerm,
				LeaderId: node.currentLeader,
			})
			if err != nil {
				log.Fatal("peer not online to receive heartbeat.")
			}

			if ret.Success{
				log.Printf("Received succesful response for the heartbeat.")
			}
		}(peerClient.ClientConn)
		
	}
}
// when candiate would like to do election, it requests votes
func (node *Node) requestVote(){
	for _,peerClient := range node.peerClients{
		//send vote request in parallel
		go func(peer *types.Peer){
			ret, err := peer.ClientConn.RequestVote(context.Background(), &pb.VoteRequest{
				Term: node.currentTerm,
				CandidateId: node.id,
			})
			if err != nil{
				log.Printf("Didn't receive vote from peer.")	
			}else{
				log.Printf("Received..")
				// submit vote to channel
				node.voteC <- ret
			}
			
		}(peerClient)
	}
}

//HandleRequestVote function to handle vote if it's a follower node
func (node *Node) HandleRequestVote(vr *pb.VoteRequest) (*pb.VoteResponse){
	if node.nodeType == FOLLOWER && node.currentTerm < vr.Term { // it means, never voted in this term
		// if successful, keep for whom voted
		node.votedFor = vr.CandidateId
		log.Printf("%s voted for %s for term %d",node.id,vr.CandidateId,vr.Term)
		return &pb.VoteResponse{
			Term: vr.Term,
			Voted: true,
			From: node.id,
		}
		
	}else if node.nodeType == FOLLOWER && node.currentTerm == vr.Term && node.votedFor != vr.CandidateId {
		return &pb.VoteResponse{
			Term: vr.Term,
			Voted: false,
			From: node.id,
		}
	}

	// if during voting, node become a candidate, return false
	return &pb.VoteResponse{
		Term: vr.Term,
		Voted: false,
		From: node.id,
	}
}

//HandleAppendEntry function to handle append entry, it can be heartbeat, it can be actual append entry
func (node *Node) HandleAppendEntry(ae *pb.AppendEntryRequest) (*pb.AppendEntryResponse) {
	c := make(chan *pb.AppendEntryResponse)
	node.appendEntryC <- appendEntryInput{appendEntry: ae, responseC: c}
	result := <-c
	return result
}


// function to handle heartbeat, append entry
func (node *Node) handleAppendEntryInput(ae appendEntryInput) (*pb.AppendEntryResponse){
	log.Printf("Handling Append Entry from %s for the term %d:",ae.appendEntry.LeaderId,ae.appendEntry.Term)
	if ae.appendEntry.Term> node.currentTerm {
		node.currentTerm = ae.appendEntry.Term
		node.currentLeader = ae.appendEntry.LeaderId
		node.nodeType = FOLLOWER
	}

	if(node.nodeType != LEADER){
		// whenever append entry receives from a leader, reset election timer
		restartElectionTimer(node.electionTimer)
	}
	return &pb.AppendEntryResponse{Success: true, Term:ae.appendEntry.Term, From: node.id}
}

func restartElectionTimer(timer *time.Timer){
	log.Printf("Restarting timer.")
	stopped := timer.Stop()
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(randomDuration())
}

//connectToPeer try to ping peer whether available or not
func connectToPeer(peer string) (pb.RaftClient, error) {
	log.Printf("Trying to connect to %s",peer)
	
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background

	if err != nil {
		log.Fatal(err)
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}
// function to check whether node requesting election won the election or not
func wonElection(voteCount int, peersLength int) (bool){
	if peersLength == 0 && voteCount >= 0 {
		return true
	} else if peersLength == 1 && voteCount >= 2 {
		return true
	} else if peersLength > 1 && voteCount >= 1+(peersLength+1)/2 {
		return true
	} else {
		log.Printf("Lose election")
	}
	return false
}
// Compute a random duration in milliseconds
func randomDuration() time.Duration {
	// Constant
	
	const DurationMax = 5000
	const DurationMin = 2500
	rand.Seed(time.Now().UnixNano())
	t:=rand.Intn(DurationMax-DurationMin)+DurationMin
	log.Printf("Timeout value: %d", t)
	return time.Duration(t) * time.Millisecond
}