package node

import (
	"context"
	"log"
	"math/rand"
	pb "megrec/raft/pb/proto"
	"megrec/raft/types"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	//ID id of the node
	ID string
	peers types.ArrayFlags
	peerClients map[string]*types.Peer // for initializing clients
	nodeType Type
	currentTerm int64
	voteCount int
	votedFor string // mostly to know whom this node voted

	lastLogIndex int64
	lastCommitIndex int64
	currentLeader string

	// for log entries
	entrylogs []*pb.Entry
	
	//timers
	electionTimer *time.Timer
	heartBeatTimer *time.Ticker
	//node channels
	voteC chan *pb.VoteResponse
	appendEntryC chan appendEntryInput
	snapshotRequestC chan snapshotRequestInput
	processC chan []byte // channel from receiving from own rest server
	commitC chan<- []byte
}

//snapshotRequestInput type for handling snapshot request response
type snapshotRequestInput struct {
	snapshotRequest *pb.SnapshotRequest
	responseC chan *pb.SnapshotResponse
}

//appendEntryInput type for handling appendnetry request response
type appendEntryInput struct {
	appendEntry *pb.AppendEntryRequest
	responseC chan *pb.AppendEntryResponse
}

//NewNode to create a new node with id and its peers list
func NewNode(id string, peers []string, processC chan []byte, commitC chan<- []byte) *Node{
	return &Node {
		ID: id,
		peers: peers,
		nodeType: FOLLOWER,
		currentTerm: 0,
		entrylogs: make([]*pb.Entry, 0, 1000),
		peerClients: make(map[string]*types.Peer),
		voteC: make(chan *pb.VoteResponse),
		appendEntryC: make(chan appendEntryInput, 5),
		snapshotRequestC: make(chan snapshotRequestInput),
		processC: processC,
		commitC: commitC,
	}
}

//GetReadyForServing to wait for all cluster nodes to be available before starts request votes
func (node *Node) GetReadyForServing() {
	
	timer := time.NewTicker(200 * time.Millisecond)
	for range timer.C{
		log.Printf("Checking all peers whether they are online or not.")
		result:= node.checkPeers()
		log.Println(result)
		if result{
		
			break
		}
	}

	timer.Stop()
	t:=randomDuration()
	log.Printf("Timeout : %d", t)
	node.electionTimer = time.NewTimer(t)
	node.heartBeatTimer = time.NewTicker(1800* time.Millisecond)
	currentTime := time.Now()
 
    log.Println("Current Time in String: ", currentTime.String())

	// start go routine to accept different data on different channels
	go func(){
		for{
			select{
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
					log.Println("Current Node Status:")
					log.Printf("Last Index: %d",node.lastLogIndex)
					log.Printf("Last commit Index: %d",node.lastCommitIndex)
					log.Printf("Number of entries: %d",len(node.entrylogs))
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
							node.currentLeader = node.ID
							node.sendHeartBeat()
						}
					}		
				}
			case ae := <- node.appendEntryC:
				ae.responseC <- node.handleAppendEntryInput(ae)
				// heartbeat, append log entry
				// if heartbeat received and follower, reset heartbeat timer
				// if new term, make yourself follower and update leader and index, start heartbeat counter
			case sr := <- node.snapshotRequestC:
				sr.responseC <- node.handleSnapshotRequestInput(sr)
			case kv := <- node.processC:
				// if this node is leader, forward to appendEntryC
				if node.nodeType == LEADER {
					// store value in logs
					node.entrylogs = append(node.entrylogs, &pb.Entry{Data: kv} )
					log.Printf("Appending entry for keyvalue")
					//increment log index
					node.lastLogIndex = node.lastLogIndex + 1
					//send to peers
					cr := node.sendAppendEntry(kv)
					log.Printf("Done %t", cr)
					// after sending commit
					node.lastCommitIndex = node.lastLogIndex
					// send commit to peers
					log.Printf("Sending commits now.")
					node.sendCommits()
					// notify to connected store
					node.commitC <- kv
					log.Printf("Commit done.")
				}else{
					// else call leader service for appendEntry
					node.forwardRequest(kv)
				}
			}
		}
	}()
}

// function to forward any request coming to follower node to leader
func (node *Node) forwardRequest(kv []byte){
	node.peerClients[node.currentLeader].ClientConn.ForwardMessage(context.Background(),&pb.ForwardRequest{Data: kv})
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
				res, err1 := checkReady(client)
				if err1 != nil {
					online = online && false
					log.Printf("Peer node with link %s still not online.", peer)
				}else{
					node.peerClients[res.Id] = &types.Peer{
						Address: peer,
						ID: res.Id,
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
func checkReady(client pb.RaftClient) (*pb.ReadyResponse, error) {
	return client.Ready(context.Background(),&pb.ReadyRequest{IsReady: true})
}

// function for leader node to send heartbeat to peers
func (node *Node) sendHeartBeat() {
	// send heartbeat every peer
	
	for _, peerClient:= range node.peerClients {
		
		go func(clientConn pb.RaftClient){
			ret, err:= clientConn.AppendEntry(context.Background(), &pb.AppendEntryRequest{
				Term: node.currentTerm,
				LeaderId: node.currentLeader,
				Type: pb.EntryType_HeartBeat,
			})
			if err != nil {
				log.Printf("peer not online to receive heartbeat.")
			}else if ret.Success{
				log.Printf("Received successful response for the heartbeat from %s",ret.From)
			}
			
		}(peerClient.ClientConn)
	}
}

// function to send append Entries
func (node *Node) sendAppendEntry(kv []byte) (bool) {

	log.Printf("key value append : %s",kv)
	entries := make([]*pb.Entry, 0, 10)
	entries = append(entries, &pb.Entry{Data: kv} )

	var wg sync.WaitGroup
	for _, peerClient:= range node.peerClients {
		wg.Add(1)
		go func(peerClient *types.Peer){
			ret, err:= peerClient.ClientConn.AppendEntry(context.Background(), &pb.AppendEntryRequest{
				Term: node.currentTerm,
				LeaderId: node.currentLeader,
				Type: pb.EntryType_Append,
				Entries: entries,
				LastLogIndex: node.lastLogIndex,
			})
			if err != nil {
				log.Printf("peer not online to receive append entry.%s : %s",peerClient.ID, err.Error())
			}

			if ret.Success{
				// if successfully sent, store current log index and commit index
				peerClient.LastLogIndex = ret.CurrentLogIndex
				log.Printf("Received successful response for the append Entry from %s.",ret.From)
			}
			wg.Done()
		}(peerClient)	
	}
	wg.Wait()
	log.Println("Done append entry.")
	return true
}

//function to send latest commits
func (node *Node) sendCommits() {

	for _, peerClient:= range node.peerClients {
		go func(peerClient *types.Peer){
			ret, err:= peerClient.ClientConn.AppendEntry(context.Background(), &pb.AppendEntryRequest{
				Term: node.currentTerm,
				LeaderId: node.currentLeader,
				Type: pb.EntryType_Commit,
				LastLogIndex: peerClient.LastLogIndex,
				CommitIndex: node.lastCommitIndex,
			})
			if err != nil {
				log.Fatal("peer not online to receive heartbeat.")
			}

			if ret.Success{
				// if successfully sent, store current log index and commit index
				peerClient.LastCommitIndex = ret.CurrentCommitIndex
				log.Printf("Received successful response for the append Entry for commitfrom %s.",ret.From)
			}
		}(peerClient)
		
	}
}


// when candiate would like to do election, it requests votes
func (node *Node) requestVote() {
	for _,peerClient := range node.peerClients{
		//send vote request in parallel
		go func(peer *types.Peer){
			ret, err := peer.ClientConn.RequestVote(context.Background(), &pb.VoteRequest{
				Term: node.currentTerm,
				CandidateId: node.ID,
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
		log.Printf("%s voted for %s for term %d",node.ID,vr.CandidateId,vr.Term)
		return &pb.VoteResponse{
			Term: vr.Term,
			Voted: true,
			From: node.ID,
		}
		
	}else if node.nodeType == FOLLOWER && node.currentTerm == vr.Term && node.votedFor != vr.CandidateId {
		return &pb.VoteResponse{
			Term: vr.Term,
			Voted: false,
			From: node.ID,
		}
	}

	// if during voting, node become a candidate, return false
	return &pb.VoteResponse{
		Term: vr.Term,
		Voted: false,
		From: node.ID,
	}
}

//HandleAppendEntry function to handle append entry, it can be heartbeat, it can be actual append entry
func (node *Node) HandleAppendEntry(ae *pb.AppendEntryRequest) (*pb.AppendEntryResponse) {
	c := make(chan *pb.AppendEntryResponse)
	node.appendEntryC <- appendEntryInput{appendEntry: ae, responseC: c}
	result := <-c
	log.Printf("returning from hanlding append entry.")
	log.Printf("%t",result.Success)
	return result
}

//HandleForwardMessage to handle a message from followers
func (node *Node) HandleForwardMessage(fr *pb.ForwardRequest) (*pb.ForwardResponse) {
	// forward to process channel
	log.Println("Forward request received from a follower.")
	node.processC <- fr.Data
	return &pb.ForwardResponse{Success: true}
}

// function to handle heartbeat, append entry
func (node *Node) handleAppendEntryInput(ae appendEntryInput) (*pb.AppendEntryResponse) {
	log.Printf("Handling Append Entry from %s for the term %d and %d:",ae.appendEntry.LeaderId, ae.appendEntry.Term, ae.appendEntry.Type)
	
	log.Printf("Current node status:")
	log.Printf("Last Index: %d",node.lastLogIndex)
	log.Printf("Last commit Index: %d",node.lastCommitIndex)
	log.Printf("Number of entries: %d",len(node.entrylogs))
	flag := false
	if ae.appendEntry.Term >= node.currentTerm && ae.appendEntry.Type == pb.EntryType_HeartBeat {
		node.currentTerm = ae.appendEntry.Term
		node.currentLeader = ae.appendEntry.LeaderId
		node.nodeType = FOLLOWER
		flag = true
	}else if ae.appendEntry.Term == node.currentTerm && ae.appendEntry.Type == pb.EntryType_Append {
		log.Printf("Handling append entry.")
		node.entrylogs = append(node.entrylogs, ae.appendEntry.Entries...)
		node.lastLogIndex = ae.appendEntry.LastLogIndex
		flag = true
		log.Printf("Done till here.")
	}else if ae.appendEntry.Term == node.currentTerm && ae.appendEntry.Type == pb.EntryType_Commit {
		log.Printf("Commit request received..........")
		node.lastCommitIndex = ae.appendEntry.CommitIndex
		node.commitC <- node.entrylogs[int(node.lastCommitIndex)-1].Data
		flag = true
	}

	if(node.nodeType != LEADER && ae.appendEntry.Type== pb.EntryType_HeartBeat) {
		// whenever append entry receives from a leader, reset election timer
		restartElectionTimer(node.electionTimer)
	}
	
	if flag{
		log.Printf("Returning from append entry.")
		return &pb.AppendEntryResponse{Success: true, Term:ae.appendEntry.Term, From: node.ID, CurrentLogIndex: node.lastLogIndex, CurrentCommitIndex: node.lastCommitIndex}
	}
	// if not successful, send error
	return &pb.AppendEntryResponse{Success: false, Term:node.currentTerm, From: node.ID}
}

//HandleSnapshotRequest for handling snapshot request from peers
func (node *Node) HandleSnapshotRequest(sr *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	c:= make(chan *pb.SnapshotResponse)
	node.snapshotRequestC <- snapshotRequestInput{snapshotRequest: sr, responseC: c}
	result := <-c
	if result == nil {
		return nil, status.Errorf(codes.InvalidArgument, "cannot get snapshot for the range")
	}
	return result, nil
}

// function to  handle snapshot request from follower to leader
func (node *Node) handleSnapshotRequestInput(sr snapshotRequestInput) (*pb.SnapshotResponse) {
	log.Printf("Handling Snapshot request.")
	if sr.snapshotRequest.StartLogIndex < int64(len(node.entrylogs)) && sr.snapshotRequest.EndLogIndex < int64(len(node.entrylogs)) {
		snapshotEntries := make([]*pb.Entry,int(sr.snapshotRequest.EndLogIndex-sr.snapshotRequest.StartLogIndex))
		// copy data from not logs
		copy(node.entrylogs[int(sr.snapshotRequest.StartLogIndex):int(sr.snapshotRequest.EndLogIndex)],snapshotEntries)
		return &pb.SnapshotResponse {
			Entries: snapshotEntries,
			StartLogIndex: sr.snapshotRequest.StartLogIndex,
			EndLogIndex: sr.snapshotRequest.EndLogIndex,
		}
	}

	return nil
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