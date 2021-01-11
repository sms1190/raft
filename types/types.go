package types

import (
	"megrec/raft/pb/proto"
)

//ArrayFlags for getting list of arguments
type ArrayFlags []string

func (i *ArrayFlags) String() string {
	return "my string representation"
}

//Set to set value
func (i *ArrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

//Peer for storing information about peer nodes
type Peer struct{
	ClientConn pb.RaftClient
	Address string
}