package kvstore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
)

//KvStore it's a kvstore with raft as a backend
type KvStore struct {
	mu sync.RWMutex
	kvStore map[string]string
	processC chan<- [] byte
}

//Command for kv store operation
type Command int32

const (
	//SET for setting key value
	SET Command = 1
	//UPDATE for updating key value
	UPDATE Command = 2
	//DELETE for deleting key value
	DELETE Command = 3
)
// for sending encoded version of key value to raft cluster
type kv struct {
	Key   string
	Value string
	Command Command
}

//NewKvStore function to create a new KvStore
func NewKvStore(processC chan<- []byte, commitC <-chan []byte ) *KvStore {

	// TODO replay using snapshot is remaining
	kvStore := &KvStore{
		kvStore: make(map[string]string),
		processC: processC,
	}

	// start a new go routine for reading data from comit channel from raft
	go kvStore.readFromRaft(commitC)
	return kvStore
}

//Get to get value of the provided key
func (kvs *KvStore) Get(key string) (string, bool) {
	log.Printf("Trying to get value for the %s key", key)
	kvs.mu.RLock()
	defer kvs.mu.RUnlock()
	v, ok := kvs.kvStore[key]
	return v, ok
}

//Set function to set value for the given key
func (kvs *KvStore) Set(key string, value string) {
	log.Printf("Trying to set value for the %s key with value %s", key, value)
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{key, value, SET}); err != nil {
		log.Fatal(err)
	}
	log.Printf("Here done")
	//sending to raft to process and raft will reply on commit channel
	kvs.processC <- buf.Bytes()
	log.Printf("Done")
}

//ErrorKeyEmpty for sending message that error key is empty.
var ErrorKeyEmpty = fmt.Errorf("key parameter is empty")

//ErrorKeyNotFound for sending message that key not present in key-store
var ErrorKeyNotFound = fmt.Errorf("key not found")
//Delete function to delete key-value from the keystore
func (kvs *KvStore) Delete(key string) (error) {
	log.Printf("Trying to delete value for the %s key", key)


	if key=="" {
		return ErrorKeyEmpty
	}

	if _, ok := kvs.kvStore[key]; !ok {
		return ErrorKeyNotFound
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{Key:key, Command:DELETE}); err != nil {
		log.Fatal(err)
	}
	log.Printf("Delete done")
	//sending to raft to process and raft will reply on commit channel
	kvs.processC <- buf.Bytes()
	log.Printf("Done")
	return nil
}

func (kvs *KvStore) readFromRaft(commitC <-chan []byte){
	log.Printf("Reading from commit stream.")
	// keep receiving data from raft commit channel
	for data := range commitC {
		if data != nil {
			log.Printf("Received commit message")
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("could not decode message (%v)", err)
			}
			kvs.mu.Lock()
			switch dataKv.Command {
			case SET:
				kvs.kvStore[dataKv.Key] = dataKv.Value
			case UPDATE:
				kvs.kvStore[dataKv.Key] = dataKv.Value
			case DELETE:
				_, ok := kvs.kvStore[dataKv.Key];
				if ok {
					delete(kvs.kvStore, dataKv.Key);
				}
			}
			for k,v :=range kvs.kvStore {
				log.Printf("Key: %s Value: %s", k, v)
			}
			kvs.mu.Unlock()
		}else{
			//TODO
		}
	}
}