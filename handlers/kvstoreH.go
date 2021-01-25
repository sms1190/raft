package handlers

import (
	"log"
	"megrec/raft/kvstore"
	"net/http"

	"github.com/gorilla/mux"
)

//Store for handling CRUD operation for Key-Value store
type Store struct {
	l *log.Logger
	kvStore *kvstore.KvStore
}

//NewStore to create a new store with key value store
func NewStore(l *log.Logger, processC chan<- []byte, commitC chan []byte)  *Store {
	kvstore := kvstore.NewKvStore(processC, commitC)
	return &Store{
		l: l,
		kvStore: kvstore,
	}
}

//Add function to handle adding key-value to the kvstore
func (s *Store) Add(rw http.ResponseWriter, rq *http.Request){
	s.l.Println("Handling add key call")
	
	
	
	key := rq.FormValue("key")
	value := rq.FormValue("value")
	s.l.Printf("Setting value for the %s", key)
	if value !="" {
		s.kvStore.Set(key,value)
	}
	rw.WriteHeader(http.StatusOK)
	rw.Write([] byte("Successfully Added the key."))
}

//Get function to handle adding key-value to the kvstore
func (s *Store) Get(rw http.ResponseWriter, rq *http.Request){
	s.l.Println("Handling get key call")
	
	vars := mux.Vars(rq)
	key := vars["key"]

	value, ok := s.kvStore.Get(key)

	if !ok{
		rw.WriteHeader(http.StatusNotFound)	
	}else{
		rw.WriteHeader(http.StatusOK)
		rw.Write([] byte(value))
	}
}

//Delete function to delete key-value pair from the key store
func (s *Store) Delete(rw http.ResponseWriter, rq *http.Request){
	s.l.Println("Handling delete key call")
	
	vars := mux.Vars(rq)
	key := vars["key"]

	err := s.kvStore.Delete(key)

	if err == kvstore.ErrorKeyEmpty {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([] byte(err.Error()))	
	}else if err == kvstore.ErrorKeyNotFound {
		rw.WriteHeader(http.StatusNotFound)
		rw.Write([] byte(err.Error()))	
	}else{
		rw.WriteHeader(http.StatusOK)
		rw.Write([] byte("It's been deleted."))
	}
}
