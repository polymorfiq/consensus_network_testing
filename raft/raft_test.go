package raft

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestCluster(t *testing.T) {
	servers := make(map[string]*Server)
	servers["A"] = NewServer("A")
	servers["B"] = NewServer("B")
	servers["C"] = NewServer("C")
	servers["D"] = NewServer("D")
	servers["E"] = NewServer("E")

	servers["A"].Connect(servers["B"])
	servers["A"].Connect(servers["C"])
	servers["A"].Connect(servers["D"])
	servers["A"].Connect(servers["E"])

	servers["B"].Connect(servers["C"])
	servers["B"].Connect(servers["D"])
	servers["B"].Connect(servers["E"])

	servers["C"].Connect(servers["D"])
	servers["C"].Connect(servers["E"])

	servers["D"].Connect(servers["E"])

	time.Sleep(3 * time.Second)

	appendToLog(servers, "A", &SetLogEntry{Key: "A", Value: []byte("A")})
	appendToLog(servers, "B", &SetLogEntry{Key: "B", Value: []byte("B")})
	appendToLog(servers, "C", &SetLogEntry{Key: "C", Value: []byte("C")})

	time.Sleep(1 * time.Second)
	if val, found, err := retrieveFromLog(servers, "A", "B"); found != true || string(val) != "B" {
		t.Errorf("Expected B to have value B, but got %s (%v, %v)", string(val), found, err)
	}

	time.Sleep(1 * time.Second)
	if val, found, err := retrieveFromLog(servers, "D", "C"); found != true || string(val) != "C" {
		t.Errorf("Expected C to have value C, but got %s (%v, %v)", string(val), found, err)
	}

	time.Sleep(1 * time.Second)
	if val, found, err := retrieveFromLog(servers, "E", "A"); found != true || string(val) != "A" {
		t.Errorf("Expected A to have value A, but got %s (%v, %v)", string(val), found, err)
	}

	// Kill the leader...
	var leader *Server
	for _, server := range servers {
		if server.State == Leader {
			leader = server
		}
	}

	t.Logf("Cutting leader network...")
	for conn := range leader.router.Connections() {
		conn.SetDropOutgoingNth(1)
	}

	time.Sleep(1 * time.Second)
	if leader.Id != "A" {
		appendToLog(servers, "A", &SetLogEntry{Key: "AfterCut", Value: []byte("StoredLater")})
	} else {
		appendToLog(servers, "C", &SetLogEntry{Key: "AfterCut", Value: []byte("StoredLater")})
	}

	if leader.Id != "B" {
		time.Sleep(1 * time.Second)
		if val, found, err := retrieveFromLog(servers, "B", "AfterCut"); found != true || string(val) != "StoredLater" {
			t.Errorf("Expected AfterCut to have value StoredLater, but got %s (%v, %v)", string(val), found, err)
		}
	} else {
		time.Sleep(1 * time.Second)
		if val, found, err := retrieveFromLog(servers, "D", "AfterCut"); found != true || string(val) != "StoredLater" {
			t.Errorf("Expected AfterCut to have value StoredLater, but got %s (%v, %v)", string(val), found, err)
		}
	}

}

func appendToLog(servers map[string]*Server, id string, entry LogEntry) error {
	ok, redirectToId, err := servers[id].Append(entry)

	var redirectId string
	if redirectToId != nil {
		redirectId = string(*redirectToId)
	}
	fmt.Printf("Appending to log... %v %s %v\n", ok, redirectId, err)
	if err != nil {
		return err
	}

	if redirectToId != nil {
		return appendToLog(servers, string(*redirectToId), entry)
	}

	if ok {
		return nil
	}

	return errors.New("could not append to raft log")
}

func retrieveFromLog(servers map[string]*Server, id string, key string) ([]byte, bool, error) {
	val, ok, redirectToId, err := servers[id].Retrieve(key)

	if err != nil {
		return nil, false, err
	}

	if redirectToId != nil {
		return retrieveFromLog(servers, string(*redirectToId), key)
	}

	if ok {
		return val, true, nil
	}

	return nil, false, nil
}
