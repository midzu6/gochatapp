package main

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var (
	host = "ws://localhost"
)

type TestConfig struct {
	ConnectionCount int
	wg              *sync.WaitGroup
}

func DialServer(wg *sync.WaitGroup) {
	dialer := websocket.DefaultDialer

	conn, _, err := dialer.Dial(fmt.Sprintf("%s%s", host, WSPort), nil)
	if err != nil {
		log.Fatal("error: ", err)
	}
	defer func() {
		wg.Done()
		conn.Close()
	}()

	log.Println("connected to the server ", conn.LocalAddr().String())

	time.Sleep(time.Second)
}

func TestConnection(t *testing.T) {
	go createWSServer()
	time.Sleep(time.Second)

	tc := TestConfig{ConnectionCount: 3, wg: &sync.WaitGroup{}}

	for range tc.ConnectionCount {
		tc.wg.Add(1)
		go DialServer(tc.wg)
	}
	tc.wg.Wait()
}
