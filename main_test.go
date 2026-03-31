package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestConnection(t *testing.T) {
	// create server
	server := NewServer()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", server.handleWs)
	testServer := httptest.NewServer(mux)

	defer testServer.Close()

	// http to ws
	wsURL := "ws" + testServer.URL[4:] + "/ws"

	// parameters
	connCount := 3
	wg := &sync.WaitGroup{}
	errCh := make(chan error, connCount)

	for i := 0; i < connCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)

			if err != nil {
				errCh <- fmt.Errorf("connection %d failed: %w", id, err)
				return
			}
			defer conn.Close()

			time.Sleep(100 * time.Millisecond)
		}(i)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		t.Error(err)
	}

	server.mu.RLock()
	actualConnectionCount := len(server.clients)
	server.mu.RUnlock()

	if actualConnectionCount != connCount {
		t.Errorf("Expected %d clients, got %d", connCount, actualConnectionCount)
	}

}
