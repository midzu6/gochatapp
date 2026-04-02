package server

import (
	client "gochatapp/internal/client"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreateServer(t *testing.T) {
	server := NewServer()
	assert.NotNil(t, server.mu)
	assert.NotNil(t, server.clients)
	assert.NotNil(t, server.broadcast)
	assert.NotNil(t, server.leaveCh)
	assert.NotNil(t, server.joinCh)
}

func TestAddClient(t *testing.T) {
	server := NewServer()
	c := client.NewClient(
		nil,
		make(chan<- *client.Message, 128),
		make(chan<- *client.Client, 128),
		256,
	)
	server.addClient(c)
	server.mu.RLock()
	_, ok := server.clients[c]
	server.mu.RUnlock()
	assert.True(t, ok)
}

func TestRemoveClient(t *testing.T) {
	s := NewServer()
	c := client.NewClient(
		nil,
		make(chan<- *client.Message, 128),
		make(chan<- *client.Client, 128),
		256,
	)
	s.addClient(c)
	s.removeClient(c)

	s.mu.RLock()
	_, ok := s.clients[c]
	s.mu.RUnlock()

	_, cok := <-c.MessagesCh

	assert.False(t, ok)
	assert.True(t, !cok)
	assert.NotPanics(t, func() {
		s.removeClient(c)
		s.removeClient(c)
	})
}

func TestServerRun_Join(t *testing.T) {
	s := NewServer()
	c := client.NewClient(
		nil,
		make(chan<- *client.Message, 128),
		make(chan<- *client.Client, 128),
		256,
	)
	go s.run()

	s.joinCh <- c

	time.Sleep(10 * time.Millisecond)

	s.mu.RLock()
	_, ok := s.clients[c]
	s.mu.RUnlock()

	assert.True(t, ok)
}

func TestServerRun_Leave(t *testing.T) {
	s := NewServer()
	c := client.NewClient(
		nil,
		make(chan<- *client.Message, 128),
		make(chan<- *client.Client, 128),
		256,
	)
	go s.run()

	s.joinCh <- c
	time.Sleep(10 * time.Millisecond)
	s.leaveCh <- c
	time.Sleep(10 * time.Millisecond)

	s.mu.RLock()
	_, ok := s.clients[c]
	s.mu.RUnlock()

	assert.False(t, ok)
}

func TestServerRun_Broadcast(t *testing.T) {
	s := NewServer()
	c := client.NewClient(
		nil,
		make(chan<- *client.Message, 128),
		make(chan<- *client.Client, 128),
		256,
	)
	go s.run()

	s.joinCh <- c
	time.Sleep(10 * time.Millisecond)
	s.broadcast <- &client.Message{Text: "hello"}

	_, ok := <-c.MessagesCh

	assert.True(t, ok)

}

func TestRun_Broadcast_ClientBufferIsFull(t *testing.T) {
	s := NewServer()
	c := client.NewClient(
		nil,
		make(chan<- *client.Message, 256),
		make(chan<- *client.Client, 128),
		1,
	)
	go s.run()
	s.joinCh <- c
	time.Sleep(10 * time.Millisecond)
	s.broadcast <- &client.Message{Text: "hello"}
	s.broadcast <- &client.Message{Text: "hello"}
	time.Sleep(10 * time.Millisecond)

	s.mu.RLock()
	_, ok := s.clients[c]
	s.mu.RUnlock()

	assert.False(t, ok)
}
