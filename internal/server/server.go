package server

import (
	c "gochatapp/internal/client"
	"log"
	"sync"
)

type Server struct {
	mu        *sync.RWMutex
	broadcast chan *c.Message
	joinCh    chan *c.Client
	leaveCh   chan *c.Client
	clients   map[*c.Client]bool
}

func NewServer() *Server {
	return &Server{
		mu:        new(sync.RWMutex),
		clients:   make(map[*c.Client]bool),
		joinCh:    make(chan *c.Client, 128),
		leaveCh:   make(chan *c.Client, 128),
		broadcast: make(chan *c.Message, 128),
	}
}

func (s *Server) AddClient(client *c.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients[client] = true
	log.Printf("client %s joined to the server\n", client.ID)
}
