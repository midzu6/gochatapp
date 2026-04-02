package server

import (
	client "gochatapp/internal/client"
	"log"
	"sync"
)

type Server struct {
	mu        *sync.RWMutex
	broadcast chan *client.Message
	joinCh    chan *client.Client
	leaveCh   chan *client.Client
	clients   map[*client.Client]bool
}

func NewServer() *Server {
	return &Server{
		mu:        new(sync.RWMutex),
		clients:   make(map[*client.Client]bool),
		joinCh:    make(chan *client.Client, 128),
		leaveCh:   make(chan *client.Client, 128),
		broadcast: make(chan *client.Message, 128),
	}
}

func (s *Server) run() {
	for {
		select {
		case c := <-s.joinCh:
			s.addClient(c)
		case c := <-s.leaveCh:
			s.removeClient(c)
		case message := <-s.broadcast:
			var toRemove []*client.Client

			s.mu.RLock()
			for c := range s.clients {
				select {
				case c.MessagesCh <- message:
				default:
					toRemove = append(toRemove, c)
				}
			}
			s.mu.RUnlock()

			for _, c := range toRemove {
				s.removeClient(c)
				log.Printf("channel is full, remove client with id %s\n", c.ID)
			}
		}
	}
}

func (s *Server) addClient(client *client.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients[client] = true
	log.Printf("client %s joined to the server\n", client.ID)
}

func (s *Server) removeClient(client *client.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.clients[client]; ok {
		delete(s.clients, client)
		close(client.MessagesCh)
		log.Printf("client %s leaved the server\n", client.ID)
	}
}
