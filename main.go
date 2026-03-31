package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var (
	WSPort = ":8081"
)

type Client struct {
	ID   string
	conn *websocket.Conn
}

type Server struct {
	mu      *sync.RWMutex
	clients map[*Client]bool
	joinCh  chan *Client
	leaveCh chan *Client
}

func NewServer() *Server {
	return &Server{
		mu:      new(sync.RWMutex),
		clients: make(map[*Client]bool),
		joinCh:  make(chan *Client, 128),
		leaveCh: make(chan *Client, 128),
	}
}

func NewClient(conn *websocket.Conn) *Client {
	ID := uuid.New().String()
	return &Client{
		ID:   ID,
		conn: conn,
	}
}

func (s *Server) handleWs(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		WriteBufferSize: 512,
		ReadBufferSize:  512,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("error upgrade http to websocket, err %v\n", err)
		return
	}

	client := NewClient(conn)
	s.joinCh <- client
}

func (s *Server) AcceptLoop() {
	for {
		select {
		case c := <-s.joinCh:
			s.addClient(c)
		case c := <-s.leaveCh:
			s.removeClient(c)
		}
	}
}

func (s *Server) addClient(c *Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients[c] = true
	log.Printf("client %s joined to the server\n", c.ID)
}

func (s *Server) removeClient(c *Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.clients[c]; ok {
		delete(s.clients, c)
		log.Printf("client %s leaved the server\n", c.ID)
	}
}

func createWSServer() {
	server := NewServer()

	http.HandleFunc("/", server.handleWs)

	log.Printf("Starting the server on port %s", WSPort)

	go server.AcceptLoop()

	log.Fatal(http.ListenAndServe(WSPort, nil))
}

func main() {
	createWSServer()
}
