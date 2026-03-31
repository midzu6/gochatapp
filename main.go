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
	mu   *sync.RWMutex
	conn *websocket.Conn
}

type Server struct {
	mu      *sync.RWMutex
	clients map[*Client]bool
}

func NewServer() *Server {
	return &Server{
		mu:      new(sync.RWMutex),
		clients: make(map[*Client]bool),
	}
}

func NewClient(conn *websocket.Conn) *Client {
	ID := uuid.New().String()
	return &Client{
		ID:   ID,
		mu:   new(sync.RWMutex),
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
		log.Printf("error upgrade http to websocket, err %v\r", err)
		return
	}

	client := NewClient(conn)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients[client] = true
}

func createWSServer() {
	server := NewServer()

	http.HandleFunc("/", server.handleWs)

	log.Printf("Starting the server on port %s", WSPort)

	log.Fatal(http.ListenAndServe(WSPort, nil))
}

func main() {
	createWSServer()
}
