package client

import (
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type Client struct {
	ID     string
	readCh chan *Message
	conn   *websocket.Conn
}

func NewClient(conn *websocket.Conn) *Client {
	ID := uuid.New().String()
	return &Client{
		ID:     ID,
		conn:   conn,
		readCh: make(chan *Message, 256),
	}
}
