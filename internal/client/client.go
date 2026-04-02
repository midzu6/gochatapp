package client

import (
	"bytes"
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	writeWait  = 3 * time.Second
	pongWait   = 3 * time.Second
	pingPeriod = (pongWait * 9) / 10
)

var (
	newline = []byte{'\n'}
	space   = []byte{' '}
)

type Client struct {
	ID         string
	MessagesCh chan *Message
	broadcast  chan<- *Message
	leave      chan<- *Client
	conn       *websocket.Conn
}

func NewClient(conn *websocket.Conn, br chan<- *Message, lv chan<- *Client, bufSize int) *Client {
	ID := uuid.New().String()
	return &Client{
		ID:         ID,
		MessagesCh: make(chan *Message, bufSize),
		broadcast:  br,
		leave:      lv,
		conn:       conn,
	}
}

func (c *Client) ReadPump() {
	defer func() {
		c.leave <- c
	}()

	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(appData string) error {
		log.Printf("pong recieved\n")
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}
		message = bytes.TrimSpace(bytes.Replace(message, newline, space, -1))

		c.broadcast <- &Message{
			Text: string(message),
		}
	}
}

func (c *Client) WritePump(ctx context.Context) {

	ticker := time.NewTicker(pingPeriod)
	defer func() {
		c.conn.Close()
		ticker.Stop()
	}()

	for {
		select {
		case <-ctx.Done():
			c.conn.WriteMessage(websocket.CloseMessage, []byte{})
			return
		case message, ok := <-c.MessagesCh:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			_, err = w.Write([]byte(message.Text))
			if err != nil {
				return
			}

			n := len(c.MessagesCh)
			for range n {
				_, err = w.Write(newline)
				if err != nil {
					return
				}
				m := <-c.MessagesCh
				_, err = w.Write([]byte(m.Text))
				if err != nil {
					return
				}
			}
			if err = w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
