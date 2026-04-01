package client

import (
	"bytes"
	"log"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
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

func NewClient(conn *websocket.Conn, br chan<- *Message, lv chan<- *Client) *Client {
	ID := uuid.New().String()
	return &Client{
		ID:         ID,
		MessagesCh: make(chan *Message, 256),
		broadcast:  br,
		leave:      lv,
		conn:       conn,
	}
}

/*
Клиент A пишет сообщение
    → ReadPump A → broadcast канал сервера
        → Server.run() рассылает по всем clients
            → пишет в WriteCh каждого клиента
                → WritePump каждого клиента читает из WriteCh
                    → отправляет по WebSocket
*/

func (c *Client) ReadPump() {
	defer func() {
		c.leave <- c
	}()

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

func (c *Client) WritePump() {
	defer func() {
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.MessagesCh:
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
		}
	}
}
