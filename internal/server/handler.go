package server

import (
	"errors"
	c "gochatapp/internal/client"
	ws "gochatapp/pkg/websocket"
	"log"
	"net/http"
)

func (s *Server) HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := ws.Upgrade(w, r)
	if err != nil {
		if errors.Is(err, ws.ErrFailUpgrade) {
			log.Printf("fail upgrade http to websocket, err %v\n", err)
			return
		}
		log.Printf("unexpected error, err %v\n", err)
		return
	}

	client := c.NewClient(conn, s.broadcast, s.leaveCh, 256)
	s.joinCh <- client
	// add client's methods

	go client.WritePump()
	go client.ReadPump()
}
