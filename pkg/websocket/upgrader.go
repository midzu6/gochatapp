package websocket

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

var (
	writeBufferSize = 512
	readBufferSize  = 512
	checkOrigin     = func(r *http.Request) bool { return true }
)

func Upgrade(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	upgrader := websocket.Upgrader{
		WriteBufferSize: writeBufferSize,
		ReadBufferSize:  readBufferSize,
		CheckOrigin:     checkOrigin,
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("error upgrade http to websocket, err %v\n", err)
		return nil, ErrFailUpgrade
	}

	return conn, nil
}
