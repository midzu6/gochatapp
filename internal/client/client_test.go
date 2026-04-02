package client

import (
	"context"
	ws "gochatapp/pkg/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

func newTestServer(t *testing.T) (*httptest.Server, chan *Message, chan *Client, chan *Client) {
	broadcast := make(chan *Message, 1)
	leave := make(chan *Client, 1)
	clientCh := make(chan *Client, 1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := ws.Upgrade(w, r)
		if err != nil {
			t.Errorf("upgrade error: %v", err)
			return
		}
		c := NewClient(conn, broadcast, leave, 256)
		clientCh <- c
	}))

	return ts, broadcast, leave, clientCh
}

func TestReadPump_Broadcast(t *testing.T) {
	server, broadcast, _, clientCh := newTestServer(t)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("cannot connect to the server")
	}
	client := <-clientCh

	go client.ReadPump()

	if err = wsConn.WriteMessage(websocket.TextMessage, []byte("hello")); err != nil {
		t.Errorf("cannot write message")
	}

	message := <-broadcast

	assert.Equal(t, message.Text, "hello")
}

func TestReadPump_Leave(t *testing.T) {
	server, _, leave, clientCh := newTestServer(t)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	wsConn, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)

	client := <-clientCh
	go client.ReadPump()

	wsConn.Close()

	c := <-leave

	assert.NotNil(t, c)
}

func TestClient_WritePump_MessagesCh(t *testing.T) {
	server, _, _, clientCh := newTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		server.Close()
	}()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	wsConn, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)

	client := <-clientCh
	go client.WritePump(ctx)

	client.MessagesCh <- &Message{Text: "hello"}

	_, message, _ := wsConn.ReadMessage()

	assert.Equal(t, string(message), "hello")
}

func TestClient_WritePump_Context_Done(t *testing.T) {
	server, _, _, clientCh := newTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		server.Close()
	}()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	wsConn, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)

	client := <-clientCh
	go client.WritePump(ctx)

	cancel()

	_, _, err := wsConn.ReadMessage()

	assert.True(t, websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure))

}

func TestClient_WritePump_MessageChannel_Is_Close(t *testing.T) {
	server, _, _, clientCh := newTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		server.Close()
		cancel()
	}()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	wsConn, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)

	client := <-clientCh
	go client.WritePump(ctx)

	close(client.MessagesCh)

	_, _, err := wsConn.ReadMessage()

	assert.True(t, websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure))

}
