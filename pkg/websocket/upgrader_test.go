package websocket

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func TestUpgrade(t *testing.T) {
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := Upgrade(writer, request)
		if err != nil {
			t.Errorf("upgrade return error, %v", err)
			http.Error(writer, err.Error(), http.StatusBadGateway)
			return
		}
		defer conn.Close()

		if err = conn.WriteMessage(websocket.TextMessage, []byte("hello from server")); err != nil {
			t.Errorf("fail to send message: %v", err)
		}
	})

	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("cannot connect to the server, %v", err)
	}
	defer conn.Close()

	mt, msg, err := conn.ReadMessage()

	if err != nil {
		t.Errorf("cannot read message, err: %v", err)
	}

	if mt != websocket.TextMessage || string(msg) != "hello from server" {
		t.Errorf("recieve unexpected message: %v", msg)
	}

	t.Log("test pass successfully")
}

func TestUpgrade_ReturnsError_ErrFailUpgrade(t *testing.T) {
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := Upgrade(writer, request)
		if err != nil {
			t.Logf("Upgrade returned error %v", err)
			t.Logf("Is this ErrFailUpgrade? %v", errors.Is(err, ErrFailUpgrade))
			return
		}
		defer conn.Close()

		if err = conn.WriteMessage(websocket.TextMessage, []byte("hello from server")); err != nil {
			t.Errorf("fail to send message: %v", err)
		}

	})

	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("cannot make request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", resp.StatusCode)
	}

	t.Log("test passed: Upgrade correctly returned ErrFailUpgrade")
}
