package websocket

import "errors"

var (
	ErrFailUpgrade = errors.New("upgrader: error upgrade http to websocket")
)
