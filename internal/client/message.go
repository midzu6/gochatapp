package client

type Message struct {
	ClientId string
	Data     string
}

func NewMessage(id string, data string) *Message {
	return &Message{ClientId: id, Data: data}
}
