package messagehandler

import (
	"sync/atomic"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var nextClientID atomic.Uint32

type MessageHandler struct {
	clientID uint32
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{clientID: nextClientID.Add(1)}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	return inner.Serialize(inner.Message{
		Type:     inner.DataMessage,
		ClientID: messageHandler.clientID,
		Items:    []fruititem.FruitItem{fruitRecord},
	})
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.Serialize(inner.Message{
		Type:     inner.EOFMessage,
		ClientID: messageHandler.clientID,
	})
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	decoded, err := inner.Deserialize(message)
	if err != nil {
		return nil, err
	}
	if decoded.ClientID != messageHandler.clientID {
		return nil, nil
	}
	return decoded.Items, nil
}
