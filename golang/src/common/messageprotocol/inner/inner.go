package inner

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageType int

const (
	DataMessage MessageType = 0
	EOFMessage  MessageType = 1
)

type Message struct {
	Type     MessageType
	ClientID uint32
	Items    []fruititem.FruitItem
}

func Serialize(message Message) (*middleware.Message, error) {
	itemsPayload := make([]interface{}, 0, len(message.Items))
	for _, fruitRecord := range message.Items {
		itemsPayload = append(itemsPayload, []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		})
	}

	envelope := []interface{}{int(message.Type), message.ClientID, itemsPayload}
	body, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func Deserialize(middlewareMessage *middleware.Message) (Message, error) {
	var envelope []interface{}
	if err := json.Unmarshal([]byte(middlewareMessage.Body), &envelope); err != nil {
		return Message{}, err
	}
	if len(envelope) < 3 {
		return Message{}, errors.New("inner: envelope must have type, client id and items")
	}

	msgType, err := parseMessageType(envelope[0])
	if err != nil {
		return Message{}, err
	}

	clientID, err := parseClientID(envelope[1])
	if err != nil {
		return Message{}, err
	}

	items, err := parseItems(envelope[2])
	if err != nil {
		return Message{}, err
	}

	return Message{Type: msgType, ClientID: clientID, Items: items}, nil
}

func parseClientID(raw interface{}) (uint32, error) {
	clientID, ok := raw.(float64)
	if !ok {
		return 0, errors.New("inner: client id is not a number")
	}
	return uint32(clientID), nil
}

func parseMessageType(raw interface{}) (MessageType, error) {
	switch v := raw.(type) {
	case float64:
		t := MessageType(int(v))
		if t != DataMessage && t != EOFMessage {
			return 0, fmt.Errorf("inner: unknown message type %d", int(v))
		}
		return t, nil
	default:
		return 0, errors.New("inner: message type is not a number")
	}
}

func parseItems(raw interface{}) ([]fruititem.FruitItem, error) {
	data, ok := raw.([]interface{})
	if !ok {
		return nil, errors.New("Datum is not an array")
	}

	fruitRecords := []fruititem.FruitItem{}
	for _, datum := range data {
		fruitPair, ok := datum.([]interface{})
		if !ok {
			return nil, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return nil, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return nil, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}
	return fruitRecords, nil
}
