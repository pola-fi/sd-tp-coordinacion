package sum

import (
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue        middleware.Middleware
	outputExchange    middleware.Middleware
	fruitItemByClient map[uint32]map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:        inputQueue,
		outputExchange:    outputExchange,
		fruitItemByClient: map[uint32]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	message, err := inner.Deserialize(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	switch message.Type {
	case inner.EOFMessage:
		if err := sum.handleEndOfRecordMessage(message.ClientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
	case inner.DataMessage:
		if err := sum.handleDataMessage(message.ClientID, message.Items); err != nil {
			slog.Error("While handling data message", "err", err)
		}
	default:
		slog.Error("Unexpected inner message type", "type", message.Type)
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientID uint32) error {
	slog.Info("Received End Of Records message")
	fruitItemMap := sum.fruitItemByClient[clientID]
	defer delete(sum.fruitItemByClient, clientID)

	for key := range fruitItemMap {
		fruitRecord := []fruititem.FruitItem{fruitItemMap[key]}
		wire, err := inner.Serialize(inner.Message{Type: inner.DataMessage, ClientID: clientID, Items: fruitRecord})
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*wire); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	wire, err := inner.Serialize(inner.Message{Type: inner.EOFMessage, ClientID: clientID})
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*wire); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(clientID uint32, fruitRecords []fruititem.FruitItem) error {
	fruitItemMap := sum.fruitItemByClient[clientID]
	if fruitItemMap == nil {
		fruitItemMap = map[string]fruititem.FruitItem{}
		sum.fruitItemByClient[clientID] = fruitItemMap
	}

	for _, fruitRecord := range fruitRecords {
		_, ok := fruitItemMap[fruitRecord.Fruit]
		if ok {
			fruitItemMap[fruitRecord.Fruit] = fruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			fruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}
