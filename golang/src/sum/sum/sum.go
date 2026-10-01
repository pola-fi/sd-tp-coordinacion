package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

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
	inputQueue            middleware.Middleware
	outputExchanges       []middleware.Middleware
	controlInputExchange  middleware.Middleware
	controlOutputExchange middleware.Middleware
	fruitItemByClient     map[uint32]map[string]fruititem.FruitItem
	fruitItemMutex        sync.Mutex
	aggregationAmount     int
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchanges := make([]middleware.Middleware, 0, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, i)}
		outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKey, connSettings)
		if err != nil {
			for _, createdOutputExchange := range outputExchanges {
				createdOutputExchange.Close()
			}
			inputQueue.Close()
			return nil, err
		}
		outputExchanges = append(outputExchanges, outputExchange)
	}

	controlOutputExchangeRouteKeys := make([]string, config.SumAmount)
	for i := range config.SumAmount {
		controlOutputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.SumPrefix, i)
	}

	controlOutputExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, controlOutputExchangeRouteKeys, connSettings)
	if err != nil {
		for _, outputExchange := range outputExchanges {
			outputExchange.Close()
		}
		inputQueue.Close()
		return nil, err
	}

	controlInputExchangeRouteKey := []string{fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)}
	controlInputExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, controlInputExchangeRouteKey, connSettings)
	if err != nil {
		controlOutputExchange.Close()
		for _, outputExchange := range outputExchanges {
			outputExchange.Close()
		}
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:            inputQueue,
		outputExchanges:       outputExchanges,
		controlInputExchange:  controlInputExchange,
		controlOutputExchange: controlOutputExchange,
		fruitItemByClient:     map[uint32]map[string]fruititem.FruitItem{},
		aggregationAmount:     config.AggregationAmount,
	}, nil
}

func (sum *Sum) Run() {
	go sum.controlInputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleControlMessage(msg, ack, nack)
	})

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleControlMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	message, err := inner.Deserialize(&msg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		return
	}
	if message.Type != inner.EOFMessage {
		slog.Error("Unexpected inner control message type", "type", message.Type)
		return
	}
	if err := sum.flushClient(message.ClientID); err != nil {
		slog.Error("While handling end of record control message", "err", err)
	}
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
	wire, err := inner.Serialize(inner.Message{Type: inner.EOFMessage, ClientID: clientID})
	if err != nil {
		slog.Debug("While serializing EOF control message", "err", err)
		return err
	}
	if err := sum.controlOutputExchange.Send(*wire); err != nil {
		slog.Debug("While sending EOF control message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) flushClient(clientID uint32) error {
	sum.fruitItemMutex.Lock()
	defer sum.fruitItemMutex.Unlock()

	fruitItemMap := sum.fruitItemByClient[clientID]
	defer delete(sum.fruitItemByClient, clientID)

	for key := range fruitItemMap {
		fruitRecord := []fruititem.FruitItem{fruitItemMap[key]}
		wire, err := inner.Serialize(inner.Message{Type: inner.DataMessage, ClientID: clientID, Items: fruitRecord})
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		outputExchange := sum.outputExchanges[sum.aggregationShard(fruitItemMap[key].Fruit)]
		if err := outputExchange.Send(*wire); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	wire, err := inner.Serialize(inner.Message{Type: inner.EOFMessage, ClientID: clientID})
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	for _, outputExchange := range sum.outputExchanges {
		if err := outputExchange.Send(*wire); err != nil {
			slog.Debug("While sending EOF message", "err", err)
			return err
		}
	}
	return nil
}

func (sum *Sum) aggregationShard(fruit string) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(fruit))
	return int(hasher.Sum32() % uint32(sum.aggregationAmount))
}

func (sum *Sum) handleDataMessage(clientID uint32, fruitRecords []fruititem.FruitItem) error {
	sum.fruitItemMutex.Lock()
	defer sum.fruitItemMutex.Unlock()

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
