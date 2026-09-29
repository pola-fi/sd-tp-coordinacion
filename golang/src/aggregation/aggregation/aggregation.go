package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	fruitItemMap  map[string]fruititem.FruitItem
	topSize       int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		fruitItemMap:  map[string]fruititem.FruitItem{},
		topSize:       config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	message, err := inner.Deserialize(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	switch message.Type {
	case inner.EOFMessage:
		if err := aggregation.handleEndOfRecordsMessage(message.ClientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
	case inner.DataMessage:
		aggregation.handleDataMessage(message.Items)
	default:
		slog.Error("Unexpected inner message type", "type", message.Type)
	}
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID uint32) error {
	slog.Info("Received End Of Records message")

	fruitTopRecords := aggregation.buildFruitTop()
	wire, err := inner.Serialize(inner.Message{Type: inner.DataMessage, ClientID: clientID, Items: fruitTopRecords})
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*wire); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(fruitRecords []fruititem.FruitItem) {
	for _, fruitRecord := range fruitRecords {
		if _, ok := aggregation.fruitItemMap[fruitRecord.Fruit]; ok {
			aggregation.fruitItemMap[fruitRecord.Fruit] = aggregation.fruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			aggregation.fruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop() []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(aggregation.fruitItemMap))
	for _, item := range aggregation.fruitItemMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
