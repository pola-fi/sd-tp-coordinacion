package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

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
	outputQueue       middleware.Middleware
	inputExchange     middleware.Middleware
	fruitItemByClient map[uint32]map[string]fruititem.FruitItem
	eofCountByClient  map[uint32]int
	sumAmount         int
	topSize           int
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
		outputQueue:       outputQueue,
		inputExchange:     inputExchange,
		fruitItemByClient: map[uint32]map[string]fruititem.FruitItem{},
		eofCountByClient:  map[uint32]int{},
		sumAmount:         config.SumAmount,
		topSize:           config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() error {
	defer aggregation.close()
	go aggregation.handleSignals()

	return aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	<-signals
	slog.Info("SIGTERM signal received")
	if err := aggregation.inputExchange.StopConsuming(); err != nil {
		slog.Error("While stopping input consumption", "err", err)
	}
}

func (aggregation *Aggregation) close() {
	if err := aggregation.inputExchange.Close(); err != nil {
		slog.Error("While closing input exchange", "err", err)
	}
	if err := aggregation.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
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
		aggregation.handleDataMessage(message.ClientID, message.Items)
	default:
		slog.Error("Unexpected inner message type", "type", message.Type)
	}
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID uint32) error {
	slog.Info("Received End Of Records message")
	aggregation.eofCountByClient[clientID]++
	if aggregation.eofCountByClient[clientID] < aggregation.sumAmount {
		return nil
	}

	fruitItemMap := aggregation.fruitItemByClient[clientID]
	defer delete(aggregation.fruitItemByClient, clientID)
	defer delete(aggregation.eofCountByClient, clientID)

	fruitTopRecords := aggregation.buildFruitTop(fruitItemMap)
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

func (aggregation *Aggregation) handleDataMessage(clientID uint32, fruitRecords []fruititem.FruitItem) {
	fruitItemMap := aggregation.fruitItemByClient[clientID]
	if fruitItemMap == nil {
		fruitItemMap = map[string]fruititem.FruitItem{}
		aggregation.fruitItemByClient[clientID] = fruitItemMap
	}

	for _, fruitRecord := range fruitRecords {
		if _, ok := fruitItemMap[fruitRecord.Fruit]; ok {
			fruitItemMap[fruitRecord.Fruit] = fruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			fruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(fruitItemMap map[string]fruititem.FruitItem) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(fruitItemMap))
	for _, item := range fruitItemMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
