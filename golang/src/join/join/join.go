package join

import (
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue          middleware.Middleware
	outputQueue         middleware.Middleware
	partialTopsByClient map[uint32]*partialTops
	aggregationAmount   int
	topSize             int
}

type partialTops struct {
	received int
	items    []fruititem.FruitItem
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:          inputQueue,
		outputQueue:         outputQueue,
		partialTopsByClient: map[uint32]*partialTops{},
		aggregationAmount:   config.AggregationAmount,
		topSize:             config.TopSize,
	}, nil
}

func (join *Join) Run() error {
	defer join.close()
	go join.handleSignals()

	return join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	<-signals
	slog.Info("SIGTERM signal received")
	if err := join.inputQueue.StopConsuming(); err != nil {
		slog.Error("While stopping input consumption", "err", err)
	}
}

func (join *Join) close() {
	if err := join.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := join.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	message, err := inner.Deserialize(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}
	if message.Type != inner.DataMessage {
		slog.Error("Unexpected inner message type", "type", message.Type)
		return
	}

	partialTop := join.partialTopsByClient[message.ClientID]
	if partialTop == nil {
		partialTop = &partialTops{}
		join.partialTopsByClient[message.ClientID] = partialTop
	}
	partialTop.received++
	partialTop.items = append(partialTop.items, message.Items...)

	if partialTop.received < join.aggregationAmount {
		return
	}
	defer delete(join.partialTopsByClient, message.ClientID)

	sort.SliceStable(partialTop.items, func(i, j int) bool {
		return partialTop.items[j].Less(partialTop.items[i])
	})
	finalTopSize := min(join.topSize, len(partialTop.items))
	finalTop := partialTop.items[:finalTopSize]

	wire, err := inner.Serialize(inner.Message{
		Type:     inner.DataMessage,
		ClientID: message.ClientID,
		Items:    finalTop,
	})
	if err != nil {
		slog.Error("While serializing top", "err", err)
		return
	}
	if err := join.outputQueue.Send(*wire); err != nil {
		slog.Error("While sending top", "err", err)
	}
}
