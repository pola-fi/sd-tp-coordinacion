package middleware

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

// constants

const (
	brokerUser       = "guest"
	brokerPassword   = "guest"
	queuePrefetch    = 1
	exchangePrefetch = 1
)

var consumerSeq atomic.Uint64

// Initialize middleware

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	broker, err := connectBroker(connectionSettings)
	if err != nil {
		return nil, err
	}
	return &queueMiddleware{name: queueName, broker: broker}, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	broker, err := connectBroker(connectionSettings)
	if err != nil {
		return nil, err
	}
	copied := append([]string(nil), keys...)
	return &exchangeMiddleware{name: exchange, keys: copied, broker: broker}, nil
}

func connectBroker(settings ConnSettings) (*rabbitBroker, error) {
	broker := &rabbitBroker{settings: settings}
	if err := broker.connect(); err != nil {
		return nil, err
	}
	return broker, nil
}

// queueMiddleware methods

type queueMiddleware struct {
	name   string
	broker *rabbitBroker
}

func (q *queueMiddleware) StartConsuming(callback func(msg Message, ack func(), nack func())) error {
	if err := q.broker.declareQueue(q.name); err != nil {
		return err
	}
	return q.broker.consume(q.name, queuePrefetch, callback)
}

func (q *queueMiddleware) StopConsuming() error {
	q.broker.stopConsuming()
	return nil
}

func (q *queueMiddleware) Send(msg Message) error {
	if err := q.broker.declareQueue(q.name); err != nil {
		return err
	}
	return q.broker.publish("", q.name, []byte(msg.Body))
}

func (q *queueMiddleware) Close() error {
	return q.broker.close()
}

// exchangeMiddleware methods

type exchangeMiddleware struct {
	name   string
	keys   []string
	broker *rabbitBroker
}

func (e *exchangeMiddleware) StartConsuming(callback func(msg Message, ack func(), nack func())) error {
	queueName, err := e.broker.declareExchangeConsumer(e.name, e.keys)
	if err != nil {
		return err
	}
	return e.broker.consume(queueName, exchangePrefetch, callback)
}

func (e *exchangeMiddleware) StopConsuming() error {
	e.broker.stopConsuming()
	return nil
}

func (e *exchangeMiddleware) Send(msg Message) error {
	if err := e.broker.declareDirectExchange(e.name); err != nil {
		return err
	}
	body := []byte(msg.Body)
	for _, key := range e.keys {
		if err := e.broker.publish(e.name, key, body); err != nil {
			return err
		}
	}
	return nil
}

func (e *exchangeMiddleware) Close() error {
	return e.broker.close()
}

// rabbitBroker methods

type rabbitBroker struct {
	settings ConnSettings

	mu             sync.Mutex
	conn           *amqp.Connection
	channel        *amqp.Channel
	consumerTag    string
	consuming      bool
	stoppedLocally bool
	consumeWG      sync.WaitGroup
}

func (b *rabbitBroker) connect() error {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", brokerUser, brokerPassword, b.settings.Hostname, b.settings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return ErrMessageMiddlewareDisconnected
	}
	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return ErrMessageMiddlewareDisconnected
	}
	b.conn = conn
	b.channel = channel
	return nil
}

func (b *rabbitBroker) declareQueue(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureChannel(); err != nil {
		return err
	}
	_, err := b.channel.QueueDeclare(name, false, false, false, false, nil)
	return classifyBrokerError(b.conn, err)
}

func (b *rabbitBroker) declareDirectExchange(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.declareDirectExchangeLocked(name)
}

func (b *rabbitBroker) declareDirectExchangeLocked(name string) error {
	if err := b.ensureChannel(); err != nil {
		return err
	}
	err := b.channel.ExchangeDeclare(name, amqp.ExchangeDirect, false, false, false, false, nil)
	return classifyBrokerError(b.conn, err)
}

func (b *rabbitBroker) declareExchangeConsumer(exchange string, keys []string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.declareDirectExchangeLocked(exchange); err != nil {
		return "", err
	}
	queue, err := b.channel.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		return "", classifyBrokerError(b.conn, err)
	}
	for _, key := range keys {
		if err := b.channel.QueueBind(queue.Name, key, exchange, false, nil); err != nil {
			return "", classifyBrokerError(b.conn, err)
		}
	}
	return queue.Name, nil
}

func (b *rabbitBroker) publish(exchange string, key string, body []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureChannel(); err != nil {
		return err
	}
	err := b.channel.Publish(exchange, key, false, false, amqp.Publishing{
		Body: body,
	})
	return classifyBrokerError(b.conn, err)
}

func (b *rabbitBroker) consume(queue string, prefetch int, callback func(msg Message, ack func(), nack func())) error {
	b.consumeWG.Add(1)
	defer b.consumeWG.Done()

	deliveries, err := b.registerConsumer(queue, prefetch)
	if err != nil {
		return err
	}
	for delivery := range deliveries {
		callback(
			Message{Body: string(delivery.Body)},
			func() { _ = delivery.Ack(false) },
			func() { _ = delivery.Nack(false, true) },
		)
	}
	return b.finishConsume()
}

func (b *rabbitBroker) registerConsumer(queue string, prefetch int) (<-chan amqp.Delivery, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureChannel(); err != nil {
		return nil, err
	}
	if prefetch > 0 {
		if err := b.channel.Qos(prefetch, 0, false); err != nil {
			return nil, classifyBrokerError(b.conn, err)
		}
	}
	tag := fmt.Sprintf("mom-%d", consumerSeq.Add(1))
	deliveries, err := b.channel.Consume(queue, tag, false, false, false, false, nil)
	if err != nil {
		return nil, classifyBrokerError(b.conn, err)
	}
	b.consumerTag = tag
	b.consuming = true
	b.stoppedLocally = false
	return deliveries, nil
}

func (b *rabbitBroker) stopConsuming() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.consuming || b.channel == nil || b.channel.IsClosed() {
		return
	}
	b.stoppedLocally = true
	if err := b.channel.Cancel(b.consumerTag, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
		return
	}
	b.consuming = false
}

func (b *rabbitBroker) finishConsume() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consuming = false
	if b.stoppedLocally {
		return nil
	}
	if b.conn == nil || b.conn.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	if b.channel != nil && b.channel.IsClosed() {
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func (b *rabbitBroker) close() error {
	b.stopConsuming()
	// Let StartConsuming drain deliveries and exit before channel/connection close.
	b.consumeWG.Wait()

	b.mu.Lock()
	defer b.mu.Unlock()

	var failed bool
	if b.channel != nil && !b.channel.IsClosed() {
		if err := b.channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			failed = true
		}
	}
	if b.conn != nil && !b.conn.IsClosed() {
		if err := b.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			failed = true
		}
	}
	b.channel = nil
	b.conn = nil
	if failed {
		return ErrMessageMiddlewareClose
	}
	return nil
}

func (b *rabbitBroker) ensureChannel() error {
	if b.conn == nil || b.conn.IsClosed() || b.channel == nil || b.channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func classifyBrokerError(conn *amqp.Connection, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, amqp.ErrClosed) || (conn != nil && conn.IsClosed()) {
		return ErrMessageMiddlewareDisconnected
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return ErrMessageMiddlewareDisconnected
	}
	return ErrMessageMiddlewareMessage
}
