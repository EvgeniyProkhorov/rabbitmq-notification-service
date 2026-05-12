package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"rabbitmq-notification-service/internal/domain"
)

const (
	ordersExchange     = "orders.exchange"
	notificationsQueue = "notifications.queue"
	deadLetterExchange = "notifications.dlx"
	deadLetterQueue    = "notifications.dlq"
)

type rabbitPublisher struct {
	url     string
	mu      sync.Mutex
	conn    *amqp.Connection
	channel *amqp.Channel
}

type eventMessage struct {
	EventID   string          `json:"event_id"`
	UserID    string          `json:"user_id"`
	OrderID   string          `json:"order_id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

// NewRabbitPublisher создаёт RabbitMQ publisher и объявляет необходимую топологию.
func NewRabbitPublisher(ctx context.Context, url string) (Publisher, error) {
	publisher := &rabbitPublisher{
		url: url,
	}

	if err := publisher.connect(ctx); err != nil {
		return nil, fmt.Errorf("connect rabbit publisher: %w", err)
	}

	return publisher, nil
}

func (p *rabbitPublisher) connect(ctx context.Context) error {
	conn, err := dialWithBackoff(ctx, p.url)
	if err != nil {
		return fmt.Errorf("connect rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open rabbitmq channel: %w", err)
	}

	p.conn = conn
	p.channel = ch

	if err := declareTopology(ch); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("declare rabbitmq topology: %w", err)
	}

	return nil
}

func (p *rabbitPublisher) reconnect(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.channel != nil {
		_ = p.channel.Close()
	}

	if p.conn != nil {
		_ = p.conn.Close()
	}

	if err := p.connect(ctx); err != nil {
		return fmt.Errorf("reconnect rabbit publisher: %w", err)
	}

	return nil
}

func (p *rabbitPublisher) publish(ctx context.Context, exchange string, routingKey string, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.channel == nil {
		return fmt.Errorf("rabbitmq channel is nil")
	}

	if err := p.channel.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	); err != nil {
		return fmt.Errorf("publish event message: %w", err)
	}

	return nil
}

// BuildOutboxMessage создаёт outbox-сообщение для последующей публикации события в RabbitMQ.
func (p *rabbitPublisher) BuildOutboxMessage(event domain.Event) (domain.OutboxMessage, error) {
	message := eventMessage{
		EventID:   event.EventID,
		UserID:    event.UserID,
		OrderID:   event.OrderID,
		EventType: string(event.EventType),
		Payload:   event.Payload,
	}

	body, err := json.Marshal(message)
	if err != nil {
		return domain.OutboxMessage{}, fmt.Errorf("marshal outbox message: %w", err)
	}

	return domain.OutboxMessage{
		ID:         uuid.NewString(),
		EventID:    event.EventID,
		Exchange:   ordersExchange,
		RoutingKey: fmt.Sprintf("order.%s", event.EventType),
		Payload:    body,
		Status:     domain.OutboxStatusPending,
		Attempts:   0,
	}, nil
}

// PublishOutboxMessage публикует подготовленное outbox-сообщение в RabbitMQ.
func (p *rabbitPublisher) PublishOutboxMessage(ctx context.Context, message domain.OutboxMessage) error {
	publishErr := p.publish(ctx, message.Exchange, message.RoutingKey, message.Payload)
	if publishErr == nil {
		return nil
	}

	if err := p.reconnect(ctx); err != nil {
		return fmt.Errorf("publish outbox message %s failed: %v; reconnect failed: %w", message.ID, publishErr, err)
	}

	if err := p.publish(ctx, message.Exchange, message.RoutingKey, message.Payload); err != nil {
		return fmt.Errorf("publish outbox message %s after reconnect: %w", message.ID, err)
	}

	return nil
}
