package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

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

// PublishEvent публикует событие заказа в RabbitMQ.
func (p *rabbitPublisher) PublishEvent(ctx context.Context, event domain.Event) error {
	message := eventMessage{
		EventID:   event.EventID,
		UserID:    event.UserID,
		OrderID:   event.OrderID,
		EventType: string(event.EventType),
		Payload:   event.Payload,
	}

	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal event message: %w", err)
	}

	routingKey := fmt.Sprintf("order.%s", event.EventType)

	publishErr := p.publish(ctx, routingKey, body)
	if publishErr == nil {
		return nil
	}

	if err := p.reconnect(ctx); err != nil {
		return fmt.Errorf("publish event %s failed: %v; reconnect failed: %w", event.EventID, publishErr, err)
	}

	if err := p.publish(ctx, routingKey, body); err != nil {
		return fmt.Errorf("publish event %s after reconnect: %w", event.EventID, err)
	}

	return nil
}

func (p *rabbitPublisher) publish(ctx context.Context, routingKey string, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.channel == nil {
		return fmt.Errorf("rabbitmq channel is nil")
	}

	if err := p.channel.PublishWithContext(
		ctx,
		ordersExchange,
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
