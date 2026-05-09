package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"rabbitmq-notification-service/internal/correlation"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// NotificationHandler описывает обработчик уведомлений, вызываемый RabbitMQ consumer.
type NotificationHandler interface {
	HandleNotification(ctx context.Context, eventID string) error
}

type rabbitConsumer struct {
	url     string
	mu      sync.Mutex
	conn    *amqp.Connection
	channel *amqp.Channel
	handler NotificationHandler
}

// NewRabbitConsumer создаёт RabbitMQ consumer для очереди уведомлений.
func NewRabbitConsumer(
	ctx context.Context,
	url string,
	handler NotificationHandler,
) (*rabbitConsumer, error) {
	consumer := &rabbitConsumer{
		url:     url,
		handler: handler,
	}

	if err := consumer.connect(ctx); err != nil {
		return nil, fmt.Errorf("connect rabbit consumer: %w", err)
	}

	return consumer, nil
}

func (c *rabbitConsumer) connect(ctx context.Context) error {
	conn, err := dialWithBackoff(ctx, c.url)
	if err != nil {
		return fmt.Errorf("connect rabbitmq consumer: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open rabbitmq consumer channel: %w", err)
	}

	if err := declareTopology(ch); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("declare rabbitmq topology for consumer: %w", err)
	}

	c.conn = conn
	c.channel = ch

	return nil
}

func (c *rabbitConsumer) reconnect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.channel != nil {
		_ = c.channel.Close()
	}

	if c.conn != nil {
		_ = c.conn.Close()
	}

	if err := c.connect(ctx); err != nil {
		return fmt.Errorf("reconnect rabbit consumer: %w", err)
	}

	return nil
}

// Start запускает чтение сообщений из RabbitMQ и переподключается при потере соединения.
func (c *rabbitConsumer) Start(ctx context.Context) error {
	for {
		err := c.consumeLoop(ctx)
		if err == nil {
			return nil
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("consumer stopped: %w", err)
		}

		log.Printf("rabbit consumer error: %v; reconnecting", err)

		if reconnectErr := c.reconnect(ctx); reconnectErr != nil {
			return fmt.Errorf("reconnect rabbit consumer: %w", reconnectErr)
		}
	}
}

func (c *rabbitConsumer) consumeLoop(ctx context.Context) error {
	c.mu.Lock()
	ch := c.channel
	c.mu.Unlock()

	if ch == nil {
		return fmt.Errorf("rabbitmq consumer channel is nil")
	}

	deliveries, err := ch.Consume(
		notificationsQueue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("start consuming notifications queue: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case delivery, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("rabbitmq deliveries channel closed")
			}

			c.handleDelivery(ctx, delivery)
		}
	}
}

func (c *rabbitConsumer) handleDelivery(ctx context.Context, delivery amqp.Delivery) {
	correlationID := correlation.NewID()
	ctx = correlation.WithID(ctx, correlationID)
	var message eventMessage

	if err := json.Unmarshal(delivery.Body, &message); err != nil {
		log.Printf(
			"correlation_id=%s decode rabbitmq message: %v",
			correlationID,
			err,
		)

		if err := delivery.Nack(false, false); err != nil {
			log.Printf(
				"correlation_id=%s nack invalid message: %v",
				correlationID,
				err,
			)
		}

		return
	}

	if message.EventID == "" {
		log.Printf(
			"correlation_id=%s rabbitmq message missing event_id",
			correlationID,
		)

		if err := delivery.Nack(false, false); err != nil {
			log.Printf(
				"correlation_id=%s nack message missing event_id: %v",
				correlationID,
				err,
			)
		}

		return
	}

	log.Printf(
		"correlation_id=%s start handling notification event_id=%s",
		correlationID,
		message.EventID,
	)

	if err := c.handler.HandleNotification(ctx, message.EventID); err != nil {
		log.Printf(
			"correlation_id=%s handle notification event_id=%s: %v",
			correlationID,
			message.EventID,
			err,
		)

		if err := delivery.Nack(false, false); err != nil {
			log.Printf(
				"correlation_id=%s nack message event_id=%s: %v",
				correlationID,
				message.EventID,
				err,
			)
		}

		return
	}

	if err := delivery.Ack(false); err != nil {
		log.Printf(
			"correlation_id=%s ack message event_id=%s: %v",
			correlationID,
			message.EventID,
			err,
		)
		return
	}

	log.Printf(
		"correlation_id=%s handled notification event_id=%s",
		correlationID,
		message.EventID,
	)
}
