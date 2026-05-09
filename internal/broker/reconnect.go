package broker

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	initialReconnectDelay = time.Second
	maxReconnectDelay     = 30 * time.Second
)

func dialWithBackoff(ctx context.Context, url string) (*amqp.Connection, error) {
	delay := initialReconnectDelay

	for {
		conn, err := amqp.Dial(url)
		if err == nil {
			return conn, nil
		}

		log.Printf("rabbitmq connect failed: %v; retry in %s", err, delay)

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("rabbitmq connect canceled: %w", ctx.Err())
		case <-time.After(delay):
		}

		delay *= 2
		if delay > maxReconnectDelay {
			delay = maxReconnectDelay
		}
	}
}
