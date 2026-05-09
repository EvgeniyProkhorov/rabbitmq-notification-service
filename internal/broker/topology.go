package broker

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func declareTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(
		ordersExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare orders exchange: %w", err)
	}

	if err := ch.ExchangeDeclare(
		deadLetterExchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare dead letter exchange: %w", err)
	}

	if _, err := ch.QueueDeclare(
		deadLetterQueue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare dead letter queue: %w", err)
	}

	if err := ch.QueueBind(
		deadLetterQueue,
		deadLetterQueue,
		deadLetterExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind dead letter queue: %w", err)
	}

	args := amqp.Table{
		"x-dead-letter-exchange":    deadLetterExchange,
		"x-dead-letter-routing-key": deadLetterQueue,
	}

	if _, err := ch.QueueDeclare(
		notificationsQueue,
		true,
		false,
		false,
		false,
		args,
	); err != nil {
		return fmt.Errorf("declare notifications queue: %w", err)
	}

	if err := ch.QueueBind(
		notificationsQueue,
		"order.*",
		ordersExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind notifications queue: %w", err)
	}

	return nil
}
