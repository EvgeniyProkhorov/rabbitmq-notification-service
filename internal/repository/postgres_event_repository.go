package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"rabbitmq-notification-service/internal/domain"
)

type postgresEventRepository struct {
	db *sql.DB
}

// NewPostgresEventRepository создаёт PostgreSQL repository для событий заказов.
func NewPostgresEventRepository(db *sql.DB) EventRepository {
	return &postgresEventRepository{
		db: db,
	}
}

// CreateWithOutbox сохраняет событие и outbox-сообщение в одной транзакции.
func (r *postgresEventRepository) CreateWithOutbox(
	ctx context.Context,
	event domain.Event,
	message domain.OutboxMessage,
) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin create event with outbox transaction: %w", err)
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	eventQuery := `
	INSERT INTO events (
		event_id,
		user_id,
		order_id,
		event_type,
		payload,
		status,
		attempts,
		error_message
	)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	if _, err = tx.ExecContext(
		ctx,
		eventQuery,
		event.EventID,
		event.UserID,
		event.OrderID,
		event.EventType,
		event.Payload,
		event.Status,
		event.Attempts,
		event.ErrorMessage,
	); err != nil {
		return fmt.Errorf("insert event: %w", err)
	}

	outboxQuery := `
	INSERT INTO outbox_messages (
		id,
		event_id,
		exchange_name,
		routing_key,
		payload,
		status,
		attempts,
		error_message
	)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	if _, err = tx.ExecContext(
		ctx,
		outboxQuery,
		message.ID,
		message.EventID,
		message.Exchange,
		message.RoutingKey,
		message.Payload,
		message.Status,
		message.Attempts,
		message.ErrorMessage,
	); err != nil {
		return fmt.Errorf("insert outbox message: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit create event with outbox transaction: %w", err)
	}

	return nil
}

// List получает список событий пользователя с фильтрами и пагинацией.
func (r *postgresEventRepository) List(ctx context.Context, filter EventFilter) ([]domain.Event, error) {
	query := `
	SELECT
    event_id,
    user_id,
    order_id,
    event_type,
    payload,
    status,
    attempts,
    error_message,
    created_at,
    updated_at
	FROM events
	WHERE user_id = $1
	AND ($2 = '' OR event_type = $2)
	AND ($3 = '' OR status = $3)
	ORDER BY created_at DESC
	LIMIT $4 OFFSET $5
	`

	rows, err := r.db.QueryContext(
		ctx,
		query,
		filter.UserID,
		filter.EventType,
		filter.Status,
		filter.Limit,
		filter.Offset,
	)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}

	defer rows.Close()

	events := make([]domain.Event, 0)

	for rows.Next() {
		var event domain.Event

		if err := rows.Scan(
			&event.EventID,
			&event.UserID,
			&event.OrderID,
			&event.EventType,
			&event.Payload,
			&event.Status,
			&event.Attempts,
			&event.ErrorMessage,
			&event.CreatedAt,
			&event.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}

	return events, nil

}

// GetByID получает событие по event_id и user_id.
func (r *postgresEventRepository) GetByID(ctx context.Context, eventID string, userID string) (domain.Event, error) {
	query := `
	SELECT
    event_id,
    user_id,
    order_id,
    event_type,
    payload,
    status,
    attempts,
    error_message,
    created_at,
    updated_at
	FROM events
	WHERE event_id = $1 AND user_id = $2
	`

	var event domain.Event

	err := r.db.QueryRowContext(ctx, query, eventID, userID).Scan(
		&event.EventID,
		&event.UserID,
		&event.OrderID,
		&event.EventType,
		&event.Payload,
		&event.Status,
		&event.Attempts,
		&event.ErrorMessage,
		&event.CreatedAt,
		&event.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Event{}, ErrEventNotFound
		}
		return domain.Event{}, fmt.Errorf("get event by id: %w", err)
	}

	return event, nil

}

// MarkAsSent помечает событие как успешно обработанное.
func (r *postgresEventRepository) MarkAsSent(ctx context.Context, eventID string) error {
	query := `
	UPDATE events
	SET status = $1,
		error_message = NULL,
		updated_at = NOW()
	WHERE event_id = $2
	`

	result, err := r.db.ExecContext(ctx, query, domain.EventStatusSent, eventID)
	if err != nil {
		return fmt.Errorf("mark event as sent: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get affected rows: %w", err)
	}

	if rowsAffected == 0 {
		return ErrEventNotFound
	}

	return nil
}

// MarkAsFailed помечает событие как окончательно не обработанное и сохраняет текст ошибки.
func (r *postgresEventRepository) MarkAsFailed(ctx context.Context, eventID string, errorMessage string) error {
	query := `
		UPDATE events
		SET status = $1,
		    error_message = $2,
		    updated_at = NOW()
		WHERE event_id = $3
	`

	result, err := r.db.ExecContext(
		ctx,
		query,
		domain.EventStatusFailed,
		errorMessage,
		eventID,
	)
	if err != nil {
		return fmt.Errorf("mark event as failed: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get affected rows: %w", err)
	}

	if rowsAffected == 0 {
		return ErrEventNotFound
	}

	return nil
}

// IncrementAttempts увеличивает количество попыток обработки события и возвращает новое значение.
func (r *postgresEventRepository) IncrementAttempts(ctx context.Context, eventID string) (int, error) {
	query := `
	UPDATE events
	SET attempts = attempts + 1,
		updated_at = NOW()
	WHERE event_id = $1
	RETURNING attempts
	`

	var attempts int
	err := r.db.QueryRowContext(ctx, query, eventID).Scan(&attempts)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrEventNotFound
		}
		return 0, fmt.Errorf("increment event attempts: %w", err)
	}

	return attempts, nil
}

// ClaimPendingOutbox забирает pending outbox-сообщения в обработку.
func (r *postgresEventRepository) ClaimPendingOutbox(ctx context.Context, limit int) ([]domain.OutboxMessage, error) {
	query := `
	WITH picked AS (
		SELECT id
		FROM outbox_messages
		WHERE status = $1
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	)
	UPDATE outbox_messages AS o
	SET status = $3,
		attempts = attempts + 1,
		updated_at = NOW()
	FROM picked
	WHERE o.id = picked.id
	RETURNING
		o.id,
		o.event_id,
		o.exchange_name,
		o.routing_key,
		o.payload,
		o.status,
		o.attempts,
		o.error_message,
		o.created_at,
		o.updated_at
	`

	rows, err := r.db.QueryContext(
		ctx,
		query,
		domain.OutboxStatusPending,
		limit,
		domain.OutboxStatusProcessing,
	)
	if err != nil {
		return nil, fmt.Errorf("claim pending outbox messages: %w", err)
	}
	defer rows.Close()

	messages := make([]domain.OutboxMessage, 0)

	for rows.Next() {
		var message domain.OutboxMessage

		if err := rows.Scan(
			&message.ID,
			&message.EventID,
			&message.Exchange,
			&message.RoutingKey,
			&message.Payload,
			&message.Status,
			&message.Attempts,
			&message.ErrorMessage,
			&message.CreatedAt,
			&message.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan outbox message: %w", err)
		}

		messages = append(messages, message)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox messages: %w", err)
	}

	return messages, nil
}

// MarkOutboxPublished помечает outbox-сообщение как успешно опубликованное.
func (r *postgresEventRepository) MarkOutboxPublished(ctx context.Context, id string) error {
	query := `
	UPDATE outbox_messages
	SET status = $1,
		error_message = NULL,
		updated_at = NOW()
	WHERE id = $2
	`

	result, err := r.db.ExecContext(ctx, query, domain.OutboxStatusPublished, id)
	if err != nil {
		return fmt.Errorf("mark outbox message as published: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get affected rows: %w", err)
	}

	if rowsAffected == 0 {
		return ErrEventNotFound
	}

	return nil
}

// MarkOutboxFailed помечает outbox-сообщение как не опубликованное и сохраняет текст ошибки.
func (r *postgresEventRepository) MarkOutboxFailed(ctx context.Context, id string, errorMessage string) error {
	query := `
	UPDATE outbox_messages
	SET status = $1,
		error_message = $2,
		updated_at = NOW()
	WHERE id = $3
	`

	result, err := r.db.ExecContext(ctx, query, domain.OutboxStatusFailed, errorMessage, id)
	if err != nil {
		return fmt.Errorf("mark outbox message as failed: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get affected rows: %w", err)
	}

	if rowsAffected == 0 {
		return ErrEventNotFound
	}

	return nil
}

// ReleaseOutboxForRetry возвращает outbox-сообщение в pending для повторной публикации.
func (r *postgresEventRepository) ReleaseOutboxForRetry(ctx context.Context, id string, errorMessage string) error {
	query := `
	UPDATE outbox_messages
	SET status = $1,
		error_message = $2,
		updated_at = NOW()
	WHERE id = $3
	`

	result, err := r.db.ExecContext(ctx, query, domain.OutboxStatusPending, errorMessage, id)
	if err != nil {
		return fmt.Errorf("release outbox message for retry: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get affected rows: %w", err)
	}

	if rowsAffected == 0 {
		return ErrEventNotFound
	}

	return nil
}
