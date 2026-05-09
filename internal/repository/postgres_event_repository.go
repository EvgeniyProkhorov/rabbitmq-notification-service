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

// Create сохраняет новое событие заказа в PostgreSQL.
func (r *postgresEventRepository) Create(ctx context.Context, event domain.Event) error {
	query := `
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
			VALUES($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := r.db.ExecContext(
		ctx,
		query,
		event.EventID,
		event.UserID,
		event.OrderID,
		event.EventType,
		event.Payload,
		event.Status,
		event.Attempts,
		event.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("create event: %w", err)
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

// GetPending получает pending-события для публикации в RabbitMQ.
func (r *postgresEventRepository) GetPending(ctx context.Context, limit int) ([]domain.Event, error) {
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
		WHERE status = $1
		ORDER BY created_at ASC
		LIMIT $2
	`

	rows, err := r.db.QueryContext(
		ctx,
		query,
		domain.EventStatusPending,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("get pending events: %w", err)
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
			return nil, fmt.Errorf("scan pending event: %w", err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending events: %w", err)
	}

	return events, nil
}

// MarkAsSent помечает событие как успешно обработанное.
func (r *postgresEventRepository) MarkAsSent(ctx context.Context, eventID string) error {
	query := `
	UPDATE events
	SET status = $1,
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
