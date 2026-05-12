package correlation

import (
	"context"

	"github.com/google/uuid"
)

type contextKey struct{}

const HeaderName = "X-Correlation-ID"

// NewID создаёт новый correlation_id.
func NewID() string {
	return uuid.NewString()
}

// WithID добавляет correlation_id в context.
func WithID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, contextKey{}, correlationID)
}

// FromContext достаёт correlation_id из context.
func FromContext(ctx context.Context) string {
	value, ok := ctx.Value(contextKey{}).(string)
	if !ok {
		return ""
	}

	return value
}
