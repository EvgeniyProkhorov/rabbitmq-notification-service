package handler

import (
	"log"
	"net/http"
	"rabbitmq-notification-service/internal/correlation"
	"time"
)

// statusResponseWriter оборачивает http.ResponseWriter и сохраняет HTTP status code для логирования.
type statusResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *statusResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

// CorrelationMiddleware добавляет correlation_id в context запроса и логирует HTTP-запрос.
func CorrelationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		correlationID := r.Header.Get(correlation.HeaderName)
		if correlationID == "" {
			correlationID = correlation.NewID()
		}

		w.Header().Set(correlation.HeaderName, correlationID)

		ctx := correlation.WithID(r.Context(), correlationID)
		r = r.WithContext(ctx)

		wrappedWriter := &statusResponseWriter{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next.ServeHTTP(wrappedWriter, r)

		log.Printf(
			"correlation_id=%s method=%s path=%s status=%d duration=%s",
			correlationID,
			r.Method,
			r.URL.Path,
			wrappedWriter.statusCode,
			time.Since(start),
		)
	})
}
