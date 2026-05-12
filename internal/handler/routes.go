package handler

import (
	"net/http"
	"rabbitmq-notification-service/internal/response"
)

// NewRouter создаёт HTTP router и регистрирует маршруты приложения.
func NewRouter(eventHandler *EventHandler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			eventHandler.CreateEvent(w, r)
		case http.MethodGet:
			eventHandler.ListEvents(w, r)
		default:
			response.WriteError(
				w,
				http.StatusMethodNotAllowed,
				"METHOD_NOT_ALLOWED",
				"method not allowed",
				nil,
			)
		}
	})

	mux.HandleFunc("/api/events/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			eventHandler.GetEventDetails(w, r)
		default:
			response.WriteError(
				w,
				http.StatusMethodNotAllowed,
				"METHOD_NOT_ALLOWED",
				"method not allowed",
				nil,
			)
		}
	})

	mux.HandleFunc("/api/admin/retry-pending", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			eventHandler.RetryPending(w, r)
		default:
			response.WriteError(
				w,
				http.StatusMethodNotAllowed,
				"METHOD_NOT_ALLOWED",
				"method not allowed",
				nil,
			)
		}
	})

	return CorrelationMiddleware(mux)
}
