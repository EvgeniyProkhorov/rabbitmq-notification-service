package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"rabbitmq-notification-service/internal/repository"
	"rabbitmq-notification-service/internal/response"
	"rabbitmq-notification-service/internal/service"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
)

// EventHandler обрабатывает HTTP-запросы, связанные с событиями заказов.
type EventHandler struct {
	service  service.EventService
	validate *validator.Validate
}

// NewEventHandler создаёт HTTP handler для работы с событиями.
func NewEventHandler(
	service service.EventService,
	validate *validator.Validate,
) *EventHandler {
	return &EventHandler{
		service:  service,
		validate: validate,
	}
}

// CreateEvent обрабатывает запрос на регистрацию события заказа.
func (h *EventHandler) CreateEvent(w http.ResponseWriter, r *http.Request) {
	var req CreateEventRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.WriteError(
			w,
			http.StatusBadRequest,
			"BAD_REQUEST",
			"invalid json body",
			nil,
		)
		return
	}

	if err := h.validate.Struct(req); err != nil {
		fields := response.ValidationFields(err)
		response.WriteError(
			w,
			http.StatusBadRequest,
			"VALIDATION_ERROR",
			"validation failed",
			fields,
		)
		return
	}

	input := service.CreateEventInput{
		UserID:    req.UserID,
		EventType: req.EventType,
		OrderID:   req.OrderID,
		Payload:   req.Payload,
	}

	output, err := h.service.CreateEvent(r.Context(), input)
	if err != nil {
		response.WriteError(
			w,
			http.StatusInternalServerError,
			"INTERNAL_ERROR",
			"internal server error",
			nil,
		)
		return
	}

	resp := CreateEventResponse{
		EventID: output.EventID,
		Status:  output.Status,
	}

	response.WriteJSON(w, http.StatusCreated, resp)

}

// ListEvents обрабатывает запрос на получение списка событий пользователя.
func (h *EventHandler) ListEvents(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	page, err := parseIntQuery(query.Get("page"), 1)
	if err != nil {
		response.WriteError(
			w,
			http.StatusBadRequest,
			"BAD_REQUEST",
			"page must be an integer",
			nil,
		)
		return
	}

	limit, err := parseIntQuery(query.Get("limit"), 20)
	if err != nil {
		response.WriteError(
			w,
			http.StatusBadRequest,
			"BAD_REQUEST",
			"limit must be an integer",
			nil,
		)
		return
	}

	req := ListEventsRequest{
		UserID:    query.Get("user_id"),
		EventType: query.Get("event_type"),
		Status:    query.Get("status"),
		Page:      page,
		Limit:     limit,
	}

	if err := h.validate.Struct(req); err != nil {
		fields := response.ValidationFields(err)
		response.WriteError(
			w,
			http.StatusBadRequest,
			"VALIDATION_ERROR",
			"validation failed",
			fields,
		)
		return
	}

	input := service.ListEventsInput{
		UserID:    req.UserID,
		EventType: req.EventType,
		Status:    req.Status,
		Page:      req.Page,
		Limit:     req.Limit,
	}

	output, err := h.service.ListEvents(r.Context(), input)
	if err != nil {
		response.WriteError(
			w,
			http.StatusInternalServerError,
			"INTERNAL_ERROR",
			"internal server error",
			nil,
		)
		return
	}

	items := make([]EventListItemResponse, 0, len(output.Items))

	for _, item := range output.Items {
		items = append(items, EventListItemResponse{
			EventID:   item.EventID,
			OrderID:   item.OrderID,
			EventType: item.EventType,
			Status:    item.Status,
			CreatedAt: item.CreatedAt.Format(time.RFC3339),
		})
	}
	response.WriteJSON(w, http.StatusOK, items)
}

// GetEventDetails обрабатывает запрос на получение детальной информации о событии.
func (h *EventHandler) GetEventDetails(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimPrefix(r.URL.Path, "/api/events/")
	userID := r.URL.Query().Get("user_id")

	req := EventDetailsRequest{
		EventID: eventID,
		UserID:  userID,
	}

	if err := h.validate.Struct(req); err != nil {
		fields := response.ValidationFields(err)

		response.WriteError(
			w,
			http.StatusBadRequest,
			"VALIDATION_ERROR",
			"validation failed",
			fields,
		)
		return
	}

	input := service.GetEventDetailsInput{
		EventID: eventID,
		UserID:  userID,
	}

	output, err := h.service.GetEventDetails(r.Context(), input)
	if err != nil {
		if errors.Is(err, repository.ErrEventNotFound) {
			response.WriteError(
				w,
				http.StatusNotFound,
				"NOT_FOUND",
				"not found",
				nil,
			)
			return
		}
		response.WriteError(
			w,
			http.StatusInternalServerError,
			"INTERNAL_ERROR",
			"internal server error",
			nil,
		)
		return
	}

	res := EventDetailsResponse{
		EventID:      output.EventID,
		UserID:       output.UserID,
		OrderID:      output.OrderID,
		EventType:    output.EventType,
		Payload:      output.Payload,
		Status:       output.Status,
		Attempts:     output.Attempts,
		ErrorMessage: output.ErrorMessage,
		CreatedAt:    output.CreatedAt.Format(time.RFC3339),
		UpdatedAt:    output.UpdatedAt.Format(time.RFC3339),
	}

	response.WriteJSON(w, http.StatusOK, res)
}

// RetryPending обрабатывает ручной запуск публикации pending-событий.
func (h *EventHandler) RetryPending(w http.ResponseWriter, r *http.Request) {
	if err := h.service.ProcessPendingEvents(r.Context()); err != nil {
		response.WriteError(
			w,
			http.StatusInternalServerError,
			"INTERNAL_ERROR",
			"internal server error",
			nil,
		)
		return
	}

	resp := RetryPendingResponse{
		Status: "processed",
	}

	response.WriteJSON(w, http.StatusOK, resp)
}
