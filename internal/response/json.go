package response

import (
	"encoding/json"
	"net/http"
)

type APIError struct {
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Fields  map[string]string `json:"fields,omitempty"`
}

type ErrorResponse struct {
	Error APIError `json:"error"`
}

// WriteJSON записывает JSON-ответ с указанным HTTP-статусом.
func WriteJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		return
	}
}

// WriteError записывает ошибку в едином формате API.
func WriteError(w http.ResponseWriter, status int, code string, message string, fields map[string]string) {
	res := ErrorResponse{
		Error: APIError{
			Code:    code,
			Message: message,
			Fields:  fields,
		},
	}
	WriteJSON(w, status, res)
}
