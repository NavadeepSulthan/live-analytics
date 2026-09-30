package platform

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"live-analytics/internal/api"
)

func WriteJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func WriteError(w http.ResponseWriter, status int, code, message string) {
	var body api.Error
	body.Error.Code = code
	body.Error.Message = message
	WriteJSON(w, status, body)
}

// WriteThrottled tells the client to back off, with a Retry-After hint.
func WriteThrottled(w http.ResponseWriter, status int, retryAfter time.Duration, message string) {
	seconds := int(retryAfter.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
	WriteError(w, status, "throttled", message)
}
