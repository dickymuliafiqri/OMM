package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"benchmark/pkg/logger"
)

type contextKey string

const (
	RequestIDKey contextKey = "requestID"
	MaxBodyBytes int64      = 1024 * 1024 // 1MB payload ceiling
)

// RequestValidationMiddleware enforces Content-Type, payload size limits, request IDs, and context timeout.
func RequestValidationMiddleware(timeout time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 1. Assign or propagate X-Request-ID
			reqID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
			if reqID == "" {
				var b [16]byte
				_, _ = rand.Read(b[:])
				reqID = "req-" + hex.EncodeToString(b[:])
			}
			w.Header().Set("X-Request-ID", reqID)

			// 2. Wrap context with RequestID and Timeout
			ctx := context.WithValue(r.Context(), RequestIDKey, reqID)
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			r = r.WithContext(ctx)

			// 3. Check Content-Type for POST, PUT, PATCH with body
			if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch {
				contentType := r.Header.Get("Content-Type")
				mediaType := strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
				if mediaType != "application/json" {
					writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{
						"error": "unsupported media type, application/json required",
					})
					return
				}

				// 4. Enforce 1MB body limit
				r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
				logger.Debug("validate.body_guard", "requestId=%s method=%s path=%s maxBytes=%d", reqID, r.Method, r.URL.Path, MaxBodyBytes)
			}

			next.ServeHTTP(w, r)
		})
	}
}

// GetRequestID extracts the request ID from context
func GetRequestID(ctx context.Context) string {
	if val, ok := ctx.Value(RequestIDKey).(string); ok {
		return val
	}
	return ""
}

// writeJSON writes a JSON response with Content-Type header
func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}
