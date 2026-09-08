package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	"benchmark/internal/metrics"
	"benchmark/pkg/logger"
)

type statusRecordingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *statusRecordingResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// RequestLoggingMiddleware logs every inbound HTTP request with its request ID, method, path, IP, status code, and duration.
// It also tracks global requestsTotal, requestsAccepted, and requestsRejected metrics.
func RequestLoggingMiddleware(m *metrics.Metrics) func(http.Handler) http.Handler {
	if m == nil {
		m = metrics.Default
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			m.RequestsTotal.Add(1)

			reqID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
			if reqID == "" {
				reqID = GetRequestID(r.Context())
			}
			if reqID == "" {
				var b [16]byte
				_, _ = rand.Read(b[:])
				reqID = "req-" + hex.EncodeToString(b[:])
			}
			w.Header().Set("X-Request-ID", reqID)
			ctx := context.WithValue(r.Context(), RequestIDKey, reqID)
			r = r.WithContext(ctx)

			rw := &statusRecordingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
			logger.Debug("http.inbound", "requestId=%s method=%s path=%s ip=%s proto=%s contentLength=%d userAgent=%q",
				reqID, r.Method, r.URL.Path, GetClientIP(r), r.Proto, r.ContentLength, r.UserAgent())
			next.ServeHTTP(rw, r)

			duration := time.Since(start)
			clientIP := GetClientIP(r)

			if rw.statusCode == http.StatusAccepted {
				m.RequestsAccepted.Add(1)
			} else if rw.statusCode >= 400 {
				m.RequestsRejected.Add(1)
			}

			logger.Info("http.request", "requestId=%s method=%s path=%s ip=%s status=%d duration=%v",
				reqID, r.Method, r.URL.Path, clientIP, rw.statusCode, duration.Round(time.Microsecond))
		})
	}
}
