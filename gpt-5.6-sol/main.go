// Command server provides a small, dependency-free HTTP service.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"
)

const (
	maxMessageBytes = 1024
	maxQueryBytes   = 4096
	maxInFlight     = 256
)

type application struct {
	visits atomic.Uint64
	slots  chan struct{}
	limit  *tokenBucket
}

// tokenBucket is a process-wide last line of defense against request floods.
// A production reverse proxy should additionally enforce per-client limits.
type tokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	last     time.Time
	rate     float64
	capacity float64
}

func newTokenBucket(rate, capacity float64) *tokenBucket {
	return &tokenBucket{tokens: capacity, last: time.Now(), rate: rate, capacity: capacity}
}

func (b *tokenBucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func newApplication() http.Handler {
	a := &application{
		slots: make(chan struct{}, maxInFlight),
		limit: newTokenBucket(100, 200),
	}
	return a.security(a.recoverPanic(a.limitRequests(a.route)))
}

func (a *application) route(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		writeError(w, http.StatusBadRequest, "request body is not allowed")
		return
	}

	switch r.URL.Path {
	case "/visit":
		a.handleVisit(w, r)
	case "/echo":
		a.handleEcho(w, r)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (a *application) handleVisit(w http.ResponseWriter, r *http.Request) {
	if !requireGET(w, r) {
		return
	}
	if r.URL.RawQuery != "" {
		writeError(w, http.StatusBadRequest, "query parameters are not allowed")
		return
	}

	var total uint64
	for {
		current := a.visits.Load()
		if current == math.MaxUint64 {
			total = current
			break
		}
		if a.visits.CompareAndSwap(current, current+1) {
			total = current + 1
			break
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(struct {
		Total uint64 `json:"total"`
	}{Total: total})
}

func (a *application) handleEcho(w http.ResponseWriter, r *http.Request) {
	if !requireGET(w, r) {
		return
	}
	if len(r.URL.RawQuery) > maxQueryBytes {
		writeError(w, http.StatusBadRequest, "query is too long")
		return
	}

	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(values) != 1 {
		writeError(w, http.StatusBadRequest, "expected exactly one msg parameter")
		return
	}
	messages, ok := values["msg"]
	if !ok || len(messages) != 1 {
		writeError(w, http.StatusBadRequest, "expected exactly one msg parameter")
		return
	}
	msg := messages[0]
	if len(msg) > maxMessageBytes || !utf8.ValidString(msg) {
		writeError(w, http.StatusBadRequest, "msg must be valid UTF-8 and at most 1024 bytes")
		return
	}

	// Plain text plus nosniff makes HTML/JavaScript input inert in browsers.
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(msg))
}

func requireGET(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet {
		return true
	}
	w.Header().Set("Allow", http.MethodGet)
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	return false
}

func (a *application) limitRequests(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.limit.allow() {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "too many requests")
			return
		}
		select {
		case a.slots <- struct{}{}:
			defer func() { <-a.slots }()
			next(w, r)
		default:
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusServiceUnavailable, "server is busy")
		}
	}
}

func (a *application) recoverPanic(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("recovered HTTP panic", "error", fmt.Sprint(recovered))
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next(w, r)
	}
}

func (a *application) security(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Cache-Control", "no-store")
		next(w, r)
	})
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error string `json:"error"`
	}{Error: message})
}

func main() {
	addr := strings.TrimSpace(os.Getenv("ADDR"))
	if addr == "" {
		addr = ":8080"
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           newApplication(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
		ErrorLog:          slog.NewLogLogger(slog.Default().Handler(), slog.LevelError),
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveErrors := make(chan error, 1)

	certFile, keyFile := os.Getenv("TLS_CERT_FILE"), os.Getenv("TLS_KEY_FILE")
	go func() {
		slog.Info("server starting", "address", addr, "tls", certFile != "")
		if (certFile == "") != (keyFile == "") {
			serveErrors <- errors.New("TLS_CERT_FILE and TLS_KEY_FILE must be set together")
			return
		}
		if certFile != "" {
			serveErrors <- server.ListenAndServeTLS(certFile, keyFile)
			return
		}
		slog.Warn("serving plain HTTP; terminate HTTPS at a trusted reverse proxy")
		serveErrors <- server.ListenAndServe()
	}()

	select {
	case err := <-serveErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		slog.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("graceful shutdown failed", "error", err)
			_ = server.Close()
		}
	}
}
