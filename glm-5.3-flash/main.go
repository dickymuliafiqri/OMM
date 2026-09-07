// main.go — single-file production web server (stdlib only).
//
// Endpoints:
//   GET /visit        increments and returns the total visitor count (in memory)
//   GET /echo?msg=... echoes the msg parameter back as JSON
//
// Hardening: atomic counter, per-IP token-bucket rate limiting, server
// timeouts, bounded header/query sizes, security headers, JSON-only output
// (XSS-safe), graceful shutdown.
//
// ponytail: counters are per-process memory (lost on restart) and rate
// limiting is per-instance; move both to Redis when running >1 replica.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ---------------------------- visitor counter ----------------------------

var visits atomic.Uint64

func handleVisit(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"visits": visits.Add(1)})
}

// --------------------------------- echo ----------------------------------

const maxMsgLen = 2048 // decoded bytes; bounded above by MaxHeaderBytes anyway

func handleEcho(w http.ResponseWriter, r *http.Request) {
	if !r.URL.Query().Has("msg") {
		writeErr(w, http.StatusBadRequest, "missing msg parameter")
		return
	}
	msg := r.URL.Query().Get("msg")
	if len(msg) > maxMsgLen {
		writeErr(w, http.StatusRequestEntityTooLarge, "msg too long")
		return
	}
	// json.Encoder escapes <, >, & and control chars; combined with the
	// application/json content type the payload can never execute as HTML.
	writeJSON(w, http.StatusOK, map[string]any{"msg": msg})
}

// ----------------------------- helpers -----------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// --------------------------- rate limiting -------------------------------

const (
	ratePerSec = 10.0            // sustained requests/second per bucket
	rateBurst  = 20.0            // burst size per bucket
	bucketTTL  = 5 * time.Minute // idle buckets are evicted
	maxBuckets = 1 << 16         // hard cap on tracker memory
)

type bucket struct {
	tokens float64
	last   time.Time
}

type limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
}

func newLimiter() *limiter {
	l := &limiter{buckets: make(map[string]*bucket)}
	go func() { // eviction loop; bounds memory under address-spoofing floods
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for range t.C {
			l.mu.Lock()
			now := time.Now()
			for k, b := range l.buckets {
				if now.Sub(b.last) > bucketTTL {
					delete(l.buckets, k)
				}
			}
			l.mu.Unlock()
		}
	}()
	return l
}

func (l *limiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	b, ok := l.buckets[key]
	if !ok {
		if len(l.buckets) >= maxBuckets {
			return false // under memory pressure, shed load
		}
		b = &bucket{tokens: rateBurst, last: now}
		l.buckets[key] = b
	}
	b.tokens = min(rateBurst, b.tokens+now.Sub(b.last).Seconds()*ratePerSec)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// clientIP returns the bucketing key. By default only the TCP peer is
// trusted; set TRUST_PROXY=1 when running behind your own reverse proxy
// that overwrites X-Forwarded-For, otherwise clients can spoof it.
func clientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			// Rightmost hop is the one appended by the direct proxy;
			// spoofed entries sit on the left, so never trust them.
			if i := strings.LastIndexByte(xff, ','); i >= 0 {
				return strings.TrimSpace(xff[i+1:])
			}
			return strings.TrimSpace(xff)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// bucketKey aggregates to /24 (IPv4) and /64 (IPv6) so one attacker with a
// whole /64 cannot mint unlimited buckets, while NAT offices still fit.
func bucketKey(ip string) string {
	if n := net.ParseIP(ip); n != nil {
		if n.To4() != nil {
			return n.Mask(net.CIDRMask(24, 32)).String()
		}
		return n.Mask(net.CIDRMask(64, 128)).String()
	}
	return ip
}

func rateLimit(l *limiter, trustProxy bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !l.allow(bucketKey(clientIP(r, trustProxy))) {
				w.Header().Set("Retry-After", "1")
				writeErr(w, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ---------------------------- middleware ----------------------------------

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cache-Control", "no-store")
		// HSTS is only meaningful once TLS terminates on this process or a
		// trusted proxy; harmless otherwise.
		h.Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		if sw.status == 0 {
			sw.status = http.StatusOK
		}
		slog.Info("http", "method", r.Method, "path", r.URL.Path,
			"status", sw.status, "remote", r.RemoteAddr, "dur", time.Since(start).String())
	})
}

// --------------------------------- main -----------------------------------

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	trustProxy := os.Getenv("TRUST_PROXY") == "1"

	// "GET /visit" also matches HEAD; other methods get an automatic 405
	// with an Allow header, unknown paths a 404 — both from the mux itself.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /visit", handleVisit)
	mux.HandleFunc("GET /echo", handleEcho)

	handler := securityHeaders(logging(rateLimit(newLimiter(), trustProxy)(mux)))

	srv := &http.Server{
		Addr:              net.JoinHostPort("", port),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second, // slowloris defense
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    8 << 10,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", srv.Addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		slog.Info("shutting down")
		shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx) // drains in-flight requests
	}
}

