// Package main menyediakan web server minimal dan tangguh dengan dua endpoint:
//
//	GET /visit       -> menambah dan mengembalikan total kunjungan (in-memory)
//	GET /echo?msg=   -> menerima parameter msg dan mengembalikannya ke pemanggil
//
// Server ditulis dengan pola production-ready + pertahanan berlapis:
//   - Validasi input ketat (panjang, karakter kontrol).
//   - Header keamanan (CSP, X-Content-Type-Options, Referrer-Policy, dll.).
//   - Batas ukuran request, rate-limit per IP, dan timeout I/O.
//   - Logging terstruktur dengan alamat client.
//   - Graceful shutdown berbasis sinyal.
//   - Penghitung kunjungan thread-safe (atomic).
//
// Dependensi: HANYA pustaka standar (net/http, log, sync, dll.) sehingga build
// kecil dan permukaan serangan minim.
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// -----------------------------------------------------------------------------
// Konstanta konfigurasi
// -----------------------------------------------------------------------------

const (
	defaultListenAddr    = ":8080"
	envListenAddr        = "LISTEN_ADDR"
	envTrustedProxies    = "TRUSTED_PROXIES"

	maxMsgBytes          = 4096
	maxRequestBytes       = 8192
	maxHeaderBytes       = 1 << 16

	readHeaderTimeout    = 5 * time.Second
	requestTimeout       = 15 * time.Second
	idleTimeout          = 60 * time.Second
	shutdownTimeout      = 30 * time.Second

	rateLimitBurst       = 30
	rateLimitRefillPerSec = 10
	rateLimitCleanupEvery = 5 * time.Minute

	nonceBytes           = 16
)

// -----------------------------------------------------------------------------
// Penghitung kunjungan (thread-safe)
// -----------------------------------------------------------------------------

type visitorCount struct {
	v atomic.Uint64
}

func (c *visitorCount) inc() uint64 { return c.v.Add(1) }
func (c *visitorCount) get() uint64 { return c.v.Load() }

// -----------------------------------------------------------------------------
// Token bucket rate limiter per IP
// -----------------------------------------------------------------------------

type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	lastRefill time.Time
}

func newTokenBucket() *tokenBucket {
	return &tokenBucket{tokens: rateLimitBurst, lastRefill: time.Now()}
}

func (b *tokenBucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens += elapsed * rateLimitRefillPerSec
	if b.tokens > rateLimitBurst {
		b.tokens = rateLimitBurst
	}
	b.lastRefill = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

type ipRateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
}

func newIPRateLimiter() *ipRateLimiter {
	return &ipRateLimiter{buckets: make(map[string]*tokenBucket)}
}

func (r *ipRateLimiter) allow(ip string) bool {
	r.mu.Lock()
	b, ok := r.buckets[ip]
	if !ok {
		b = newTokenBucket()
		r.buckets[ip] = b
	}
	r.mu.Unlock()
	return b.allow()
}

func (r *ipRateLimiter) sweep() {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := time.Now().Add(-10 * time.Minute)
	for ip, b := range r.buckets {
		b.mu.Lock()
		if b.lastRefill.Before(cutoff) {
			delete(r.buckets, ip)
		}
		b.mu.Unlock()
	}
}

// -----------------------------------------------------------------------------
// Daftar proxy tepercaya
// -----------------------------------------------------------------------------

type trustedProxies struct {
	cidrs []*net.IPNet
}

func parseTrustedProxies(raw string) *trustedProxies {
	tp := &trustedProxies{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if !strings.Contains(part, "/") {
			if strings.Contains(part, ":") {
				part += "/128"
			} else {
				part += "/32"
			}
// -----------------------------------------------------------------------------
// Pembantu respons
// -----------------------------------------------------------------------------

// generateNonce menghasilkan nonce base64 acak untuk Content-Security-Policy.
func generateNonce() string {
	b := make([]byte, nonceBytes)
	if _, err := rand.Read(b); err != nil {
		ts := time.Now().UnixNano()
		return base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(ts, 10)))
	}
	return base64.StdEncoding.EncodeToString(b)
}

// writeJSON menulis JSON dengan status code tertentu.
func writeJSON(w http.ResponseWriter, status int, payload any) {
	buf, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, `{"error":"encode error"}`+"\n", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(buf)
	_, _ = w.Write([]byte("\n"))
}

// writeText menulis teks polos UTF-8 dengan status code.
func writeText(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

// clientIP mengekstrak alamat IP dengan memperhitungkan proxy tepercaya.
func (s *Server) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	remoteIP := net.ParseIP(host)

	if remoteIP != nil && s.trusted.contains(remoteIP) {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			first := strings.TrimSpace(parts[0])
			if ip := net.ParseIP(first); ip != nil {
				return ip.String()
			}
		}
		if xrip := r.Header.Get("X-Real-IP"); xrip != "" {
			if ip := net.ParseIP(strings.TrimSpace(xrip)); ip != nil {
				return ip.String()
			}
		}
	}

	if remoteIP != nil {
		return remoteIP.String()
	}
	return "0.0.0.0"
}

// -----------------------------------------------------------------------------
// Validasi input untuk /echo
// -----------------------------------------------------------------------------

var (
	ErrEmptyMsg   = errors.New("parameter msg wajib diisi")
	ErrMsgTooLong = errors.New("parameter msg melebihi batas maksimum")
	ErrMsgInvalid = errors.New("parameter msg mengandung karakter yang tidak diizinkan")
)

// validateMsg menyaring karakter kontrol/NULL dan membatasi panjang.
func validateMsg(raw string) (string, error) {
	if raw == "" {
		return "", ErrEmptyMsg
	}
	if len(raw) > maxMsgBytes {
		return "", ErrMsgTooLong
	}
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if c == 0 {
			return "", ErrMsgInvalid
		}
		if c < 0x20 && c != '\t' && c != '\n' && c != '\r' {
			return "", ErrMsgInvalid
		}
// -----------------------------------------------------------------------------
// Server
// -----------------------------------------------------------------------------

type Server struct {
	count     visitorCount
	limiter   *ipRateLimiter
	trusted   *trustedProxies
	startTime time.Time
}

func newServer(trusted *trustedProxies) *Server {
	return &Server{
		limiter:   newIPRateLimiter(),
		trusted:   trusted,
		startTime: time.Now(),
	}
}

// Handler membangun pipeline middleware.
func (s *Server) Handler() http.Handler {
	return s.chain(
		s.securityHeaders,
		s.maxBodyMiddleware,
		s.recoverMiddleware,
		s.loggingMiddleware,
		s.rateLimitMiddleware,
		s.timeoutMiddleware,
	)
}

// chain merangkai handler terakhir dengan middleware.
func (s *Server) chain(mws ...func(http.Handler) http.Handler) http.Handler {
	// innermost: methodMiddleware(dispatchingHandler)
	var h http.Handler = s.methodMiddleware(s.dispatchingHandler())
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// dispatchingHandler memilih handler untuk path tertentu.
func (s *Server) dispatchingHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/visit", s.handleVisit)
	mux.HandleFunc("/echo", s.handleEcho)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/", s.handleNotFound)
	return mux
}

// -----------------------------------------------------------------------------
// Middleware
// -----------------------------------------------------------------------------

// securityHeaders menambahkan header perlindungan standar ke setiap respons.
func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()

		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Permissions-Policy", "geolocation=(), camera=(), microphone=(), payment=()")
		h.Set("Cross-Origin-Opener-Policy", "same-origin")
		h.Set("Cross-Origin-Resource-Policy", "same-origin")

		nonce := generateNonce()
		csp := strings.Join([]string{
			"default-src 'none'",
			"frame-ancestors 'none'",
			"base-uri 'none'",
			"form-action 'none'",
			"style-src 'nonce-" + nonce + "'",
			"img-src 'self' data:",
		}, "; ")
		h.Set("Content-Security-Policy", csp)

		if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
			h.Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		}

		next.ServeHTTP(w, r)
	})
}

// maxBodyMiddleware membatasi ukuran body agar anti-DoS.
func (s *Server) maxBodyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
		next.ServeHTTP(w, r)
	})
}

// recoverMiddleware menangkap panic agar server tetap hidup.
func (s *Server) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("panic: %v path=%s ip=%s", rec, r.URL.Path, s.clientIP(r))
				writeJSON(w, http.StatusInternalServerError, map[string]string{
					"error": "internal server error",
				})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// loggingMiddleware mencatat setiap request.
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		log.Printf("http method=%s path=%s status=%d bytes=%d ip=%s ua=%q dur=%s",
			r.Method,
			r.URL.Path,
			rw.status,
			rw.bytes,
			s.clientIP(r),
			r.Header.Get("User-Agent"),
			time.Since(start),
		)
	})
}

// statusRecorder merekam status code dan jumlah byte yang ditulis.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusRecorder) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusRecorder) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

// rateLimitMiddleware menolak request dengan 429 bila IP melebihi kuota.
func (s *Server) rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := s.clientIP(r)
		if !s.limiter.allow(ip) {
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusTooManyRequests, map[string]string{
				"error": "rate limit exceeded",
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// timeoutMiddleware memberlakukan batas waktu per request.
func (s *Server) timeoutMiddleware(next http.Handler) http.Handler {
// -----------------------------------------------------------------------------
// Handler endpoint
// -----------------------------------------------------------------------------

// handleVisit menambah penghitung kunjungan dan mengembalikan total.
func (s *Server) handleVisit(w http.ResponseWriter, r *http.Request) {
	total := s.count.inc()
	writeJSON(w, http.StatusOK, map[string]any{
		"endpoint":    "visit",
		"total":       total,
		"server_time": time.Now().UTC().Format(time.RFC3339),
	})
}

// handleEcho memvalidasi parameter msg dan mengembalikannya.
//
// Keamanan:
//   - Membatasi panjang msg untuk mencegah log injection / DoS.
//   - Menolak NULL byte dan karakter kontrol yang dapat merusak terminal/log.
//   - Mengirim Content-Type JSON sehingga input diperlakukan sebagai data
//     (anti-XSS) bukan markup.
func (s *Server) handleEcho(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("msg")
	msg, err := safeDecodeMsg(raw)
	if err != nil {
		switch {
		case errors.Is(err, ErrEmptyMsg):
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "parameter msg wajib diisi",
			})
		case errors.Is(err, ErrMsgTooLong):
			writeJSON(w, http.StatusRequestURITooLong, map[string]string{
				"error": "parameter msg melebihi batas maksimum",
			})
		default:
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "parameter msg mengandung karakter yang tidak diizinkan",
			})
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"endpoint": "echo",
		"msg":      msg,
	})
}

// handleHealth adalah endpoint health-check untuk orchestrator.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"uptime_sec": int64(time.Since(s.startTime).Seconds()),
		"visitors":   s.count.get(),
	})
}

// handleNotFound merespons 404 tanpa membocorkan path internal.
func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusNotFound, map[string]string{
		"error": "not found",
	})
}

// -----------------------------------------------------------------------------
// Utilitas tambahan
// -----------------------------------------------------------------------------

func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func validateListenAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "" {
		host = "0.0.0.0"
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return err
	}
	if p < 1 || p > 65535 {
		return errors.New("port di luar jangkauan 1-65535")
	}
	if ip := net.ParseIP(host); ip == nil {
		if strings.ContainsAny(host, " /\\") {
			return errors.New("hostname mengandung karakter terlarang")
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// Entry point + graceful shutdown
// -----------------------------------------------------------------------------

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC | log.Lmicroseconds)

	addr := getenvDefault(envListenAddr, defaultListenAddr)
	if err := validateListenAddr(addr); err != nil {
		log.Fatalf("config: alamat listen tidak valid %q: %v", addr, err)
	}

	trusted := parseTrustedProxies(getenvDefault(envTrustedProxies, ""))

	srv := newServer(trusted)

	// Goroutine pembersih rate limiter.
	go func() {
		ticker := time.NewTicker(rateLimitCleanupEvery)
		defer ticker.Stop()
		for range ticker.C {
			srv.limiter.sweep()
		}
	}()

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       requestTimeout,
		WriteTimeout:      requestTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
		ErrorLog:          log.New(os.Stderr, "http: ", log.LstdFlags),
	}

	idleConnsClosed := make(chan struct{})
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Printf("shutdown: sinyal diterima, mematikan server (timeout=%s)...", shutdownTimeout)

		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			log.Printf("shutdown: error pada httpServer.Shutdown: %v", err)
		}
		close(idleConnsClosed)
	}()

	log.Printf("listening on %s (trusted_proxies=%d)", addr, len(trusted.cidrs))
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}

	<-idleConnsClosed
	log.Printf("shutdown: server berhenti dengan bersih. bye.")
}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// methodMiddleware hanya mengizinkan GET dan HEAD untuk endpoint publik.
func (s *Server) methodMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
				"error": "method not allowed",
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}
		if c == 0x7F {
			return "", ErrMsgInvalid
		}
	}
	return raw, nil
}

func safeDecodeMsg(raw string) (string, error) {
	return validateMsg(raw)
}
		}
		_, cidr, err := net.ParseCIDR(part)
		if err != nil {
			log.Printf("config: mengabaikan TRUSTED_PROXIES entry invalid %q: %v", part, err)
			continue
		}
		tp.cidrs = append(tp.cidrs, cidr)
	}
	return tp
}

func (tp *trustedProxies) contains(ip net.IP) bool {
	if ip == nil {
		return false
	}
	for _, c := range tp.cidrs {
		if c.Contains(ip) {
			return true
		}
	}
	return false
}