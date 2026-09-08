package server

import (
	"net/http"
	"strings"
	"time"

	"benchmark/internal/config"
	"benchmark/internal/metrics"
)

// RouterConfig defines the configuration and optional handler implementations for the server router.
type RouterConfig struct {
	Config        *config.Config
	RateLimiter   *RateLimiter
	Pool          JobSubmitter
	SSRF          *SSRFValidator
	Metrics       *metrics.Metrics
	StartTime     time.Time
	RootHandler   http.HandlerFunc
	HealthHandler http.HandlerFunc
	ReadyHandler  http.HandlerFunc
	BenchHandler  http.HandlerFunc
	StatusHandler http.HandlerFunc
}

// NewRouter constructs the HTTP router with all security and operational middlewares applied.
func NewRouter(rc RouterConfig) http.Handler {
	mux := http.NewServeMux()

	pool := rc.Pool
	if pool == nil {
		pool = NewMemoryJobSubmitter(rc.Config.MaxConcurrentJobs)
	}

	startTime := rc.StartTime
	if startTime.IsZero() {
		startTime = time.Now()
	}

	met := rc.Metrics
	if met == nil {
		met = metrics.Default
	}

	// Root Handler
	rootH := rc.RootHandler
	if rootH == nil {
		rh := NewRootHandler(startTime)
		rootH = rh.ServeHTTP
	}

	// Health & Readiness Handlers
	healthH := rc.HealthHandler
	readyH := rc.ReadyHandler
	if healthH == nil || readyH == nil {
		hh := NewHealthHandler(pool, startTime)
		if healthH == nil {
			healthH = hh.Healthz
		}
		if readyH == nil {
			readyH = hh.Readyz
		}
	}

	// Benchmark Submission Handler
	benchH := rc.BenchHandler
	if benchH == nil {
		ssrfVal := rc.SSRF
		if ssrfVal == nil && rc.Config != nil && rc.Config.AllowLocalhost {
			ssrfVal = NewSSRFValidator(true)
		}
		bh := NewBenchHandler(rc.Config, pool, ssrfVal, rc.RateLimiter)
		benchH = bh.ServeHTTP
	}

	// Status Handler
	statusH := rc.StatusHandler
	if statusH == nil {
		sh := NewStatusHandler(pool, startTime, met)
		statusH = sh.ServeHTTP
	}

	// 1. Register Public Routes
	mux.HandleFunc("GET /{$}", rootH)
	mux.HandleFunc("GET /healthz", healthH)
	mux.HandleFunc("GET /readyz", readyH)

	// 2. Register Authenticated Routes wrapped with AuthMiddleware
	authWrap := AuthMiddleware(rc.Config.BenchSecret)
	mux.Handle("POST /api/bench", authWrap(benchH))
	mux.Handle("GET /api/bench/status", authWrap(statusH))

	// Middlewares chain:
	// Inbound -> SecurityHeaders -> CORS -> RequestLogging -> RateLimit -> RequestValidation -> JSONErrorHandler -> Mux
	timeoutDuration := time.Duration(rc.Config.JobTimeoutSec) * time.Second
	if timeoutDuration <= 0 {
		timeoutDuration = 300 * time.Second
	}

	var handler http.Handler = mux
	handler = jsonErrorHandlerMiddleware(handler)
	handler = RequestValidationMiddleware(timeoutDuration)(handler)
	if rc.RateLimiter != nil {
		handler = RateLimitMiddleware(rc.RateLimiter)(handler)
	}
	handler = RequestLoggingMiddleware(met)(handler)
	handler = CORSMiddleware(rc.Config.AllowedOrigins)(handler)
	handler = SecurityHeadersMiddleware(handler)

	return handler
}

func defaultHealthzHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "alive",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

// jsonErrorHandlerMiddleware intercepts standard library text 404/405 errors and formats them as JSON.
func jsonErrorHandlerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw := &jsonErrorResponseWriter{ResponseWriter: w}
		next.ServeHTTP(rw, r)
	})
}

type jsonErrorResponseWriter struct {
	http.ResponseWriter
	interceptionDone bool
}

func (w *jsonErrorResponseWriter) WriteHeader(statusCode int) {
	ct := w.Header().Get("Content-Type")

	// If it's a 404 or 405 without explicit application/json header (e.g. from Go stdlib mux)
	if (statusCode == http.StatusNotFound || statusCode == http.StatusMethodNotAllowed) && !strings.Contains(ct, "application/json") {
		w.Header().Set("Content-Type", "application/json")
		w.ResponseWriter.WriteHeader(statusCode)

		if statusCode == http.StatusNotFound {
			_, _ = w.ResponseWriter.Write([]byte(`{"error":"not found"}` + "\n"))
		} else {
			_, _ = w.ResponseWriter.Write([]byte(`{"error":"method not allowed"}` + "\n"))
		}
		w.interceptionDone = true
		return
	}

	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *jsonErrorResponseWriter) Write(b []byte) (int, error) {
	if w.interceptionDone {
		// Ignore standard library plain text body
		return len(b), nil
	}
	return w.ResponseWriter.Write(b)
}
