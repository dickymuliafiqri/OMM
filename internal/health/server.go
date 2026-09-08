package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"benchmark/internal/queue"
	"benchmark/internal/storage"
	"benchmark/pkg/logger"
)

// Server manages HTTP observability and health check endpoints
type Server struct {
	httpServer *http.Server
	repo       storage.Repository
	pool       *queue.WorkerPool
	startTime  time.Time
}

// HealthResponse is the JSON format for the /healthz endpoint
type HealthResponse struct {
	Status     string            `json:"status"`
	Uptime     string            `json:"uptime"`
	Timestamp  string            `json:"timestamp"`
	Database   string            `json:"database"`
	WorkerPool WorkerPoolMetrics `json:"worker_pool"`
}

// WorkerPoolMetrics summarizes process queue metrics
type WorkerPoolMetrics struct {
	ActiveJobs  int `json:"active_jobs"`
	QueueLength int `json:"queue_length"`
}

// RootResponse is the JSON format for the / endpoint
type RootResponse struct {
	Service     string            `json:"service"`
	Description string            `json:"description"`
	Status      string            `json:"status"`
	Uptime      string            `json:"uptime"`
	Timestamp   string            `json:"timestamp"`
	Endpoints   map[string]string `json:"endpoints"`
}

// NewServer creates a new HTTP health server instance
func NewServer(port int, repo storage.Repository, pool *queue.WorkerPool) *Server {
	if port <= 0 {
		port = 8080
	}

	s := &Server{
		repo:      repo,
		pool:      pool,
		startTime: time.Now(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRoot)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/stats", s.handleStats)

	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	return s
}

// Start runs the HTTP server in the background
func (s *Server) Start() error {
	logger.Sys("HEALTH", "HTTP server aktif di %s (/, /healthz, /readyz, /stats)", s.httpServer.Addr)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("health check server error: %w", err)
	}
	return nil
}

// Stop gracefully shuts down the HTTP server
func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// handleHealthz handles the liveness / health check endpoint
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	dbStatus := "connected"
	isHealthy := true

	if s.repo != nil {
		if err := s.repo.Ping(ctx); err != nil {
			dbStatus = fmt.Sprintf("unhealthy: %v", err)
			isHealthy = false
		}
	}

	activeJobs := 0
	queueLen := 0
	if s.pool != nil {
		activeJobs = s.pool.ActiveJobs()
		queueLen = s.pool.QueueLength()
	}

	statusStr := "healthy"
	httpCode := http.StatusOK
	if !isHealthy {
		statusStr = "degraded"
		httpCode = http.StatusServiceUnavailable
	}

	resp := HealthResponse{
		Status:    statusStr,
		Uptime:    time.Since(s.startTime).Round(time.Second).String(),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Database:  dbStatus,
		WorkerPool: WorkerPoolMetrics{
			ActiveJobs:  activeJobs,
			QueueLength: queueLen,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpCode)
	_ = json.NewEncoder(w).Encode(resp)
}

// handleReadyz handles the readiness probe endpoint
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if s.repo != nil {
		if err := s.repo.Ping(ctx); err != nil {
			http.Error(w, fmt.Sprintf("Database not ready: %v", err), http.StatusServiceUnavailable)
			return
		}
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

// handleStats serves system summary statistics in JSON format
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	var stats *storage.SystemStats
	var err error
	if s.repo != nil {
		stats, err = s.repo.GetSystemStats(ctx)
		if err != nil {
			http.Error(w, fmt.Sprintf("Gagal query system stats: %v", err), http.StatusInternalServerError)
			return
		}
	}

	payload := map[string]interface{}{
		"system_stats": stats,
		"uptime":       time.Since(s.startTime).Round(time.Second).String(),
		"timestamp":    time.Now().UTC().Format(time.RFC3339),
	}

	if s.pool != nil {
		payload["worker_pool"] = map[string]interface{}{
			"active_jobs":  s.pool.ActiveJobs(),
			"queue_length": s.pool.QueueLength(),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

// handleRoot serves the overview and index metadata at path /
func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	resp := RootResponse{
		Service:     "OMM (On My Mark)",
		Description: "AI Go SWE-bench Brownfield Benchmark Platform",
		Status:      "running",
		Uptime:      time.Since(s.startTime).Round(time.Second).String(),
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		Endpoints: map[string]string{
			"/":        "Service overview and discovery",
			"/healthz": "Liveness and health check probe",
			"/readyz":  "Readiness check probe",
			"/stats":   "System statistics and benchmark queue metrics",
		},
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
