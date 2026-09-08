package server

import (
	"net/http"
	"time"

	"benchmark/pkg/logger"
)

// HealthHandler manages liveness (/healthz) and readiness (/readyz) probes.
type HealthHandler struct {
	pool      JobSubmitter
	startTime time.Time
}

// NewHealthHandler creates a HealthHandler instance.
func NewHealthHandler(pool JobSubmitter, startTime time.Time) *HealthHandler {
	return &HealthHandler{
		pool:      pool,
		startTime: startTime,
	}
}

// Healthz handles GET /healthz (liveness probe).
func (h *HealthHandler) Healthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
			"error": "method not allowed",
		})
		return
	}

	uptime := time.Since(h.startTime).Round(time.Second).String()
	logger.Debug("health.liveness", "status=alive uptime=%s", uptime)

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "alive",
		"uptime": uptime,
	})
}

// Readyz handles GET /readyz (readiness probe based on worker pool capacity).
func (h *HealthHandler) Readyz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
			"error": "method not allowed",
		})
		return
	}

	stats := h.pool.Stats()

	status := "ready"
	httpCode := http.StatusOK

	if stats.AvailableSlots <= 0 {
		status = "busy"
		httpCode = http.StatusServiceUnavailable
	}

	logger.Debug("health.readiness", "status=%s active=%d available=%d max=%d", status, stats.ActiveJobs, stats.AvailableSlots, stats.MaxConcurrent)

	writeJSON(w, httpCode, map[string]any{
		"status": status,
		"workers": map[string]int{
			"active":    stats.ActiveJobs,
			"max":       stats.MaxConcurrent,
			"available": stats.AvailableSlots,
		},
	})
}
