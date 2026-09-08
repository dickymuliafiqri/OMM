package server

import (
	"net/http"
	"time"

	"benchmark/internal/metrics"
	"benchmark/pkg/logger"
)

// StatusHandler manages GET /api/bench/status requests.
type StatusHandler struct {
	pool      JobSubmitter
	startTime time.Time
	metrics   *metrics.Metrics
}

// NewStatusHandler creates a StatusHandler instance with optional metrics registry.
func NewStatusHandler(pool JobSubmitter, startTime time.Time, m ...*metrics.Metrics) *StatusHandler {
	met := metrics.Default
	if len(m) > 0 && m[0] != nil {
		met = m[0]
	}

	return &StatusHandler{
		pool:      pool,
		startTime: startTime,
		metrics:   met,
	}
}

func (h *StatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
			"error": "method not allowed",
		})
		return
	}

	stats := h.pool.Stats()
	uptime := time.Since(h.startTime).Round(time.Second).String()
	snap := h.metrics.Snapshot()

	logger.Debug("status.query", "activeJobs=%d totalProcessed=%d totalFailed=%d uptime=%s", stats.ActiveJobs, stats.TotalProcessed, stats.TotalFailed, uptime)

	writeJSON(w, http.StatusOK, map[string]any{
		"activeJobs":       stats.ActiveJobs,
		"maxConcurrent":    stats.MaxConcurrent,
		"availableSlots":   stats.AvailableSlots,
		"totalProcessed":   stats.TotalProcessed,
		"totalFailed":      stats.TotalFailed,
		"uptime":           uptime,
		"requestsTotal":    snap.RequestsTotal,
		"requestsAccepted": snap.RequestsAccepted,
		"requestsRejected": snap.RequestsRejected,
		"jobsCompleted":    snap.JobsCompleted,
		"jobsFailed":       snap.JobsFailed,
		"jobsTimeout":      snap.JobsTimeout,
		"callbackErrors":   snap.CallbackErrors,
		"metrics":          snap,
	})
}
