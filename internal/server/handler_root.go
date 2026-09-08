package server

import (
	"net/http"
	"time"

	"benchmark/pkg/logger"
)

// RootHandler manages requests to the root path ("/").
type RootHandler struct {
	startTime time.Time
}

// NewRootHandler creates a new RootHandler instance.
func NewRootHandler(startTime time.Time) *RootHandler {
	return &RootHandler{
		startTime: startTime,
	}
}

// ServeHTTP handles requests to the root endpoint ("/") returning server identification and discovery routes.
func (h *RootHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "not found",
		})
		return
	}

	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
			"error": "method not allowed",
		})
		return
	}

	uptime := time.Since(h.startTime).Round(time.Second).String()
	logger.Debug("root.discovery", "client=%s uptime=%s", GetClientIP(r), uptime)

	writeJSON(w, http.StatusOK, map[string]any{
		"name":        "OMM Benchmark Server",
		"service":     "omm-bench",
		"version":     "1.0.0",
		"status":      "running",
		"uptime":      uptime,
		"description": "Stateless Native Go SWE-bench Concurrency Ladder API Server",
		"endpoints": map[string]string{
			"root":   "GET /",
			"health": "GET /healthz",
			"ready":  "GET /readyz",
			"bench":  "POST /api/bench",
			"status": "GET /api/bench/status",
		},
	})
}
