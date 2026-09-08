package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"benchmark/internal/config"
	"benchmark/pkg/logger"
)

var (
	uuidRegex  = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	modelRegex = regexp.MustCompile(`^[a-zA-Z0-9/_.:@-]+$`)
)

// BenchRequest represents the inbound payload from omm-web to initiate a benchmark.
type BenchRequest struct {
	JobID          string `json:"jobId"`
	CallbackURL    string `json:"callbackUrl"`
	CallbackSecret string `json:"callbackSecret"`
	BaseURL        string `json:"baseUrl"`
	APIKey         string `json:"apiKey"`
	Model          string `json:"model"`
}

// BenchHandler handles POST /api/bench requests.
type BenchHandler struct {
	cfg         *config.Config
	pool        JobSubmitter
	ssrf        *SSRFValidator
	origins     map[string]struct{}
	rateLimiter *RateLimiter
}

// NewBenchHandler creates a new handler for starting benchmark runs.
func NewBenchHandler(cfg *config.Config, pool JobSubmitter, ssrf *SSRFValidator, rl ...*RateLimiter) *BenchHandler {
	if ssrf == nil {
		if cfg != nil && cfg.AllowLocalhost {
			ssrf = NewSSRFValidator(true)
		} else {
			ssrf = DefaultSSRFValidator
		}
	}

	originsMap := make(map[string]struct{})
	if cfg != nil {
		for _, origin := range cfg.AllowedOrigins {
			cleaned := strings.TrimRight(strings.TrimSpace(origin), "/")
			if cleaned != "" {
				originsMap[cleaned] = struct{}{}
			}
		}
	}

	var rateLimiter *RateLimiter
	if len(rl) > 0 {
		rateLimiter = rl[0]
	}

	return &BenchHandler{
		cfg:         cfg,
		pool:        pool,
		ssrf:        ssrf,
		origins:     originsMap,
		rateLimiter: rateLimiter,
	}
}

func (h *BenchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
			"error": "method not allowed",
		})
		return
	}

	var req BenchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "invalid request",
			"details": "gagal mem-parsing payload JSON: format tidak valid",
		})
		return
	}

	logger.Debug("bench.decode", "jobId=%s model=%s baseUrl=%s callbackUrl=%s", req.JobID, req.Model, req.BaseURL, req.CallbackURL)

	// 1. Validate Job ID (UUID format)
	req.JobID = strings.TrimSpace(req.JobID)
	if req.JobID == "" || !uuidRegex.MatchString(req.JobID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "invalid request",
			"details": "jobId wajib diisi dan harus berupa UUID v4 yang valid",
		})
		return
	}

	// 2. Validate Callback Secret
	req.CallbackSecret = strings.TrimSpace(req.CallbackSecret)
	if len(req.CallbackSecret) < 16 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "invalid request",
			"details": "callbackSecret wajib diisi (minimal 16 karakter)",
		})
		return
	}

	// 3. Validate API Key (never logged)
	req.APIKey = strings.TrimSpace(req.APIKey)
	if req.APIKey == "" || len(req.APIKey) > 512 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "invalid request",
			"details": "apiKey wajib diisi dan maksimal 512 karakter",
		})
		return
	}

	// 4. Validate Model identifier
	req.Model = strings.TrimSpace(req.Model)
	if req.Model == "" || len(req.Model) > 128 || !modelRegex.MatchString(req.Model) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "invalid request",
			"details": "model wajib diisi (alfanumerik, /, -, _, ., @, :, maks 128 karakter)",
		})
		return
	}

	logger.Debug("bench.fields_ok", "jobId=%s model=%s secretLen=%d apiKeyLen=%d", req.JobID, req.Model, len(req.CallbackSecret), len(req.APIKey))

	// 5. Validate Callback URL & SSRF
	req.CallbackURL = strings.TrimSpace(req.CallbackURL)
	if err := h.ssrf.ValidateURL(req.CallbackURL); err != nil {
		logger.Warn("ssrf.blocked", "url=%s reason=%v", req.CallbackURL, err)
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{
			"error":   "invalid url",
			"field":   "callbackUrl",
			"details": err.Error(),
		})
		return
	}

	logger.Debug("bench.callback_safe", "jobId=%s callbackUrl=%s", req.JobID, req.CallbackURL)

	cbURL, err := url.Parse(req.CallbackURL)
	if err == nil {
		// Optional check against allowed origins whitelist if origins are configured
		if len(h.origins) > 0 {
			cbOrigin := fmt.Sprintf("%s://%s", cbURL.Scheme, cbURL.Host)
			_, isWhitelisted := h.origins[cbOrigin]
			isLocalhostAllowed := (h.cfg == nil || h.cfg.AllowLocalhost) && isLocalhostOrigin(cbOrigin)
			if !isWhitelisted && !isLocalhostAllowed {
				logger.Warn("bench.origin_rejected", "jobId=%s origin=%s allowed_origins=%v allow_localhost=%v",
					req.JobID, cbOrigin, h.cfg.AllowedOrigins, h.cfg.AllowLocalhost)
				writeJSON(w, http.StatusUnprocessableEntity, map[string]string{
					"error":   "invalid url",
					"field":   "callbackUrl",
					"details": "origin callbackUrl tidak termasuk dalam daftar domain yang diizinkan",
				})
				return
			}
			logger.Debug("bench.origin_allowed", "jobId=%s origin=%s isWhitelisted=%t isLocalhost=%t", req.JobID, cbOrigin, isWhitelisted, isLocalhostAllowed)
		} else {
			logger.Debug("bench.origin_check_skipped", "jobId=%s callbackUrl=%s", req.JobID, req.CallbackURL)
		}

		// Per-source rate limiting on callback domain
		if h.rateLimiter != nil {
			domain := cbURL.Hostname()
			allowed, remaining, retryAfter, _ := h.rateLimiter.CheckSource("domain:" + domain)
			if !allowed {
				logger.Warn("ratelimit.exceeded", "ip=%s source=%s remaining=%d", GetClientIP(r), domain, remaining)
				w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfter))
				writeJSON(w, http.StatusTooManyRequests, map[string]any{
					"error":      "rate limit exceeded",
					"source":     domain,
					"retryAfter": retryAfter,
				})
				return
			}
		}
	}

	// 6. Validate AI Base URL & SSRF
	req.BaseURL = strings.TrimSpace(req.BaseURL)
	if err := h.ssrf.ValidateURL(req.BaseURL); err != nil {
		logger.Warn("ssrf.blocked", "url=%s reason=%v", req.BaseURL, err)
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{
			"error":   "invalid url",
			"field":   "baseUrl",
			"details": err.Error(),
		})
		return
	}

	logger.Debug("bench.baseurl_safe", "jobId=%s baseUrl=%s", req.JobID, req.BaseURL)

	// 7. Check worker pool capacity & submit job
	stats := h.pool.Stats()
	logger.Debug("bench.pool_stats", "jobId=%s activeJobs=%d availableSlots=%d totalProcessed=%d", req.JobID, stats.ActiveJobs, stats.AvailableSlots, stats.TotalProcessed)
	if stats.AvailableSlots <= 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":         "server busy",
			"queueCapacity": 0,
		})
		return
	}

	job := &BenchJob{
		JobID:          req.JobID,
		CallbackURL:    req.CallbackURL,
		CallbackSecret: req.CallbackSecret,
		BaseURL:        req.BaseURL,
		APIKey:         req.APIKey,
		Model:          req.Model,
		CreatedAt:      time.Now().UTC(),
	}

	if err := h.pool.Submit(r.Context(), job); err != nil {
		if errors.Is(err, ErrPoolFull) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"error":         "server busy",
				"queueCapacity": 0,
			})
			return
		}

		logger.Error("BENCH", "Gagal submit job %s: %v", req.JobID, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "internal server error: gagal mendaftarkan pekerjaan benchmark",
		})
		return
	}

	reqID := GetRequestID(r.Context())
	logger.Info("bench.accept", "jobId=%s model=%s requestId=%s", req.JobID, req.Model, reqID)

	// 8. Return 202 Accepted
	writeJSON(w, http.StatusAccepted, map[string]string{
		"jobId":   req.JobID,
		"status":  "ACCEPTED",
		"message": "Benchmark job queued for execution",
	})
}
