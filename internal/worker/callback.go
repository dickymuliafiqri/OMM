package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"benchmark/internal/metrics"
	"benchmark/pkg/logger"
)

// ProgressPayload represents progress updates during benchmark execution.
type ProgressPayload struct {
	Type             string `json:"type"`                       // "progress"
	Phase            string `json:"phase"`
	Message          string `json:"message"`
	Progress         int    `json:"progress"`                   // 0-100
	PromptTokens     int    `json:"promptTokens,omitempty"`
	CompletionTokens int    `json:"completionTokens,omitempty"`
	TotalTokens      int    `json:"totalTokens,omitempty"`
}

// LogPayload represents a single streaming log line sent to omm-web.
type LogPayload struct {
	Type      string `json:"type"`      // "log"
	Line      string `json:"line"`
	Level     string `json:"level"`     // info, success, error, warn, system
	Timestamp string `json:"timestamp"` // ISO 8601
}

// BenchmarkRun contains summary metrics of a completed benchmark execution.
type BenchmarkRun struct {
	ID               string  `json:"id"`
	JobID            string  `json:"jobId"`
	ProviderBaseURL  string  `json:"providerBaseUrl"`
	ModelName        string  `json:"modelName"`
	TotalScore       int     `json:"totalScore"`
	MaxScore         int     `json:"maxScore"`
	ExecutionTimeMs  int64   `json:"executionTimeMs"`
	PromptTokens     int     `json:"promptTokens"`
	CompletionTokens int     `json:"completionTokens"`
	TotalTokens      int     `json:"totalTokens"`
	EstimatedCostUSD float64 `json:"estimatedCostUsd"`
	CostTier         string  `json:"costTier"`
	Status           string  `json:"status"` // "SUCCESS", "PARTIAL", "FAILED"
	ErrorSummary     string  `json:"errorSummary,omitempty"`
	BenchmarkMode    string  `json:"benchmarkMode"`   // "swe"
	MaxTierAchieved  string  `json:"maxTierAchieved"` // "NONE", "JUNIOR", "MID", "SENIOR", "STAFF"
	CreatedAt        string  `json:"createdAt"`
	IsCheckpoint     bool    `json:"isCheckpoint,omitempty"`
}

// TaskRun represents performance metrics for a single SWE-bench task.
type TaskRun struct {
	ID            string `json:"id,omitempty"`
	RunID         string `json:"runId,omitempty"`
	TaskID        string `json:"taskId"`
	TaskTitle     string `json:"taskTitle"`
	Tier          string `json:"tier"` // "JUNIOR", "MID", "SENIOR", "STAFF"
	PointsAwarded int    `json:"pointsAwarded"`
	MaxPoints     int    `json:"maxPoints"`
	Resolved      bool   `json:"resolved"`
	HasRace       bool   `json:"hasRace"`
	Attempts      int    `json:"attempts"`
	TestOutput    string `json:"testOutput,omitempty"`
	CreatedAt     string `json:"createdAt,omitempty"`
}

// ResultPayload delivers the final benchmark results to omm-web.
type ResultPayload struct {
	Type         string       `json:"type"` // "result"
	BenchmarkRun BenchmarkRun `json:"benchmarkRun"`
	TaskRuns     []TaskRun    `json:"taskRuns"`
}

// ErrorPayload notifies omm-web of fatal errors during benchmark execution.
type ErrorPayload struct {
	Type         string        `json:"type"` // "error"
	Message      string        `json:"message"`
	Phase        string        `json:"phase"`
	BenchmarkRun *BenchmarkRun `json:"benchmarkRun,omitempty"`
	TaskRuns     []TaskRun     `json:"taskRuns,omitempty"`
}

// CallbackClient handles transmitting HTTP callbacks to omm-web.
type CallbackClient struct {
	httpClient  *http.Client
	backoffBase time.Duration
	maxRetries  int
	metrics     *metrics.Metrics
}

// NewCallbackClient creates a CallbackClient instance with strict timeouts.
func NewCallbackClient(timeout time.Duration, backoffBase time.Duration, m ...*metrics.Metrics) *CallbackClient {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if backoffBase <= 0 {
		backoffBase = 1 * time.Second
	}

	met := metrics.Default
	if len(m) > 0 && m[0] != nil {
		met = m[0]
	}

	return &CallbackClient{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		backoffBase: backoffBase,
		maxRetries:  3,
		metrics:     met,
	}
}

func (c *CallbackClient) getMetrics() *metrics.Metrics {
	if c.metrics != nil {
		return c.metrics
	}
	return metrics.Default
}

// DefaultCallbackClient is the standard client with 10s timeout and 1s base backoff.
var DefaultCallbackClient = NewCallbackClient(10*time.Second, 1*time.Second)

// postJSON executes an HTTP POST with JSON body and HMAC secret header.
func (c *CallbackClient) postJSON(ctx context.Context, targetURL, secret string, payload any) error {
	start := time.Now()
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("gagal serialisasi payload callback: %w", err)
	}

	logger.Debug("callback.http_post", "target=%s bytes=%d", targetURL, len(data))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("gagal membuat request callback: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if secret != "" {
		req.Header.Set("X-Bench-Secret", secret)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("gagal mengirim callback ke %s: %w", targetURL, err)
	}
	defer resp.Body.Close()

	logger.Debug("callback.http_resp", "target=%s status=%d latency_ms=%d", targetURL, resp.StatusCode, time.Since(start).Milliseconds())

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("callback mengembalikan HTTP status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// postWithRetry performs a request with exponential backoff retries.
func (c *CallbackClient) postWithRetry(ctx context.Context, targetURL, secret string, payload any) error {
	var lastErr error
	backoff := c.backoffBase

	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		// Respect context cancellation
		if err := ctx.Err(); err != nil {
			return err
		}

		logger.Debug("callback.retry_attempt", "attempt=%d/%d target=%s backoff=%s", attempt, c.maxRetries, targetURL, backoff)

		err := c.postJSON(ctx, targetURL, secret, payload)
		if err == nil {
			return nil
		}

		lastErr = err
		logger.Warn("CALLBACK", "Gagal mengirim callback (percobaan %d/%d): %v", attempt, c.maxRetries, err)

		if attempt < c.maxRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
			}
		}
	}

	return fmt.Errorf("callback gagal setelah %d percobaan: %w", c.maxRetries, lastErr)
}

// SendProgress sends a non-blocking progress update callback to omm-web.
func (c *CallbackClient) SendProgress(ctx context.Context, callbackURL, secret string, payload ProgressPayload) error {
	if payload.Type == "" {
		payload.Type = "progress"
	}

	logger.Debug("callback.send_progress", "url=%s phase=%s progress=%d", callbackURL, payload.Phase, payload.Progress)

	// Progress updates are best-effort (do not block or retry endlessly)
	err := c.postJSON(ctx, callbackURL, secret, payload)
	if err != nil {
		c.getMetrics().CallbackErrors.Add(1)
		logger.Warn("CALLBACK", "Gagal mengirim progress callback ke %s: %v", callbackURL, err)
	}
	return err
}

// SendLog sends a log line callback to omm-web.
func (c *CallbackClient) SendLog(ctx context.Context, callbackURL, secret string, payload LogPayload) error {
	if payload.Type == "" {
		payload.Type = "log"
	}
	if payload.Timestamp == "" {
		payload.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	logger.Debug("callback.send_log", "url=%s level=%s bytes=%d", callbackURL, payload.Level, len(payload.Line))

	// Log lines are best-effort
	err := c.postJSON(ctx, callbackURL, secret, payload)
	if err != nil {
		c.getMetrics().CallbackErrors.Add(1)
		logger.Warn("CALLBACK", "Gagal mengirim log callback ke %s: %v", callbackURL, err)
	}
	return err
}

// SendResult delivers the benchmark execution outcome with exponential backoff retries.
func (c *CallbackClient) SendResult(ctx context.Context, callbackURL, secret string, payload ResultPayload) error {
	if payload.Type == "" {
		payload.Type = "result"
	}

	logger.Debug("callback.send_result", "url=%s run_id=%s score=%d/%d status=%s", callbackURL, payload.BenchmarkRun.ID, payload.BenchmarkRun.TotalScore, payload.BenchmarkRun.MaxScore, payload.BenchmarkRun.Status)

	// If ctx has been cancelled or timed out (e.g. job timeout),
	// use a detached context with a dedicated timeout so that the result/partial
	// callback can still be delivered reliably to omm-web.
	sendCtx := ctx
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		sendCtx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
	}

	err := c.postWithRetry(sendCtx, callbackURL, secret, payload)
	if err != nil {
		c.getMetrics().CallbackErrors.Add(1)
		logger.Warn("CALLBACK", "Peringatan: Gagal mengirimkan final result callback ke %s: %v", callbackURL, err)
	}
	return err
}

// SendError delivers fatal error notifications to omm-web with exponential backoff retries.
func (c *CallbackClient) SendError(ctx context.Context, callbackURL, secret string, payload ErrorPayload) error {
	if payload.Type == "" {
		payload.Type = "error"
	}

	logger.Debug("callback.send_error", "url=%s phase=%s msg=%s", callbackURL, payload.Phase, payload.Message)

	// If ctx has been cancelled or timed out (e.g. job timeout),
	// use a detached context with a dedicated timeout so that the error callback
	// can still be delivered reliably to omm-web.
	sendCtx := ctx
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		sendCtx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
	}

	err := c.postWithRetry(sendCtx, callbackURL, secret, payload)
	if err != nil {
		c.getMetrics().CallbackErrors.Add(1)
		logger.Warn("CALLBACK", "Peringatan: Gagal mengirimkan error callback ke %s: %v", callbackURL, err)
	}
	return err
}
