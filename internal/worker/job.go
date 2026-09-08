package worker

import (
	"errors"
	"time"
)

// Sentinel errors for worker pool operations.
var (
	ErrPoolFull   = errors.New("server busy: worker pool full")
	ErrPoolClosed = errors.New("worker pool is closed")
)

// BenchJob represents a benchmark execution job requested by omm-web.
type BenchJob struct {
	JobID          string    `json:"jobId"`
	CallbackURL    string    `json:"callbackUrl"`
	CallbackSecret string    `json:"callbackSecret"`
	BaseURL        string    `json:"baseUrl"`
	APIKey         string    `json:"apiKey"`
	Model          string    `json:"model"`
	CreatedAt      time.Time `json:"createdAt"`
}

// ZeroAPIKey securely zeroes out the API key memory in place.
func (j *BenchJob) ZeroAPIKey() {
	if j == nil || j.APIKey == "" {
		return
	}
	b := []byte(j.APIKey)
	for i := range b {
		b[i] = 0
	}
	j.APIKey = ""
}

// PoolStats provides metrics on worker pool concurrency and throughput.
type PoolStats struct {
	ActiveJobs     int    `json:"activeJobs"`
	MaxConcurrent  int    `json:"maxConcurrent"`
	AvailableSlots int    `json:"availableSlots"`
	TotalProcessed int64  `json:"totalProcessed"`
	TotalFailed    int64  `json:"totalFailed"`
	Uptime         string `json:"uptime,omitempty"`
}
