package queue

import (
	"context"
	"time"

	"benchmark/internal/storage"
)

// JobStatus represents the lifecycle status of a job
type JobStatus string

const (
	StatusQueued    JobStatus = "QUEUED"
	StatusRunning   JobStatus = "RUNNING"
	StatusCompleted JobStatus = "COMPLETED"
	StatusFailed    JobStatus = "FAILED"
)

// ProgressInfo holds detailed real-time progress of a benchmark task
type ProgressInfo struct {
	Step        int           // Current step/task index (1-based, e.g. 1..4)
	Total       int           // Total steps (e.g. 4)
	TaskTitle   string        // Title of the task
	Tier        string        // Tier name, e.g. "Junior", "Mid-Level", "Senior", "Staff"
	TierNumber  int           // Tier number (1..4)
	Attempt     int           // Current attempt/turn (1..2)
	MaxAttempts int           // 2
	StatusText  string        // Milestone status message
	Phase       string        // "PREPARING", "REASONING", "CODING", "EVALUATING", "DONE"
	Snippet     string        // Tail of reasoning or code
	Elapsed     time.Duration // Time elapsed in current stage
	Timeout     time.Duration // Stage timeout duration
}

// ProgressCallback is a function that reports job progress status in real-time
type ProgressCallback func(info ProgressInfo)

// BenchmarkJob defines a benchmark job enqueued for processing
type BenchmarkJob struct {
	ID         string           // Unique job UUID
	TelegramID int64            // Telegram user ID
	ChatID     int64            // Telegram Chat ID for progress messages
	MessageID  int              // Telegram message ID to be updated (edited)
	BaseURL    string           // AI endpoint URL
	APIKey     string           // AI API key (stored temporarily in memory)
	Model      string           // Name of the AI model being benchmarked
	CreatedAt  time.Time        // Job creation timestamp
	Ctx        context.Context  // Execution context
	Cancel     context.CancelFunc
	OnProgress ProgressCallback // Progress callback
	ResultChan chan *JobResult  // Channel receiving the final result
}

// JobResult stores the final result of a job execution
type JobResult struct {
	JobID    string
	Run      *storage.BenchmarkRun
	SWETasks []storage.SWETaskRun
	Duration time.Duration
	Error    error
}
