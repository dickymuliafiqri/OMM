package storage

import (
	"time"
)

// User represents a Telegram bot user entity
type User struct {
	TelegramID   int64     `json:"telegram_id"`
	Username     string    `json:"username"`
	FirstName    string    `json:"first_name"`
	Role         string    `json:"role"` // "user", "admin", "vip"
	DailyQuota   int       `json:"daily_quota"`
	SavedBaseURL string    `json:"saved_base_url,omitempty"`
	SavedAPIKey  string    `json:"saved_api_key,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// BenchmarkRun represents a single benchmark execution by an AI model
type BenchmarkRun struct {
	ID               string    `json:"id"` // UUID v4
	TelegramID       int64     `json:"telegram_id"`
	ProviderBaseURL  string    `json:"provider_base_url"`
	ModelName        string    `json:"model_name"`
	TotalScore       int       `json:"total_score"`
	MaxScore         int       `json:"max_score"`
	ExecutionTimeMs  int64     `json:"execution_time_ms"`
	PromptTokens     int       `json:"prompt_tokens"`
	CompletionTokens int       `json:"completion_tokens"`
	TotalTokens      int       `json:"total_tokens"`
	EstimatedCostUSD float64   `json:"estimated_cost_usd"`
	CostTier         string    `json:"cost_tier"`
	CodeHash         string    `json:"code_hash"`
	CodeSnippet      string    `json:"code_snippet"`
	Status           string    `json:"status"` // 'SUCCESS', 'FAILED_COMPILE', 'SECURITY_VIOLATION'
	ErrorSummary     string    `json:"error_summary,omitempty"`
	BenchmarkMode    string    `json:"benchmark_mode"`     // 'swe' or 'legacy'
	MaxTierAchieved  string    `json:"max_tier_achieved"`  // 'JUNIOR', 'MID', 'SENIOR', 'STAFF', 'NONE'
	CreatedAt        time.Time `json:"created_at"`
}

// SWETaskRun represents the execution details of a single task in the SWE-bench ladder
type SWETaskRun struct {
	ID            string    `json:"id"`
	RunID         string    `json:"run_id"`
	TaskID        string    `json:"task_id"`
	TaskTitle     string    `json:"task_title"`
	Tier          string    `json:"tier"`
	PointsAwarded int       `json:"points_awarded"`
	MaxPoints     int       `json:"max_points"`
	Resolved      bool      `json:"resolved"`
	HasRace       bool      `json:"has_race"`
	Attempts      int       `json:"attempts"`
	TestOutput    string    `json:"test_output,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

// BenchmarkDetail represents score details for each test category
type BenchmarkDetail struct {
	ID           int64    `json:"id"`
	RunID        string   `json:"run_id"`
	CategoryName string   `json:"category_name"`
	Score        int      `json:"score"`
	MaxScore     int      `json:"max_score"`
	Details      []string `json:"details"`
}

// LeaderboardEntry represents model aggregation metrics for the leaderboard
type LeaderboardEntry struct {
	Rank        int     `json:"rank"`
	ModelName   string  `json:"model_name"`
	TotalRuns   int     `json:"total_runs"`
	AvgScore    float64 `json:"avg_score"`
	PeakScore   int     `json:"peak_score"`
	LowestScore int     `json:"lowest_score"`
	PassRate    float64 `json:"pass_rate"` // Percentage of scores >= 70 (standard passing grade)
	AvgCostUSD  float64 `json:"avg_cost_usd"`
	CostTier    string  `json:"cost_tier"`
	ValueScore  float64 `json:"value_score"`
	TotalTokens int     `json:"total_tokens"` // Estimated total tokens required to complete the benchmark
}

// CategoryLeaderboardEntry represents a specific ranking per test category
type CategoryLeaderboardEntry struct {
	Rank         int     `json:"rank"`
	ModelName    string  `json:"model_name"`
	CategoryName string  `json:"category_name"`
	PeakScore    int     `json:"peak_score"` // Highest score achieved in this category
	AvgScore     float64 `json:"avg_score"`
	MaxScore     int     `json:"max_score"` // Maximum category point value (e.g., 20)
	TotalRuns    int     `json:"total_runs"`
}

// SystemStats summarizes bot operational metrics for monitoring and admin panel purposes
type SystemStats struct {
	TotalRuns        int     `json:"total_runs"`
	SuccessRuns      int     `json:"success_runs"`
	FailedRuns       int     `json:"failed_runs"`
	ViolationRuns    int     `json:"violation_runs"`
	ErrorRatePercent float64 `json:"error_rate_percent"`
	TotalUsers       int     `json:"total_users"`
	BannedUsers      int     `json:"banned_users"`
	UniqueModels     int     `json:"unique_models"`
	TotalCostUSD     float64 `json:"total_cost_usd"`
}

// CachedModel represents model metadata and token pricing cached from OpenRouter
type CachedModel struct {
	ID                  string    `json:"id"`
	Name                string    `json:"name"`
	PromptPricePerM     float64   `json:"prompt_price_per_m"`
	CompletionPricePerM float64   `json:"completion_price_per_m"`
	ContextLength       int       `json:"context_length"`
	UpdatedAt           time.Time `json:"updated_at"`
}

