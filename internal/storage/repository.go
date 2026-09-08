package storage

import (
	"context"
	"time"
)

// Repository defines the data persistence interface for the application
type Repository interface {
	// Migrate executes DDL table schema migrations
	Migrate(ctx context.Context) error

	// UpsertUser registers or updates Telegram user info
	UpsertUser(ctx context.Context, u *User) error

	// GetUser retrieves a user profile by Telegram ID
	GetUser(ctx context.Context, telegramID int64) (*User, error)

	// SaveUserCredentials stores base_url and api_key in the database for recurring tests
	SaveUserCredentials(ctx context.Context, telegramID int64, baseURL, apiKey string) error

	// ResetUserCredentials deletes a user's saved base_url and api_key
	ResetUserCredentials(ctx context.Context, telegramID int64) error

	// SaveSWERun saves SWE-bench ladder execution results along with task details in a single transaction
	SaveSWERun(ctx context.Context, run *BenchmarkRun, taskRuns []SWETaskRun) error

	// GetSWETaskRuns retrieves the list of SWE task completions for a given run ID
	GetSWETaskRuns(ctx context.Context, runID string) ([]SWETaskRun, error)

	// GetLeaderboard returns the list of top models based on score aggregation
	GetLeaderboard(ctx context.Context, limit, offset int) ([]LeaderboardEntry, error)

	// GetValueLeaderboard returns the list of top models ordered by score-to-cost ratio and cost efficiency
	GetValueLeaderboard(ctx context.Context, limit, offset int) ([]LeaderboardEntry, error)

	// GetCategoryLeaderboard returns top model rankings specific to a given category
	GetCategoryLeaderboard(ctx context.Context, categoryPattern string, limit, offset int) ([]CategoryLeaderboardEntry, error)

	// GetUserHistory retrieves benchmark history run by a specific user
	GetUserHistory(ctx context.Context, telegramID int64, limit, offset int) ([]BenchmarkRun, error)

	// GetRunDetails retrieves the full details of a single benchmark run
	GetRunDetails(ctx context.Context, runID string) (*BenchmarkRun, []BenchmarkDetail, error)

	// CheckAndUpdateRateLimit checks the hourly usage quota and updates it
	CheckAndUpdateRateLimit(ctx context.Context, telegramID int64, hourlyLimit int) (allowed bool, remaining int, retryAfter time.Duration, err error)

	// BanUser marks a user's status as banned
	BanUser(ctx context.Context, telegramID int64) error

	// UnbanUser restores a user's status to a normal user
	UnbanUser(ctx context.Context, telegramID int64) error

	// IsUserBanned checks if a user is banned from the system
	IsUserBanned(ctx context.Context, telegramID int64) (bool, error)

	// ResetUserRateLimit resets a user's usage rate limit
	ResetUserRateLimit(ctx context.Context, telegramID int64) error

	// GetSystemStats summarizes system metrics for the administration panel
	GetSystemStats(ctx context.Context) (*SystemStats, error)

	// SaveCachedModels saves or updates the collection of models synced from OpenRouter to the database
	SaveCachedModels(ctx context.Context, models []CachedModel) error

	// GetAllCachedModels retrieves all models stored in the database cache
	GetAllCachedModels(ctx context.Context) ([]CachedModel, error)

	// GetCachedModel looks up a specific model from the database by ID
	GetCachedModel(ctx context.Context, id string) (*CachedModel, error)

	// Ping checks if the database connection is healthy
	Ping(ctx context.Context) error

	// Close closes the database connection
	Close() error
}

