package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"benchmark/pkg/logger"
)

var (
	ErrUserNotFound = errors.New("pengguna tidak ditemukan")
	ErrRunNotFound  = errors.New("data benchmark tidak ditemukan")
)

// SQLiteRepository is an implementation of Repository using SQL (Turso LibSQL / SQLite)
type SQLiteRepository struct {
	db *sql.DB
}

// NewRepository creates a new SQLiteRepository instance
func NewRepository(db *sql.DB) *SQLiteRepository {
	return &SQLiteRepository{db: db}
}

// Migrate creates tables and indexes required by the system
func (r *SQLiteRepository) Migrate(ctx context.Context) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS users (
			telegram_id INTEGER PRIMARY KEY,
			username TEXT,
			first_name TEXT,
			role TEXT DEFAULT 'user',
			daily_quota INTEGER DEFAULT 10,
			saved_base_url TEXT,
			saved_api_key TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS benchmark_runs (
			id TEXT PRIMARY KEY,
			telegram_id INTEGER NOT NULL,
			provider_base_url TEXT NOT NULL,
			model_name TEXT NOT NULL,
			total_score INTEGER NOT NULL,
			max_score INTEGER DEFAULT 100,
			execution_time_ms INTEGER NOT NULL,
			prompt_tokens INTEGER DEFAULT 0,
			completion_tokens INTEGER DEFAULT 0,
			total_tokens INTEGER DEFAULT 0,
			estimated_cost_usd REAL DEFAULT 0,
			cost_tier TEXT DEFAULT '',
			code_hash TEXT,
			code_snippet TEXT,
			status TEXT NOT NULL,
			error_summary TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY(telegram_id) REFERENCES users(telegram_id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS benchmark_details (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id TEXT NOT NULL,
			category_name TEXT NOT NULL,
			score INTEGER NOT NULL,
			max_score INTEGER NOT NULL,
			details_json TEXT,
			FOREIGN KEY(run_id) REFERENCES benchmark_runs(id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS user_rate_limits (
			telegram_id INTEGER PRIMARY KEY,
			run_count INTEGER DEFAULT 0,
			window_start DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE INDEX IF NOT EXISTS idx_runs_model_score ON benchmark_runs(model_name, total_score DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_runs_user ON benchmark_runs(telegram_id, created_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_details_run_id ON benchmark_details(run_id);`,
		`CREATE TABLE IF NOT EXISTS swe_task_runs (
			id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL,
			task_id TEXT NOT NULL,
			task_title TEXT NOT NULL,
			tier TEXT NOT NULL,
			points_awarded INTEGER NOT NULL,
			max_points INTEGER NOT NULL,
			resolved BOOLEAN NOT NULL DEFAULT FALSE,
			has_race BOOLEAN NOT NULL DEFAULT FALSE,
			attempts INTEGER NOT NULL DEFAULT 1,
			test_output TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY(run_id) REFERENCES benchmark_runs(id) ON DELETE CASCADE
		);`,
		`CREATE INDEX IF NOT EXISTS idx_swe_task_runs_run_id ON swe_task_runs(run_id);`,
		`CREATE TABLE IF NOT EXISTS cached_models (
			id TEXT PRIMARY KEY,
			name TEXT,
			prompt_price_per_m REAL NOT NULL,
			completion_price_per_m REAL NOT NULL,
			context_length INTEGER DEFAULT 0,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE INDEX IF NOT EXISTS idx_cached_models_name ON cached_models(name);`,
		`CREATE INDEX IF NOT EXISTS idx_cached_models_updated ON cached_models(updated_at DESC);`,
	}

	for _, query := range queries {
		if _, err := r.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("gagal eksekusi migrasi DDL: %w\nQuery: %s", err, query)
		}
	}

	// Column addition migrations if legacy tables already exist
	alterQueries := []string{
		`ALTER TABLE users ADD COLUMN saved_base_url TEXT;`,
		`ALTER TABLE users ADD COLUMN saved_api_key TEXT;`,
		`ALTER TABLE benchmark_runs ADD COLUMN prompt_tokens INTEGER DEFAULT 0;`,
		`ALTER TABLE benchmark_runs ADD COLUMN completion_tokens INTEGER DEFAULT 0;`,
		`ALTER TABLE benchmark_runs ADD COLUMN total_tokens INTEGER DEFAULT 0;`,
		`ALTER TABLE benchmark_runs ADD COLUMN estimated_cost_usd REAL DEFAULT 0;`,
		`ALTER TABLE benchmark_runs ADD COLUMN cost_tier TEXT DEFAULT '';`,
		`ALTER TABLE benchmark_runs ADD COLUMN benchmark_mode TEXT DEFAULT 'swe';`,
		`ALTER TABLE benchmark_runs ADD COLUMN max_tier_achieved TEXT DEFAULT 'NONE';`,
	}
	for _, alter := range alterQueries {
		_, _ = r.db.ExecContext(ctx, alter)
	}

	logger.DB("MIGRATE", "Schema tables and indexes verified/migrated")
	return nil
}

// UpsertUser registers or updates a user in the database
func (r *SQLiteRepository) UpsertUser(ctx context.Context, u *User) error {
	query := `
		INSERT INTO users (telegram_id, username, first_name, role, daily_quota, saved_base_url, saved_api_key, updated_at)
		VALUES (?, ?, ?, COALESCE(NULLIF(?, ''), 'user'), COALESCE(NULLIF(?, 0), 10), ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(telegram_id) DO UPDATE SET
			username = excluded.username,
			first_name = excluded.first_name,
			saved_base_url = COALESCE(NULLIF(excluded.saved_base_url, ''), users.saved_base_url),
			saved_api_key = COALESCE(NULLIF(excluded.saved_api_key, ''), users.saved_api_key),
			updated_at = CURRENT_TIMESTAMP;
	`
	_, err := r.db.ExecContext(ctx, query, u.TelegramID, u.Username, u.FirstName, u.Role, u.DailyQuota, u.SavedBaseURL, u.SavedAPIKey)
	if err != nil {
		return fmt.Errorf("gagal upsert user: %w", err)
	}
	return nil
}

// GetUser retrieves user profile data along with saved credentials
func (r *SQLiteRepository) GetUser(ctx context.Context, telegramID int64) (*User, error) {
	query := `SELECT telegram_id, username, first_name, role, daily_quota, saved_base_url, saved_api_key, created_at, updated_at FROM users WHERE telegram_id = ?`
	row := r.db.QueryRowContext(ctx, query, telegramID)

	var u User
	var createdAt, updatedAt string
	var savedURL, savedKey sql.NullString
	err := row.Scan(&u.TelegramID, &u.Username, &u.FirstName, &u.Role, &u.DailyQuota, &savedURL, &savedKey, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("gagal query user: %w", err)
	}

	u.SavedBaseURL = savedURL.String
	u.SavedAPIKey = savedKey.String
	u.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	u.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)
	return &u, nil
}

// SaveUserCredentials stores base_url and api_key in the database for recurring tests
func (r *SQLiteRepository) SaveUserCredentials(ctx context.Context, telegramID int64, baseURL, apiKey string) error {
	query := `
		INSERT INTO users (telegram_id, saved_base_url, saved_api_key, updated_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(telegram_id) DO UPDATE SET
			saved_base_url = excluded.saved_base_url,
			saved_api_key = excluded.saved_api_key,
			updated_at = CURRENT_TIMESTAMP;
	`
	_, err := r.db.ExecContext(ctx, query, telegramID, baseURL, apiKey)
	if err == nil {
		logger.DB("SAVE_CREDS", "user=%d provider=%s credentials saved to DB", telegramID, baseURL)
	}
	return err
}

// ResetUserCredentials clears a user's saved base_url and api_key from the database
func (r *SQLiteRepository) ResetUserCredentials(ctx context.Context, telegramID int64) error {
	query := `UPDATE users SET saved_base_url = NULL, saved_api_key = NULL, updated_at = CURRENT_TIMESTAMP WHERE telegram_id = ?;`
	_, err := r.db.ExecContext(ctx, query, telegramID)
	if err == nil {
		logger.DB("RESET_CREDS", "user=%d credentials cleared from DB", telegramID)
	}
	return err
}

// SaveSWERun saves SWE-bench ladder execution results along with task details in a single transaction
func (r *SQLiteRepository) SaveSWERun(ctx context.Context, run *BenchmarkRun, taskRuns []SWETaskRun) error {
	if run.ID == "" {
		run.ID = uuid.New().String()
	}
	if run.MaxScore == 0 {
		run.MaxScore = 100
	}
	if run.BenchmarkMode == "" {
		run.BenchmarkMode = "swe"
	}
	if run.MaxTierAchieved == "" {
		run.MaxTierAchieved = "NONE"
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("gagal memulai transaksi: %w", err)
	}
	defer tx.Rollback()
	// 0. Ensure user exists before inserting benchmark_runs to prevent foreign key violation
	ensureUserQuery := `
		INSERT OR IGNORE INTO users (telegram_id, username, first_name, role)
		VALUES (?, 'user', 'User', 'user');
	`
	_, _ = tx.ExecContext(ctx, ensureUserQuery, run.TelegramID)

	// 1. Insert run header
	insertRunQuery := `
		INSERT INTO benchmark_runs (
			id, telegram_id, provider_base_url, model_name, total_score, max_score,
			execution_time_ms, prompt_tokens, completion_tokens, total_tokens,
			estimated_cost_usd, cost_tier,
			code_hash, code_snippet, status, error_summary,
			benchmark_mode, max_tier_achieved, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP);
	`
	_, err = tx.ExecContext(ctx, insertRunQuery,
		run.ID, run.TelegramID, run.ProviderBaseURL, run.ModelName, run.TotalScore, run.MaxScore,
		run.ExecutionTimeMs, run.PromptTokens, run.CompletionTokens, run.TotalTokens,
		run.EstimatedCostUSD, run.CostTier,
		run.CodeHash, run.CodeSnippet, run.Status, run.ErrorSummary,
		run.BenchmarkMode, run.MaxTierAchieved,
	)
	if err != nil {
		return fmt.Errorf("gagal insert benchmark_runs (swe): %w", err)
	}

	// 2. Insert swe_task_runs details
	insertTaskQuery := `
		INSERT INTO swe_task_runs (
			id, run_id, task_id, task_title, tier, points_awarded, max_points,
			resolved, has_race, attempts, test_output, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP);
	`
	stmt, err := tx.PrepareContext(ctx, insertTaskQuery)
	if err != nil {
		return fmt.Errorf("gagal prepare statement swe_task_runs: %w", err)
	}
	defer stmt.Close()

	for _, tr := range taskRuns {
		taskRunID := tr.ID
		if taskRunID == "" {
			taskRunID = uuid.New().String()
		}
		_, err = stmt.ExecContext(ctx,
			taskRunID, run.ID, tr.TaskID, tr.TaskTitle, tr.Tier, tr.PointsAwarded, tr.MaxPoints,
			tr.Resolved, tr.HasRace, tr.Attempts, tr.TestOutput,
		)
		if err != nil {
			return fmt.Errorf("gagal insert swe_task_runs (%s): %w", tr.TaskID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("gagal commit transaksi: %w", err)
	}

	logger.DB("SAVE_SWE_RUN", "run_id=%s user=%d model=%s score=%d/%d max_tier=%s tasks=%d",
		run.ID, run.TelegramID, run.ModelName, run.TotalScore, run.MaxScore, run.MaxTierAchieved, len(taskRuns))

	return nil
}

// GetSWETaskRuns retrieves SWE task execution details for a given runID
func (r *SQLiteRepository) GetSWETaskRuns(ctx context.Context, runID string) ([]SWETaskRun, error) {
	query := `
		SELECT id, run_id, task_id, task_title, tier, points_awarded, max_points,
		       resolved, has_race, attempts, COALESCE(test_output, ''), created_at
		FROM swe_task_runs
		WHERE run_id = ?
		ORDER BY created_at ASC;
	`
	rows, err := r.db.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, fmt.Errorf("gagal query swe_task_runs: %w", err)
	}
	defer rows.Close()

	var list []SWETaskRun
	for rows.Next() {
		var tr SWETaskRun
		var createdAt string
		if err := rows.Scan(
			&tr.ID, &tr.RunID, &tr.TaskID, &tr.TaskTitle, &tr.Tier, &tr.PointsAwarded, &tr.MaxPoints,
			&tr.Resolved, &tr.HasRace, &tr.Attempts, &tr.TestOutput, &createdAt,
		); err != nil {
			return nil, fmt.Errorf("gagal scan swe_task_runs: %w", err)
		}
		tr.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		list = append(list, tr)
	}
	return list, nil
}

// GetLeaderboard retrieves top model rankings based on peak score
func (r *SQLiteRepository) GetLeaderboard(ctx context.Context, limit, offset int) ([]LeaderboardEntry, error) {
	if limit <= 0 {
		limit = 10
	}
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT 
			model_name,
			COUNT(id) AS total_runs,
			ROUND(AVG(total_score), 2) AS avg_score,
			MAX(total_score) AS peak_score,
			MIN(total_score) AS lowest_score,
			ROUND(100.0 * SUM(CASE WHEN total_score >= 70 THEN 1 ELSE 0 END) / COUNT(id), 1) AS pass_rate,
			ROUND(AVG(estimated_cost_usd), 6) AS avg_cost,
			COALESCE(MAX(cost_tier), '') AS cost_tier,
			ROUND(MAX(total_score) / (CASE WHEN AVG(estimated_cost_usd) <= 0.00001 THEN 0.0001 ELSE AVG(estimated_cost_usd) END), 1) AS value_score,
			COALESCE(CAST(ROUND(AVG(total_tokens)) AS INTEGER), 0) AS total_tokens
		FROM benchmark_runs
		WHERE status != 'SECURITY_VIOLATION'
		GROUP BY model_name
		ORDER BY peak_score DESC, avg_cost ASC, total_tokens ASC, total_runs DESC
		LIMIT ? OFFSET ?;
	`
	rows, err := r.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("gagal query leaderboard: %w", err)
	}
	defer rows.Close()

	var entries []LeaderboardEntry
	rank := offset + 1

	for rows.Next() {
		var e LeaderboardEntry
		if err := rows.Scan(&e.ModelName, &e.TotalRuns, &e.AvgScore, &e.PeakScore, &e.LowestScore, &e.PassRate, &e.AvgCostUSD, &e.CostTier, &e.ValueScore, &e.TotalTokens); err != nil {
			return nil, fmt.Errorf("gagal scan leaderboard row: %w", err)
		}
		e.Rank = rank
		rank++
		entries = append(entries, e)
	}

	return entries, nil
}

// GetValueLeaderboard retrieves model rankings based on peak score ratio and cost efficiency (Value for Money)
func (r *SQLiteRepository) GetValueLeaderboard(ctx context.Context, limit, offset int) ([]LeaderboardEntry, error) {
	if limit <= 0 {
		limit = 10
	}
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT 
			model_name,
			COUNT(id) AS total_runs,
			ROUND(AVG(total_score), 2) AS avg_score,
			MAX(total_score) AS peak_score,
			MIN(total_score) AS lowest_score,
			ROUND(100.0 * SUM(CASE WHEN total_score >= 70 THEN 1 ELSE 0 END) / COUNT(id), 1) AS pass_rate,
			ROUND(AVG(estimated_cost_usd), 6) AS avg_cost,
			COALESCE(MAX(cost_tier), '') AS cost_tier,
			ROUND(MAX(total_score) / (CASE WHEN AVG(estimated_cost_usd) <= 0.00001 THEN 0.0001 ELSE AVG(estimated_cost_usd) END), 1) AS value_score,
			COALESCE(CAST(ROUND(AVG(total_tokens)) AS INTEGER), 0) AS total_tokens
		FROM benchmark_runs
		WHERE status != 'SECURITY_VIOLATION'
		GROUP BY model_name
		ORDER BY value_score DESC, peak_score DESC, avg_cost ASC, total_tokens ASC
		LIMIT ? OFFSET ?;
	`
	rows, err := r.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("gagal query value leaderboard: %w", err)
	}
	defer rows.Close()

	var entries []LeaderboardEntry
	rank := offset + 1

	for rows.Next() {
		var e LeaderboardEntry
		if err := rows.Scan(&e.ModelName, &e.TotalRuns, &e.AvgScore, &e.PeakScore, &e.LowestScore, &e.PassRate, &e.AvgCostUSD, &e.CostTier, &e.ValueScore, &e.TotalTokens); err != nil {
			return nil, fmt.Errorf("gagal scan value leaderboard row: %w", err)
		}
		e.Rank = rank
		rank++
		entries = append(entries, e)
	}

	return entries, nil
}

// GetCategoryLeaderboard retrieves top model rankings specific to a given category based on highest score
func (r *SQLiteRepository) GetCategoryLeaderboard(ctx context.Context, categoryPattern string, limit, offset int) ([]CategoryLeaderboardEntry, error) {
	if limit <= 0 {
		limit = 10
	}
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT 
			r.model_name,
			d.category_name,
			MAX(d.score) AS peak_score,
			ROUND(AVG(d.score), 2) AS avg_score,
			MAX(d.max_score) AS max_score,
			COUNT(d.id) AS total_runs
		FROM benchmark_details d
		JOIN benchmark_runs r ON d.run_id = r.id
		WHERE d.category_name LIKE ? AND r.status != 'SECURITY_VIOLATION'
		GROUP BY r.model_name, d.category_name
		ORDER BY peak_score DESC, total_runs DESC, avg_score DESC
		LIMIT ? OFFSET ?;
	`
	rows, err := r.db.QueryContext(ctx, query, "%"+categoryPattern+"%", limit, offset)
	if err != nil {
		return nil, fmt.Errorf("gagal query category leaderboard: %w", err)
	}
	defer rows.Close()

	var entries []CategoryLeaderboardEntry
	rank := offset + 1

	for rows.Next() {
		var e CategoryLeaderboardEntry
		if err := rows.Scan(&e.ModelName, &e.CategoryName, &e.PeakScore, &e.AvgScore, &e.MaxScore, &e.TotalRuns); err != nil {
			return nil, fmt.Errorf("gagal scan category leaderboard row: %w", err)
		}
		e.Rank = rank
		rank++
		entries = append(entries, e)
	}

	return entries, nil
}


// GetUserHistory retrieves benchmark test history for a specific user
func (r *SQLiteRepository) GetUserHistory(ctx context.Context, telegramID int64, limit, offset int) ([]BenchmarkRun, error) {
	if limit <= 0 {
		limit = 5
	}
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT 
			id, telegram_id, provider_base_url, model_name, total_score, max_score,
			execution_time_ms, prompt_tokens, completion_tokens, total_tokens,
			estimated_cost_usd, cost_tier,
			code_hash, status, error_summary, created_at
		FROM benchmark_runs
		WHERE telegram_id = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?;
	`
	rows, err := r.db.QueryContext(ctx, query, telegramID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("gagal query user history: %w", err)
	}
	defer rows.Close()

	var runs []BenchmarkRun
	for rows.Next() {
		var run BenchmarkRun
		var createdAt string
		var costTier, codeHash, errorSummary sql.NullString

		err := rows.Scan(
			&run.ID, &run.TelegramID, &run.ProviderBaseURL, &run.ModelName, &run.TotalScore, &run.MaxScore,
			&run.ExecutionTimeMs, &run.PromptTokens, &run.CompletionTokens, &run.TotalTokens,
			&run.EstimatedCostUSD, &costTier,
			&codeHash, &run.Status, &errorSummary, &createdAt,
		)
		if err != nil {
			return nil, fmt.Errorf("gagal scan history row: %w", err)
		}

		run.CostTier = costTier.String
		run.CodeHash = codeHash.String
		run.ErrorSummary = errorSummary.String
		run.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		runs = append(runs, run)
	}

	return runs, nil
}

// GetRunDetails retrieves a single benchmark record along with its 8 category details
func (r *SQLiteRepository) GetRunDetails(ctx context.Context, runID string) (*BenchmarkRun, []BenchmarkDetail, error) {
	runQuery := `
		SELECT 
			id, telegram_id, provider_base_url, model_name, total_score, max_score,
			execution_time_ms, prompt_tokens, completion_tokens, total_tokens,
			estimated_cost_usd, cost_tier,
			code_hash, code_snippet, status, error_summary, created_at
		FROM benchmark_runs
		WHERE id = ?;
	`
	row := r.db.QueryRowContext(ctx, runQuery, runID)

	var run BenchmarkRun
	var createdAt string
	var costTier, codeHash, codeSnippet, errorSummary sql.NullString

	err := row.Scan(
		&run.ID, &run.TelegramID, &run.ProviderBaseURL, &run.ModelName, &run.TotalScore, &run.MaxScore,
		&run.ExecutionTimeMs, &run.PromptTokens, &run.CompletionTokens, &run.TotalTokens,
		&run.EstimatedCostUSD, &costTier,
		&codeHash, &codeSnippet, &run.Status, &errorSummary, &createdAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, ErrRunNotFound
		}
		return nil, nil, fmt.Errorf("gagal query benchmark run: %w", err)
	}

	run.CostTier = costTier.String
	run.CodeHash = codeHash.String
	run.CodeSnippet = codeSnippet.String
	run.ErrorSummary = errorSummary.String
	run.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)

	detailsQuery := `
		SELECT id, run_id, category_name, score, max_score, details_json
		FROM benchmark_details
		WHERE run_id = ?
		ORDER BY id ASC;
	`
	rows, err := r.db.QueryContext(ctx, detailsQuery, runID)
	if err != nil {
		return nil, nil, fmt.Errorf("gagal query benchmark details: %w", err)
	}
	defer rows.Close()

	var details []BenchmarkDetail
	for rows.Next() {
		var det BenchmarkDetail
		var detailsJSON sql.NullString
		if err := rows.Scan(&det.ID, &det.RunID, &det.CategoryName, &det.Score, &det.MaxScore, &detailsJSON); err != nil {
			return nil, nil, fmt.Errorf("gagal scan detail row: %w", err)
		}
		if detailsJSON.Valid && detailsJSON.String != "" {
			_ = json.Unmarshal([]byte(detailsJSON.String), &det.Details)
		}
		details = append(details, det)
	}

	return &run, details, nil
}

// CheckAndUpdateRateLimit checks the hourly benchmark quota per user (Token Bucket)
func (r *SQLiteRepository) CheckAndUpdateRateLimit(ctx context.Context, telegramID int64, hourlyLimit int) (bool, int, time.Duration, error) {
	if hourlyLimit <= 0 {
		hourlyLimit = 5
	}

	windowDuration := 1 * time.Hour
	now := time.Now()

	query := `SELECT run_count, window_start FROM user_rate_limits WHERE telegram_id = ?;`
	row := r.db.QueryRowContext(ctx, query, telegramID)

	var runCount int
	var windowStartStr string
	err := row.Scan(&runCount, &windowStartStr)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Record does not exist: create a new one with run_count = 1
			insertQuery := `INSERT INTO user_rate_limits (telegram_id, run_count, window_start) VALUES (?, 1, CURRENT_TIMESTAMP);`
			if _, err := r.db.ExecContext(ctx, insertQuery, telegramID); err != nil {
				return false, 0, 0, fmt.Errorf("gagal insert initial rate limit: %w", err)
			}
			return true, hourlyLimit - 1, 0, nil
		}
		return false, 0, 0, fmt.Errorf("gagal query rate limit: %w", err)
	}

	// Parse window start
	windowStart, parseErr := time.Parse(time.RFC3339, windowStartStr)
	if parseErr != nil {
		windowStart = now
	}

	elapsed := now.Sub(windowStart)
	if elapsed >= windowDuration {
		// 1-hour time window has elapsed, reset quota to 1
		updateQuery := `UPDATE user_rate_limits SET run_count = 1, window_start = CURRENT_TIMESTAMP WHERE telegram_id = ?;`
		if _, err := r.db.ExecContext(ctx, updateQuery, telegramID); err != nil {
			return false, 0, 0, fmt.Errorf("gagal reset rate limit window: %w", err)
		}
		return true, hourlyLimit - 1, 0, nil
	}

	// Still within the same time window
	if runCount >= hourlyLimit {
		retryAfter := windowDuration - elapsed
		return false, 0, retryAfter, nil
	}

	// Quota still available, increment usage count
	incrementQuery := `UPDATE user_rate_limits SET run_count = run_count + 1 WHERE telegram_id = ?;`
	if _, err := r.db.ExecContext(ctx, incrementQuery, telegramID); err != nil {
		return false, 0, 0, fmt.Errorf("gagal inkremen rate limit: %w", err)
	}

	remaining := hourlyLimit - (runCount + 1)
	return true, remaining, 0, nil
}

// BanUser marks a user's status as banned
func (r *SQLiteRepository) BanUser(ctx context.Context, telegramID int64) error {
	query := `
		INSERT INTO users (telegram_id, role, updated_at)
		VALUES (?, 'banned', CURRENT_TIMESTAMP)
		ON CONFLICT(telegram_id) DO UPDATE SET
			role = 'banned',
			updated_at = CURRENT_TIMESTAMP;
	`
	_, err := r.db.ExecContext(ctx, query, telegramID)
	if err == nil {
		logger.DB("BAN_USER", "telegram_id=%d marked as banned", telegramID)
	}
	return err
}

// UnbanUser restores a user's status to a normal user
func (r *SQLiteRepository) UnbanUser(ctx context.Context, telegramID int64) error {
	query := `UPDATE users SET role = 'user', updated_at = CURRENT_TIMESTAMP WHERE telegram_id = ?;`
	_, err := r.db.ExecContext(ctx, query, telegramID)
	if err == nil {
		logger.DB("UNBAN_USER", "telegram_id=%d unbanned", telegramID)
	}
	return err
}

// IsUserBanned checks if a user has banned status
func (r *SQLiteRepository) IsUserBanned(ctx context.Context, telegramID int64) (bool, error) {
	var role string
	query := `SELECT role FROM users WHERE telegram_id = ?;`
	err := r.db.QueryRowContext(ctx, query, telegramID).Scan(&role)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return strings.ToLower(role) == "banned", nil
}

// ResetUserRateLimit resets a user's quota limit / rate limit window
func (r *SQLiteRepository) ResetUserRateLimit(ctx context.Context, telegramID int64) error {
	query := `DELETE FROM user_rate_limits WHERE telegram_id = ?;`
	_, err := r.db.ExecContext(ctx, query, telegramID)
	if err == nil {
		logger.DB("RESET_LIMIT", "telegram_id=%d rate limit reset", telegramID)
	}
	return err
}

// GetSystemStats summarizes system metrics for the administration panel
func (r *SQLiteRepository) GetSystemStats(ctx context.Context) (*SystemStats, error) {
	stats := &SystemStats{}

	queryRuns := `
		SELECT 
			COUNT(id) AS total_runs,
			COALESCE(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success_runs,
			COALESCE(SUM(CASE WHEN status = 'SECURITY_VIOLATION' THEN 1 ELSE 0 END), 0) AS violation_runs,
			COALESCE(SUM(CASE WHEN status != 'SUCCESS' THEN 1 ELSE 0 END), 0) AS failed_runs,
			COUNT(DISTINCT model_name) AS unique_models,
			COALESCE(SUM(estimated_cost_usd), 0.0) AS total_cost
		FROM benchmark_runs;
	`
	err := r.db.QueryRowContext(ctx, queryRuns).Scan(
		&stats.TotalRuns,
		&stats.SuccessRuns,
		&stats.ViolationRuns,
		&stats.FailedRuns,
		&stats.UniqueModels,
		&stats.TotalCostUSD,
	)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("gagal query stats runs: %w", err)
	}

	if stats.TotalRuns > 0 {
		stats.ErrorRatePercent = (float64(stats.FailedRuns) / float64(stats.TotalRuns)) * 100.0
	}

	queryUsers := `
		SELECT 
			COUNT(telegram_id) AS total_users,
			COALESCE(SUM(CASE WHEN role = 'banned' THEN 1 ELSE 0 END), 0) AS banned_users
		FROM users;
	`
	err = r.db.QueryRowContext(ctx, queryUsers).Scan(
		&stats.TotalUsers,
		&stats.BannedUsers,
	)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("gagal query stats users: %w", err)
	}

	return stats, nil
}

// SaveCachedModels saves or updates the collection of models synced from OpenRouter to the database
// using batched multi-row upsert to prevent database lock starvation and bot blocking
func (r *SQLiteRepository) SaveCachedModels(ctx context.Context, models []CachedModel) error {
	if len(models) == 0 {
		return nil
	}

	// Filter models that have a valid ID
	validModels := make([]CachedModel, 0, len(models))
	for _, m := range models {
		if strings.TrimSpace(m.ID) != "" {
			validModels = append(validModels, m)
		}
	}
	if len(validModels) == 0 {
		return nil
	}

	// Batching upsert: split dataset into chunks of 50 models.
	// 50 models * 5 parameters = 250 parameters per query (well below SQLite's safe limit of 999 parameters).
	// This approach reduces network round-trips to Turso from ~500 queries to only ~10 queries.
	const batchSize = 50
	total := len(validModels)

	for i := 0; i < total; i += batchSize {
		// 1. Check context cancellation between batches
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		end := i + batchSize
		if end > total {
			end = total
		}
		chunk := validModels[i:end]

		// 2. Build dynamic multi-row upsert query
		var sb strings.Builder
		sb.WriteString("INSERT INTO cached_models (id, name, prompt_price_per_m, completion_price_per_m, context_length, updated_at) VALUES ")

		args := make([]interface{}, 0, len(chunk)*5)
		for j, m := range chunk {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("(?, ?, ?, ?, ?, CURRENT_TIMESTAMP)")
			args = append(args, m.ID, m.Name, m.PromptPricePerM, m.CompletionPricePerM, m.ContextLength)
		}

		sb.WriteString(` ON CONFLICT(id) DO UPDATE SET
			name = excluded.name,
			prompt_price_per_m = excluded.prompt_price_per_m,
			completion_price_per_m = excluded.completion_price_per_m,
			context_length = excluded.context_length,
			updated_at = CURRENT_TIMESTAMP;`)

		// 3. Execute batch query directly (auto-commit, minimizing lock duration)
		if _, err := r.db.ExecContext(ctx, sb.String(), args...); err != nil {
			return fmt.Errorf("gagal batch upsert cached_models (%d-%d): %w", i+1, end, err)
		}

		// 4. Cooperative yield: pause briefly (5ms) between batches so other queries
		// (such as rate limiter, user verification, or bot message handling) can execute without lock starvation
		if end < total {
			time.Sleep(5 * time.Millisecond)
		}
	}

	logger.DB("CACHE_MODELS", "Berhasil menyimpan %d model ke basis data (batch upsert)", total)
	return nil
}


// GetAllCachedModels retrieves all models stored in the database cache
func (r *SQLiteRepository) GetAllCachedModels(ctx context.Context) ([]CachedModel, error) {
	query := `
		SELECT id, name, prompt_price_per_m, completion_price_per_m, context_length, updated_at
		FROM cached_models
		ORDER BY id ASC;
	`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("gagal query cached_models: %w", err)
	}
	defer rows.Close()

	var list []CachedModel
	for rows.Next() {
		var m CachedModel
		var name sql.NullString
		var updatedAt string
		if err := rows.Scan(&m.ID, &name, &m.PromptPricePerM, &m.CompletionPricePerM, &m.ContextLength, &updatedAt); err != nil {
			return nil, fmt.Errorf("gagal scan cached_model: %w", err)
		}
		m.Name = name.String
		m.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)
		list = append(list, m)
	}
	return list, nil
}

// GetCachedModel looks up a specific model from the database by ID
func (r *SQLiteRepository) GetCachedModel(ctx context.Context, id string) (*CachedModel, error) {
	query := `
		SELECT id, name, prompt_price_per_m, completion_price_per_m, context_length, updated_at
		FROM cached_models
		WHERE id = ? OR id LIKE ?
		LIMIT 1;
	`
	row := r.db.QueryRowContext(ctx, query, id, "%/"+id)
	var m CachedModel
	var name sql.NullString
	var updatedAt string
	if err := row.Scan(&m.ID, &name, &m.PromptPricePerM, &m.CompletionPricePerM, &m.ContextLength, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("gagal query cached_model %s: %w", id, err)
	}
	m.Name = name.String
	m.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)
	return &m, nil
}

// Ping checks if the database connection is active and responsive
func (r *SQLiteRepository) Ping(ctx context.Context) error {
	return r.db.PingContext(ctx)
}

// Close closes the database connection resources
func (r *SQLiteRepository) Close() error {
	return r.db.Close()
}
