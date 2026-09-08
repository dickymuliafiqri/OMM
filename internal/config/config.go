package config

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds application operational parameters read from environment/.env
type Config struct {
	TelegramBotToken string
	BotAdminIDs      []int64
	TursoDatabaseURL string
	TursoAuthToken   string
	MaxWorkers       int
	MaxQueueSize     int
	HourlyLimit      int
	HealthCheckPort  int
	WhitelistMode    bool
	WhitelistIDs     []int64
	ASTStrictMode         bool
	BenchmarkTimeout      int // Seconds, total benchmark timeout
	SWETaskTimeoutMinutes int // Minutes, per-stage SWE ladder task timeout (default 5)
	LogLevel              string
}

// LoadEnv loads key-value pairs from .env files into process environment (os.Setenv).
// If no file arguments are provided, it searches for ".env" in the current working directory,
// or parent directories ("../.env", "../../.env") for flexibility when running from subfolders.
func LoadEnv(filenames ...string) error {
	if len(filenames) == 0 {
		candidates := []string{".env", "../.env", "../../.env"}
		for _, c := range candidates {
			if _, err := os.Stat(c); err == nil {
				return parseAndSetEnv(c)
			}
		}
		// Tolerant if .env file is not found at all (e.g., in Docker/K8s with native env)
		return nil
	}

	for _, filename := range filenames {
		if err := parseAndSetEnv(filename); err != nil {
			return err
		}
	}
	return nil
}

func parseAndSetEnv(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	// Remove UTF-8 BOM if present (often added by editors on Windows)
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))

	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}

		// Support "export KEY=VAL" format
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}

		idx := strings.Index(line, "=")
		if idx == -1 {
			continue
		}

		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])

		if key == "" {
			continue
		}

		// Unquote value if wrapped in double or single quotes
		if len(val) >= 2 {
			if (strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"")) ||
				(strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'")) {
				val = val[1 : len(val)-1]
			} else {
				// Clean inline comments if not quoted (e.g., VAL # comment)
				if cIdx := strings.Index(val, " #"); cIdx != -1 {
					val = strings.TrimSpace(val[:cIdx])
				}
			}
		} else {
			if cIdx := strings.Index(val, " #"); cIdx != -1 {
				val = strings.TrimSpace(val[:cIdx])
			}
		}

		// Prefer OS environment if already set (Twelve-Factor App rule)
		if os.Getenv(key) == "" {
			_ = os.Setenv(key, val)
		}
	}

	return scanner.Err()
}

// Load reads the .env file, then validates and parses parameters into the Config struct
func Load() (*Config, error) {
	_ = LoadEnv()

	botToken := GetEnv("TELEGRAM_BOT_TOKEN", "")
	if botToken == "" {
		return nil, fmt.Errorf("TELEGRAM_BOT_TOKEN tidak ditemukan di environment maupun .env")
	}

	workers, _ := strconv.Atoi(GetEnv("MAX_CONCURRENT_WORKERS", "3"))
	if workers <= 0 {
		workers = 3
	}

	queueCap, _ := strconv.Atoi(GetEnv("MAX_QUEUE_SIZE", "20"))
	if queueCap <= 0 {
		queueCap = 20
	}

	hourlyLimit, _ := strconv.Atoi(GetEnv("PER_USER_HOURLY_LIMIT", GetEnv("PER_USER_DAILY_LIMIT", "5")))
	if hourlyLimit <= 0 {
		hourlyLimit = 5
	}

	healthPort, _ := strconv.Atoi(GetEnv("HEALTH_CHECK_PORT", "8080"))
	if healthPort <= 0 {
		healthPort = 8080
	}

	whitelistMode := strings.ToLower(GetEnv("WHITELIST_MODE", "false")) == "true"
	whitelistIDs := parseInt64Slice(GetEnv("WHITELIST_USER_IDS", ""))
	adminIDs := parseInt64Slice(GetEnv("BOT_ADMIN_IDS", ""))

	astStrict := strings.ToLower(GetEnv("AST_STRICT_MODE", "true")) != "false"

	benchTimeout, _ := strconv.Atoi(GetEnv("BENCHMARK_TIMEOUT_SECONDS", "300"))
	if benchTimeout <= 0 {
		benchTimeout = 300
	}

	sweStageTimeout, _ := strconv.Atoi(GetEnv("SWE_STAGE_TIMEOUT_MINUTES", "5"))
	if sweStageTimeout <= 0 {
		sweStageTimeout = 5
	}

	return &Config{
		TelegramBotToken:      botToken,
		BotAdminIDs:           adminIDs,
		TursoDatabaseURL:      GetEnv("TURSO_DATABASE_URL", ":memory:"),
		TursoAuthToken:        GetEnv("TURSO_AUTH_TOKEN", ""),
		MaxWorkers:            workers,
		MaxQueueSize:          queueCap,
		HourlyLimit:           hourlyLimit,
		HealthCheckPort:       healthPort,
		WhitelistMode:         whitelistMode,
		WhitelistIDs:          whitelistIDs,
		ASTStrictMode:         astStrict,
		BenchmarkTimeout:      benchTimeout,
		SWETaskTimeoutMinutes: sweStageTimeout,
		LogLevel:              GetEnv("LOG_LEVEL", "info"),
	}, nil
}

// GetEnv retrieves an environment variable value with a fallback default
func GetEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		return strings.TrimSpace(val)
	}
	return defaultVal
}

func parseInt64Slice(s string) []int64 {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var res []int64
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if id, err := strconv.ParseInt(part, 10, 64); err == nil {
			res = append(res, id)
		}
	}
	return res
}
