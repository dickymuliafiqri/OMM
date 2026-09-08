package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Default configuration constants
const (
	DefaultListenAddr         = ":8080"
	DefaultMaxConcurrentJobs  = 3
	DefaultJobTimeoutSec      = 300
	DefaultGlobalRateLimit    = 30
	DefaultPerSourceRateLimit = 5
	DefaultLogLevel           = "info"
)

// Config holds runtime configuration for the OMM benchmark server
type Config struct {
	// Server
	ListenAddr string // HOST:PORT (default ":9090")

	// Security
	BenchSecret    string   // Shared HMAC secret antara omm-web dan omm-bench (wajib)
	AllowedOrigins []string // CORS whitelist (domain omm-web)
	AllowLocalhost bool     // Izinkan request HTTP ke/dari localhost (default: true)

	// Worker
	MaxConcurrentJobs int // Maks evaluasi paralel (default: 3)
	JobTimeoutSec     int // Timeout per job dalam detik (default: 300)

	// Rate Limiting
	GlobalRateLimit    int // Maks request per menit global (default: 30)
	PerSourceRateLimit int // Maks request per menit per source (default: 5)

	// Logging
	LogLevel string // Log level: debug, info, warn, error (default: "info")
}

// Load loads configuration from environment variables and optional .env file
func Load() (*Config, error) {
	// Optionally load .env file if present (without overwriting existing env)
	_ = loadDotEnv(".env")

	cfg := &Config{
		ListenAddr:         getEnv("OMM_LISTEN_ADDR", DefaultListenAddr),
		BenchSecret:        strings.TrimSpace(os.Getenv("OMM_BENCH_SECRET")),
		AllowedOrigins:     parseCommaSeparated(os.Getenv("OMM_ALLOWED_ORIGINS")),
		AllowLocalhost:     getEnvBool("OMM_ALLOW_LOCALHOST", true),
		MaxConcurrentJobs:  getEnvInt("OMM_MAX_CONCURRENT_JOBS", DefaultMaxConcurrentJobs),
		JobTimeoutSec:      getEnvInt("OMM_JOB_TIMEOUT_SEC", DefaultJobTimeoutSec),
		GlobalRateLimit:    getEnvInt("OMM_GLOBAL_RATE_LIMIT", DefaultGlobalRateLimit),
		PerSourceRateLimit: getEnvInt("OMM_PER_SOURCE_RATE_LIMIT", DefaultPerSourceRateLimit),
		LogLevel:           strings.ToLower(getEnv("OMM_LOG_LEVEL", DefaultLogLevel)),
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate ensures configuration values are within acceptable bounds
func (c *Config) Validate() error {
	if c.BenchSecret == "" {
		return errors.New("konfigurasi tidak valid: OMM_BENCH_SECRET wajib diisi")
	}

	if c.MaxConcurrentJobs <= 0 {
		return fmt.Errorf("konfigurasi tidak valid: OMM_MAX_CONCURRENT_JOBS harus > 0 (diberikan: %d)", c.MaxConcurrentJobs)
	}

	if c.JobTimeoutSec <= 0 {
		return fmt.Errorf("konfigurasi tidak valid: OMM_JOB_TIMEOUT_SEC harus > 0 (diberikan: %d)", c.JobTimeoutSec)
	}

	if c.GlobalRateLimit <= 0 {
		return fmt.Errorf("konfigurasi tidak valid: OMM_GLOBAL_RATE_LIMIT harus > 0 (diberikan: %d)", c.GlobalRateLimit)
	}

	if c.PerSourceRateLimit <= 0 {
		return fmt.Errorf("konfigurasi tidak valid: OMM_PER_SOURCE_RATE_LIMIT harus > 0 (diberikan: %d)", c.PerSourceRateLimit)
	}

	return nil
}

// Helper functions for reading environment variables

func getEnv(key, defaultVal string) string {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return defaultVal
	}
	return val
}

func getEnvInt(key string, defaultVal int) int {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return defaultVal
	}
	parsed, err := strconv.Atoi(val)
	if err != nil || parsed <= 0 {
		return defaultVal
	}
	return parsed
}

func getEnvBool(key string, defaultVal bool) bool {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return defaultVal
	}
	parsed, err := strconv.ParseBool(val)
	if err != nil {
		return defaultVal
	}
	return parsed
}

func parseCommaSeparated(val string) []string {
	raw := strings.Split(val, ",")
	var result []string
	for _, item := range raw {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// loadDotEnv parses a .env file and sets variables only if not already present in environment
func loadDotEnv(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		// Strip surrounding quotes if present
		if len(val) >= 2 && ((val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'')) {
			val = val[1 : len(val)-1]
		}

		// Only set if not already defined in environment
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, val)
		}
	}

	return scanner.Err()
}
