package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func clearEnv() {
	keys := []string{
		"OMM_LISTEN_ADDR",
		"OMM_BENCH_SECRET",
		"OMM_ALLOWED_ORIGINS",
		"OMM_ALLOW_LOCALHOST",
		"OMM_MAX_CONCURRENT_JOBS",
		"OMM_JOB_TIMEOUT_SEC",
		"OMM_GLOBAL_RATE_LIMIT",
		"OMM_PER_SOURCE_RATE_LIMIT",
		"OMM_LOG_LEVEL",
	}
	for _, k := range keys {
		_ = os.Unsetenv(k)
	}
}

func TestLoad_MissingSecret_ReturnsError(t *testing.T) {
	clearEnv()

	_, err := Load()
	if err == nil {
		t.Fatal("expected error when OMM_BENCH_SECRET is missing, got nil")
	}
}

func TestLoad_DefaultsApplied(t *testing.T) {
	clearEnv()
	_ = os.Setenv("OMM_BENCH_SECRET", "super-secret-key-1234567890123456")
	defer clearEnv()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ListenAddr != DefaultListenAddr {
		t.Errorf("expected ListenAddr %s, got %s", DefaultListenAddr, cfg.ListenAddr)
	}
	if cfg.MaxConcurrentJobs != DefaultMaxConcurrentJobs {
		t.Errorf("expected MaxConcurrentJobs %d, got %d", DefaultMaxConcurrentJobs, cfg.MaxConcurrentJobs)
	}
	if cfg.JobTimeoutSec != DefaultJobTimeoutSec {
		t.Errorf("expected JobTimeoutSec %d, got %d", DefaultJobTimeoutSec, cfg.JobTimeoutSec)
	}
	if cfg.GlobalRateLimit != DefaultGlobalRateLimit {
		t.Errorf("expected GlobalRateLimit %d, got %d", DefaultGlobalRateLimit, cfg.GlobalRateLimit)
	}
	if cfg.PerSourceRateLimit != DefaultPerSourceRateLimit {
		t.Errorf("expected PerSourceRateLimit %d, got %d", DefaultPerSourceRateLimit, cfg.PerSourceRateLimit)
	}
	if cfg.LogLevel != DefaultLogLevel {
		t.Errorf("expected LogLevel %s, got %s", DefaultLogLevel, cfg.LogLevel)
	}
	if len(cfg.AllowedOrigins) != 0 {
		t.Errorf("expected empty AllowedOrigins, got %v", cfg.AllowedOrigins)
	}
	if !cfg.AllowLocalhost {
		t.Errorf("expected AllowLocalhost to be true by default, got %v", cfg.AllowLocalhost)
	}
}

func TestLoad_CustomEnvVariables(t *testing.T) {
	clearEnv()
	_ = os.Setenv("OMM_LISTEN_ADDR", "127.0.0.1:8080")
	_ = os.Setenv("OMM_BENCH_SECRET", "custom-secret-key-abcdef12345678")
	_ = os.Setenv("OMM_ALLOWED_ORIGINS", "https://example.com, https://omm.web ")
	_ = os.Setenv("OMM_ALLOW_LOCALHOST", "false")
	_ = os.Setenv("OMM_MAX_CONCURRENT_JOBS", "5")
	_ = os.Setenv("OMM_JOB_TIMEOUT_SEC", "600")
	_ = os.Setenv("OMM_GLOBAL_RATE_LIMIT", "50")
	_ = os.Setenv("OMM_PER_SOURCE_RATE_LIMIT", "10")
	_ = os.Setenv("OMM_LOG_LEVEL", "DEBUG")
	defer clearEnv()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ListenAddr != "127.0.0.1:8080" {
		t.Errorf("expected ListenAddr 127.0.0.1:8080, got %s", cfg.ListenAddr)
	}
	if cfg.BenchSecret != "custom-secret-key-abcdef12345678" {
		t.Errorf("expected BenchSecret custom-secret-key-abcdef12345678, got %s", cfg.BenchSecret)
	}
	expectedOrigins := []string{"https://example.com", "https://omm.web"}
	if !reflect.DeepEqual(cfg.AllowedOrigins, expectedOrigins) {
		t.Errorf("expected AllowedOrigins %v, got %v", expectedOrigins, cfg.AllowedOrigins)
	}
	if cfg.AllowLocalhost {
		t.Errorf("expected AllowLocalhost to be false when configured, got true")
	}
	if cfg.MaxConcurrentJobs != 5 {
		t.Errorf("expected MaxConcurrentJobs 5, got %d", cfg.MaxConcurrentJobs)
	}
	if cfg.JobTimeoutSec != 600 {
		t.Errorf("expected JobTimeoutSec 600, got %d", cfg.JobTimeoutSec)
	}
	if cfg.GlobalRateLimit != 50 {
		t.Errorf("expected GlobalRateLimit 50, got %d", cfg.GlobalRateLimit)
	}
	if cfg.PerSourceRateLimit != 10 {
		t.Errorf("expected PerSourceRateLimit 10, got %d", cfg.PerSourceRateLimit)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected LogLevel debug, got %s", cfg.LogLevel)
	}
}

func TestValidate_InvalidNumbers(t *testing.T) {
	baseCfg := Config{
		ListenAddr:         ":9090",
		BenchSecret:        "valid-secret",
		MaxConcurrentJobs:  3,
		JobTimeoutSec:      300,
		GlobalRateLimit:    30,
		PerSourceRateLimit: 5,
	}

	tests := []struct {
		name    string
		mutate  func(c *Config)
		wantErr bool
	}{
		{
			name: "zero MaxConcurrentJobs",
			mutate: func(c *Config) {
				c.MaxConcurrentJobs = 0
			},
			wantErr: true,
		},
		{
			name: "negative JobTimeoutSec",
			mutate: func(c *Config) {
				c.JobTimeoutSec = -1
			},
			wantErr: true,
		},
		{
			name: "zero GlobalRateLimit",
			mutate: func(c *Config) {
				c.GlobalRateLimit = 0
			},
			wantErr: true,
		},
		{
			name: "zero PerSourceRateLimit",
			mutate: func(c *Config) {
				c.PerSourceRateLimit = 0
			},
			wantErr: true,
		},
		{
			name: "empty BenchSecret",
			mutate: func(c *Config) {
				c.BenchSecret = ""
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := baseCfg
			tc.mutate(&c)
			err := c.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestLoadDotEnv(t *testing.T) {
	tempDir := t.TempDir()
	envPath := filepath.Join(tempDir, ".env")

	content := `
# Comment line
OMM_LISTEN_ADDR=":9191"
OMM_BENCH_SECRET='dotenv-secret-key'
OMM_ALLOWED_ORIGINS="http://localhost:3000, http://localhost:4000"
`
	if err := os.WriteFile(envPath, []byte(content), 0600); err != nil {
		t.Fatalf("failed to write test .env: %v", err)
	}

	clearEnv()
	defer clearEnv()

	if err := loadDotEnv(envPath); err != nil {
		t.Fatalf("loadDotEnv failed: %v", err)
	}

	if os.Getenv("OMM_LISTEN_ADDR") != ":9191" {
		t.Errorf("expected :9191, got %s", os.Getenv("OMM_LISTEN_ADDR"))
	}
	if os.Getenv("OMM_BENCH_SECRET") != "dotenv-secret-key" {
		t.Errorf("expected dotenv-secret-key, got %s", os.Getenv("OMM_BENCH_SECRET"))
	}
}
