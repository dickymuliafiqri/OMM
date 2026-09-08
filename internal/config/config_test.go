package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadEnv_BasicAndQuotes(t *testing.T) {
	tempDir := t.TempDir()
	envPath := filepath.Join(tempDir, ".env")

	envContent := `# Komentar
TELEGRAM_BOT_TOKEN="test_token_12345"
export TURSO_DATABASE_URL=libsql://example.turso.io
TURSO_AUTH_TOKEN='secret_jwt_token'
MAX_CONCURRENT_WORKERS=5
MAX_QUEUE_SIZE=25 # inline comment
PER_USER_HOURLY_LIMIT=10
HEALTH_CHECK_PORT=9090
WHITELIST_MODE=true
WHITELIST_USER_IDS="111, 222, 333"
BOT_ADMIN_IDS="999, 888"
`
	if err := os.WriteFile(envPath, []byte(envContent), 0644); err != nil {
		t.Fatalf("Gagal menulis file test .env: %v", err)
	}

	// Clean up env before test
	_ = os.Unsetenv("TELEGRAM_BOT_TOKEN")
	_ = os.Unsetenv("TURSO_DATABASE_URL")
	_ = os.Unsetenv("TURSO_AUTH_TOKEN")
	_ = os.Unsetenv("MAX_CONCURRENT_WORKERS")
	_ = os.Unsetenv("MAX_QUEUE_SIZE")
	_ = os.Unsetenv("PER_USER_HOURLY_LIMIT")
	_ = os.Unsetenv("HEALTH_CHECK_PORT")
	_ = os.Unsetenv("WHITELIST_MODE")
	_ = os.Unsetenv("WHITELIST_USER_IDS")
	_ = os.Unsetenv("BOT_ADMIN_IDS")

	if err := LoadEnv(envPath); err != nil {
		t.Fatalf("LoadEnv gagal: %v", err)
	}

	if os.Getenv("TELEGRAM_BOT_TOKEN") != "test_token_12345" {
		t.Errorf("TELEGRAM_BOT_TOKEN salah: %s", os.Getenv("TELEGRAM_BOT_TOKEN"))
	}
	if os.Getenv("TURSO_DATABASE_URL") != "libsql://example.turso.io" {
		t.Errorf("TURSO_DATABASE_URL salah: %s", os.Getenv("TURSO_DATABASE_URL"))
	}
	if os.Getenv("TURSO_AUTH_TOKEN") != "secret_jwt_token" {
		t.Errorf("TURSO_AUTH_TOKEN salah: %s", os.Getenv("TURSO_AUTH_TOKEN"))
	}
	if os.Getenv("MAX_QUEUE_SIZE") != "25" {
		t.Errorf("MAX_QUEUE_SIZE inline comment handling salah: %s", os.Getenv("MAX_QUEUE_SIZE"))
	}
}

func TestLoadEnv_WithBOM(t *testing.T) {
	tempDir := t.TempDir()
	envPath := filepath.Join(tempDir, ".env")

	// Prepend UTF-8 BOM
	bomData := append([]byte("\xef\xbb\xbf"), []byte("TELEGRAM_BOT_TOKEN=bom_token\n")...)
	if err := os.WriteFile(envPath, bomData, 0644); err != nil {
		t.Fatalf("Gagal menulis file test .env: %v", err)
	}

	_ = os.Unsetenv("TELEGRAM_BOT_TOKEN")
	if err := LoadEnv(envPath); err != nil {
		t.Fatalf("LoadEnv dengan BOM gagal: %v", err)
	}

	if os.Getenv("TELEGRAM_BOT_TOKEN") != "bom_token" {
		t.Errorf("BOM tidak teratasi dengan benar: %s", os.Getenv("TELEGRAM_BOT_TOKEN"))
	}
}

func TestConfig_Load(t *testing.T) {
	_ = os.Setenv("TELEGRAM_BOT_TOKEN", "mock_bot_token")
	_ = os.Setenv("MAX_CONCURRENT_WORKERS", "4")
	_ = os.Setenv("WHITELIST_USER_IDS", "1001, 1002")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() gagal: %v", err)
	}

	if cfg.TelegramBotToken != "mock_bot_token" {
		t.Errorf("TelegramBotToken salah: %s", cfg.TelegramBotToken)
	}
	if cfg.MaxWorkers != 4 {
		t.Errorf("MaxWorkers salah: %d", cfg.MaxWorkers)
	}
	if len(cfg.WhitelistIDs) != 2 || cfg.WhitelistIDs[0] != 1001 {
		t.Errorf("WhitelistIDs salah: %v", cfg.WhitelistIDs)
	}
}
