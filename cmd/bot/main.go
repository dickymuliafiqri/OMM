package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"benchmark/internal/benchmark"
	"benchmark/internal/bot"
	"benchmark/internal/config"
	"benchmark/internal/health"
	"benchmark/internal/pricing"
	"benchmark/internal/queue"
	"benchmark/internal/sandbox"
	"benchmark/internal/storage"
	"benchmark/pkg/logger"
)

func main() {
	logger.Sys("BOOT", "Memulai On My Mark AI Benchmark Bot...")

	// 1. Load configuration from .env & environment
	cfg, err := config.Load()
	if err != nil {
		logger.Error("BOOT", "Konfigurasi gagal dimuat: %v", err)
		os.Exit(1)
	}

	// 2. Initialize Turso / LibSQL database
	db, err := storage.NewTursoDB(cfg.TursoDatabaseURL, cfg.TursoAuthToken)
	if err != nil {
		logger.Error("BOOT", "Gagal inisialisasi basis data: %v", err)
		os.Exit(1)
	}
	defer db.Close()

	repo := storage.NewRepository(db)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := repo.Migrate(ctx); err != nil {
		logger.Error("BOOT", "Gagal migrasi basis data: %v", err)
		os.Exit(1)
	}
	logger.Sys("BOOT", "Basis data Turso/LibSQL berhasil dihubungkan dan dimigrasi")

	// 3. Initialize Sandbox & Benchmark Engine (use AST_STRICT_MODE from configuration)
	runner := sandbox.NewRunner(cfg.ASTStrictMode)
	engine := benchmark.NewEngine()

	// 4. Initialize worker pool
	pool := queue.NewWorkerPool(cfg.MaxWorkers, cfg.MaxQueueSize, runner, engine, repo)
	pool.Start()
	logger.Sys("BOOT", "Worker Pool aktif (workers=%d queue_capacity=%d)", cfg.MaxWorkers, cfg.MaxQueueSize)

	// Periodically sync token pricing (OpenRouter API) asynchronously in background and persist to Turso database
	go pricing.StartPeriodicSync(ctx, repo, 12*time.Hour)

	// 5. Initialize Health Check & Observability Server
	healthServer := health.NewServer(cfg.HealthCheckPort, repo, pool)
	go func() {
		if err := healthServer.Start(); err != nil {
			logger.Warn("HEALTH", "Health check server error: %v", err)
		}
	}()

	// 6. Initialize & run Telegram Bot
	botInstance, err := bot.NewBot(bot.BotConfig{
		Token:         cfg.TelegramBotToken,
		HourlyLimit:   cfg.HourlyLimit,
		AdminIDs:      cfg.BotAdminIDs,
		WhitelistMode: cfg.WhitelistMode,
		WhitelistIDs:  cfg.WhitelistIDs,
	}, pool, repo)
	if err != nil {
		logger.Error("BOOT", "Gagal inisialisasi Telegram Bot: %v", err)
		os.Exit(1)
	}

	

	// Handle graceful shutdown upon receiving SIGINT/SIGTERM signals
	// Use WaitGroup so main() waits until cleanup finishes before exiting
	var shutdownWg sync.WaitGroup
	shutdownWg.Add(1)
	go func() {
		defer shutdownWg.Done()
		<-ctx.Done()
		logger.Sys("SHUTDOWN", "Menerima sinyal interrupt, menghentikan worker pool dan menutup koneksi...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()

		_ = healthServer.Stop(shutdownCtx)
		_ = pool.Stop(10 * time.Second)
		_ = repo.Close()
		logger.Sys("SHUTDOWN", "Server berhasil dihentikan secara aman")
	}()

	if err := botInstance.Start(ctx); err != nil && err != context.Canceled {
		logger.Error("BOT", "Fatal bot error: %v", err)
		os.Exit(1)
	}

	// Wait for shutdown goroutine to complete before process exits
	shutdownWg.Wait()
}
