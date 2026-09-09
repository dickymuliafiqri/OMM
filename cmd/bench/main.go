package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"benchmark/internal/config"
	"benchmark/internal/metrics"
	"benchmark/internal/server"
	"benchmark/internal/swe"
	"benchmark/internal/worker"
	_ "benchmark/internal/swe/tasks"
	"benchmark/pkg/logger"
)

const (
	defaultShutdownTimeout = 30 * time.Second
)

func main() {
	// 1. Parse configuration from environment & optional .env
	cfg, err := config.Load()
	if err != nil {
		logger.Error("CONFIG", "Gagal memuat konfigurasi: %v", err)
		os.Exit(1)
	}

	// 2. Configure logger level
	logger.SetLevel(logger.ParseLevel(cfg.LogLevel))

	// 3. Log startup information (secrets are never logged)
	originsJSON, _ := json.Marshal(cfg.AllowedOrigins)
	logger.Info("server.start", "listen=%s workers=%d ratelimit=%d/min", cfg.ListenAddr, cfg.MaxConcurrentJobs, cfg.GlobalRateLimit)
	logger.Info("config.loaded", "secret=configured origins=%s", string(originsJSON))

	// 4. Inisialisasi Rate Limiter, Worker Pool, & HTTP router dengan middleware lengkap
	rateLimiter := server.NewRateLimiter(cfg.GlobalRateLimit, cfg.PerSourceRateLimit)
	defer rateLimiter.Close()

	cbClient := worker.NewCallbackClient(10*time.Second, 1*time.Second)
	ladderRunner := worker.NewDefaultLadderRunner(swe.NewEvaluator(true))
	ladderRunner.TaskTimeout = time.Duration(cfg.TaskTimeoutSec) * time.Second
	ladderRunner.MaxInferenceRetries = 1
	executor := worker.NewDefaultExecutor(cbClient, ladderRunner, metrics.Default)
	pool := worker.NewPool(context.Background(), cfg.MaxConcurrentJobs, time.Duration(cfg.JobTimeoutSec)*time.Second, executor, metrics.Default)

	router := server.NewRouter(server.RouterConfig{
		Config:      cfg,
		RateLimiter: rateLimiter,
		Pool:        pool,
		Metrics:     metrics.Default,
	})

	logger.Debug("subsystems.init", "workers=%d timeout=%ds global_ratelimit=%d/min persource_ratelimit=%d/min allow_localhost=%t log_level=%s", cfg.MaxConcurrentJobs, cfg.JobTimeoutSec, cfg.GlobalRateLimit, cfg.PerSourceRateLimit, cfg.AllowLocalhost, cfg.LogLevel)

	// 5. Configure HTTP Server with secure timeouts
	httpSrv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// 6. Graceful shutdown setup (SIGINT / SIGTERM)
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	serverErr := make(chan error, 1)
	go func() {
		logger.Sys("SERVER", "HTTP server aktif dan mendengarkan di %s", cfg.ListenAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// 7. Wait for shutdown signal or fatal server error
	select {
	case err := <-serverErr:
		logger.Error("SERVER", "Fatal server error: %v", err)
		os.Exit(1)

	case sig := <-stopChan:
		logger.Sys("SHUTDOWN", "Signal %s diterima, memulai graceful shutdown...", sig.String())

		// Drain active benchmark jobs first
		drainTimeout := defaultShutdownTimeout / 2
		logger.Sys("SHUTDOWN", "Menunggu pekerjaan benchmark selesai (drain timeout: %v)...", drainTimeout)
		if err := pool.Drain(drainTimeout); err != nil {
			logger.Warn("SHUTDOWN", "Peringatan worker pool drain: %v", err)
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout/2)
		defer cancel()

		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			logger.Error("SHUTDOWN", "Server shutdown paksa karena error: %v", err)
			_ = httpSrv.Close()
			os.Exit(1)
		}

		logger.Sys("SHUTDOWN", "Server berhasil ditutup dengan bersih.")
	}
}
