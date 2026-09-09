package worker

import (
	"context"
	"fmt"
	"time"

	"benchmark/internal/metrics"
	"benchmark/pkg/logger"
)

// JobExecutor defines the interface for executing a benchmark job.
type JobExecutor interface {
	Execute(ctx context.Context, job *BenchJob)
}

// DefaultExecutor orchestrates the execution lifecycle of a benchmark job.
type DefaultExecutor struct {
	callback *CallbackClient
	ladder   LadderRunner
	metrics  *metrics.Metrics
}

// NewDefaultExecutor creates an executor with the given callback client and ladder runner.
func NewDefaultExecutor(callback *CallbackClient, ladder LadderRunner, m ...*metrics.Metrics) *DefaultExecutor {
	if callback == nil {
		callback = DefaultCallbackClient
	}
	if ladder == nil {
		ladder = NewDefaultLadderRunner(nil)
	}

	met := metrics.Default
	if len(m) > 0 && m[0] != nil {
		met = m[0]
	}

	exec := &DefaultExecutor{
		callback: callback,
		ladder:   ladder,
		metrics:  met,
	}

	if defRunner, ok := ladder.(*DefaultLadderRunner); ok && defRunner != nil {
		defRunner.OnCheckpoint = func(job *BenchJob, cp *LadderResult) {
			go func() {
				chkCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = exec.dispatchCheckpoint(chkCtx, job, cp)
			}()
		}
	}

	return exec
}

// Execute orchestrates the job lifecycle: progress callbacks, ladder evaluation, result dispatch, and API key zeroing.
func (e *DefaultExecutor) Execute(ctx context.Context, job *BenchJob) {
	// 1. Ensure API key is zeroed out immediately upon exit
	defer func() {
		job.ZeroAPIKey()
		logger.Debug("executor.zero_key", "jobId=%s apiKey wiped from memory", job.JobID)
	}()

	logger.Sys("WORKER", "Memulai eksekusi benchmark | jobId=%s model=%s", job.JobID, job.Model)
	logger.Debug("executor.start", "jobId=%s model=%s baseUrl=%s callbackUrl=%s", job.JobID, job.Model, job.BaseURL, job.CallbackURL)

	currentPhase := "INITIALIZING"

	// 2. Initial progress callback: INITIALIZING (0%)
	_ = e.callback.SendProgress(ctx, job.CallbackURL, job.CallbackSecret, ProgressPayload{
		Type:     "progress",
		Phase:    "INITIALIZING",
		Message:  fmt.Sprintf("Job benchmark dimulai untuk model %s", job.Model),
		Progress: 0,
	})

	// 3. Define callback hooks for streaming progress & logs
	onProgress := func(phase string, progress int, message string, tokens ...int) {
		currentPhase = phase
		payload := ProgressPayload{
			Type:     "progress",
			Phase:    phase,
			Message:  message,
			Progress: progress,
		}
		if len(tokens) >= 2 {
			payload.PromptTokens = tokens[0]
			payload.CompletionTokens = tokens[1]
			if len(tokens) >= 3 {
				payload.TotalTokens = tokens[2]
			} else {
				payload.TotalTokens = payload.PromptTokens + payload.CompletionTokens
			}
		}
		logger.Debug("executor.progress", "jobId=%s phase=%s progress=%d%% tokens=%d msg=%s", job.JobID, phase, progress, payload.TotalTokens, message)
		_ = e.callback.SendProgress(ctx, job.CallbackURL, job.CallbackSecret, payload)
	}

	onLog := func(line string, level string) {
		logger.Debug("executor.client_log", "jobId=%s level=%s msg=%s", job.JobID, level, line)
		_ = e.callback.SendLog(ctx, job.CallbackURL, job.CallbackSecret, LogPayload{
			Type:      "log",
			Line:      line,
			Level:     level,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}

	// 4. Run 4-Tier Ladder evaluation
	ladderRes, err := e.ladder.RunLadder(ctx, job, onProgress, onLog)
	if err != nil {
		logger.Error("bench.error", "jobId=%s phase=%s error=%q", job.JobID, currentPhase, err.Error())
		if e.metrics != nil {
			e.metrics.JobsFailed.Add(1)
		}

		// Fair partial-credit path: the ladder may have completed some tasks
		// before failing (timeout, cancellation, infra error). Dispatch the
		// earned results first so omm-web can persist and score them, then
		// send the error notification.
		var benchRun *BenchmarkRun
		var taskRuns []TaskRun
		if ladderRes != nil && len(ladderRes.TaskResults) > 0 {
			logger.Info("bench.partial", "jobId=%s earnedPts=%d/%d completedTasks=%d status=%s",
				job.JobID, ladderRes.TotalScore, ladderRes.MaxScore, len(ladderRes.TaskResults), ladderRes.Status)
			if sendErr := e.dispatchResult(ctx, job, ladderRes, err.Error()); sendErr != nil {
				logger.Warn("WORKER", "Gagal mengirimkan partial result callback jobId=%s: %v", job.JobID, sendErr)
			} else {
				logger.Sys("WORKER", "Partial result terkirim jobId=%s (Skor %d/%d, %d task selesai)",
					job.JobID, ladderRes.TotalScore, ladderRes.MaxScore, len(ladderRes.TaskResults))
			}

			nowStr := time.Now().UTC().Format(time.RFC3339)
			benchRun = &BenchmarkRun{
				ID:               job.JobID,
				JobID:            job.JobID,
				ProviderBaseURL:  job.BaseURL,
				ModelName:        job.Model,
				TotalScore:       ladderRes.TotalScore,
				MaxScore:         ladderRes.MaxScore,
				ExecutionTimeMs:  ladderRes.ExecutionTimeMs,
				PromptTokens:     ladderRes.PromptTokens,
				CompletionTokens: ladderRes.CompletionTokens,
				TotalTokens:      ladderRes.TotalTokens,
				EstimatedCostUSD: ladderRes.EstimatedCostUSD,
				CostTier:         ladderRes.CostTier,
				Status:           ladderRes.Status,
				BenchmarkMode:    "swe",
				MaxTierAchieved:  ladderRes.MaxTierAchieved,
				ErrorSummary:     err.Error(),
				CreatedAt:        nowStr,
			}
			for _, res := range ladderRes.TaskResults {
				taskRuns = append(taskRuns, TaskRun{
					RunID:         job.JobID,
					TaskID:        res.TaskID,
					TaskTitle:     res.TaskID,
					Tier:          string(res.Tier),
					PointsAwarded: res.Points,
					MaxPoints:     res.MaxPoints,
					Resolved:      res.Resolved,
					HasRace:       res.HasRace,
					Attempts:      res.Attempts,
					TestOutput:    res.TestOutput,
					CreatedAt:     nowStr,
				})
			}
		}

		// Send error notification callback to omm-web (includes partial results)
		errCtx := ctx
		if ctx.Err() != nil {
			var cancel context.CancelFunc
			errCtx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
		}
		_ = e.callback.SendError(errCtx, job.CallbackURL, job.CallbackSecret, ErrorPayload{
			Type:         "error",
			Message:      fmt.Sprintf("Kegagalan eksekusi benchmark: %v", err),
			Phase:        currentPhase,
			BenchmarkRun: benchRun,
			TaskRuns:     taskRuns,
		})
		return
	}

	durationSec := float64(ladderRes.ExecutionTimeMs) / 1000.0
	logger.Info("bench.complete", "jobId=%s score=%d/%d tier=%s duration=%.1fs",
		job.JobID, ladderRes.TotalScore, ladderRes.MaxScore, ladderRes.MaxTierAchieved, durationSec)

	if e.metrics != nil {
		e.metrics.JobsCompleted.Add(1)
	}

	// 5. Final progress callback: COMPLETING (100%)
	_ = e.callback.SendProgress(ctx, job.CallbackURL, job.CallbackSecret, ProgressPayload{
		Type:             "progress",
		Phase:            "COMPLETING",
		Message:          "Evaluasi selesai. Mengagregasi skor dan mengirimkan hasil...",
		Progress:         100,
		PromptTokens:     ladderRes.PromptTokens,
		CompletionTokens: ladderRes.CompletionTokens,
		TotalTokens:      ladderRes.TotalTokens,
	})

	// 6-7. Build and dispatch the final result callback with retries
	var errSummary string
	if ladderRes.Status == "FAILED" {
		errSummary = "Model gagal menyelesaikan fase pengujian."
	} else if ladderRes.Status == "PARTIAL" {
		errSummary = "Evaluasi dihentikan lebih awal: model gagal menyelesaikan salah satu task/tier sehingga fase berikutnya dilewati."
	}
	if err := e.dispatchResult(ctx, job, ladderRes, errSummary); err != nil {
		logger.Warn("WORKER", "Gagal mengirimkan callback hasil akhir jobId=%s: %v", job.JobID, err)
	} else {
		logger.Sys("WORKER", "Sukses mengirimkan hasil akhir jobId=%s (Skor %d/%d, Tier: %s)",
			job.JobID, ladderRes.TotalScore, ladderRes.MaxScore, ladderRes.MaxTierAchieved)
	}
}

// dispatchResult builds the BenchmarkRun/TaskRuns payload from a ladder result
// (full or partial) and delivers it to omm-web with retries. Shared by the
// success path and the fair partial-credit error path.
func (e *DefaultExecutor) dispatchResult(ctx context.Context, job *BenchJob, ladderRes *LadderResult, errorSummary ...string) error {
	nowStr := time.Now().UTC().Format(time.RFC3339)
	benchRun := BenchmarkRun{
		ID:               job.JobID,
		JobID:            job.JobID,
		ProviderBaseURL:  job.BaseURL,
		ModelName:        job.Model,
		TotalScore:       ladderRes.TotalScore,
		MaxScore:         ladderRes.MaxScore,
		ExecutionTimeMs:  ladderRes.ExecutionTimeMs,
		PromptTokens:     ladderRes.PromptTokens,
		CompletionTokens: ladderRes.CompletionTokens,
		TotalTokens:      ladderRes.TotalTokens,
		EstimatedCostUSD: ladderRes.EstimatedCostUSD,
		CostTier:         ladderRes.CostTier,
		Status:           ladderRes.Status,
		BenchmarkMode:    "swe",
		MaxTierAchieved:  ladderRes.MaxTierAchieved,
		CreatedAt:        nowStr,
	}

	var taskRuns []TaskRun
	for _, res := range ladderRes.TaskResults {
		taskRuns = append(taskRuns, TaskRun{
			RunID:         job.JobID,
			TaskID:        res.TaskID,
			TaskTitle:     res.TaskID,
			Tier:          string(res.Tier),
			PointsAwarded: res.Points,
			MaxPoints:     res.MaxPoints,
			Resolved:      res.Resolved,
			HasRace:       res.HasRace,
			Attempts:      res.Attempts,
			TestOutput:    res.TestOutput,
			CreatedAt:     nowStr,
		})
	}

	// Attach the failure reason when dispatching partial results so omm-web
	// can explain why the run was interrupted.
	if len(errorSummary) > 0 && errorSummary[0] != "" {
		benchRun.ErrorSummary = errorSummary[0]
	}

	resultPayload := ResultPayload{
		Type:         "result",
		BenchmarkRun: benchRun,
		TaskRuns:     taskRuns,
	}

	logger.Debug("executor.result_dispatch", "jobId=%s tasks=%d totalScore=%d/%d tokens=%d costUSD=$%.4f status=%s",
		job.JobID, len(taskRuns), benchRun.TotalScore, benchRun.MaxScore, benchRun.TotalTokens, benchRun.EstimatedCostUSD, benchRun.Status)

	cbCtx := ctx
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		cbCtx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
	}

	return e.callback.SendResult(cbCtx, job.CallbackURL, job.CallbackSecret, resultPayload)
}

// dispatchCheckpoint delivers intermediate benchmark progress to omm-web as tests complete.
func (e *DefaultExecutor) dispatchCheckpoint(ctx context.Context, job *BenchJob, ladderRes *LadderResult) error {
	nowStr := time.Now().UTC().Format(time.RFC3339)
	benchRun := BenchmarkRun{
		ID:               job.JobID,
		JobID:            job.JobID,
		ProviderBaseURL:  job.BaseURL,
		ModelName:        job.Model,
		TotalScore:       ladderRes.TotalScore,
		MaxScore:         ladderRes.MaxScore,
		ExecutionTimeMs:  ladderRes.ExecutionTimeMs,
		PromptTokens:     ladderRes.PromptTokens,
		CompletionTokens: ladderRes.CompletionTokens,
		TotalTokens:      ladderRes.TotalTokens,
		EstimatedCostUSD: ladderRes.EstimatedCostUSD,
		CostTier:         ladderRes.CostTier,
		Status:           ladderRes.Status,
		BenchmarkMode:    "swe",
		MaxTierAchieved:  ladderRes.MaxTierAchieved,
		CreatedAt:        nowStr,
		IsCheckpoint:     true,
	}

	var taskRuns []TaskRun
	for _, res := range ladderRes.TaskResults {
		taskRuns = append(taskRuns, TaskRun{
			RunID:         job.JobID,
			TaskID:        res.TaskID,
			TaskTitle:     res.TaskID,
			Tier:          string(res.Tier),
			PointsAwarded: res.Points,
			MaxPoints:     res.MaxPoints,
			Resolved:      res.Resolved,
			HasRace:       res.HasRace,
			Attempts:      res.Attempts,
			TestOutput:    res.TestOutput,
			CreatedAt:     nowStr,
		})
	}

	resultPayload := ResultPayload{
		Type:         "result",
		BenchmarkRun: benchRun,
		TaskRuns:     taskRuns,
	}

	logger.Debug("executor.checkpoint_dispatch", "jobId=%s tasks=%d score=%d/%d", job.JobID, len(taskRuns), benchRun.TotalScore, benchRun.MaxScore)
	return e.callback.SendResult(ctx, job.CallbackURL, job.CallbackSecret, resultPayload)
}
