package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"benchmark/internal/ai"
	"benchmark/internal/swe"
	"benchmark/pkg/logger"
)

// LadderResult encapsulates the outcome of a complete 4-tier ladder benchmark session.
type LadderResult struct {
	TotalScore       int               `json:"totalScore"`
	MaxScore         int               `json:"maxScore"`
	ExecutionTimeMs  int64             `json:"executionTimeMs"`
	PromptTokens     int               `json:"promptTokens"`
	CompletionTokens int               `json:"completionTokens"`
	TotalTokens      int               `json:"totalTokens"`
	EstimatedCostUSD float64           `json:"estimatedCostUsd"`
	CostTier         string            `json:"costTier"`
	Status           string            `json:"status"`          // "SUCCESS", "PARTIAL", "FAILED"
	MaxTierAchieved  string            `json:"maxTierAchieved"` // "NONE", "JUNIOR", "MID", "SENIOR", "STAFF"
	TaskResults      []*swe.EvalResult `json:"taskResults"`
}

// LadderRunner coordinates the execution of tasks across the 4-tier ladder.
type LadderRunner interface {
	RunLadder(
		ctx context.Context,
		job *BenchJob,
		onProgress func(phase string, progress int, message string, tokens ...int),
		onLog func(line string, level string),
	) (*LadderResult, error)
}

// TaskEvaluator defines the evaluation contract for SWE tasks.
type TaskEvaluator interface {
	Evaluate(ctx context.Context, task *swe.Task, solutionCode string, attempt int) (*swe.EvalResult, error)
}

// DefaultLadderRunner implements standard 4-tier ladder evaluation with self-healing feedback.
type DefaultLadderRunner struct {
	Evaluator           TaskEvaluator
	Tasks               []*swe.Task // Optional custom task set; if nil, uses swe.DefaultLadder()
	OnCheckpoint        func(job *BenchJob, res *LadderResult)
	TaskTimeout         time.Duration // Timeout for each individual task (default: 180s)
	MaxInferenceRetries int           // Number of retries on transient inference failure (default: 0)
}

// NewDefaultLadderRunner creates a DefaultLadderRunner.
func NewDefaultLadderRunner(evaluator TaskEvaluator) *DefaultLadderRunner {
	if evaluator == nil {
		evaluator = swe.NewEvaluator(true)
	}
	return &DefaultLadderRunner{
		Evaluator:           evaluator,
		TaskTimeout:         180 * time.Second,
		MaxInferenceRetries: 0,
	}
}

// RunLadder runs the full 4-tier ladder evaluation using the provided AI credentials.
func (r *DefaultLadderRunner) RunLadder(
	ctx context.Context,
	job *BenchJob,
	onProgress func(phase string, progress int, message string, tokens ...int),
	onLog func(line string, level string),
) (*LadderResult, error) {
	start := time.Now()

	// 1. Resolve task list (prefer full 8-task ladder, fallback to default 4-task ladder)
	taskList := r.Tasks
	if len(taskList) == 0 {
		var err error
		taskList, err = swe.FullLadder()
		if err != nil || len(taskList) == 0 {
			taskList, err = swe.DefaultLadder()
			if err != nil {
				return nil, fmt.Errorf("gagal memuat daftar task ladder: %w", err)
			}
		}
	}

	taskTimeout := r.TaskTimeout
	if taskTimeout <= 0 {
		taskTimeout = 180 * time.Second
	}

	// 2. Initialize AI client with timeout aligned with per-task timeout
	aiClient, err := ai.NewClient(ai.Config{
		BaseURL: job.BaseURL,
		APIKey:  job.APIKey,
		Model:   job.Model,
		Timeout: taskTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("gagal menginisialisasi AI client: %w", err)
	}

	totalScore := 0
	maxScore := 0
	totalPromptTokens := 0
	totalCompletionTokens := 0
	maxTierAchieved := "NONE"
	var taskResults []*swe.EvalResult

	totalTasks := len(taskList)
	logger.Debug("ladder.tasks_loaded", "jobId=%s totalTasks=%d taskTimeout=%v", job.JobID, totalTasks, taskTimeout)

	for i, task := range taskList {
		if err := ctx.Err(); err != nil {
			// Interruption (timeout/cancel/infra failure): return the partial
			// results collected so far so completed tasks can still be scored
			// fairly. The caller decides how to deliver them alongside the error.
			return buildPartialLadderResult(taskList, taskResults, totalScore, totalPromptTokens, totalCompletionTokens, start, err)
		}

		maxScore += task.Points
		phase := tierToPhase(task.Tier)

		logger.Debug("ladder.task_begin", "jobId=%s taskIndex=%d/%d taskId=%s tier=%s pts=%d taskTimeout=%v", job.JobID, i+1, totalTasks, task.ID, task.Tier, task.Points, taskTimeout)

		// Calculate progress mapped across 5% to 95%
		progressPct := 5 + int(float64(i)/float64(totalTasks)*90.0)
		logger.Info("bench.progress", "jobId=%s phase=%s progress=%d", job.JobID, phase, progressPct)
		if onProgress != nil {
			onProgress(phase, progressPct, fmt.Sprintf("Menguji Tier %s (%d pts): %s", task.Tier, task.Points, task.Title), totalPromptTokens, totalCompletionTokens)
		}

		if onLog != nil {
			onLog(fmt.Sprintf("▶ [%d/%d] [%s] %s (%s - %d pts)", i+1, totalTasks, task.Tier, task.Title, task.ID, task.Points), "info")
		}

		// Dedicated per-task timeout context
		taskCtx, taskCancel := context.WithTimeout(ctx, taskTimeout)

		messages := []ai.ChatMessage{
			{Role: "system", Content: swe.SWESystemPrompt},
			{Role: "user", Content: swe.BuildTaskPrompt(task)},
		}

		var lastEvalRes *swe.EvalResult
		var lastErrDesc string

		// Self-healing feedback loop: max 2 attempts (Turn 1: 100%, Turn 2: 80%)
		for attempt := 1; attempt <= 2; attempt++ {
			if err := taskCtx.Err(); err != nil {
				// Interruption: check if parent job was cancelled
				if ctx.Err() != nil {
					taskCancel()
					if lastEvalRes != nil {
						taskResults = append(taskResults, lastEvalRes)
						totalScore += lastEvalRes.Points
					}
					return buildPartialLadderResult(taskList, taskResults, totalScore, totalPromptTokens, totalCompletionTokens, start, ctx.Err())
				}
				// Per-task timeout expired
				lastErrDesc = fmt.Sprintf("Batas waktu per-task terlampaui (%v)", taskTimeout)
				logger.Warn("ladder.task_timeout", "jobId=%s taskId=%s attempt=%d timeout=%v", job.JobID, task.ID, attempt, taskTimeout)
				if onLog != nil {
					onLog(fmt.Sprintf("  ⏱️ Batas waktu per-task terlampaui (%v). Task %s dihentikan.", taskTimeout, task.ID), "error")
				}
				break
			}

			logger.Debug("ladder.attempt_start", "jobId=%s taskId=%s attempt=%d/2 promptLen=%d", job.JobID, task.ID, attempt, len(messages[len(messages)-1].Content))

			if onLog != nil {
				if attempt == 1 {
					onLog(fmt.Sprintf("  ↳ Turn 1: Mengirim prompt bug-fixing ke model %s...", job.Model), "info")
				} else {
					onLog("  ↳ Turn 2: Mengirim umpan balik kompilator/test runner untuk perbaikan mandiri...", "warn")
				}
			}

			// Execute AI inference with transient retry logic
			var chatRes *ai.ChatResult
			var chatErr error
			maxRetries := r.MaxInferenceRetries
			if maxRetries < 0 {
				maxRetries = 0
			}
			for retry := 0; retry <= maxRetries; retry++ {
				chatRes, chatErr = aiClient.Chat(taskCtx, messages)
				if chatErr == nil {
					break
				}
				if taskCtx.Err() != nil {
					break
				}
				if retry < maxRetries {
					logger.Warn("ai.inference_retry", "jobId=%s taskId=%s attempt=%d retry=%d err=%v", job.JobID, task.ID, attempt, retry+1, chatErr)
					if onLog != nil {
						onLog(fmt.Sprintf("  ⚠️ Error inferensi AI (%v). Mencoba kembali (%d/%d)...", chatErr, retry+1, maxRetries), "warn")
					}
					retryDelay := 1 * time.Second
					if d, ok := taskCtx.Deadline(); ok {
						if rem := time.Until(d); rem < 5*time.Second && rem > 100*time.Millisecond {
							retryDelay = rem / 10
							if retryDelay < 50*time.Millisecond {
								retryDelay = 50 * time.Millisecond
							}
						}
					}
					select {
					case <-taskCtx.Done():
					case <-time.After(retryDelay):
					}
				}
			}

			if chatErr != nil {
				lastErrDesc = fmt.Sprintf("Error inferensi AI: %v", chatErr)
				logger.Warn("bench.task.fail", "jobId=%s taskId=%s attempt=%d reason=ai_inference_error", job.JobID, task.ID, attempt)
				if onLog != nil {
					onLog(fmt.Sprintf("  ❌ Error inferensi AI pada Turn %d: %v", attempt, chatErr), "error")
				}
				if ctx.Err() != nil {
					taskCancel()
					if lastEvalRes != nil {
						taskResults = append(taskResults, lastEvalRes)
						totalScore += lastEvalRes.Points
					}
					return buildPartialLadderResult(taskList, taskResults, totalScore, totalPromptTokens, totalCompletionTokens, start, ctx.Err())
				}
				break
			}

			logger.Debug("ladder.ai_received", "jobId=%s taskId=%s attempt=%d inTokens=%d outTokens=%d contentBytes=%d", job.JobID, task.ID, attempt, chatRes.PromptTokens, chatRes.CompletionTokens, len(chatRes.Content))

			totalPromptTokens += chatRes.PromptTokens
			totalCompletionTokens += chatRes.CompletionTokens

			if onProgress != nil {
				onProgress(phase, progressPct, fmt.Sprintf("Inference selesai: %d in / %d out tokens", chatRes.PromptTokens, chatRes.CompletionTokens), totalPromptTokens, totalCompletionTokens)
			}

			code, extractErr := swe.ExtractSWECode(chatRes.Content)
			if extractErr != nil {
				lastErrDesc = fmt.Sprintf("Gagal mengekstrak blok kode Go: %v", extractErr)
				logger.Warn("bench.task.fail", "jobId=%s taskId=%s attempt=%d reason=code_extraction_failed", job.JobID, task.ID, attempt)
				if onLog != nil {
					onLog(fmt.Sprintf("  ⚠️ Gagal mengekstrak blok kode Go: %v", extractErr), "warn")
				}
				if attempt < 2 {
					messages = append(messages, ai.ChatMessage{Role: "assistant", Content: chatRes.Content})
					messages = append(messages, ai.ChatMessage{Role: "user", Content: "Mohon kembalikan KODE GO UTUH yang dapat dikompilasi di dalam blok ```go ... ```."})
					continue
				}
				break
			}

			logger.Debug("ladder.code_extracted", "jobId=%s taskId=%s attempt=%d codeLines=%d bytes=%d", job.JobID, task.ID, attempt, strings.Count(code, "\n")+1, len(code))

			evalRes, evalErr := r.Evaluator.Evaluate(taskCtx, task, code, attempt)
			if evalErr != nil {
				lastErrDesc = fmt.Sprintf("Error evaluator sandbox: %v", evalErr)
				logger.Warn("bench.task.fail", "jobId=%s taskId=%s attempt=%d reason=evaluator_error", job.JobID, task.ID, attempt)
				if onLog != nil {
					onLog(fmt.Sprintf("  ❌ Error evaluator sandbox: %v", evalErr), "error")
				}
				if ctx.Err() != nil {
					taskCancel()
					if lastEvalRes != nil {
						taskResults = append(taskResults, lastEvalRes)
						totalScore += lastEvalRes.Points
					}
					return buildPartialLadderResult(taskList, taskResults, totalScore, totalPromptTokens, totalCompletionTokens, start, ctx.Err())
				}
				break
			}

			lastEvalRes = evalRes

			logger.Debug("ladder.eval_finished", "jobId=%s taskId=%s attempt=%d resolved=%v hasRace=%v pts=%d durationMs=%d compileErr=%v", job.JobID, task.ID, attempt, evalRes.Resolved, evalRes.HasRace, evalRes.Points, evalRes.DurationMs, evalRes.CompileErr != "")

			if evalRes.Resolved && !evalRes.HasRace {
				logger.Info("bench.task.pass", "jobId=%s taskId=%s pts=%d/%d attempt=%d", job.JobID, task.ID, evalRes.Points, task.Points, attempt)
				if onLog != nil {
					onLog(fmt.Sprintf("  ✅ LOLOS pada Turn %d! (%d/%d pts, Durasi: %dms, Bebas Data Race)", attempt, evalRes.Points, task.Points, evalRes.DurationMs), "success")
				}
				break
			}

			failReason := "test_failure"
			if evalRes.HasRace {
				failReason = "data_race"
			} else if evalRes.CompileErr != "" {
				failReason = "compile_error"
			}
			logger.Warn("bench.task.fail", "jobId=%s taskId=%s attempt=%d reason=%s", job.JobID, task.ID, attempt, failReason)

			if attempt < 2 {
				failureMsg := evalRes.TestOutput
				if evalRes.CompileErr != "" {
					failureMsg = evalRes.CompileErr
				}
				logger.Debug("ladder.self_healing_prep", "jobId=%s taskId=%s failureMsgLen=%d preparing feedback prompt", job.JobID, task.ID, len(failureMsg))
				messages = append(messages, ai.ChatMessage{Role: "assistant", Content: chatRes.Content})
				messages = append(messages, ai.ChatMessage{Role: "user", Content: swe.BuildSelfHealingPrompt(task, failureMsg)})
			} else {
				if onLog != nil {
					onLog(fmt.Sprintf("  ❌ Gagal setelah Turn 2: tidak berhasil memperbaiki task (%d/%d pts)", evalRes.Points, task.Points), "error")
				}
			}
		}

		if lastEvalRes == nil {
			lastEvalRes = &swe.EvalResult{
				TaskID:     task.ID,
				Tier:       task.Tier,
				Points:     0,
				MaxPoints:  task.Points,
				Resolved:   false,
				TestOutput: lastErrDesc,
				DurationMs: 0,
				Attempts:   0,
			}
		}
		taskCancel()

		taskResults = append(taskResults, lastEvalRes)
		totalScore += lastEvalRes.Points

		if r.OnCheckpoint != nil {
			cpMaxScore := maxScore
			for _, rem := range taskList[len(taskResults):] {
				cpMaxScore += rem.Points
			}
			cpCostUSD, cpCostTier := EstimateCost(totalPromptTokens, totalCompletionTokens)
			r.OnCheckpoint(job, &LadderResult{
				TotalScore:       totalScore,
				MaxScore:         cpMaxScore,
				ExecutionTimeMs:  time.Since(start).Milliseconds(),
				PromptTokens:     totalPromptTokens,
				CompletionTokens: totalCompletionTokens,
				TotalTokens:      totalPromptTokens + totalCompletionTokens,
				EstimatedCostUSD: cpCostUSD,
				CostTier:         cpCostTier,
				Status:           "PARTIAL",
				MaxTierAchieved:  computeMaxTierAchieved(taskResults),
				TaskResults:      taskResults,
			})
		}

		// Early termination: jika error atau model gagal menyelesaikan fase dan terdapat
		// test lagi setelah fase itu, lewatkan fase berikutnya dan langsung hitung skor jika ada.
		if !lastEvalRes.Resolved || lastEvalRes.HasRace {
			remainingTasks := totalTasks - (i + 1)
			if remainingTasks > 0 {
				logger.Info("ladder.early_termination", "jobId=%s taskId=%s tier=%s pts=%d resolved=%v hasRace=%v remainingTasks=%d",
					job.JobID, task.ID, task.Tier, lastEvalRes.Points, lastEvalRes.Resolved, lastEvalRes.HasRace, remainingTasks)
				if onLog != nil {
					onLog(fmt.Sprintf("  ⏭️ Model gagal menyelesaikan %s (%s). Evaluasi dihentikan lebih awal — melewatkan %d task berikutnya dan langsung menghitung skor akhir...", phase, task.ID, remainingTasks), "warn")
				}
				break
			}
		}
	}

	// For interrupted runs, include the points of tasks that were never
	// attempted so maxScore still reflects the full ladder.
	if len(taskResults) < totalTasks {
		for _, task := range taskList[len(taskResults):] {
			maxScore += task.Points
		}
	}

	// Compute maxTierAchieved: highest tier where ALL tasks in that tier and preceding tiers passed
	maxTierAchieved = computeMaxTierAchieved(taskResults)

	durationMs := time.Since(start).Milliseconds()
	totTokens := totalPromptTokens + totalCompletionTokens
	costUSD, costTier := EstimateCost(totalPromptTokens, totalCompletionTokens)

	status := "SUCCESS"
	if totalScore == 0 {
		status = "FAILED"
	} else if totalScore < maxScore {
		status = "PARTIAL"
	}

	return &LadderResult{
		TotalScore:       totalScore,
		MaxScore:         maxScore,
		ExecutionTimeMs:  durationMs,
		PromptTokens:     totalPromptTokens,
		CompletionTokens: totalCompletionTokens,
		TotalTokens:      totTokens,
		EstimatedCostUSD: costUSD,
		CostTier:         costTier,
		Status:           status,
		MaxTierAchieved:  maxTierAchieved,
		TaskResults:      taskResults,
	}, nil
}

func tierToPhase(t swe.Tier) string {
	switch swe.NormalizeTier(string(t)) {
	case "LOW":
		return "TESTING_LOW"
	case "MID":
		return "TESTING_MID"
	case "HIGH":
		return "TESTING_HIGH"
	case "ULTRA":
		return "TESTING_ULTRA"
	default:
		return "TESTING_LOW"
	}
}

// computeMaxTierAchieved returns the highest tier where ALL tasks in that tier
// and all preceding tiers passed (resolved, race-free).
func computeMaxTierAchieved(results []*swe.EvalResult) string {
	tierPassed := make(map[string]bool)
	for _, tier := range []string{"LOW", "MID", "HIGH", "ULTRA"} {
		totalInTier := 0
		passedInTier := 0
		for _, res := range results {
			if swe.NormalizeTier(string(res.Tier)) == tier {
				totalInTier++
				if res.Resolved && !res.HasRace {
					passedInTier++
				}
			}
		}
		tierPassed[tier] = totalInTier > 0 && totalInTier == passedInTier
	}

	maxTier := "NONE"
	if tierPassed["LOW"] {
		maxTier = "LOW"
		if tierPassed["MID"] {
			maxTier = "MID"
			if tierPassed["HIGH"] {
				maxTier = "HIGH"
				if tierPassed["ULTRA"] {
					maxTier = "ULTRA"
				}
			}
		}
	}
	return maxTier
}

// buildPartialLadderResult aggregates whatever tasks were completed before the
// ladder was interrupted (context cancelled, timeout, fatal error). Points
// already earned from completed tests are preserved so omm-web can store and
// score the run fairly, regardless of the failure cause.
func buildPartialLadderResult(
	taskList []*swe.Task,
	taskResults []*swe.EvalResult,
	totalScore int,
	totalPromptTokens int,
	totalCompletionTokens int,
	start time.Time,
	interruptErr error,
) (*LadderResult, error) {
	completed := taskResults
	if completed == nil {
		completed = []*swe.EvalResult{}
	}

	// maxScore covers every task in the ladder: attempted tasks (via their
	// results) plus the tasks that were never reached, so the denominator
	// stays comparable with fully-completed runs.
	maxScore := 0
	for _, res := range completed {
		for _, t := range taskList {
			if t.ID == res.TaskID {
				maxScore += t.Points
				break
			}
		}
	}
	for i := len(completed); i < len(taskList); i++ {
		maxScore += taskList[i].Points
	}

	status := "FAILED"
	if totalScore > 0 {
		status = "PARTIAL"
	}

	totTokens := totalPromptTokens + totalCompletionTokens
	costUSD, costTier := EstimateCost(totalPromptTokens, totalCompletionTokens)

	return &LadderResult{
		TotalScore:       totalScore,
		MaxScore:         maxScore,
		ExecutionTimeMs:  time.Since(start).Milliseconds(),
		PromptTokens:     totalPromptTokens,
		CompletionTokens: totalCompletionTokens,
		TotalTokens:      totTokens,
		EstimatedCostUSD: costUSD,
		CostTier:         costTier,
		Status:           status,
		MaxTierAchieved:  computeMaxTierAchieved(completed),
		TaskResults:      completed,
	}, interruptErr
}
