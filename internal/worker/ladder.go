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
		onProgress func(phase string, progress int, message string),
		onLog func(line string, level string),
	) (*LadderResult, error)
}

// DefaultLadderRunner implements standard 4-tier ladder evaluation with self-healing feedback.
type DefaultLadderRunner struct {
	Evaluator *swe.Evaluator
	Tasks     []*swe.Task // Optional custom task set; if nil, uses swe.DefaultLadder()
}

// NewDefaultLadderRunner creates a DefaultLadderRunner.
func NewDefaultLadderRunner(evaluator *swe.Evaluator) *DefaultLadderRunner {
	if evaluator == nil {
		evaluator = swe.NewEvaluator(true)
	}
	return &DefaultLadderRunner{
		Evaluator: evaluator,
	}
}

// RunLadder runs the full 4-tier ladder evaluation using the provided AI credentials.
func (r *DefaultLadderRunner) RunLadder(
	ctx context.Context,
	job *BenchJob,
	onProgress func(phase string, progress int, message string),
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

	// 2. Initialize AI client
	aiClient, err := ai.NewClient(ai.Config{
		BaseURL: job.BaseURL,
		APIKey:  job.APIKey,
		Model:   job.Model,
		Timeout: 120 * time.Second,
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
	logger.Debug("ladder.tasks_loaded", "jobId=%s totalTasks=%d", job.JobID, totalTasks)

	for i, task := range taskList {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		maxScore += task.Points
		phase := tierToPhase(task.Tier)

		logger.Debug("ladder.task_begin", "jobId=%s taskIndex=%d/%d taskId=%s tier=%s pts=%d", job.JobID, i+1, totalTasks, task.ID, task.Tier, task.Points)

		// Calculate progress mapped across 5% to 95%
		progressPct := 5 + int(float64(i)/float64(totalTasks)*90.0)
		logger.Info("bench.progress", "jobId=%s phase=%s progress=%d", job.JobID, phase, progressPct)
		if onProgress != nil {
			onProgress(phase, progressPct, fmt.Sprintf("Menguji Tier %s (%d pts): %s", task.Tier, task.Points, task.Title))
		}

		if onLog != nil {
			onLog(fmt.Sprintf("▶ [%d/%d] [%s] %s (%s - %d pts)", i+1, totalTasks, task.Tier, task.Title, task.ID, task.Points), "info")
		}

		messages := []ai.ChatMessage{
			{Role: "system", Content: swe.SWESystemPrompt},
			{Role: "user", Content: swe.BuildTaskPrompt(task)},
		}

		var lastEvalRes *swe.EvalResult

		// Self-healing feedback loop: max 2 attempts (Turn 1: 100%, Turn 2: 80%)
		for attempt := 1; attempt <= 2; attempt++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			logger.Debug("ladder.attempt_start", "jobId=%s taskId=%s attempt=%d/2 promptLen=%d", job.JobID, task.ID, attempt, len(messages[len(messages)-1].Content))

			if onLog != nil {
				if attempt == 1 {
					onLog(fmt.Sprintf("  ↳ Turn 1: Mengirim prompt bug-fixing ke model %s...", job.Model), "info")
				} else {
					onLog(fmt.Sprintf("  ↳ Turn 2: Mengirim umpan balik kompilator/test runner untuk perbaikan mandiri...", ), "warn")
				}
			}

			chatRes, chatErr := aiClient.Chat(ctx, messages)
			if chatErr != nil {
				logger.Warn("bench.task.fail", "jobId=%s taskId=%s attempt=%d reason=ai_inference_error", job.JobID, task.ID, attempt)
				if onLog != nil {
					onLog(fmt.Sprintf("  ❌ Error inferensi AI pada Turn %d: %v", attempt, chatErr), "error")
				}
				break
			}

			logger.Debug("ladder.ai_received", "jobId=%s taskId=%s attempt=%d inTokens=%d outTokens=%d contentBytes=%d", job.JobID, task.ID, attempt, chatRes.PromptTokens, chatRes.CompletionTokens, len(chatRes.Content))

			totalPromptTokens += chatRes.PromptTokens
			totalCompletionTokens += chatRes.CompletionTokens

			code, extractErr := swe.ExtractSWECode(chatRes.Content)
			if extractErr != nil {
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

			evalRes, evalErr := r.Evaluator.Evaluate(ctx, task, code, attempt)
			if evalErr != nil {
				logger.Warn("bench.task.fail", "jobId=%s taskId=%s attempt=%d reason=evaluator_error", job.JobID, task.ID, attempt)
				if onLog != nil {
					onLog(fmt.Sprintf("  ❌ Error evaluator sandbox: %v", evalErr), "error")
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
				DurationMs: 0,
				Attempts:   0,
			}
		}

		taskResults = append(taskResults, lastEvalRes)
		totalScore += lastEvalRes.Points
	}

	// Compute maxTierAchieved: highest tier where ALL tasks in that tier and preceding tiers passed
	tierPassed := make(map[swe.Tier]bool)
	for _, tier := range []swe.Tier{swe.TierJunior, swe.TierMid, swe.TierSenior, swe.TierStaff} {
		totalInTier := 0
		passedInTier := 0
		for _, res := range taskResults {
			if res.Tier == tier {
				totalInTier++
				if res.Resolved && !res.HasRace {
					passedInTier++
				}
			}
		}
		if totalInTier > 0 && totalInTier == passedInTier {
			tierPassed[tier] = true
		} else {
			tierPassed[tier] = false
		}
	}

	maxTierAchieved = "NONE"
	if tierPassed[swe.TierJunior] {
		maxTierAchieved = "JUNIOR"
		if tierPassed[swe.TierMid] {
			maxTierAchieved = "MID"
			if tierPassed[swe.TierSenior] {
				maxTierAchieved = "SENIOR"
				if tierPassed[swe.TierStaff] {
					maxTierAchieved = "STAFF"
				}
			}
		}
	}

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
	switch strings.ToUpper(string(t)) {
	case "JUNIOR":
		return "TESTING_JUNIOR"
	case "MID":
		return "TESTING_MID"
	case "SENIOR":
		return "TESTING_SENIOR"
	case "STAFF":
		return "TESTING_STAFF"
	default:
		return "TESTING_JUNIOR"
	}
}
