package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"benchmark/internal/ai"
	"benchmark/internal/pricing"
	"benchmark/internal/storage"
	"benchmark/internal/swe"
	_ "benchmark/internal/swe/tasks"
	"benchmark/pkg/logger"
)

var (
	ErrQueueFull     = errors.New("antrean pemrosesan benchmark sedang penuh, silakan coba beberapa saat lagi")
	ErrPoolClosed    = errors.New("worker pool telah dihentikan")
	ErrUserJobActive = errors.New("pengguna sudah memiliki proses benchmark yang sedang berjalan di antrean")
)

// WorkerPool manages the execution of the benchmark queue in a controlled, parallel manner
type WorkerPool struct {
	maxWorkers     int
	queueCapacity  int
	jobs           chan *BenchmarkJob
	sweEvaluator   *swe.Evaluator
	repo           storage.Repository
	sweTaskTimeout time.Duration

	activeUsersMu sync.Mutex
	activeUsers   map[int64]bool

	activeJobs atomic.Int32
	closed     atomic.Bool
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewWorkerPool creates a new WorkerPool instance
func NewWorkerPool(maxWorkers, queueCap int, repo storage.Repository) *WorkerPool {
	if maxWorkers <= 0 {
		maxWorkers = 3
	}
	if queueCap <= 0 {
		queueCap = 20
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		maxWorkers:     maxWorkers,
		queueCapacity:  queueCap,
		jobs:           make(chan *BenchmarkJob, queueCap),
		sweEvaluator:   swe.NewEvaluator(true),
		repo:           repo,
		sweTaskTimeout: 5 * time.Minute, // Default 5 minutes per SWE stage
		activeUsers:    make(map[int64]bool),
		ctx:            ctx,
		cancel:         cancel,
	}
}

// SetSWETaskTimeout sets the maximum duration allowed for each SWE task stage
func (p *WorkerPool) SetSWETaskTimeout(d time.Duration) {
	if d > 0 {
		p.sweTaskTimeout = d
	}
}

// SWETaskTimeout returns the configured timeout for each SWE task stage
func (p *WorkerPool) SWETaskTimeout() time.Duration {
	if p.sweTaskTimeout <= 0 {
		return 5 * time.Minute
	}
	return p.sweTaskTimeout
}

// Start launches worker goroutines according to maxWorkers
func (p *WorkerPool) Start() {
	for i := 1; i <= p.maxWorkers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

// Submit adds a new job to the processing queue (applying backpressure if full and deduplication per user)
func (p *WorkerPool) Submit(job *BenchmarkJob) error {
	if p.closed.Load() {
		return ErrPoolClosed
	}

	p.activeUsersMu.Lock()
	if p.activeUsers[job.TelegramID] {
		p.activeUsersMu.Unlock()
		return ErrUserJobActive
	}
	p.activeUsers[job.TelegramID] = true
	p.activeUsersMu.Unlock()

	select {
	case p.jobs <- job:
		logger.Queue(job.ID, "SUBMITTED", fmt.Sprintf("user_id=%d model=%s queue=%d/%d", job.TelegramID, job.Model, len(p.jobs), cap(p.jobs)))
		return nil
	default:
		// If queue is full, unregister active user
		p.activeUsersMu.Lock()
		delete(p.activeUsers, job.TelegramID)
		p.activeUsersMu.Unlock()
		logger.Warn("QUEUE", "job=%s user_id=%d rejected: queue full (%d/%d)", job.ID, job.TelegramID, len(p.jobs), cap(p.jobs))
		return ErrQueueFull
	}
}

// IsUserActive checks whether a user has an active job in the queue or currently executing
func (p *WorkerPool) IsUserActive(telegramID int64) bool {
	p.activeUsersMu.Lock()
	defer p.activeUsersMu.Unlock()
	return p.activeUsers[telegramID]
}

// ActiveJobs returns the number of jobs currently being processed
func (p *WorkerPool) ActiveJobs() int {
	return int(p.activeJobs.Load())
}

// QueueLength returns the number of queued jobs currently waiting
func (p *WorkerPool) QueueLength() int {
	return len(p.jobs)
}

// MaxWorkers returns the maximum worker pool capacity
func (p *WorkerPool) MaxWorkers() int {
	return p.maxWorkers
}

// QueueCapacity returns the maximum queue capacity
func (p *WorkerPool) QueueCapacity() int {
	return p.queueCapacity
}

// Stop stops the worker pool gracefully (graceful shutdown)
func (p *WorkerPool) Stop(timeout time.Duration) error {
	if p.closed.Swap(true) {
		return nil
	}

	p.cancel()
	close(p.jobs)

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout (%v) menunggu worker selesai", timeout)
	}
}

// worker is a goroutine that processes jobs from the queue
func (p *WorkerPool) worker(id int) {
	defer p.wg.Done()

	for job := range p.jobs {
		p.activeJobs.Add(1)
		logger.Queue(job.ID, "STARTED", fmt.Sprintf("worker=%d model=%s active=%d/%d", id, job.Model, p.activeJobs.Load(), p.maxWorkers))
		p.processJob(job)
		p.activeJobs.Add(-1)
	}
}

// isSWEJobTimeout detects if an error or output indicates a timeout in AI chat, sandbox, or context
func isSWEJobTimeout(err error, output string) bool {
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return true
		}
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline exceeded") {
			return true
		}
	}
	outLower := strings.ToLower(output)
	if strings.Contains(outLower, "test timed out") ||
		strings.Contains(outLower, "panic: test timed out") ||
		strings.Contains(outLower, "context deadline exceeded") {
		return true
	}
	return false
}

// processJob executes the complete cycle of a benchmark job in SWE-bench 4-tier ladder mode
func (p *WorkerPool) processJob(job *BenchmarkJob) {
	defer func() {
		p.activeUsersMu.Lock()
		delete(p.activeUsers, job.TelegramID)
		p.activeUsersMu.Unlock()
	}()

	startTime := time.Now()
	stageTimeout := p.SWETaskTimeout()

	notify := func(info ProgressInfo) {
		if job.OnProgress != nil {
			job.OnProgress(info)
		}
	}

	sendResult := func(res *JobResult) {
		if job.ResultChan != nil {
			select {
			case job.ResultChan <- res:
			default:
			}
		}
	}

	// 1. Initialize AI Client
	notify(ProgressInfo{
		Step:       0,
		Total:      4,
		StatusText: "Menyiapkan pengujian SWE-bench 4-Tier Ladder...",
		Phase:      "PREPARING",
	})

	aiClient, err := ai.NewClient(ai.Config{
		BaseURL: job.BaseURL,
		APIKey:  job.APIKey,
		Model:   job.Model,
		Timeout: stageTimeout,
	})
	if err != nil {
		sendResult(&JobResult{
			JobID: job.ID,
			Error: fmt.Errorf("konfigurasi AI client tidak valid: %w", err),
		})
		return
	}

	// 2. Fetch 4 standard tasks (Tier 1 Junior 20, Tier 2 Mid 25, Tier 3 Senior 30, Tier 4 Staff 25 = 100 Pts)
	ladder, err := swe.DefaultLadder()
	if err != nil {
		sendResult(&JobResult{
			JobID: job.ID,
			Error: fmt.Errorf("gagal memuat task SWE ladder: %w", err),
		})
		return
	}

	totalPromptTokens := 0
	totalCompletionTokens := 0
	totalTokens := 0
	totalScore := 0
	maxTierAchieved := swe.Tier("NONE")
	var sweTaskRuns []storage.SWETaskRun
	var lastFixedCode string

	totalTasks := len(ladder)

	var timedOut bool
	var timeoutReason string
	var ladderFailed bool
	var failReason string

	for taskIdx, task := range ladder {
		step := taskIdx + 1
		tierName := string(task.Tier)
		tierNum := swe.TierOrder(task.Tier)

		if job.Ctx.Err() != nil {
			timedOut = true
			timeoutReason = "Batas waktu (timeout) pekerjaan SWE_JOB terlampaui"
		}

		if timedOut || ladderFailed {
			cancelReason := timeoutReason
			if ladderFailed {
				cancelReason = failReason
			}
			sweTaskRuns = append(sweTaskRuns, storage.SWETaskRun{
				ID:            uuid.New().String(),
				RunID:         job.ID,
				TaskID:        task.ID,
				TaskTitle:     task.Title,
				Tier:          string(task.Tier),
				PointsAwarded: 0,
				MaxPoints:     task.Points,
				Resolved:      false,
				HasRace:       false,
				Attempts:      0,
				TestOutput:    "Dibatalkan: " + cancelReason,
			})
			notify(ProgressInfo{
				Step:        step,
				Total:       totalTasks,
				TaskTitle:   task.Title,
				Tier:        tierName,
				TierNumber:  tierNum,
				Attempt:     0,
				MaxAttempts: 2,
				StatusText:  fmt.Sprintf("❌ Dibatalkan: %s (0/%d pts)", cancelReason, task.Points),
				Phase:       "DONE",
			})
			continue
		}

		stageStartTime := time.Now()
		stageCtx, stageCancel := context.WithTimeout(job.Ctx, stageTimeout)

		taskNotify := func(attempt int, phase, snippet, status string) {
			notify(ProgressInfo{
				Step:        step,
				Total:       totalTasks,
				TaskTitle:   task.Title,
				Tier:        tierName,
				TierNumber:  tierNum,
				Attempt:     attempt,
				MaxAttempts: 2,
				StatusText:  status,
				Phase:       phase,
				Snippet:     snippet,
				Elapsed:     time.Since(stageStartTime),
				Timeout:     stageTimeout,
			})
		}

		taskNotify(1, "PREPARING", "", fmt.Sprintf("Menguji Tier %d (%s): %s...", tierNum, tierName, task.Title))

		messages := []ai.ChatMessage{
			{Role: "system", Content: swe.SWESystemPrompt},
			{Role: "user", Content: swe.BuildTaskPrompt(task)},
		}

		var lastEvalRes *swe.EvalResult
		var taskAttempts int

		for attempt := 1; attempt <= 2; attempt++ {
			if stageCtx.Err() != nil || job.Ctx.Err() != nil {
				timedOut = true
				timeoutReason = fmt.Sprintf("Batas waktu tahap %s (%v) terlampaui", task.Tier, stageTimeout)
				break
			}
			taskAttempts = attempt

			if attempt > 1 {
				taskNotify(attempt, "PREPARING", "", fmt.Sprintf("Percobaan %d gagal. Mengirim feedback error/race ke AI (Turn %d/2)...", attempt-1, attempt))
			}

			// Stream response from AI with live snippet callback
			chatRes, chatErr := aiClient.ChatStream(stageCtx, messages, func(chunk ai.StreamChunk) {
				phaseStr := "CODING"
				if chunk.Phase == ai.PhaseReasoning {
					phaseStr = "REASONING"
				}
				taskNotify(attempt, phaseStr, chunk.Tail, "")
			})

			if chatErr != nil {
				logger.Error("SWE_JOB", "job=%s task=%s attempt=%d chat error: %v", job.ID, task.ID, attempt, chatErr)
				if isSWEJobTimeout(chatErr, "") || stageCtx.Err() != nil {
					timedOut = true
					timeoutReason = fmt.Sprintf("Timeout saat inferensi AI pada Tier %s (Turn %d): %v", task.Tier, attempt, chatErr)
				}
				lastEvalRes = &swe.EvalResult{
					TaskID:     task.ID,
					Tier:       task.Tier,
					MaxPoints:  task.Points,
					Attempts:   attempt,
					CompileErr: chatErr.Error(),
					TestOutput: chatErr.Error(),
				}
				break
			}

			totalPromptTokens += chatRes.PromptTokens
			totalCompletionTokens += chatRes.CompletionTokens
			totalTokens += chatRes.TotalTokens

			code, extractErr := swe.ExtractSWECode(chatRes.Content)
			if extractErr != nil {
				logger.Warn("SWE_JOB", "job=%s task=%s attempt=%d extract error: %v", job.ID, task.ID, attempt, extractErr)
				lastEvalRes = &swe.EvalResult{
					TaskID:     task.ID,
					Tier:       task.Tier,
					MaxPoints:  task.Points,
					Attempts:   attempt,
					CompileErr: extractErr.Error(),
					TestOutput: extractErr.Error(),
				}
				if attempt < 2 && stageCtx.Err() == nil {
					messages = append(messages, ai.ChatMessage{Role: "assistant", Content: chatRes.Content})
					messages = append(messages, ai.ChatMessage{Role: "user", Content: "Your response did not contain valid Go code with a 'package' declaration. Return the COMPLETE fixed Go code inside a single ```go ... ``` code block."})
					continue
				}
				break
			}

			lastFixedCode = code

			taskNotify(attempt, "EVALUATING", "", "Menjalankan sandbox evaluator (go test -race)...")

			evalRes, evalErr := p.sweEvaluator.Evaluate(stageCtx, task, code, attempt)
			if evalErr != nil {
				logger.Error("SWE_JOB", "job=%s task=%s attempt=%d evaluator error: %v", job.ID, task.ID, attempt, evalErr)
				if isSWEJobTimeout(evalErr, "") || stageCtx.Err() != nil {
					timedOut = true
					timeoutReason = fmt.Sprintf("Timeout pada sandbox evaluator (Turn %d): %v", attempt, evalErr)
				}
				lastEvalRes = &swe.EvalResult{
					TaskID:     task.ID,
					Tier:       task.Tier,
					MaxPoints:  task.Points,
					Attempts:   attempt,
					CompileErr: evalErr.Error(),
					TestOutput: evalErr.Error(),
				}
				break
			}

			lastEvalRes = evalRes

			if evalRes.Resolved && !evalRes.HasRace {
				taskNotify(attempt, "DONE", "", fmt.Sprintf("Lolos: Tier %s (%d/%d pts) [Turn %d]", task.Tier, evalRes.Points, task.Points, attempt))
				break
			}

			if isSWEJobTimeout(nil, evalRes.TestOutput) || stageCtx.Err() != nil {
				if attempt >= 2 || stageCtx.Err() != nil || job.Ctx.Err() != nil {
					timedOut = true
					timeoutReason = fmt.Sprintf("Timeout pengujian test suite pada Tier %s (Turn %d)", task.Tier, attempt)
					break
				}
			}

			if attempt < 2 && stageCtx.Err() == nil {
				failureMsg := evalRes.TestOutput
				if evalRes.CompileErr != "" {
					failureMsg = evalRes.CompileErr
				}
				messages = append(messages, ai.ChatMessage{Role: "assistant", Content: chatRes.Content})
				messages = append(messages, ai.ChatMessage{Role: "user", Content: swe.BuildSelfHealingPrompt(task, failureMsg)})
			} else {
				taskNotify(attempt, "DONE", "", fmt.Sprintf("Gagal: Tier %s (0/%d pts)", task.Tier, task.Points))
			}
		}

		stageCancel()

		if lastEvalRes == nil {
			lastEvalRes = &swe.EvalResult{
				TaskID:    task.ID,
				Tier:      task.Tier,
				MaxPoints: task.Points,
				Attempts:  taskAttempts,
			}
		}

		if timedOut {
			ladderFailed = true
			failReason = timeoutReason
			// Aturan: Jika proses SWE_JOB timeout, proses itu dan seterusnya gagal (0 poin), hanya dihitung yang berhasil saja.
			lastEvalRes.Points = 0
			lastEvalRes.Resolved = false
			notify(ProgressInfo{
				Step:        step,
				Total:       totalTasks,
				TaskTitle:   task.Title,
				Tier:        tierName,
				TierNumber:  tierNum,
				Attempt:     taskAttempts,
				MaxAttempts: 2,
				StatusText:  fmt.Sprintf("⏱️ Timeout tahap terlampaui (%s). Task ini dan seluruh task seterusnya gagal (0 poin).", timeoutReason),
				Phase:       "DONE",
			})
		} else if !lastEvalRes.Resolved || lastEvalRes.HasRace {
			ladderFailed = true
			failReason = fmt.Sprintf("Gagal pada Tier %s (%s). Tahap berikutnya dibatalkan.", task.Tier, task.Title)
		} else {
			// Hanya dihitung yang berhasil saja
			if lastEvalRes.Resolved && !lastEvalRes.HasRace {
				totalScore += lastEvalRes.Points
				if swe.TierOrder(task.Tier) > swe.TierOrder(maxTierAchieved) {
					maxTierAchieved = task.Tier
				}
			}
		}

		sweTaskRuns = append(sweTaskRuns, storage.SWETaskRun{
			ID:            uuid.New().String(),
			RunID:         job.ID,
			TaskID:        task.ID,
			TaskTitle:     task.Title,
			Tier:          string(task.Tier),
			PointsAwarded: lastEvalRes.Points,
			MaxPoints:     task.Points,
			Resolved:      lastEvalRes.Resolved,
			HasRace:       lastEvalRes.HasRace,
			Attempts:      lastEvalRes.Attempts,
			TestOutput:    lastEvalRes.TestOutput,
		})
	}

	// 3. Run Status & Cost
	runStatus := "SUCCESS"
	if timedOut {
		if totalScore == 0 {
			runStatus = "FAILED"
		} else {
			runStatus = "PARTIAL_TIMEOUT"
		}
	} else if ladderFailed {
		if totalScore == 0 {
			runStatus = "FAILED"
		} else {
			runStatus = "PARTIAL_FAILED"
		}
	} else if totalScore == 0 && len(sweTaskRuns) > 0 && !sweTaskRuns[0].Resolved {
		runStatus = "FAILED"
	}

	costEst := pricing.CalculateCost(job.Model, totalPromptTokens, totalCompletionTokens, totalScore)
	grade, _ := swe.CalculateGrade(totalScore)

	var codeHash string
	if lastFixedCode != "" {
		hash := sha256.Sum256([]byte(lastFixedCode))
		codeHash = hex.EncodeToString(hash[:])
	}

	executionDuration := time.Since(startTime)

	run := &storage.BenchmarkRun{
		ID:               job.ID,
		TelegramID:       job.TelegramID,
		ProviderBaseURL:  job.BaseURL,
		ModelName:        job.Model,
		TotalScore:       totalScore,
		MaxScore:         100,
		ExecutionTimeMs:  executionDuration.Milliseconds(),
		PromptTokens:     totalPromptTokens,
		CompletionTokens: totalCompletionTokens,
		TotalTokens:      totalTokens,
		EstimatedCostUSD: costEst.EstimatedCostUSD,
		CostTier:         costEst.Tier,
		CodeHash:         codeHash,
		CodeSnippet:      lastFixedCode,
		Status:           runStatus,
		BenchmarkMode:    "swe",
		MaxTierAchieved:  string(maxTierAchieved),
	}

	// 4. Persistence to Turso / LibSQL Database
	if p.repo != nil {
		saveCtx, saveCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer saveCancel()
		if err := p.repo.SaveSWERun(saveCtx, run, sweTaskRuns); err != nil {
			logger.Warn("DB", "job=%s gagal menyimpan SWE benchmark run: %v", job.ID, err)
		}
	}

	logger.BenchFinished(job.ID, job.Model, totalScore, 100, grade, executionDuration, totalTokens)

	sendResult(&JobResult{
		JobID:    job.ID,
		Run:      run,
		SWETasks: sweTaskRuns,
		Duration: executionDuration,
		Error:    nil,
	})
}
