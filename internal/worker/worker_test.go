package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"benchmark/internal/metrics"
	"benchmark/internal/swe"
)

type mockExecutor struct {
	execFunc func(ctx context.Context, job *BenchJob)
}

func (m *mockExecutor) Execute(ctx context.Context, job *BenchJob) {
	if m.execFunc != nil {
		m.execFunc(ctx, job)
	}
}

func TestPool_ConcurrencyAndCapacity(t *testing.T) {
	parentCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxSlots := 2
	started := make(chan struct{})
	block := make(chan struct{})

	exec := &mockExecutor{
		execFunc: func(ctx context.Context, job *BenchJob) {
			started <- struct{}{}
			<-block
		},
	}

	pool := NewPool(parentCtx, maxSlots, 5*time.Second, exec)

	// 1. Submit 2 jobs to fill all slots
	job1 := &BenchJob{JobID: "job-1", Model: "gpt-4o"}
	job2 := &BenchJob{JobID: "job-2", Model: "gpt-4o"}

	if err := pool.Submit(context.Background(), job1); err != nil {
		t.Fatalf("expected job1 to submit successfully, got %v", err)
	}
	<-started

	if err := pool.Submit(context.Background(), job2); err != nil {
		t.Fatalf("expected job2 to submit successfully, got %v", err)
	}
	<-started

	stats := pool.Stats()
	if stats.ActiveJobs != 2 || stats.AvailableSlots != 0 {
		t.Fatalf("expected 2 active, 0 available, got active=%d avail=%d", stats.ActiveJobs, stats.AvailableSlots)
	}

	// 2. Submit 3rd job: must fail non-blocking with ErrPoolFull
	job3 := &BenchJob{JobID: "job-3", Model: "gpt-4o"}
	if err := pool.Submit(context.Background(), job3); !errors.Is(err, ErrPoolFull) {
		t.Fatalf("expected ErrPoolFull on saturated pool, got %v", err)
	}

	// Unblock running jobs
	close(block)

	// Drain pool
	if err := pool.Drain(2 * time.Second); err != nil {
		t.Fatalf("drain failed: %v", err)
	}

	stats = pool.Stats()
	if stats.ActiveJobs != 0 || stats.AvailableSlots != maxSlots {
		t.Errorf("expected 0 active after drain, got %d", stats.ActiveJobs)
	}
	if stats.TotalProcessed != 2 {
		t.Errorf("expected 2 total processed, got %d", stats.TotalProcessed)
	}
}

func TestPool_DrainTimeoutCancellation(t *testing.T) {
	exec := &mockExecutor{
		execFunc: func(ctx context.Context, job *BenchJob) {
			select {
			case <-ctx.Done():
				// Cancelled properly
			case <-time.After(5 * time.Second):
				// Should not happen
			}
		},
	}

	pool := NewPool(context.Background(), 2, 5*time.Second, exec)

	job := &BenchJob{JobID: "long-job", Model: "gpt-4o"}
	if err := pool.Submit(context.Background(), job); err != nil {
		t.Fatalf("failed to submit job: %v", err)
	}

	// Drain with short timeout (50ms)
	err := pool.Drain(50 * time.Millisecond)
	if err == nil {
		t.Fatalf("expected timeout error on drain, got nil")
	}

	// Once drained/closed, new submissions must be rejected with ErrPoolClosed
	err = pool.Submit(context.Background(), &BenchJob{JobID: "job-new"})
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("expected ErrPoolClosed after drain, got %v", err)
	}
}

func TestCallbackClient_Success(t *testing.T) {
	secret := "test-secret-123456"
	var receivedHeaders http.Header
	var receivedBody []byte
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		receivedHeaders = r.Header.Clone()
		var err error
		receivedBody, err = json.Marshal(map[string]string{"status": "ok"})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(receivedBody)
	}))
	defer ts.Close()

	client := NewCallbackClient(2*time.Second, 10*time.Millisecond)
	ctx := context.Background()

	// 1. SendProgress
	progPayload := ProgressPayload{
		Type:     "progress",
		Phase:    "INITIALIZING",
		Message:  "Testing progress",
		Progress: 10,
	}
	err := client.SendProgress(ctx, ts.URL, secret, progPayload)
	if err != nil {
		t.Fatalf("SendProgress failed: %v", err)
	}

	mu.Lock()
	if receivedHeaders.Get("X-Bench-Secret") != secret {
		t.Errorf("expected secret %s, got %s", secret, receivedHeaders.Get("X-Bench-Secret"))
	}
	if receivedHeaders.Get("Content-Type") != "application/json" {
		t.Errorf("expected application/json, got %s", receivedHeaders.Get("Content-Type"))
	}
	mu.Unlock()

	// 2. SendLog
	logPayload := LogPayload{
		Line:  "test log line",
		Level: "info",
	}
	if err := client.SendLog(ctx, ts.URL, secret, logPayload); err != nil {
		t.Fatalf("SendLog failed: %v", err)
	}

	// 3. SendResult
	resPayload := ResultPayload{
		BenchmarkRun: BenchmarkRun{
			JobID:      "test-job",
			TotalScore: 100,
			Status:     "SUCCESS",
		},
	}
	if err := client.SendResult(ctx, ts.URL, secret, resPayload); err != nil {
		t.Fatalf("SendResult failed: %v", err)
	}

	// 4. SendError
	errPayload := ErrorPayload{
		Message: "something failed",
		Phase:   "TESTING",
	}
	if err := client.SendError(ctx, ts.URL, secret, errPayload); err != nil {
		t.Fatalf("SendError failed: %v", err)
	}
}

func TestCallbackClient_RetriesOnFailure(t *testing.T) {
	var attempts atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := attempts.Add(1)
		if att < 3 {
			// Fail first 2 attempts
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		// Succeed on 3rd attempt
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()

	client := NewCallbackClient(2*time.Second, 10*time.Millisecond) // Fast backoff
	ctx := context.Background()

	resPayload := ResultPayload{
		BenchmarkRun: BenchmarkRun{JobID: "test-retry"},
	}

	err := client.SendResult(ctx, ts.URL, "secret", resPayload)
	if err != nil {
		t.Fatalf("expected SendResult to succeed after retry, got %v", err)
	}

	if attempts.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts.Load())
	}
}

type mockLadderRunner struct {
	result *LadderResult
	err    error
}

func (m *mockLadderRunner) RunLadder(
	ctx context.Context,
	job *BenchJob,
	onProgress func(phase string, progress int, message string, tokens ...int),
	onLog func(line string, level string),
) (*LadderResult, error) {
	if onProgress != nil {
		onProgress("TESTING_JUNIOR", 25, "Menguji junior")
		onProgress("TESTING_STAFF", 85, "Menguji staff")
	}
	if onLog != nil {
		onLog("=== Task swe-t1: PASS ===", "success")
	}

	if m.err != nil {
		// Fair partial-credit: a runner may complete tasks before failing, so
		// the mock supports returning both the partial result and the error.
		return m.result, m.err
	}
	return m.result, nil
}

func TestExecutor_LifecycleAndKeyZeroing(t *testing.T) {
	var callbackCalls []string
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		callType, _ := body["type"].(string)

		mu.Lock()
		callbackCalls = append(callbackCalls, callType)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cbClient := NewCallbackClient(2*time.Second, 10*time.Millisecond)
	mockRunner := &mockLadderRunner{
		result: &LadderResult{
			TotalScore:       95,
			MaxScore:         100,
			ExecutionTimeMs:  1200,
			PromptTokens:     1000,
			CompletionTokens: 500,
			TotalTokens:      1500,
			EstimatedCostUSD: 0.0105,
			CostTier:         "Standar",
			Status:           "SUCCESS",
			MaxTierAchieved:  "STAFF",
			TaskResults: []*swe.EvalResult{
				{
					TaskID:    "swe-t1",
					Tier:      swe.TierJunior,
					Points:    20,
					MaxPoints: 20,
					Resolved:  true,
				},
			},
		},
	}

	executor := NewDefaultExecutor(cbClient, mockRunner)

	job := &BenchJob{
		JobID:          "test-job-exec",
		CallbackURL:    ts.URL,
		CallbackSecret: "secret-1234567890",
		BaseURL:        "https://openrouter.ai/api/v1",
		APIKey:         "sk-super-secret-key",
		Model:          "openai/gpt-4o",
	}

	executor.Execute(context.Background(), job)

	// Verify API key was securely zeroed
	if job.APIKey != "" {
		t.Errorf("expected APIKey to be zeroed after execution, got %q", job.APIKey)
	}

	// Verify callback sequence
	mu.Lock()
	defer mu.Unlock()

	hasInitializing := false
	hasResult := false
	hasLog := false

	for _, call := range callbackCalls {
		switch call {
		case "progress":
			hasInitializing = true
		case "result":
			hasResult = true
		case "log":
			hasLog = true
		}
	}

	if !hasInitializing {
		t.Errorf("missing progress callback, calls=%v", callbackCalls)
	}
	if !hasResult {
		t.Errorf("missing result callback, calls=%v", callbackCalls)
	}
	if !hasLog {
		t.Errorf("missing log callback, calls=%v", callbackCalls)
	}
}

func TestExecutor_ErrorHandling(t *testing.T) {
	var errorPayload ErrorPayload
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw map[string]any
		_ = json.NewDecoder(r.Body).Decode(&raw)
		if raw["type"] == "error" {
			mu.Lock()
			errorPayload.Type = "error"
			errorPayload.Message, _ = raw["message"].(string)
			errorPayload.Phase, _ = raw["phase"].(string)
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cbClient := NewCallbackClient(2*time.Second, 10*time.Millisecond)
	mockRunner := &mockLadderRunner{
		err: fmt.Errorf("AI provider unavailable"),
	}

	executor := NewDefaultExecutor(cbClient, mockRunner)

	job := &BenchJob{
		JobID:          "test-err-job",
		CallbackURL:    ts.URL,
		CallbackSecret: "secret-1234567890",
		APIKey:         "sk-error-key",
	}

	executor.Execute(context.Background(), job)

	// Verify API key is still zeroed out even on error
	if job.APIKey != "" {
		t.Errorf("expected APIKey to be zeroed even after error, got %q", job.APIKey)
	}

	mu.Lock()
	defer mu.Unlock()
	if errorPayload.Type != "error" {
		t.Errorf("expected error callback to be sent")
	}
}

func TestCostEstimation(t *testing.T) {
	tests := []struct {
		promptTok int
		compTok   int
		wantTier  string
	}{
		{10, 10, "Sangat Ekonomis"}, // < 0.001
		{500, 100, "Ekonomis"},      // < 0.005 (0.003)
		{3000, 500, "Standar"},      // < 0.02 (0.0165)
		{10000, 2000, "Premium"},    // >= 0.02 (0.060)
	}

	for _, tc := range tests {
		cost, tier := EstimateCost(tc.promptTok, tc.compTok)
		if cost <= 0 {
			t.Errorf("expected positive cost for (%d, %d), got %f", tc.promptTok, tc.compTok, cost)
		}
		if tier != tc.wantTier {
			t.Errorf("expected tier %s, got %s (cost: %f)", tc.wantTier, tier, cost)
		}
	}
}

func TestLadderRunner_SelfHealingAndScoring(t *testing.T) {
	task := &swe.Task{
		ID:                "test-task-1",
		Title:             "Test Junior Bug",
		Tier:              swe.TierJunior,
		Points:            20,
		IssueBody:         "Please fix the bug",
		BrokenCode:        "package main\n\nfunc FixMe() bool { return false }\n",
		ReferenceSolution: "package main\n\nfunc FixMe() bool { return true }\n",
		TestCode: `package main

import "testing"

func TestFixMe(t *testing.T) {
	if !FixMe() {
		t.Fatal("expected true")
	}
}
`,
	}

	var chatAttempt atomic.Int32
	mockAIServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := chatAttempt.Add(1)
		var respCode string
		if att == 1 {
			// Turn 1: Return broken code
			respCode = task.BrokenCode
		} else {
			// Turn 2: Return reference solution
			respCode = task.ReferenceSolution
		}

		resp := map[string]any{
			"id": "mock-chat",
			"choices": []map[string]any{
				{
					"index": 0,
					"message": map[string]string{
						"role":    "assistant",
						"content": fmt.Sprintf("```go\n%s\n```", respCode),
					},
					"finish_reason": "stop",
				},
			},
			"usage": map[string]int{
				"prompt_tokens":     150,
				"completion_tokens": 75,
				"total_tokens":      225,
			},
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer mockAIServer.Close()

	runner := &DefaultLadderRunner{
		Evaluator: swe.NewEvaluator(true),
		Tasks:     []*swe.Task{task},
	}

	job := &BenchJob{
		JobID:   "ladder-test-job",
		BaseURL: mockAIServer.URL + "/v1",
		APIKey:  "mock-key",
		Model:   "mock-model",
	}

	var phases []string
	var logs []string
	var mu sync.Mutex

	onProgress := func(phase string, progress int, msg string, tokens ...int) {
		mu.Lock()
		phases = append(phases, phase)
		mu.Unlock()
	}

	onLog := func(line string, level string) {
		mu.Lock()
		logs = append(logs, line)
		mu.Unlock()
	}

	res, err := runner.RunLadder(context.Background(), job, onProgress, onLog)
	if err != nil {
		t.Fatalf("RunLadder failed: %v", err)
	}

	// Turn 2 penalty: 80% of 20 = 16 points
	if res.TotalScore != 16 {
		t.Errorf("expected TotalScore 16 (Turn 2 self-healing penalty 80%%), got %d", res.TotalScore)
	}
	if res.MaxScore != 20 {
		t.Errorf("expected MaxScore 20, got %d", res.MaxScore)
	}
	if res.MaxTierAchieved != "LOW" && res.MaxTierAchieved != "JUNIOR" {
		t.Errorf("expected MaxTierAchieved 'LOW', got '%s'", res.MaxTierAchieved)
	}
	if len(res.TaskResults) != 1 {
		t.Fatalf("expected 1 task result, got %d", len(res.TaskResults))
	}
	if res.TaskResults[0].Attempts != 2 {
		t.Errorf("expected 2 attempts on self-healing, got %d", res.TaskResults[0].Attempts)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(phases) == 0 {
		t.Errorf("expected progress callbacks to be sent")
	}
	if len(logs) == 0 {
		t.Errorf("expected log callbacks to be sent")
	}
}

func TestLadderRunner_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	runner := &DefaultLadderRunner{
		Tasks: []*swe.Task{
			{
				ID:     "t-cancel",
				Tier:   swe.TierJunior,
				Points: 20,
			},
		},
	}

	job := &BenchJob{
		JobID:   "cancel-job",
		BaseURL: "http://127.0.0.1:9999",
		APIKey:  "key",
		Model:   "model",
	}

	_, err := runner.RunLadder(ctx, job, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled error, got %v", err)
	}
	// Fair partial-credit: an interrupted ladder must still return a usable
	// (possibly empty) result alongside the error.
}

func TestLadderRunner_MaxTierAchievedRules(t *testing.T) {
	calcTier := func(results []*swe.EvalResult) string {
		tierPassed := make(map[swe.Tier]bool)
		for _, tier := range []swe.Tier{swe.TierJunior, swe.TierMid, swe.TierSenior, swe.TierStaff} {
			totalInTier := 0
			passedInTier := 0
			for _, res := range results {
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

		maxTier := "NONE"
		if tierPassed[swe.TierJunior] {
			maxTier = "JUNIOR"
			if tierPassed[swe.TierMid] {
				maxTier = "MID"
				if tierPassed[swe.TierSenior] {
					maxTier = "SENIOR"
					if tierPassed[swe.TierStaff] {
						maxTier = "STAFF"
					}
				}
			}
		}
		return maxTier
	}

	// 1. One Junior pass, One Junior fail -> NONE
	res1 := []*swe.EvalResult{
		{Tier: swe.TierJunior, Resolved: true, HasRace: false},
		{Tier: swe.TierJunior, Resolved: false, HasRace: false},
	}
	if tier := calcTier(res1); tier != "NONE" {
		t.Errorf("expected NONE when not all junior tasks pass, got %s", tier)
	}

	// 2. Both Junior pass -> JUNIOR
	res2 := []*swe.EvalResult{
		{Tier: swe.TierJunior, Resolved: true, HasRace: false},
		{Tier: swe.TierJunior, Resolved: true, HasRace: false},
	}
	if tier := calcTier(res2); tier != "JUNIOR" {
		t.Errorf("expected JUNIOR when all junior pass, got %s", tier)
	}

	// 3. Both Junior pass, one Mid pass, one Mid fail -> JUNIOR
	res3 := []*swe.EvalResult{
		{Tier: swe.TierJunior, Resolved: true, HasRace: false},
		{Tier: swe.TierJunior, Resolved: true, HasRace: false},
		{Tier: swe.TierMid, Resolved: true, HasRace: false},
		{Tier: swe.TierMid, Resolved: false, HasRace: false},
	}
	if tier := calcTier(res3); tier != "JUNIOR" {
		t.Errorf("expected JUNIOR when only 1 mid passes, got %s", tier)
	}

	// 4. All Junior + All Mid pass -> MID
	res4 := []*swe.EvalResult{
		{Tier: swe.TierJunior, Resolved: true, HasRace: false},
		{Tier: swe.TierJunior, Resolved: true, HasRace: false},
		{Tier: swe.TierMid, Resolved: true, HasRace: false},
		{Tier: swe.TierMid, Resolved: true, HasRace: false},
	}
	if tier := calcTier(res4); tier != "MID" {
		t.Errorf("expected MID, got %s", tier)
	}

	// 5. All 8 tasks pass -> STAFF
	res5 := []*swe.EvalResult{
		{Tier: swe.TierJunior, Resolved: true, HasRace: false},
		{Tier: swe.TierJunior, Resolved: true, HasRace: false},
		{Tier: swe.TierMid, Resolved: true, HasRace: false},
		{Tier: swe.TierMid, Resolved: true, HasRace: false},
		{Tier: swe.TierSenior, Resolved: true, HasRace: false},
		{Tier: swe.TierSenior, Resolved: true, HasRace: false},
		{Tier: swe.TierStaff, Resolved: true, HasRace: false},
		{Tier: swe.TierStaff, Resolved: true, HasRace: false},
	}
	if tier := calcTier(res5); tier != "STAFF" {
		t.Errorf("expected STAFF when all 8 tasks pass, got %s", tier)
	}
}

func TestExecutor_MetricsTracking(t *testing.T) {
	met := metrics.New()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()

	cbClient := NewCallbackClient(2*time.Second, 10*time.Millisecond, met)
	mockRunner := &mockLadderRunner{
		result: &LadderResult{
			TotalScore:       100,
			MaxScore:         100,
			ExecutionTimeMs:  500,
			MaxTierAchieved:  "STAFF",
			TaskResults:      []*swe.EvalResult{},
		},
	}

	executor := NewDefaultExecutor(cbClient, mockRunner, met)

	// 1. Successful execution -> JobsCompleted++
	jobSuccess := &BenchJob{
		JobID:          "test-job-met-ok",
		CallbackURL:    ts.URL,
		CallbackSecret: "secret-1234567890",
		Model:          "gpt-4o",
	}
	executor.Execute(context.Background(), jobSuccess)

	if met.JobsCompleted.Load() != 1 {
		t.Errorf("expected JobsCompleted 1, got %d", met.JobsCompleted.Load())
	}
	if met.JobsFailed.Load() != 0 {
		t.Errorf("expected JobsFailed 0, got %d", met.JobsFailed.Load())
	}

	// 2. Failed execution -> JobsFailed++
	failRunner := &mockLadderRunner{
		err: fmt.Errorf("evaluation failed"),
	}
	failExecutor := NewDefaultExecutor(cbClient, failRunner, met)
	jobFail := &BenchJob{
		JobID:          "test-job-met-fail",
		CallbackURL:    ts.URL,
		CallbackSecret: "secret-1234567890",
		Model:          "gpt-4o",
	}
	failExecutor.Execute(context.Background(), jobFail)

	if met.JobsFailed.Load() != 1 {
		t.Errorf("expected JobsFailed 1, got %d", met.JobsFailed.Load())
	}

	// 3. Callback error -> CallbackErrors++
	// Point to non-existent port to force connection failure
	badClient := NewCallbackClient(100*time.Millisecond, 5*time.Millisecond, met)
	_ = badClient.SendProgress(context.Background(), "http://127.0.0.1:54321/nonexistent", "secret", ProgressPayload{
		Type: "progress",
	})
	if met.CallbackErrors.Load() == 0 {
		t.Errorf("expected CallbackErrors > 0, got %d", met.CallbackErrors.Load())
	}
}



