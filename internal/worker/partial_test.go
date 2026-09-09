package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"benchmark/internal/metrics"
	"benchmark/internal/swe"
)

// TestLadderRunner_PartialResultOnInterruption verifies the fair partial-credit
// contract: when the ladder is interrupted (timeout/cancel/infra failure) after
// completing some tasks, RunLadder must return (partialResult, err) instead of
// (nil, err) so earned points are never discarded — regardless of the cause.
func TestLadderRunner_PartialResultOnInterruption(t *testing.T) {
	taskList := []*swe.Task{
		{ID: "t-j1", Tier: swe.TierJunior, Points: 20},
		{ID: "t-j2", Tier: swe.TierJunior, Points: 20},
		{ID: "t-m1", Tier: swe.TierMid, Points: 25},
	}

	// Simulate: first two tasks completed & passed, then the context is
	// cancelled before the third task can run.
	taskResults := []*swe.EvalResult{
		{TaskID: "t-j1", Tier: swe.TierJunior, Points: 20, MaxPoints: 20, Resolved: true},
		{TaskID: "t-j2", Tier: swe.TierJunior, Points: 16, MaxPoints: 20, Resolved: true},
	}
	totalScore := 36

	res, err := buildPartialLadderResult(
		taskList, taskResults, totalScore,
		1200, 800,
		time.Now().Add(-2*time.Second),
		context.DeadlineExceeded,
	)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the interrupt error to be preserved, got %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil partial result")
	}
	if res.TotalScore != 36 {
		t.Errorf("expected earned score 36 to be preserved, got %d", res.TotalScore)
	}
	if res.MaxScore != 65 {
		t.Errorf("expected maxScore 65 (20+20+25, full ladder), got %d", res.MaxScore)
	}
	if res.Status != "PARTIAL" {
		t.Errorf("expected status PARTIAL when score > 0, got %s", res.Status)
	}
	if res.MaxTierAchieved != "LOW" && res.MaxTierAchieved != "JUNIOR" {
		t.Errorf("expected maxTierAchieved LOW (both low tasks passed), got %s", res.MaxTierAchieved)
	}
	if len(res.TaskResults) != 2 {
		t.Errorf("expected 2 completed task results, got %d", len(res.TaskResults))
	}
	if res.PromptTokens != 1200 || res.CompletionTokens != 800 || res.TotalTokens != 2000 {
		t.Errorf("expected tokens preserved (1200/800/2000), got %d/%d/%d", res.PromptTokens, res.CompletionTokens, res.TotalTokens)
	}
	if res.EstimatedCostUSD <= 0 {
		t.Errorf("expected estimated cost > 0 for consumed tokens, got %f", res.EstimatedCostUSD)
	}
	if res.ExecutionTimeMs < 2000 {
		t.Errorf("expected execution time >= 2000ms, got %d", res.ExecutionTimeMs)
	}
}

// TestLadderRunner_PartialResultZeroProgress verifies that an interruption with
// zero completed tasks yields a FAILED (not PARTIAL) result with the full
// ladder maxScore and empty task list — no points are invented.
func TestLadderRunner_PartialResultZeroProgress(t *testing.T) {
	taskList := []*swe.Task{
		{ID: "t-j1", Tier: swe.TierJunior, Points: 20},
		{ID: "t-m1", Tier: swe.TierMid, Points: 25},
	}

	res, err := buildPartialLadderResult(
		taskList, nil, 0, 0, 0, time.Now(),
		errors.New("ai client init gagal"),
	)

	if err == nil || !strings.Contains(err.Error(), "ai client init gagal") {
		t.Fatalf("expected interrupt error preserved, got %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil result even with zero progress")
	}
	if res.TotalScore != 0 {
		t.Errorf("expected 0 score, got %d", res.TotalScore)
	}
	if res.Status != "FAILED" {
		t.Errorf("expected status FAILED when nothing earned, got %s", res.Status)
	}
	if res.MaxScore != 45 {
		t.Errorf("expected maxScore 45 (full ladder), got %d", res.MaxScore)
	}
	if len(res.TaskResults) != 0 {
		t.Errorf("expected empty task results, got %d", len(res.TaskResults))
	}
	if res.MaxTierAchieved != "NONE" {
		t.Errorf("expected maxTierAchieved NONE, got %s", res.MaxTierAchieved)
	}
}

// TestExecutor_PartialResultDispatchOnLadderError verifies the executor sends
// the partial ResultPayload BEFORE the ErrorPayload when the ladder fails with
// earned results, so omm-web can persist the points fairly.
func TestExecutor_PartialResultDispatchOnLadderError(t *testing.T) {
	var callOrder []string
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload map[string]any
		_ = json.Unmarshal(body, &payload)

		mu.Lock()
		if ptype, _ := payload["type"].(string); ptype != "" {
			callOrder = append(callOrder, ptype)
		}
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()

	partial := &LadderResult{
		TotalScore:      45,
		MaxScore:        100,
		Status:          "PARTIAL",
		MaxTierAchieved: "JUNIOR",
		ExecutionTimeMs: 1500,
		TaskResults: []*swe.EvalResult{
			{TaskID: "t-j1", Tier: swe.TierJunior, Points: 45, MaxPoints: 45, Resolved: true},
		},
	}
	runner := &mockLadderRunner{
		result: partial,
		err:    context.DeadlineExceeded,
	}

	cbClient := NewCallbackClient(2*time.Second, 10*time.Millisecond, metrics.New())
	executor := NewDefaultExecutor(cbClient, runner)
	job := &BenchJob{
		JobID:          "partial-job",
		CallbackURL:    ts.URL,
		CallbackSecret: "secret-1234567890",
		Model:          "test-model",
	}

	executor.Execute(context.Background(), job)

	mu.Lock()
	defer mu.Unlock()

	// The executor also fires progress/log callbacks; only the terminal
	// payloads matter for the fair partial-credit contract.
	var terminalOrder []string
	for _, ptype := range callOrder {
		if ptype == "result" || ptype == "error" {
			terminalOrder = append(terminalOrder, ptype)
		}
	}

	if len(terminalOrder) != 2 {
		t.Fatalf("expected exactly 2 terminal callbacks (result then error), got %v (all: %v)", terminalOrder, callOrder)
	}
	if terminalOrder[0] != "result" {
		t.Errorf("expected partial result dispatched FIRST, got order %v", terminalOrder)
	}
	if terminalOrder[1] != "error" {
		t.Errorf("expected error dispatched SECOND, got order %v", terminalOrder)
	}
}

// TestCallbackClient_SendResult_DetachedContextOnExpiredContext verifies that
// when the job's main context has expired or timed out, SendResult detaches the
// context to guarantee delivery of the final or partial result to omm-web.
func TestCallbackClient_SendResult_DetachedContextOnExpiredContext(t *testing.T) {
	var received atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Store(true)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()

	client := NewCallbackClient(2*time.Second, 10*time.Millisecond)

	// Create an already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resPayload := ResultPayload{
		BenchmarkRun: BenchmarkRun{JobID: "test-detached-ctx", TotalScore: 25, Status: "PARTIAL"},
	}

	// SendResult should succeed using its detached context despite the expired parent ctx
	err := client.SendResult(ctx, ts.URL, "secret", resPayload)
	if err != nil {
		t.Fatalf("expected SendResult to succeed with detached context, got: %v", err)
	}
	if !received.Load() {
		t.Errorf("expected server to receive callback payload")
	}
}

// TestLadderRunner_EarlyTerminationOnFailure verifies that when a task fails
// or encounters an error during the ladder run, subsequent tasks are skipped
// immediately, and the score accumulated so far is finalized.
func TestLadderRunner_EarlyTerminationOnFailure(t *testing.T) {
	var chatCallCount atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chatCallCount.Add(1)
		// Return failure/error (HTTP 500) on Task 1
		http.Error(w, `{"error":"model inference error"}`, http.StatusInternalServerError)
	}))
	defer ts.Close()

	runner := &DefaultLadderRunner{
		Tasks: []*swe.Task{
			{ID: "t-j1", Tier: swe.TierJunior, Points: 25},
			{ID: "t-j2", Tier: swe.TierJunior, Points: 25},
			{ID: "t-m1", Tier: swe.TierMid, Points: 50},
		},
		Evaluator:             swe.NewEvaluator(true),
		SkipModelVerification: true,
	}

	job := &BenchJob{
		JobID:   "test-early-term",
		BaseURL: ts.URL,
		Model:   "test-model",
	}

	res, err := runner.RunLadder(context.Background(), job, nil, nil)
	if err != nil {
		t.Fatalf("expected nil error on early termination (handled as completed ladder result), got: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil LadderResult")
	}

	// Task 1 failed on Turn 1 with AI error, so tasks 2 and 3 should be skipped!
	if len(res.TaskResults) != 1 {
		t.Errorf("expected 1 task evaluated before early termination, got %d", len(res.TaskResults))
	}
	if res.TotalScore != 0 {
		t.Errorf("expected TotalScore 0, got %d", res.TotalScore)
	}
	if res.MaxScore != 100 { // 25 + 25 + 50
		t.Errorf("expected full MaxScore 100, got %d", res.MaxScore)
	}
	if res.Status != "FAILED" {
		t.Errorf("expected status FAILED, got %s", res.Status)
	}

	// AI server should only have received 1 call (Task 1 Turn 1), not for Task 2 or Task 3
	if chatCallCount.Load() != 1 {
		t.Errorf("expected exactly 1 AI call before early termination, got %d", chatCallCount.Load())
	}
}

// TestLadderRunner_EarlyTerminationWithPartialScore verifies that when Task 1 passes
// and Task 2 fails, subsequent tasks (Task 3) are skipped, and the accumulated
// score (from Task 1) is accurately preserved and status is set to PARTIAL.
func TestLadderRunner_EarlyTerminationWithPartialScore(t *testing.T) {
	var chatCallCount atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callNum := chatCallCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if callNum == 1 {
			// Task 1: return valid solution that passes evaluation
			resp := map[string]any{
				"id":     "chat-task1",
				"object": "chat.completion",
				"choices": []map[string]any{
					{
						"message": map[string]string{
							"role":    "assistant",
							"content": "```go\npackage main\n\nfunc Add(a, b int) int {\n\treturn a + b\n}\n```",
						},
					},
				},
				"usage": map[string]int{
					"prompt_tokens":     40,
					"completion_tokens": 30,
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		// Task 2: fail with HTTP 500
		http.Error(w, `{"error":"ai error on task 2"}`, http.StatusInternalServerError)
	}))
	defer ts.Close()

	runner := &DefaultLadderRunner{
		Tasks: []*swe.Task{
			{
				ID:         "t-j1",
				Tier:       swe.TierJunior,
				Points:     25,
				BrokenCode: "package main\n\nfunc Add(a, b int) int { return 0 }\n",
				TestCode:   "package main\n\nimport \"testing\"\n\nfunc TestAdd(t *testing.T) {\n\tif Add(2, 3) != 5 {\n\t\tt.Fatal(\"fail\")\n\t}\n}\n",
			},
			{
				ID:         "t-j2",
				Tier:       swe.TierJunior,
				Points:     25,
				BrokenCode: "package main\n\nfunc Sub(a, b int) int { return 0 }\n",
				TestCode:   "package main\n\nimport \"testing\"\n\nfunc TestSub(t *testing.T) {\n\tif Sub(5, 3) != 2 {\n\t\tt.Fatal(\"fail\")\n\t}\n}\n",
			},
			{
				ID:         "t-m1",
				Tier:       swe.TierMid,
				Points:     50,
				BrokenCode: "package main\n\nfunc Mul(a, b int) int { return 0 }\n",
				TestCode:   "package main\n\nimport \"testing\"\n\nfunc TestMul(t *testing.T) {\n\tif Mul(2, 3) != 6 {\n\t\tt.Fatal(\"fail\")\n\t}\n}\n",
			},
		},
		Evaluator:             &mockPassEvaluator{},
		SkipModelVerification: true,
	}

	job := &BenchJob{
		JobID:   "test-partial-early-term",
		BaseURL: ts.URL,
		Model:   "test-model",
	}

	res, err := runner.RunLadder(context.Background(), job, nil, nil)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil LadderResult")
	}

	// Task 1 passed (25 pts), Task 2 failed, Task 3 was skipped
	if len(res.TaskResults) != 2 {
		t.Errorf("expected 2 tasks evaluated (Task 1 and Task 2), got %d", len(res.TaskResults))
	}
	if res.TotalScore != 25 {
		t.Errorf("expected TotalScore 25, got %d", res.TotalScore)
	}
	if res.MaxScore != 100 { // 25 + 25 + 50
		t.Errorf("expected full MaxScore 100, got %d", res.MaxScore)
	}
	if res.Status != "PARTIAL" {
		t.Errorf("expected status PARTIAL, got %s", res.Status)
	}
	if chatCallCount.Load() != 2 {
		t.Errorf("expected exactly 2 AI calls before skipping Task 3, got %d", chatCallCount.Load())
	}
}

// TestLadderRunner_PerTaskTimeoutAndInferenceRetry validates:
// 1. Transient AI inference error retries up to MaxInferenceRetries.
func TestLadderRunner_PerTaskTimeoutAndInferenceRetry(t *testing.T) {
	var chatCallCount atomic.Int32
	testDone := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "close")
		callNum := chatCallCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if callNum == 1 {
			// Task 1 Attempt 1: Transient 500 error
			http.Error(w, `{"error":"temporary overload"}`, http.StatusInternalServerError)
			return
		}
		if callNum == 2 {
			// Task 1 Retry: Success!
			resp := map[string]any{
				"choices": []map[string]any{
					{
						"message": map[string]any{
							"role":    "assistant",
							"content": "```go\npackage main\n\nfunc Add(a, b int) int { return a + b }\n```",
						},
					},
				},
				"usage": map[string]int{
					"prompt_tokens":     50,
					"completion_tokens": 50,
					"total_tokens":      100,
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		// Task 2: Simulate slow AI inference that exceeds per-task timeout
		select {
		case <-r.Context().Done():
			return
		case <-testDone:
			return
		}
	}))
	defer func() {
		close(testDone)
		ts.Close()
	}()

	runner := &DefaultLadderRunner{
		Tasks: []*swe.Task{
			{
				ID:         "t-retry-pass",
				Tier:       swe.TierLow,
				Points:     20,
				BrokenCode: "package main\n\nfunc Add(a, b int) int { return 0 }\n",
				TestCode:   "package main\n\nimport \"testing\"\n\nfunc TestAdd(t *testing.T) {\n\tif Add(2, 3) != 5 {\n\t\tt.Fatal(\"fail\")\n\t}\n}\n",
			},
			{
				ID:         "t-timeout-fail",
				Tier:       swe.TierLow,
				Points:     20,
				BrokenCode: "package main\n\nfunc Sub(a, b int) int { return 0 }\n",
				TestCode:   "package main\n\nimport \"testing\"\n\nfunc TestSub(t *testing.T) {\n\tif Sub(5, 3) != 2 {\n\t\tt.Fatal(\"fail\")\n\t}\n}\n",
			},
			{
				ID:         "t-skipped",
				Tier:       swe.TierMid,
				Points:     30,
				BrokenCode: "package main\n\nfunc Mul(a, b int) int { return 0 }\n",
				TestCode:   "package main\n\nimport \"testing\"\n\nfunc TestMul(t *testing.T) {\n\tif Mul(2, 3) != 6 {\n\t\tt.Fatal(\"fail\")\n\t}\n}\n",
			},
		},
		Evaluator:             &mockPassEvaluator{},
		TaskTimeout:           400 * time.Millisecond, // Strict per-task timeout
		MaxInferenceRetries:   1,                      // Retry once on transient error
		SkipModelVerification: true,
	}

	job := &BenchJob{
		JobID:   "test-per-task-timeout",
		BaseURL: ts.URL,
		Model:   "test-model",
	}

	parentCtx := context.Background()
	res, err := runner.RunLadder(parentCtx, job, nil, nil)
	if err != nil {
		t.Fatalf("expected nil error on ladder completion, got: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil LadderResult")
	}

	// Task 1 should have passed (20 pts) via retry
	// Task 2 timed out and failed
	// Task 3 was skipped via early termination
	if res.TotalScore != 20 {
		t.Errorf("expected TotalScore 20, got %d", res.TotalScore)
	}
	if res.Status != "PARTIAL" {
		t.Errorf("expected Status PARTIAL, got %s", res.Status)
	}
	if len(res.TaskResults) != 2 {
		t.Errorf("expected 2 tasks evaluated (1 pass, 1 timeout), got %d", len(res.TaskResults))
	}
	if chatCallCount.Load() < 3 {
		t.Errorf("expected at least 3 AI calls (1 fail, 1 retry-pass, 1 timeout), got %d", chatCallCount.Load())
	}
}

type mockPassEvaluator struct{}

func (m *mockPassEvaluator) Evaluate(ctx context.Context, task *swe.Task, solutionCode string, attempt int) (*swe.EvalResult, error) {
	return &swe.EvalResult{
		TaskID:    task.ID,
		Tier:      task.Tier,
		Points:    task.Points,
		MaxPoints: task.Points,
		Resolved:  true,
		Attempts:  attempt,
	}, nil
}