package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"benchmark/internal/swe"
)

func TestLadderRunner_Parallel_ConcurrentExecution(t *testing.T) {
	var activeGoroutines atomic.Int32
	var maxActive atomic.Int32
	var checkpointCount atomic.Int32

	aiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := activeGoroutines.Add(1)
		defer activeGoroutines.Add(-1)

		for {
			oldMax := maxActive.Load()
			if current <= oldMax || maxActive.CompareAndSwap(oldMax, current) {
				break
			}
		}

		// Simulate artificial inference latency of 80ms per task
		time.Sleep(80 * time.Millisecond)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{
					"message": map[string]string{
						"role":    "assistant",
						"content": "```go\npackage main\n\nfunc Solution() {}\n```",
					},
				},
			},
			"usage": map[string]int{
				"prompt_tokens":     50,
				"completion_tokens": 50,
				"total_tokens":      100,
			},
		})
	}))
	defer aiServer.Close()

	runner := &DefaultLadderRunner{
		Tasks: []*swe.Task{
			{ID: "t-junior", Tier: swe.TierJunior, Points: 25},
			{ID: "t-mid", Tier: swe.TierMid, Points: 25},
			{ID: "t-senior", Tier: swe.TierSenior, Points: 25},
			{ID: "t-staff", Tier: swe.TierStaff, Points: 25},
		},
		Evaluator:             &mockPassEvaluator{},
		SkipModelVerification: true,
		ParallelExecution:     true,
		MaxParallelTasks:      4,
		TaskTimeout:           2 * time.Second,
		OnCheckpoint: func(job *BenchJob, res *LadderResult) {
			checkpointCount.Add(1)
		},
	}

	job := &BenchJob{
		JobID:   "job-parallel-test",
		BaseURL: aiServer.URL,
		Model:   "test-model",
	}

	start := time.Now()
	res, err := runner.RunLadder(context.Background(), job, nil, nil)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("expected RunLadder to succeed, got error: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil LadderResult")
	}

	// Total points should be 100
	if res.TotalScore != 100 {
		t.Errorf("expected totalScore 100, got %d", res.TotalScore)
	}
	if res.Status != "SUCCESS" {
		t.Errorf("expected status SUCCESS, got %s", res.Status)
	}
	if len(res.TaskResults) != 4 {
		t.Errorf("expected 4 task results, got %d", len(res.TaskResults))
	}

	// Verify concurrency occurred
	if maxActive.Load() < 2 {
		t.Errorf("expected concurrent execution (maxActive >= 2), got maxActive = %d", maxActive.Load())
	}

	// 4 tasks * 80ms = 320ms if sequential. Parallel should complete in well under 300ms.
	if duration > 300*time.Millisecond {
		t.Errorf("expected parallel execution to complete faster than sequential (< 300ms), took %v", duration)
	}

	// Verify checkpoint emissions (4 tasks should emit 4 checkpoints)
	if checkpointCount.Load() != 4 {
		t.Errorf("expected 4 checkpoint emissions, got %d", checkpointCount.Load())
	}
}

func TestLadderRunner_Parallel_BoundedConcurrency(t *testing.T) {
	var activeGoroutines atomic.Int32
	var maxActive atomic.Int32

	aiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := activeGoroutines.Add(1)
		defer activeGoroutines.Add(-1)

		for {
			oldMax := maxActive.Load()
			if current <= oldMax || maxActive.CompareAndSwap(oldMax, current) {
				break
			}
		}

		time.Sleep(50 * time.Millisecond)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{
					"message": map[string]string{
						"role":    "assistant",
						"content": "```go\npackage main\n```",
					},
				},
			},
			"usage": map[string]int{"prompt_tokens": 10, "completion_tokens": 10},
		})
	}))
	defer aiServer.Close()

	runner := &DefaultLadderRunner{
		Tasks: []*swe.Task{
			{ID: "t-1", Tier: swe.TierJunior, Points: 20},
			{ID: "t-2", Tier: swe.TierJunior, Points: 20},
			{ID: "t-3", Tier: swe.TierMid, Points: 20},
			{ID: "t-4", Tier: swe.TierMid, Points: 20},
		},
		Evaluator:             &mockPassEvaluator{},
		SkipModelVerification: true,
		ParallelExecution:     true,
		MaxParallelTasks:      2, // Restrict to 2 concurrent workers
		TaskTimeout:           2 * time.Second,
	}

	job := &BenchJob{
		JobID:   "job-bounded-parallel",
		BaseURL: aiServer.URL,
		Model:   "test-model",
	}

	res, err := runner.RunLadder(context.Background(), job, nil, nil)
	if err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if res == nil {
		t.Fatal("expected result")
	}

	// Max concurrent tasks should not exceed 2
	if maxActive.Load() > 2 {
		t.Errorf("expected max concurrent workers <= 2, got %d", maxActive.Load())
	}
}

func TestLadderRunner_Parallel_ContextCancellation(t *testing.T) {
	blockCh := make(chan struct{})

	aiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		select {
		case <-r.Context().Done():
			return
		case <-blockCh:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []map[string]any{
					{"message": map[string]string{"role": "assistant", "content": "```go\npackage main\n```"}},
				},
			})
			return
		}
	}))
	defer aiServer.Close()
	defer close(blockCh) // Executed before aiServer.Close() in LIFO

	runner := &DefaultLadderRunner{
		Tasks: []*swe.Task{
			{ID: "t-1", Tier: swe.TierJunior, Points: 25},
			{ID: "t-2", Tier: swe.TierJunior, Points: 25},
		},
		Evaluator:             &mockPassEvaluator{},
		SkipModelVerification: true,
		ParallelExecution:     true,
		MaxParallelTasks:      2,
		TaskTimeout:           5 * time.Second,
	}

	job := &BenchJob{
		JobID:   "job-cancel-parallel",
		BaseURL: aiServer.URL,
		Model:   "test-model",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	res, err := runner.RunLadder(ctx, job, nil, nil)
	if err == nil {
		t.Fatal("expected context cancellation error, got nil")
	}
	if res == nil {
		t.Fatal("expected partial result on cancellation, got nil")
	}
	if res.Status != "FAILED" && res.Status != "PARTIAL" {
		t.Errorf("expected status FAILED or PARTIAL on cancellation, got %s", res.Status)
	}
}
