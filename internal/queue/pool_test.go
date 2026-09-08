package queue

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWorkerPool_QueueCapacityAndBackpressure(t *testing.T) {
	// Worker pool without running workers (do not call Start) so jobs accumulate in the buffer
	pool := NewWorkerPool(1, 2, nil)

	job1 := &BenchmarkJob{ID: "job-1", TelegramID: 101}
	job2 := &BenchmarkJob{ID: "job-2", TelegramID: 102}
	job3 := &BenchmarkJob{ID: "job-3", TelegramID: 103}

	if err := pool.Submit(job1); err != nil {
		t.Fatalf("Submit job1 gagal: %v", err)
	}
	if err := pool.Submit(job2); err != nil {
		t.Fatalf("Submit job2 gagal: %v", err)
	}

	if pool.QueueLength() != 2 {
		t.Errorf("Panjang antrean harusnya 2, dapat: %d", pool.QueueLength())
	}

	// The 3rd job must be rejected because the buffer capacity of 2 is full
	err := pool.Submit(job3)
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("Ekspektasi ErrQueueFull saat kapasitas penuh, dapat: %v", err)
	}

	// Stop pool
	if err := pool.Stop(1 * time.Second); err != nil {
		t.Errorf("Stop pool error: %v", err)
	}

	// Submit after stop must be rejected
	errAfterStop := pool.Submit(&BenchmarkJob{ID: "job-4", TelegramID: 104})
	if !errors.Is(errAfterStop, ErrPoolClosed) {
		t.Errorf("Ekspektasi ErrPoolClosed setelah pool stop, dapat: %v", errAfterStop)
	}
}

func TestWorkerPool_Deduplication(t *testing.T) {
	pool := NewWorkerPool(1, 5, nil)
	defer func() { _ = pool.Stop(1 * time.Second) }()

	jobA := &BenchmarkJob{ID: "job-a", TelegramID: 999}
	if err := pool.Submit(jobA); err != nil {
		t.Fatalf("Submit jobA harus berhasil: %v", err)
	}

	if !pool.IsUserActive(999) {
		t.Errorf("User 999 harus terdeteksi aktif")
	}

	// Submitting a second job by the same user must be rejected by deduplication
	jobB := &BenchmarkJob{ID: "job-b", TelegramID: 999}
	err := pool.Submit(jobB)
	if !errors.Is(err, ErrUserJobActive) {
		t.Errorf("Ekspektasi ErrUserJobActive untuk user yang sama, dapat: %v", err)
	}
}

func TestWorkerPool_StartAndActive(t *testing.T) {
	pool := NewWorkerPool(2, 5, nil)
	pool.Start()
	defer func() { _ = pool.Stop(1 * time.Second) }()

	if pool.ActiveJobs() != 0 {
		t.Errorf("Awalnya active jobs harus 0, dapat: %d", pool.ActiveJobs())
	}
}

func TestIsSWEJobTimeout(t *testing.T) {
	if !isSWEJobTimeout(context.DeadlineExceeded, "") {
		t.Errorf("expected true for context.DeadlineExceeded")
	}
	if !isSWEJobTimeout(errors.New("timeout waiting for response"), "") {
		t.Errorf("expected true for error with timeout string")
	}
	if !isSWEJobTimeout(nil, "panic: test timed out after 30s\nFAIL") {
		t.Errorf("expected true for panic test timed out in output")
	}
	if !isSWEJobTimeout(nil, "context deadline exceeded") {
		t.Errorf("expected true for context deadline exceeded in output")
	}
	if isSWEJobTimeout(nil, "--- FAIL: TestSomething (0.01s)\n    assertion error") {
		t.Errorf("expected false for standard assertion failure")
	}
}

func TestProcessJob_TimeoutAbortsSubsequentTasks(t *testing.T) {
	pool := NewWorkerPool(1, 2, nil)
	defer func() { _ = pool.Stop(1 * time.Second) }()

	// Create a job whose context is already timed out/cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately to simulate timeout

	resultChan := make(chan *JobResult, 1)
	job := &BenchmarkJob{
		ID:         "test-timeout-job",
		TelegramID: 888,
		Model:      "test-model",
		BaseURL:    "http://127.0.0.1:9999/v1",
		APIKey:     "test-key",
		Ctx:        ctx,
		ResultChan: resultChan,
	}

	pool.processJob(job)

	select {
	case res := <-resultChan:
		if res.Error != nil {
			t.Fatalf("unexpected job error: %v", res.Error)
		}
		if res.Run.TotalScore != 0 {
			t.Errorf("expected 0 total score on timeout, got %d", res.Run.TotalScore)
		}
		if len(res.SWETasks) != 4 {
			t.Fatalf("expected 4 SWE tasks recorded, got %d", len(res.SWETasks))
		}
		for i, tr := range res.SWETasks {
			if tr.PointsAwarded != 0 {
				t.Errorf("task[%d] expected 0 points, got %d", i, tr.PointsAwarded)
			}
			if tr.Resolved {
				t.Errorf("task[%d] expected Resolved=false, got true", i)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for job result")
	}
}

func TestProcessJob_StageTimeoutEnforced(t *testing.T) {
	// Server simulates slow AI inference (delays 200ms)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"package main\"}}]}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()

	pool := NewWorkerPool(1, 2, nil)
	defer func() { _ = pool.Stop(1 * time.Second) }()

	// Configure a very short stage timeout (50ms)
	pool.SetSWETaskTimeout(50 * time.Millisecond)

	resultChan := make(chan *JobResult, 1)
	job := &BenchmarkJob{
		ID:         "test-stage-timeout",
		TelegramID: 777,
		Model:      "slow-model",
		BaseURL:    server.URL,
		APIKey:     "test-key",
		Ctx:        context.Background(),
		ResultChan: resultChan,
	}

	pool.processJob(job)

	select {
	case res := <-resultChan:
		if res.Error != nil {
			t.Fatalf("unexpected error: %v", res.Error)
		}
		if res.Run.TotalScore != 0 {
			t.Errorf("expected 0 total score on stage timeout, got %d", res.Run.TotalScore)
		}
		if res.Run.Status != "FAILED" && res.Run.Status != "PARTIAL_TIMEOUT" {
			t.Errorf("expected FAILED or PARTIAL_TIMEOUT, got %s", res.Run.Status)
		}
		if len(res.SWETasks) != 4 {
			t.Fatalf("expected 4 task runs, got %d", len(res.SWETasks))
		}
		for i, tr := range res.SWETasks {
			if tr.PointsAwarded != 0 {
				t.Errorf("task[%d] expected 0 points, got %d", i, tr.PointsAwarded)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("test timed out waiting for job result")
	}
}

func TestProcessJob_ProgressCallbackStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking...\"}}]}\n\n"))
		flusher.Flush()
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"```go\\npackage main\\n```\"}}]}\n\n"))
		flusher.Flush()
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	}))
	defer server.Close()

	pool := NewWorkerPool(1, 2, nil)
	defer func() { _ = pool.Stop(1 * time.Second) }()
	pool.SetSWETaskTimeout(1 * time.Second)

	var receivedPhases []string
	resultChan := make(chan *JobResult, 1)

	job := &BenchmarkJob{
		ID:         "test-stream-progress",
		TelegramID: 666,
		Model:      "test-model",
		BaseURL:    server.URL,
		APIKey:     "test-key",
		Ctx:        context.Background(),
		ResultChan: resultChan,
		OnProgress: func(info ProgressInfo) {
			if info.Phase != "" {
				receivedPhases = append(receivedPhases, info.Phase)
			}
		},
	}

	pool.processJob(job)

	select {
	case <-resultChan:
		hasReasoning := false
		hasCoding := false
		for _, p := range receivedPhases {
			if p == "REASONING" {
				hasReasoning = true
			}
			if p == "CODING" {
				hasCoding = true
			}
		}
		if !hasReasoning {
			t.Errorf("expected to receive REASONING phase, got: %v", receivedPhases)
		}
		if !hasCoding {
			t.Errorf("expected to receive CODING phase, got: %v", receivedPhases)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("test timed out")
	}
}

func TestProcessJob_PhaseFailureAbortsSubsequentTasks(t *testing.T) {
	// Server returns code that will fail the test suite
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"```go\\npackage main\\nfunc dummy() {}\\n```\"}}]}\n\n"))
		flusher.Flush()
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	}))
	defer server.Close()

	pool := NewWorkerPool(1, 2, nil)
	defer func() { _ = pool.Stop(1 * time.Second) }()
	pool.SetSWETaskTimeout(15 * time.Second)

	resultChan := make(chan *JobResult, 1)
	var cancelledNotified int

	job := &BenchmarkJob{
		ID:         "test-phase-fail",
		TelegramID: 555,
		Model:      "failing-model",
		BaseURL:    server.URL,
		APIKey:     "test-key",
		Ctx:        context.Background(),
		ResultChan: resultChan,
		OnProgress: func(info ProgressInfo) {
			if strings.Contains(info.StatusText, "Dibatalkan") {
				cancelledNotified++
			}
		},
	}

	pool.processJob(job)

	select {
	case res := <-resultChan:
		if res.Error != nil {
			t.Fatalf("unexpected error: %v", res.Error)
		}
		if res.Run.TotalScore != 0 {
			t.Errorf("expected 0 total score, got %d", res.Run.TotalScore)
		}
		if res.Run.Status != "FAILED" {
			t.Errorf("expected status FAILED, got %s", res.Run.Status)
		}
		if len(res.SWETasks) != 4 {
			t.Fatalf("expected 4 task records, got %d", len(res.SWETasks))
		}
		// Task 1 was attempted and failed
		if res.SWETasks[0].Attempts != 2 {
			t.Errorf("task 1 should have 2 attempts, got %d", res.SWETasks[0].Attempts)
		}
		if res.SWETasks[0].Resolved {
			t.Errorf("task 1 should not be resolved")
		}
		// Tasks 2, 3, 4 should be cancelled with 0 attempts and 0 points
		for i := 1; i < 4; i++ {
			if res.SWETasks[i].Attempts != 0 {
				t.Errorf("task %d should have 0 attempts, got %d", i+1, res.SWETasks[i].Attempts)
			}
			if res.SWETasks[i].PointsAwarded != 0 {
				t.Errorf("task %d should have 0 points, got %d", i+1, res.SWETasks[i].PointsAwarded)
			}
			if !strings.Contains(res.SWETasks[i].TestOutput, "Dibatalkan") {
				t.Errorf("task %d output should contain 'Dibatalkan', got: %s", i+1, res.SWETasks[i].TestOutput)
			}
		}
		if cancelledNotified != 3 {
			t.Errorf("expected 3 cancelled task notifications, got %d", cancelledNotified)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("test timed out waiting for job result")
	}
}

