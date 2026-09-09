package worker

import (
	"context"
	"encoding/json"
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

type recordingEvaluator struct {
	called atomic.Int32
}

func (r *recordingEvaluator) Evaluate(ctx context.Context, task *swe.Task, solutionCode string, attempt int) (*swe.EvalResult, error) {
	r.called.Add(1)
	return &swe.EvalResult{
		TaskID:    task.ID,
		Tier:      task.Tier,
		Points:    task.Points,
		MaxPoints: task.Points,
		Resolved:  true,
		Attempts:  attempt,
	}, nil
}

func TestLadderRunner_PreflightVerification_Failure(t *testing.T) {
	var pingReceived atomic.Bool

	aiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pingReceived.Store(true)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]string{
				"message": "API key tidak valid atau kedaluwarsa",
			},
		})
	}))
	defer aiServer.Close()

	eval := &recordingEvaluator{}
	runner := &DefaultLadderRunner{
		Evaluator: eval,
		Tasks: []*swe.Task{
			{ID: "task-1", Tier: swe.TierJunior, Points: 20},
		},
		TaskTimeout: 5 * time.Second,
	}

	job := &BenchJob{
		JobID:   "verify-fail-job",
		BaseURL: aiServer.URL,
		APIKey:  "invalid-key",
		Model:   "gpt-4o",
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

	if err == nil {
		t.Fatalf("expected error when AI verification fails, got nil")
	}

	if !strings.Contains(err.Error(), "verifikasi model AI gagal") {
		t.Errorf("expected error to mention 'verifikasi model AI gagal', got: %v", err)
	}

	if !strings.Contains(err.Error(), "API key tidak valid") {
		t.Errorf("expected error to preserve provider message, got: %v", err)
	}

	if res != nil {
		t.Errorf("expected nil LadderResult on verification failure, got: %+v", res)
	}

	if !pingReceived.Load() {
		t.Errorf("expected ping request to be sent to AI server")
	}

	if eval.called.Load() != 0 {
		t.Errorf("evaluator must not be called when verification fails, called %d times", eval.called.Load())
	}

	mu.Lock()
	defer mu.Unlock()

	hasVerifyingPhase := false
	for _, p := range phases {
		if p == "VERIFYING_MODEL" {
			hasVerifyingPhase = true
			break
		}
	}
	if !hasVerifyingPhase {
		t.Errorf("expected VERIFYING_MODEL phase progress, got: %v", phases)
	}

	hasErrorLog := false
	for _, l := range logs {
		if strings.Contains(l, "❌ Verifikasi model gagal") {
			hasErrorLog = true
			break
		}
	}
	if !hasErrorLog {
		t.Errorf("expected error log line about verification failure, got: %v", logs)
	}
}

func TestLadderRunner_PreflightVerification_Success(t *testing.T) {
	var pingReceived atomic.Bool
	var chatReceived atomic.Bool

	aiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		messages, _ := body["messages"].([]any)

		w.Header().Set("Content-Type", "application/json")

		if len(messages) == 1 {
			firstMsg, _ := messages[0].(map[string]any)
			if firstMsg["content"] == "ping" {
				pingReceived.Store(true)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"choices": []map[string]any{
						{"message": map[string]string{"role": "assistant", "content": "pong"}},
					},
				})
				return
			}
		}

		chatReceived.Store(true)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{"message": map[string]string{"role": "assistant", "content": "```go\npackage main\n```"}},
			},
			"usage": map[string]int{
				"prompt_tokens":     100,
				"completion_tokens": 50,
				"total_tokens":      150,
			},
		})
	}))
	defer aiServer.Close()

	eval := &recordingEvaluator{}
	runner := &DefaultLadderRunner{
		Evaluator: eval,
		Tasks: []*swe.Task{
			{ID: "task-1", Tier: swe.TierJunior, Points: 20},
		},
		TaskTimeout: 5 * time.Second,
	}

	job := &BenchJob{
		JobID:   "verify-success-job",
		BaseURL: aiServer.URL,
		APIKey:  "valid-key",
		Model:   "gpt-4o",
	}

	var logs []string
	var mu sync.Mutex

	onLog := func(line string, level string) {
		mu.Lock()
		logs = append(logs, line)
		mu.Unlock()
	}

	res, err := runner.RunLadder(context.Background(), job, nil, onLog)
	if err != nil {
		t.Fatalf("expected RunLadder to succeed, got: %v", err)
	}

	if res == nil {
		t.Fatal("expected non-nil LadderResult")
	}

	if !pingReceived.Load() {
		t.Errorf("expected pre-flight ping to be executed")
	}

	if !chatReceived.Load() {
		t.Errorf("expected task inference to be executed after verification")
	}

	if eval.called.Load() != 1 {
		t.Errorf("expected evaluator to be called once, got %d", eval.called.Load())
	}

	mu.Lock()
	defer mu.Unlock()

	hasSuccessLog := false
	for _, l := range logs {
		if strings.Contains(l, "✅ Verifikasi model AI") {
			hasSuccessLog = true
			break
		}
	}
	if !hasSuccessLog {
		t.Errorf("expected success log line, got: %v", logs)
	}
}

func TestExecutor_PreflightVerificationFailure_Lifecycle(t *testing.T) {
	// 1. Mock callback receiver (omm-web)
	var callbackCalls []string
	var lastErrorPayload *ErrorPayload
	var mu sync.Mutex

	cbServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var p map[string]any
		_ = json.Unmarshal(body, &p)
		pType, _ := p["type"].(string)

		mu.Lock()
		callbackCalls = append(callbackCalls, pType)
		if pType == "error" {
			var errP ErrorPayload
			_ = json.Unmarshal(body, &errP)
			lastErrorPayload = &errP
		}
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer cbServer.Close()

	// 2. Mock AI Server returning 404 Model Not Found
	aiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]string{
				"message": "The model 'nonexistent-model' does not exist or you do not have access.",
			},
		})
	}))
	defer aiServer.Close()

	met := metrics.New()
	cbClient := NewCallbackClient(2*time.Second, 10*time.Millisecond, met)
	runner := NewDefaultLadderRunner(&recordingEvaluator{})
	runner.Tasks = []*swe.Task{
		{ID: "task-1", Tier: swe.TierJunior, Points: 20},
	}
	executor := NewDefaultExecutor(cbClient, runner, met)

	job := &BenchJob{
		JobID:          "job-verify-lifecycle",
		CallbackURL:    cbServer.URL,
		CallbackSecret: "secret-1234567890123456",
		BaseURL:        aiServer.URL,
		APIKey:         "secret-ai-key-to-zero",
		Model:          "nonexistent-model",
	}

	executor.Execute(context.Background(), job)

	// API key MUST be zeroed out
	if job.APIKey != "" {
		t.Errorf("expected API key to be zeroed out, got: %s", job.APIKey)
	}

	// Metrics: JobsFailed must be 1, JobsCompleted must be 0
	if met.JobsFailed.Load() != 1 {
		t.Errorf("expected JobsFailed 1, got %d", met.JobsFailed.Load())
	}
	if met.JobsCompleted.Load() != 0 {
		t.Errorf("expected JobsCompleted 0, got %d", met.JobsCompleted.Load())
	}

	mu.Lock()
	defer mu.Unlock()

	// Error callback MUST have been dispatched
	if lastErrorPayload == nil {
		t.Fatalf("expected error payload to be dispatched to callback URL, calls: %v", callbackCalls)
	}

	if lastErrorPayload.Phase != "VERIFYING_MODEL" {
		t.Errorf("expected error phase 'VERIFYING_MODEL', got '%s'", lastErrorPayload.Phase)
	}

	if !strings.Contains(lastErrorPayload.Message, "verifikasi model AI gagal") {
		t.Errorf("expected error message to mention 'verifikasi model AI gagal', got '%s'", lastErrorPayload.Message)
	}

	if !strings.Contains(lastErrorPayload.Message, "nonexistent-model") {
		t.Errorf("expected error message to contain model name, got '%s'", lastErrorPayload.Message)
	}

	// Result payload must NOT be dispatched
	for _, call := range callbackCalls {
		if call == "result" {
			t.Errorf("unexpected 'result' callback dispatched on pre-flight verification failure")
		}
	}
}
