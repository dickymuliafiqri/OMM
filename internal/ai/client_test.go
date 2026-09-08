package ai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClient_GenerateCode_Success(t *testing.T) {
	mockResponse := chatResponse{
		ID:    "chatcmpl-test",
		Model: "mock-model",
		Choices: []chatChoice{
			{
				Index: 0,
				Message: struct {
					Role    string `json:"role"`
					Content string `json:"content"`
				}{
					Role: "assistant",
					Content: "```go\npackage main\nimport \"net/http\"\nfunc main() {\n  http.ListenAndServe(\":8080\", nil)\n}\n```",
				},
				FinishReason: "stop",
			},
		},
		Usage: chatUsage{
			PromptTokens:     85,
			CompletionTokens: 120,
			TotalTokens:      205,
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("Ekspektasi method POST, dapat: %s", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer test-secret-key" {
			t.Errorf("Authorization header tidak valid: %s", r.Header.Get("Authorization"))
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(mockResponse)
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL: server.URL,
		APIKey:  "test-secret-key",
		Model:   "mock-model",
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient gagal: %v", err)
	}

	result, err := client.GenerateCode(context.Background())
	if err != nil {
		t.Fatalf("GenerateCode gagal: %v", err)
	}

	if !strings.Contains(result.GoCode, "package main") || !strings.Contains(result.GoCode, "func main()") {
		t.Errorf("Kode Go hasil inferensi tidak valid: %s", result.GoCode)
	}
	if result.TotalTokens != 205 {
		t.Errorf("TotalTokens tidak sesuai: %d", result.TotalTokens)
	}
	if result.Model != "mock-model" {
		t.Errorf("Model tidak sesuai: %s", result.Model)
	}
}

func TestClient_GenerateCode_APIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]string{
				"message": "Invalid API key provided",
				"type":    "invalid_request_error",
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL: server.URL,
		APIKey:  "bad-key",
		Model:   "mock-model",
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient gagal: %v", err)
	}

	_, err = client.GenerateCode(context.Background())
	if err == nil {
		t.Fatalf("Ekspektasi error saat API key tidak valid, namun sukses")
	}
	if !strings.Contains(err.Error(), "401") || !strings.Contains(err.Error(), "Invalid API key") {
		t.Errorf("Pesan error tidak memuat informasi HTTP 401: %v", err)
	}
}

func TestClient_GenerateCode_Timeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL: server.URL,
		APIKey:  "key",
		Model:   "mock",
		Timeout: 50 * time.Millisecond, // Very short timeout
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.GenerateCode(context.Background())
	if err == nil {
		t.Fatalf("Ekspektasi timeout error, namun berhasil")
	}
	if !strings.Contains(err.Error(), "timeout") && !strings.Contains(err.Error(), "deadline") {
		t.Errorf("Ekspektasi pesan timeout, didapatkan: %v", err)
	}
}

func TestClient_ListModels(t *testing.T) {
	mockResponse := map[string]any{
		"data": []map[string]any{
			{"id": "gpt-4o"},
			{"id": "deepseek-chat"},
			{"id": "claude-3-5-sonnet"},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("Ekspektasi GET untuk /models, dapat: %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/models") {
			t.Errorf("Ekspektasi path berakhiran /models, dapat: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(mockResponse)
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL: server.URL,
		APIKey:  "test-key",
	})
	if err != nil {
		t.Fatalf("NewClient gagal: %v", err)
	}

	models, err := client.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels gagal: %v", err)
	}

	if len(models) != 3 {
		t.Fatalf("Ekspektasi 3 models, dapat: %d", len(models))
	}
	// Models must be sorted alphabetically
	if models[0] != "claude-3-5-sonnet" || models[1] != "deepseek-chat" || models[2] != "gpt-4o" {
		t.Errorf("Urutan model tidak sesuai sorting: %+v", models)
	}
}

func TestClient_TestModel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req chatRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		if req.Model == "valid-model" {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(chatResponse{
				Choices: []chatChoice{{Index: 0}},
			})
			return
		}

		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]string{
				"message": "Model not found or access denied",
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL: server.URL,
		APIKey:  "key",
	})
	if err != nil {
		t.Fatalf("NewClient gagal: %v", err)
	}

	// 1. Valid model
	if err := client.TestModel(context.Background(), "valid-model"); err != nil {
		t.Errorf("TestModel harusnya berhasil untuk valid-model: %v", err)
	}

	// 2. Invalid model
	if err := client.TestModel(context.Background(), "unknown-model"); err == nil {
		t.Errorf("TestModel harusnya error untuk unknown-model")
	} else if !strings.Contains(err.Error(), "404") {
		t.Errorf("Error harus memuat 404: %v", err)
	}
}

func TestClient_ListModels_HTMLResponse(t *testing.T) {
	// Simulate a website endpoint (e.g. https://ollama.com/models) that returns HTTP 200 HTML
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<!DOCTYPE html><html><head><title>Ollama Library</title></head><body>Models</body></html>"))
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL: server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.ListModels(context.Background())
	if err == nil {
		t.Fatalf("Ekspektasi error saat response adalah HTML")
	}
	if !strings.Contains(err.Error(), "HTML") {
		t.Errorf("Ekspektasi pesan error mendeteksi HTML, dapat: %v", err)
	}
}

func TestClient_ListModels_OllamaFallback(t *testing.T) {
	// Simulate Ollama native endpoint (/api/tags) when /models returns 404
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/tags" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"models": []map[string]any{
					{"name": "llama3:latest", "model": "llama3:latest"},
					{"name": "qwen2.5-coder:7b", "model": "qwen2.5-coder:7b"},
				},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("404 page not found"))
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL: server.URL, // Without /v1
	})
	if err != nil {
		t.Fatal(err)
	}

	models, err := client.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels harusnya berhasil via fallback /api/tags: %v", err)
	}
	if len(models) != 2 {
		t.Fatalf("Ekspektasi 2 model ditemukan dari Ollama, dapat: %d", len(models))
	}
	if models[0] != "llama3:latest" || models[1] != "qwen2.5-coder:7b" {
		t.Errorf("Model tidak sesuai: %+v", models)
	}
}

func TestClient_GenerateCodeWithSelfHealing_SuccessTurn1(t *testing.T) {
	mockResponse := chatResponse{
		ID:    "chatcmpl-turn1",
		Model: "mock-model",
		Choices: []chatChoice{
			{
				Index: 0,
				Message: struct {
					Role    string `json:"role"`
					Content string `json:"content"`
				}{
					Role:    "assistant",
					Content: "```go\npackage main\nimport \"net/http\"\nfunc main() {\n  http.ListenAndServe(\":8080\", nil)\n}\n```",
				},
			},
		},
		Usage: chatUsage{
			PromptTokens:     100,
			CompletionTokens: 50,
			TotalTokens:      150,
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(mockResponse)
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL: server.URL,
		Model:   "mock-model",
	})
	if err != nil {
		t.Fatal(err)
	}

	mockVerifier := func(code string) (bool, string, error) {
		return true, "", nil
	}

	res, err := client.GenerateCodeWithSelfHealing(context.Background(), 3, mockVerifier, nil)
	if err != nil {
		t.Fatalf("GenerateCodeWithSelfHealing gagal: %v", err)
	}

	if res.Attempts != 1 {
		t.Errorf("Ekspektasi Attempts == 1, dapat: %d", res.Attempts)
	}
	if res.TotalTokens != 150 {
		t.Errorf("Ekspektasi TotalTokens == 150, dapat: %d", res.TotalTokens)
	}
}

func TestClient_GenerateCodeWithSelfHealing_RepairOnTurn2(t *testing.T) {
	reqCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount++
		var req chatRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		w.Header().Set("Content-Type", "application/json")

		if reqCount == 1 {
			// Turn 1: code with compilation error
			_ = json.NewEncoder(w).Encode(chatResponse{
				Choices: []chatChoice{
					{
						Message: struct {
							Role    string `json:"role"`
							Content string `json:"content"`
						}{
							Role:    "assistant",
							Content: "```go\npackage main\nfunc main() { undefinedFunc() }\n```",
						},
					},
				},
				Usage: chatUsage{
					PromptTokens:     100,
					CompletionTokens: 40,
					TotalTokens:      140,
				},
			})
			return
		}

		// Turn 2: verify that the request carries the error message from turn 1
		hasFeedback := false
		for _, m := range req.Messages {
			if strings.Contains(m.Content, "undefined: undefinedFunc") {
				hasFeedback = true
				break
			}
		}
		if !hasFeedback {
			t.Errorf("Turn 2 tidak memuat pesan feedback kompilasi error!")
		}

		// Turn 2 produces the correct code
		_ = json.NewEncoder(w).Encode(chatResponse{
			Choices: []chatChoice{
				{
					Message: struct {
						Role    string `json:"role"`
						Content string `json:"content"`
					}{
						Role:    "assistant",
						Content: "```go\npackage main\nimport \"fmt\"\nfunc main() { fmt.Println(\"fixed\") }\n```",
					},
				},
			},
			Usage: chatUsage{
				PromptTokens:     160,
				CompletionTokens: 50,
				TotalTokens:      210,
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL: server.URL,
		Model:   "mock-repair-model",
	})
	if err != nil {
		t.Fatal(err)
	}

	progressCalled := false
	progressCallback := func(msg string) {
		if strings.Contains(msg, "Kompilasi ke-1 gagal") {
			progressCalled = true
		}
	}

	mockVerifier := func(code string) (bool, string, error) {
		if strings.Contains(code, "undefinedFunc") {
			return false, "./main.go:2:15: undefined: undefinedFunc", nil
		}
		return true, "", nil
	}

	res, err := client.GenerateCodeWithSelfHealing(context.Background(), 3, mockVerifier, progressCallback)
	if err != nil {
		t.Fatalf("GenerateCodeWithSelfHealing gagal: %v", err)
	}

	if res.Attempts != 2 {
		t.Errorf("Ekspektasi Attempts == 2, dapat: %d", res.Attempts)
	}
	if !progressCalled {
		t.Errorf("Progress callback harusnya dipanggil saat attempt 1 gagal")
	}
	// Token accumulation: 140 + 210 = 350
	if res.TotalTokens != 350 {
		t.Errorf("Ekspektasi total token terakumulasi 350, dapat: %d", res.TotalTokens)
	}
	if res.PromptTokens != 260 {
		t.Errorf("Ekspektasi total prompt token 260, dapat: %d", res.PromptTokens)
	}
	if res.CompletionTokens != 90 {
		t.Errorf("Ekspektasi total completion token 90, dapat: %d", res.CompletionTokens)
	}
	if !strings.Contains(res.GoCode, "fixed") {
		t.Errorf("Kode hasil perbaikan tidak sesuai: %s", res.GoCode)
	}
}

func TestClient_GenerateCodeWithSelfHealing_ExhaustedRetries(t *testing.T) {
	reqCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(chatResponse{
			Choices: []chatChoice{
				{
					Message: struct {
						Role    string `json:"role"`
						Content string `json:"content"`
					}{
						Role:    "assistant",
						Content: "```go\npackage main\nfunc main() { syntax error }\n```",
					},
				},
			},
			Usage: chatUsage{
				PromptTokens:     100,
				CompletionTokens: 20,
				TotalTokens:      120,
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL: server.URL,
		Model:   "failing-model",
	})
	if err != nil {
		t.Fatal(err)
	}

	mockVerifier := func(code string) (bool, string, error) {
		return false, "syntax error", nil
	}

	res, err := client.GenerateCodeWithSelfHealing(context.Background(), 3, mockVerifier, nil)
	if err != nil {
		t.Fatalf("GenerateCodeWithSelfHealing tidak boleh mengembalikan error sistem saat retry habis: %v", err)
	}

	if res.Attempts != 3 {
		t.Errorf("Ekspektasi Attempts == 3 setelah retry habis, dapat: %d", res.Attempts)
	}
	if reqCount != 3 {
		t.Errorf("Ekspektasi 3 requests ke server, dapat: %d", reqCount)
	}
	if res.TotalTokens != 360 {
		t.Errorf("Ekspektasi 360 tokens terakumulasi, dapat: %d", res.TotalTokens)
	}
}

func TestClient_GenerateCode_SlowStreamingBodyNotCanceled(t *testing.T) {
	mockResponse := chatResponse{
		ID:    "chatcmpl-stream-test",
		Model: "gpt-oss:120b",
		Choices: []chatChoice{
			{
				Index: 0,
				Message: struct {
					Role    string `json:"role"`
					Content string `json:"content"`
				}{
					Role:    "assistant",
					Content: "```go\npackage main\nimport \"net/http\"\nfunc main() {\n  http.ListenAndServe(\":8080\", nil)\n}\n```",
				},
			},
		},
		Usage: chatUsage{
			PromptTokens:     500,
			CompletionTokens: 800,
			TotalTokens:      1300,
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Flush header first to simulate streaming body
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		// Brief delay to simulate network data streaming transfer
		time.Sleep(60 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(mockResponse)
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL: server.URL,
		Model:   "gpt-oss:120b",
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := client.GenerateCodeWithSelfHealing(context.Background(), 1, nil, nil)
	if err != nil {
		t.Fatalf("GenerateCodeWithSelfHealing gagal saat membaca body streaming: %v", err)
	}

	if !strings.Contains(res.GoCode, "package main") {
		t.Errorf("Kode Go tidak terbaca dengan benar: %s", res.GoCode)
	}
	if res.TotalTokens != 1300 {
		t.Errorf("TotalTokens tidak sesuai: %d", res.TotalTokens)
	}
}

func TestExtractTail(t *testing.T) {
	if extractTail("", 10) != "" {
		t.Errorf("expected empty string")
	}
	short := "hello"
	if extractTail(short, 10) != "hello" {
		t.Errorf("expected 'hello', got '%s'", extractTail(short, 10))
	}
	long := "abcdefghijklmnopqrstuvwxyz"
	tail := extractTail(long, 5)
	if tail != "...vwxyz" {
		t.Errorf("expected '...vwxyz', got '%s'", tail)
	}
}

func TestEstimateTokens(t *testing.T) {
	msgs := []ChatMessage{
		{Role: "user", Content: "12345678"}, // 8 chars -> ~2 tokens
	}
	content := "123456789012" // 12 chars -> ~3 tokens
	p, c, tot := estimateTokens(msgs, content)
	if p != 2 || c != 3 || tot != 5 {
		t.Errorf("unexpected token estimation: p=%d, c=%d, tot=%d", p, c, tot)
	}
}

func TestClient_ChatStream_ReasoningAndCoding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("expected flusher")
		}

		// SSE chunk 1: Reasoning
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"I think we need a mutex.\"}}]}\n\n"))
		flusher.Flush()

		// SSE chunk 2: Code
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"```go\\npackage main\\n```\"}}]}\n\n"))
		flusher.Flush()

		// SSE chunk 3: Usage
		_, _ = w.Write([]byte("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":20,\"total_tokens\":30}}\n\n"))
		flusher.Flush()

		// SSE chunk 4: Done
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL: server.URL,
		Model:   "test-reasoning-model",
	})
	if err != nil {
		t.Fatal(err)
	}

	var phases []StreamPhase
	var snippets []string

	res, err := client.ChatStream(context.Background(), []ChatMessage{{Role: "user", Content: "fix this"}}, func(chunk StreamChunk) {
		phases = append(phases, chunk.Phase)
		snippets = append(snippets, chunk.Tail)
	})
	if err != nil {
		t.Fatalf("ChatStream failed: %v", err)
	}

	if len(phases) < 2 {
		t.Fatalf("expected at least 2 stream chunks, got %d", len(phases))
	}
	if phases[0] != PhaseReasoning {
		t.Errorf("expected first phase PhaseReasoning, got %s", phases[0])
	}
	if phases[1] != PhaseCoding {
		t.Errorf("expected second phase PhaseCoding, got %s", phases[1])
	}
	if !strings.Contains(res.Content, "package main") {
		t.Errorf("expected content to contain 'package main', got '%s'", res.Content)
	}
	if res.TotalTokens != 30 {
		t.Errorf("expected 30 total tokens, got %d", res.TotalTokens)
	}
}

func TestClient_ChatStream_ThinkTags(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		// Chunk with open think tag
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"<think>Analyzing bug\"}}]}\n\n"))
		flusher.Flush()

		// Chunk closing think tag and starting code
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"</think>```go\\npackage main\\n```\"}}]}\n\n"))
		flusher.Flush()

		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL: server.URL,
		Model:   "test-think-model",
	})
	if err != nil {
		t.Fatal(err)
	}

	var phases []StreamPhase
	res, err := client.ChatStream(context.Background(), []ChatMessage{{Role: "user", Content: "test"}}, func(chunk StreamChunk) {
		phases = append(phases, chunk.Phase)
	})
	if err != nil {
		t.Fatalf("ChatStream failed: %v", err)
	}

	if len(phases) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(phases))
	}
	if phases[0] != PhaseReasoning {
		t.Errorf("chunk 1 expected PhaseReasoning, got %s", phases[0])
	}
	if phases[1] != PhaseCoding {
		t.Errorf("chunk 2 expected PhaseCoding, got %s", phases[1])
	}
	if !strings.Contains(res.Content, "package main") {
		t.Errorf("expected res.Content to contain 'package main'")
	}
}

func TestClient_ChatStream_FallbackOn400(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)

		// If stream requested, reject with 400 Bad Request
		if stream, ok := body["stream"].(bool); ok && stream {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]string{
					"message": "streaming not supported on this endpoint",
				},
			})
			return
		}

		// Non-streaming fallback responds with 200 OK
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(chatResponse{
			Choices: []chatChoice{
				{
					Message: struct {
						Role    string `json:"role"`
						Content string `json:"content"`
					}{
						Role:    "assistant",
						Content: "fallback non-streaming response",
					},
				},
			},
			Usage: chatUsage{TotalTokens: 42},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL: server.URL,
		Model:   "test-model",
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := client.ChatStream(context.Background(), []ChatMessage{{Role: "user", Content: "hello"}}, nil)
	if err != nil {
		t.Fatalf("ChatStream expected to fallback and succeed, got err: %v", err)
	}
	if res.Content != "fallback non-streaming response" {
		t.Errorf("unexpected content: %s", res.Content)
	}
	if callCount != 2 {
		t.Errorf("expected 2 calls (1 stream, 1 fallback), got %d", callCount)
	}
}

func TestClient_ExplicitConfigAndMultiTurnChat(t *testing.T) {
	reqCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount++
		var req chatRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		if reqCount == 1 {
			// Turn 1
			if len(req.Messages) != 1 {
				t.Errorf("expected 1 message in turn 1, got %d", len(req.Messages))
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(chatResponse{
				Choices: []chatChoice{
					{
						Message: struct {
							Role    string `json:"role"`
							Content string `json:"content"`
						}{
							Role:    "assistant",
							Content: "Turn 1 answer",
						},
					},
				},
				Usage: chatUsage{
					PromptTokens:     50,
					CompletionTokens: 25,
					TotalTokens:      75,
				},
			})
			return
		}

		// Turn 2
		if len(req.Messages) != 3 {
			t.Errorf("expected 3 messages in turn 2, got %d", len(req.Messages))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(chatResponse{
			Choices: []chatChoice{
				{
					Message: struct {
						Role    string `json:"role"`
						Content string `json:"content"`
					}{
						Role:    "assistant",
						Content: "Turn 2 improved answer",
					},
				},
			},
			Usage: chatUsage{
				PromptTokens:     100,
				CompletionTokens: 40,
				TotalTokens:      140,
			},
		})
	}))
	defer server.Close()

	// Instantiate using ClientConfig
	cfg := ClientConfig{
		BaseURL: server.URL,
		APIKey:  "sk-test-explicit-key",
		Model:   "test-explicit-model",
		Timeout: 5 * time.Second,
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient failed with ClientConfig: %v", err)
	}

	// Turn 1
	messages := []ChatMessage{
		{Role: "user", Content: "initial question"},
	}
	res1, err := client.Chat(context.Background(), messages)
	if err != nil {
		t.Fatalf("Chat turn 1 failed: %v", err)
	}
	if res1.Content != "Turn 1 answer" {
		t.Errorf("unexpected content turn 1: %s", res1.Content)
	}

	// Turn 2
	messages = append(messages, ChatMessage{Role: "assistant", Content: res1.Content})
	messages = append(messages, ChatMessage{Role: "user", Content: "feedback on answer"})

	res2, err := client.Chat(context.Background(), messages)
	if err != nil {
		t.Fatalf("Chat turn 2 failed: %v", err)
	}
	if res2.Content != "Turn 2 improved answer" {
		t.Errorf("unexpected content turn 2: %s", res2.Content)
	}

	totalPromptTokens := res1.PromptTokens + res2.PromptTokens
	totalCompletionTokens := res1.CompletionTokens + res2.CompletionTokens
	totalTokens := res1.TotalTokens + res2.TotalTokens

	if totalPromptTokens != 150 {
		t.Errorf("expected 150 prompt tokens, got %d", totalPromptTokens)
	}
	if totalCompletionTokens != 65 {
		t.Errorf("expected 65 completion tokens, got %d", totalCompletionTokens)
	}
	if totalTokens != 215 {
		t.Errorf("expected 215 total tokens, got %d", totalTokens)
	}
}

