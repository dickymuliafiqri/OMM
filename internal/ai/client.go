package ai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"benchmark/pkg/logger"
)

// Config holds connection parameters for the AI provider (OpenAI-compatible)
type Config struct {
	BaseURL         string        // Provider API base URL (e.g. https://api.openai.com/v1 or https://openrouter.ai/api/v1)
	APIKey          string        // Secret API key
	Model           string        // AI model name (e.g. gpt-4o, deepseek-chat, claude-3-5-sonnet)
	Timeout         time.Duration // AI inference request timeout (default 3 minutes)
	Temperature     float64       // Model creativity temperature (default 0.2 for deterministic code)
	ReasoningEffort string        // AI reasoning effort level ("low", "medium", "high", default "low")
}

// GenerationResult stores the AI code generation result along with its metrics
type GenerationResult struct {
	RawResponse      string        `json:"raw_response"`
	GoCode           string        `json:"go_code"`
	PromptTokens     int           `json:"prompt_tokens"`
	CompletionTokens int           `json:"completion_tokens"`
	TotalTokens      int           `json:"total_tokens"`
	Latency          time.Duration `json:"latency"`
	Model            string        `json:"model"`
	Attempts         int           `json:"attempts"`
}

// CompileVerifier is a function that verifies code compilation (e.g. runner.VerifyCompile)
type CompileVerifier func(sourceCode string) (passed bool, stderrMsg string, err error)

// ProgressCallback is an optional function to notify caller interface about status
type ProgressCallback func(message string)

// Client is a universal client to interact with OpenAI-compatible AI APIs
type Client struct {
	rootBaseURL     string
	chatURL         string
	modelsURL       string
	apiKey          string
	model           string
	timeout         time.Duration
	temperature     float64
	reasoningEffort string
	httpClient      *http.Client
}

// NewClient initializes a new AI client
func NewClient(cfg Config) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("base_url tidak boleh kosong")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 3 * time.Minute // Default 3 minutes
	}
	if cfg.Temperature <= 0 {
		cfg.Temperature = 0.2
	}

	reasoningEffort := cfg.ReasoningEffort
	if reasoningEffort == "" {
		reasoningEffort = "low"
	}

	rawURL := strings.TrimRight(cfg.BaseURL, "/")
	rootURL := strings.TrimSuffix(rawURL, "/chat/completions")
	rootURL = strings.TrimSuffix(rootURL, "/models")
	rootURL = strings.TrimRight(rootURL, "/")

	return &Client{
		rootBaseURL:     rootURL,
		chatURL:         rootURL + "/chat/completions",
		modelsURL:       rootURL + "/models",
		apiKey:          cfg.APIKey,
		model:           cfg.Model,
		timeout:         cfg.Timeout,
		temperature:     cfg.Temperature,
		reasoningEffort: reasoningEffort,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
	}, nil
}

// SetModel updates the model used by the client
func (c *Client) SetModel(model string) {
	c.model = model
}

// SetReasoningEffort updates the reasoning effort level (e.g. "low", "medium", "high")
func (c *Client) SetReasoningEffort(effort string) {
	if effort == "" {
		effort = "low"
	}
	c.reasoningEffort = effort
}

// ReasoningEffort returns the current reasoning effort level
func (c *Client) ReasoningEffort() string {
	return c.reasoningEffort
}

// ListModels retrieves the list of available models from the /models endpoint or fallbacks (/v1/models, /api/tags)
func (c *Client) ListModels(ctx context.Context) ([]string, error) {
	start := time.Now()

	// Set of candidate endpoints tried intelligently:
	// 1. Default endpoint c.modelsURL
	// 2. If base URL does not end with /v1, try rootBaseURL + /v1/models (OpenAI standard)
	// 3. Native Ollama endpoint: rootBaseURL + /api/tags
	candidateURLs := []string{c.modelsURL}
	cleanRoot := strings.TrimRight(c.rootBaseURL, "/")
	if !strings.HasSuffix(cleanRoot, "/v1") {
		candidateURLs = append(candidateURLs, cleanRoot+"/v1/models")
	}
	candidateURLs = append(candidateURLs, cleanRoot+"/api/tags")

	var lastErr error
	for _, targetURL := range candidateURLs {
		reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		models, err := c.tryFetchModels(reqCtx, targetURL, start)
		cancel()
		if err == nil && len(models) > 0 {
			sort.Strings(models)
			// Synchronize chatURL if successful via /v1/models or /api/tags
			if strings.HasSuffix(targetURL, "/v1/models") || strings.HasSuffix(targetURL, "/api/tags") {
				c.modelsURL = targetURL
				c.chatURL = cleanRoot + "/v1/chat/completions"
			}
			logger.AIRes(c.rootBaseURL, "HTTP 200 OK", time.Since(start), 0, 0, 0, fmt.Sprintf("models_found=%d", len(models)))
			return models, nil
		}
		if err != nil {
			lastErr = err
		}
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("tidak ada model yang ditemukan dari provider")
}

func (c *Client) tryFetchModels(ctx context.Context, targetURL string, start time.Time) ([]string, error) {
	endpointPath := "/models"
	if u, err := url.Parse(targetURL); err == nil && u.Path != "" {
		endpointPath = u.Path
	}
	logger.AIReq(c.rootBaseURL, endpointPath, "ListModels", "")

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, fmt.Errorf("gagal membuat request ke %s: %w", targetURL, err)
	}

	if c.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("HTTP-Referer", "https://github.com/dickymuliafiqri/OMM")
	httpReq.Header.Set("X-Title", "Go Server Benchmark Bot")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		logger.AIRes(c.rootBaseURL, "ERROR", time.Since(start), 0, 0, 0, fmt.Sprintf("err=%v", err))
		return nil, fmt.Errorf("gagal menghubungi endpoint %s: %w", targetURL, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.AIRes(c.rootBaseURL, fmt.Sprintf("HTTP %d", resp.StatusCode), time.Since(start), 0, 0, 0, fmt.Sprintf("read_err=%v", err))
		return nil, fmt.Errorf("gagal membaca body response %s: %w", targetURL, err)
	}

	if resp.StatusCode != http.StatusOK {
		logger.AIRes(c.rootBaseURL, fmt.Sprintf("HTTP %d", resp.StatusCode), time.Since(start), 0, 0, 0, "status_not_ok")
		return nil, fmt.Errorf("endpoint %s mengembalikan HTTP %d: %s", targetURL, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	// Detect whether the response is an HTML web page instead of an API JSON response
	trimmedBody := strings.TrimSpace(string(respBody))
	isHTML := strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/html") ||
		strings.HasPrefix(trimmedBody, "<!") ||
		strings.HasPrefix(trimmedBody, "<html") ||
		strings.HasPrefix(trimmedBody, "<head")
	if isHTML {
		logger.AIRes(c.rootBaseURL, fmt.Sprintf("HTTP %d", resp.StatusCode), time.Since(start), 0, 0, 0, "html_response_not_json")
		return nil, fmt.Errorf("endpoint mengembalikan halaman web HTML (bukan API JSON). Pastikan URL adalah endpoint API (contoh: http://localhost:11434/v1 untuk Ollama lokal, bukan website https://ollama.com)")
	}

	var parsed struct {
		Data []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"data"`
		Models []struct {
			ID    string `json:"id"`
			Name  string `json:"name"`
			Model string `json:"model"`
		} `json:"models"`
	}

	if err := json.Unmarshal(respBody, &parsed); err != nil {
		logger.AIRes(c.rootBaseURL, fmt.Sprintf("HTTP %d", resp.StatusCode), time.Since(start), 0, 0, 0, "json_parse_err")
		return nil, fmt.Errorf("gagal parse JSON /models: %w", err)
	}

	unique := make(map[string]bool)
	var models []string

	addModel := func(id string) {
		id = strings.TrimSpace(id)
		if id != "" && !unique[id] {
			unique[id] = true
			models = append(models, id)
		}
	}

	for _, item := range parsed.Data {
		if item.ID != "" {
			addModel(item.ID)
		} else if item.Name != "" {
			addModel(item.Name)
		}
	}

	for _, item := range parsed.Models {
		if item.ID != "" {
			addModel(item.ID)
		} else if item.Name != "" {
			addModel(item.Name)
		} else if item.Model != "" {
			addModel(item.Model)
		}
	}

	if len(models) == 0 {
		return nil, fmt.Errorf("tidak ada ID model yang ditemukan pada respons endpoint %s", targetURL)
	}

	return models, nil
}

// TestModel performs a lightweight ping request to verify that the model is active and credentials are valid
func (c *Client) TestModel(ctx context.Context, model string) error {
	if strings.TrimSpace(model) == "" {
		return fmt.Errorf("nama model tidak boleh kosong")
	}

	start := time.Now()
	logger.AIReq(c.rootBaseURL, "/chat/completions", "PingTest", "model="+model)

	testReq := chatRequest{
		Model: model,
		Messages: []ChatMessage{
			{Role: "user", Content: "ping"},
		},
		Temperature: 0.1,
	}

	bodyBytes, err := json.Marshal(testReq)
	if err != nil {
		logger.AIRes(model, "ERROR", time.Since(start), 0, 0, 0, fmt.Sprintf("marshal_err=%v", err))
		return fmt.Errorf("gagal serialisasi ping payload: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.chatURL, bytes.NewReader(bodyBytes))
	if err != nil {
		logger.AIRes(model, "ERROR", time.Since(start), 0, 0, 0, fmt.Sprintf("http_req_err=%v", err))
		return fmt.Errorf("gagal membuat http request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	httpReq.Header.Set("HTTP-Referer", "https://github.com/dickymuliafiqri/OMM")
	httpReq.Header.Set("X-Title", "Go Server Benchmark Bot")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		logger.AIRes(model, "ERROR", time.Since(start), 0, 0, 0, fmt.Sprintf("err=%v", err))
		if reqCtx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("timeout koneksi saat menguji model %s (melebihi 25s)", model)
		}
		return fmt.Errorf("koneksi gagal saat menguji model: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.AIRes(model, fmt.Sprintf("HTTP %d", resp.StatusCode), time.Since(start), 0, 0, 0, fmt.Sprintf("read_err=%v", err))
		return fmt.Errorf("gagal membaca respons uji model: %w", err)
	}

	// Detect whether the response is HTML
	trimmedBody := strings.TrimSpace(string(respBody))
	isHTML := strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/html") ||
		strings.HasPrefix(trimmedBody, "<!") ||
		strings.HasPrefix(trimmedBody, "<html")
	if isHTML {
		logger.AIRes(model, fmt.Sprintf("HTTP %d", resp.StatusCode), time.Since(start), 0, 0, 0, "html_response_not_json")
		return fmt.Errorf("endpoint mengembalikan halaman web HTML (bukan API JSON). Pastikan Base URL adalah API provider")
	}

	if resp.StatusCode != http.StatusOK {
		// If 404 and rootBaseURL does not end with /v1, retry once with /v1/chat/completions
		if resp.StatusCode == http.StatusNotFound && !strings.HasSuffix(c.rootBaseURL, "/v1") {
			fallbackURL := strings.TrimRight(c.rootBaseURL, "/") + "/v1/chat/completions"
			fallbackReq, fErr := http.NewRequestWithContext(reqCtx, http.MethodPost, fallbackURL, bytes.NewReader(bodyBytes))
			if fErr == nil {
				fallbackReq.Header.Set("Content-Type", "application/json")
				if c.apiKey != "" {
					fallbackReq.Header.Set("Authorization", "Bearer "+c.apiKey)
				}
				fResp, doErr := c.httpClient.Do(fallbackReq)
				if doErr == nil {
					defer fResp.Body.Close()
					fBody, _ := io.ReadAll(fResp.Body)
					if fResp.StatusCode == http.StatusOK {
						c.chatURL = fallbackURL
						logger.AIRes(model, "HTTP 200 OK", time.Since(start), 0, 0, 0, "ping=OK (via /v1 fallback)")
						return nil
					}
					respBody = fBody
					resp.StatusCode = fResp.StatusCode
				}
			}
		}

		logger.AIRes(model, fmt.Sprintf("HTTP %d", resp.StatusCode), time.Since(start), 0, 0, 0, "status_not_ok")
		var errResp chatResponse
		_ = json.Unmarshal(respBody, &errResp)
		if errResp.Error != nil && errResp.Error.Message != "" {
			return fmt.Errorf("HTTP %d: %s", resp.StatusCode, errResp.Error.Message)
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	logger.AIRes(model, "HTTP 200 OK", time.Since(start), 0, 0, 0, "ping=OK")
	return nil
}

// ChatMessage represents a single message in a chat completions conversation
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatResult stores AI response text and token usage
type ChatResult struct {
	Content          string        `json:"content"`
	PromptTokens     int           `json:"prompt_tokens"`
	CompletionTokens int           `json:"completion_tokens"`
	TotalTokens      int           `json:"total_tokens"`
	Duration         time.Duration `json:"duration"`
}

// StreamPhase represents the current phase of AI generation in a stream
type StreamPhase string

const (
	PhaseReasoning StreamPhase = "REASONING" // Model is reasoning / thinking
	PhaseCoding    StreamPhase = "CODING"    // Model is generating code / answer
)

// StreamChunk conveys progress during an active SSE stream
type StreamChunk struct {
	Phase    StreamPhase // REASONING or CODING
	Delta    string      // Latest token or delta text
	Tail     string      // Trailing ~180-200 chars of current reasoning or response
	FullText string      // Full response accumulated so far
}

// StreamHandler is invoked when new tokens or stream chunks arrive
type StreamHandler func(chunk StreamChunk)

// ReasoningConfig defines reasoning configuration for providers like OpenRouter
type ReasoningConfig struct {
	Effort string `json:"effort,omitempty"`
}

type chatRequest struct {
	Model           string           `json:"model"`
	Messages        []ChatMessage    `json:"messages"`
	Temperature     float64          `json:"temperature"`
	ReasoningEffort string           `json:"reasoning_effort,omitempty"`
	Reasoning       *ReasoningConfig `json:"reasoning,omitempty"`
}

type chatChoice struct {
	Index   int `json:"index"`
	Message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"message"`
	FinishReason string `json:"finish_reason"`
}

type chatUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type chatResponse struct {
	ID      string       `json:"id"`
	Model   string       `json:"model"`
	Choices []chatChoice `json:"choices"`
	Usage   chatUsage    `json:"usage"`
	Error   *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    any    `json:"code"`
	} `json:"error,omitempty"`
}

// Chat sends multi-turn conversation history to the AI model and returns the response message
func (c *Client) Chat(ctx context.Context, messages []ChatMessage) (*ChatResult, error) {
	if strings.TrimSpace(c.model) == "" {
		return nil, fmt.Errorf("nama model tidak boleh kosong untuk inferensi AI")
	}

	start := time.Now()
	logger.AIReq(c.rootBaseURL, "/chat/completions", "Chat", fmt.Sprintf("model=%s messages=%d reasoning=%s", c.model, len(messages), c.reasoningEffort))

	reqBody := chatRequest{
		Model:           c.model,
		Messages:        messages,
		Temperature:     c.temperature,
		ReasoningEffort: c.reasoningEffort,
		Reasoning: &ReasoningConfig{
			Effort: c.reasoningEffort,
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		logger.AIRes(c.model, "ERROR", time.Since(start), 0, 0, 0, fmt.Sprintf("marshal_err=%v", err))
		return nil, fmt.Errorf("gagal serialisasi request payload: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.chatURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("gagal membuat http request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	httpReq.Header.Set("HTTP-Referer", "https://github.com/dickymuliafiqri/OMM")
	httpReq.Header.Set("X-Title", "Go Server Benchmark Bot")

	resp, err := c.httpClient.Do(httpReq)
	turnLatency := time.Since(start)
	if err != nil {
		if reqCtx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("timeout inferensi AI (%v terlampaui): %w", c.timeout, err)
		}
		return nil, fmt.Errorf("gagal menghubungi AI provider di %s: %w", c.chatURL, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		if reqCtx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("timeout inferensi AI saat membaca response (%v terlampaui): %w", c.timeout, err)
		}
		return nil, fmt.Errorf("gagal membaca response body: %w", err)
	}

	trimmedBody := strings.TrimSpace(string(respBody))
	isHTML := strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/html") ||
		strings.HasPrefix(trimmedBody, "<!") ||
		strings.HasPrefix(trimmedBody, "<html")
	if isHTML {
		logger.AIRes(c.model, fmt.Sprintf("HTTP %d", resp.StatusCode), turnLatency, 0, 0, 0, "html_response_not_json")
		return nil, fmt.Errorf("AI provider mengembalikan halaman web HTML (bukan API JSON). Pastikan Base URL adalah endpoint API valid")
	}

	var chatResp chatResponse
	if err := json.Unmarshal(respBody, &chatResp); err != nil {
		logger.AIRes(c.model, fmt.Sprintf("HTTP %d", resp.StatusCode), turnLatency, 0, 0, 0, "json_parse_err")
		return nil, fmt.Errorf("gagal parse json response AI (HTTP %d): %w (Body: %s)", resp.StatusCode, err, string(respBody))
	}

	if resp.StatusCode != http.StatusOK {
		logger.AIRes(c.model, fmt.Sprintf("HTTP %d", resp.StatusCode), turnLatency, 0, 0, 0, "status_not_ok")
		errMsg := fmt.Sprintf("AI provider mengembalikan status HTTP %d", resp.StatusCode)
		if chatResp.Error != nil && chatResp.Error.Message != "" {
			errMsg += fmt.Sprintf(": %s (type: %s)", chatResp.Error.Message, chatResp.Error.Type)
		} else {
			errMsg += fmt.Sprintf(": %s", string(respBody))
		}
		return nil, fmt.Errorf("%s", errMsg)
	}

	if len(chatResp.Choices) == 0 {
		logger.AIRes(c.model, fmt.Sprintf("HTTP %d", resp.StatusCode), turnLatency, 0, 0, 0, "empty_choices")
		return nil, fmt.Errorf("respons AI tidak memiliki choices (kosong)")
	}

	promptTok := chatResp.Usage.PromptTokens
	compTok := chatResp.Usage.CompletionTokens
	totTok := chatResp.Usage.TotalTokens
	if totTok == 0 && (promptTok > 0 || compTok > 0) {
		totTok = promptTok + compTok
	}

	rawContent := chatResp.Choices[0].Message.Content
	logger.AIRes(c.model, "HTTP 200 OK", turnLatency, promptTok, compTok, totTok, fmt.Sprintf("content_len=%d", len(rawContent)))

	return &ChatResult{
		Content:          rawContent,
		PromptTokens:     promptTok,
		CompletionTokens: compTok,
		TotalTokens:      totTok,
		Duration:         turnLatency,
	}, nil
}

// extractTail returns the last maxRunes runes of s, prefixed with "..." if truncated
func extractTail(s string, maxRunes int) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	return "..." + string(r[len(r)-maxRunes:])
}

// estimateTokens provides a heuristic token count when provider does not send usage stats in stream
func estimateTokens(messages []ChatMessage, content string) (int, int, int) {
	promptChars := 0
	for _, m := range messages {
		promptChars += len(m.Content)
	}
	promptTok := promptChars / 4
	if promptTok < 1 {
		promptTok = 1
	}
	compTok := len(content) / 4
	if compTok < 1 {
		compTok = 1
	}
	return promptTok, compTok, promptTok + compTok
}

// ChatStream sends multi-turn conversation history to the AI model with SSE streaming enabled.
// It invokes onStream with StreamChunk on new tokens and returns the complete ChatResult.
func (c *Client) ChatStream(ctx context.Context, messages []ChatMessage, onStream StreamHandler) (*ChatResult, error) {
	if strings.TrimSpace(c.model) == "" {
		return nil, fmt.Errorf("nama model tidak boleh kosong untuk inferensi AI")
	}

	start := time.Now()
	logger.AIReq(c.rootBaseURL, "/chat/completions", "ChatStream", fmt.Sprintf("model=%s messages=%d", c.model, len(messages)))

	type streamOpts struct {
		IncludeUsage bool `json:"include_usage"`
	}
	reqBody := struct {
		Model           string           `json:"model"`
		Messages        []ChatMessage    `json:"messages"`
		Temperature     float64          `json:"temperature"`
		Stream          bool             `json:"stream"`
		ReasoningEffort string           `json:"reasoning_effort,omitempty"`
		Reasoning       *ReasoningConfig `json:"reasoning,omitempty"`
		StreamOptions   *streamOpts      `json:"stream_options,omitempty"`
	}{
		Model:           c.model,
		Messages:        messages,
		Temperature:     c.temperature,
		Stream:          true,
		ReasoningEffort: c.reasoningEffort,
		Reasoning: &ReasoningConfig{
			Effort: c.reasoningEffort,
		},
		StreamOptions: &streamOpts{
			IncludeUsage: true,
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		logger.AIRes(c.model, "ERROR", time.Since(start), 0, 0, 0, fmt.Sprintf("marshal_err=%v", err))
		return nil, fmt.Errorf("gagal serialisasi request payload: %w", err)
	}

	reqCtx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && c.timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.chatURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("gagal membuat http request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	if c.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	httpReq.Header.Set("HTTP-Referer", "https://github.com/dickymuliafiqri/OMM")
	httpReq.Header.Set("X-Title", "Go Server Benchmark Bot")

	resp, err := c.httpClient.Do(httpReq)
	turnLatency := time.Since(start)
	if err != nil {
		if reqCtx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("timeout inferensi AI (%v terlampaui): %w", c.timeout, err)
		}
		return nil, fmt.Errorf("gagal menghubungi AI provider di %s: %w", c.chatURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		logger.AIRes(c.model, fmt.Sprintf("HTTP %d", resp.StatusCode), turnLatency, 0, 0, 0, "status_not_ok")

		// If streaming is not supported by endpoint (e.g. 400 Bad Request or 422), fallback to standard Chat
		if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnprocessableEntity {
			logger.Warn("AI", "Streaming gagal (HTTP %d), fallback ke Chat non-streaming...", resp.StatusCode)
			return c.Chat(ctx, messages)
		}

		var chatResp chatResponse
		_ = json.Unmarshal(respBody, &chatResp)
		errMsg := fmt.Sprintf("AI provider mengembalikan status HTTP %d", resp.StatusCode)
		if chatResp.Error != nil && chatResp.Error.Message != "" {
			errMsg += fmt.Sprintf(": %s (type: %s)", chatResp.Error.Message, chatResp.Error.Type)
		} else {
			errMsg += fmt.Sprintf(": %s", strings.TrimSpace(string(respBody)))
		}
		return nil, fmt.Errorf("%s", errMsg)
	}

	var fullContent strings.Builder
	var fullReasoning strings.Builder
	var promptTok, compTok, totTok int

	reader := bufio.NewReader(resp.Body)

	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil && len(line) == 0 {
			if errors.Is(readErr, io.EOF) {
				break
			}
			if reqCtx.Err() == context.DeadlineExceeded {
				return nil, fmt.Errorf("timeout saat streaming respons AI: %w", readErr)
			}
			return nil, fmt.Errorf("gagal membaca stream dari AI: %w", readErr)
		}

		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, ":") {
			if readErr != nil && errors.Is(readErr, io.EOF) {
				break
			}
			continue
		}

		if !strings.HasPrefix(line, "data:") {
			if readErr != nil && errors.Is(readErr, io.EOF) {
				break
			}
			continue
		}

		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			break
		}

		var chunk struct {
			Choices []struct {
				Delta struct {
					Content          string `json:"content"`
					ReasoningContent string `json:"reasoning_content"`
					Reasoning        string `json:"reasoning"`
				} `json:"delta"`
				FinishReason any `json:"finish_reason"`
			} `json:"choices"`
			Usage *struct {
				PromptTokens     int `json:"prompt_tokens"`
				CompletionTokens int `json:"completion_tokens"`
				TotalTokens      int `json:"total_tokens"`
			} `json:"usage"`
		}

		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			if readErr != nil && errors.Is(readErr, io.EOF) {
				break
			}
			continue
		}

		if chunk.Usage != nil {
			if chunk.Usage.PromptTokens > 0 {
				promptTok = chunk.Usage.PromptTokens
			}
			if chunk.Usage.CompletionTokens > 0 {
				compTok = chunk.Usage.CompletionTokens
			}
			if chunk.Usage.TotalTokens > 0 {
				totTok = chunk.Usage.TotalTokens
			}
		}

		if len(chunk.Choices) == 0 {
			if readErr != nil && errors.Is(readErr, io.EOF) {
				break
			}
			continue
		}

		delta := chunk.Choices[0].Delta
		reasoningText := delta.ReasoningContent
		if reasoningText == "" {
			reasoningText = delta.Reasoning
		}

		var phase StreamPhase = PhaseCoding
		var currentTail string

		if reasoningText != "" {
			fullReasoning.WriteString(reasoningText)
			phase = PhaseReasoning
			currentTail = extractTail(fullReasoning.String(), 180)
		} else if delta.Content != "" {
			fullContent.WriteString(delta.Content)
			currStr := fullContent.String()

			// Check if currently inside <think>...</think>
			if strings.Contains(currStr, "<think>") && !strings.Contains(currStr, "</think>") {
				phase = PhaseReasoning
				idx := strings.Index(currStr, "<think>")
				insideThink := currStr[idx+7:]
				currentTail = extractTail(insideThink, 180)
			} else {
				phase = PhaseCoding
				if idx := strings.LastIndex(currStr, "</think>"); idx != -1 {
					afterThink := currStr[idx+8:]
					currentTail = extractTail(afterThink, 180)
				} else {
					currentTail = extractTail(currStr, 180)
				}
			}
		}

		if onStream != nil && (delta.Content != "" || reasoningText != "") {
			onStream(StreamChunk{
				Phase:    phase,
				Delta:    delta.Content + reasoningText,
				Tail:     currentTail,
				FullText: fullContent.String(),
			})
		}

		if readErr != nil && errors.Is(readErr, io.EOF) {
			break
		}
	}

	finalContent := fullContent.String()
	if strings.TrimSpace(finalContent) == "" && fullReasoning.Len() > 0 {
		finalContent = fullReasoning.String()
	}

	if totTok == 0 {
		pTok, cTok, tTok := estimateTokens(messages, finalContent)
		promptTok = pTok
		compTok = cTok
		totTok = tTok
	}

	turnLatency = time.Since(start)
	logger.AIRes(c.model, "HTTP 200 OK", turnLatency, promptTok, compTok, totTok, fmt.Sprintf("stream_len=%d", len(finalContent)))

	return &ChatResult{
		Content:          finalContent,
		PromptTokens:     promptTok,
		CompletionTokens: compTok,
		TotalTokens:      totTok,
		Duration:         turnLatency,
	}, nil
}

// GenerateCodeWithSelfHealing sends a prompt to the AI model with a self-healing loop (compiler feedback).
// If the code fails to compile, the compiler error message (go build -race) is sent back to the AI
// as a multi-turn chat message for repair, up to maxAttempts times (max 3 times).
func (c *Client) GenerateCodeWithSelfHealing(
	ctx context.Context,
	maxAttempts int,
	verifier CompileVerifier,
	progressFn ProgressCallback,
	customPrompt ...string,
) (*GenerationResult, error) {
	if strings.TrimSpace(c.model) == "" {
		return nil, fmt.Errorf("nama model tidak boleh kosong untuk inferensi AI")
	}

	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	userPrompt := "Write a complete Go program in a single file inside ```go ... ``` code block."
	if len(customPrompt) > 0 && strings.TrimSpace(customPrompt[0]) != "" {
		userPrompt = customPrompt[0]
	}

	messages := []ChatMessage{
		{Role: "system", Content: "You are an expert Go systems engineer."},
		{Role: "user", Content: userPrompt},
	}

	totalPromptTokens := 0
	totalCompletionTokens := 0
	totalTokens := 0
	var totalLatency time.Duration

	var lastGoCode string
	var lastRawContent string

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		start := time.Now()
		logger.AIReq(c.rootBaseURL, "/chat/completions", "GenerateCode", fmt.Sprintf("model=%s attempt=%d/%d messages=%d", c.model, attempt, maxAttempts, len(messages)))

		reqBody := chatRequest{
			Model:           c.model,
			Messages:        messages,
			Temperature:     c.temperature,
			ReasoningEffort: c.reasoningEffort,
			Reasoning: &ReasoningConfig{
				Effort: c.reasoningEffort,
			},
		}

		bodyBytes, err := json.Marshal(reqBody)
		if err != nil {
			logger.AIRes(c.model, "ERROR", time.Since(start), 0, 0, 0, fmt.Sprintf("marshal_err=%v", err))
			return nil, fmt.Errorf("gagal serialisasi request payload: %w", err)
		}

		var respBody []byte
		var respStatusCode int
		var isHTML bool
		var turnLatency time.Duration

		callErr := func() error {
			reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
			defer cancel()

			httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.chatURL, bytes.NewReader(bodyBytes))
			if err != nil {
				return fmt.Errorf("gagal membuat http request: %w", err)
			}

			httpReq.Header.Set("Content-Type", "application/json")
			if c.apiKey != "" {
				httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
			}
			// Additional headers for OpenRouter / aggregators compatibility
			httpReq.Header.Set("HTTP-Referer", "https://github.com/dickymuliafiqri/OMM")
			httpReq.Header.Set("X-Title", "Go Server Benchmark Bot")

			resp, err := c.httpClient.Do(httpReq)
			turnLatency = time.Since(start)
			if err != nil {
				if reqCtx.Err() == context.DeadlineExceeded {
					return fmt.Errorf("timeout inferensi AI (%v terlampaui): %w", c.timeout, err)
				}
				return fmt.Errorf("gagal menghubungi AI provider di %s: %w", c.chatURL, err)
			}
			defer resp.Body.Close()

			respStatusCode = resp.StatusCode

			b, err := io.ReadAll(resp.Body)
			if err != nil {
				if reqCtx.Err() == context.DeadlineExceeded {
					return fmt.Errorf("timeout inferensi AI saat membaca response (%v terlampaui): %w", c.timeout, err)
				}
				return fmt.Errorf("gagal membaca response body: %w", err)
			}
			respBody = b

			trimmedBody := strings.TrimSpace(string(respBody))
			isHTML = strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/html") ||
				strings.HasPrefix(trimmedBody, "<!") ||
				strings.HasPrefix(trimmedBody, "<html")
			return nil
		}()

		totalLatency += turnLatency

		if callErr != nil {
			logger.AIRes(c.model, "ERROR", turnLatency, 0, 0, 0, fmt.Sprintf("err=%v", callErr))
			return nil, callErr
		}

		if isHTML {
			logger.AIRes(c.model, fmt.Sprintf("HTTP %d", respStatusCode), turnLatency, 0, 0, 0, "html_response_not_json")
			return nil, fmt.Errorf("AI provider mengembalikan halaman web HTML (bukan API JSON). Pastikan Base URL adalah endpoint API valid")
		}

		var chatResp chatResponse
		if err := json.Unmarshal(respBody, &chatResp); err != nil {
			logger.AIRes(c.model, fmt.Sprintf("HTTP %d", respStatusCode), turnLatency, 0, 0, 0, "json_parse_err")
			return nil, fmt.Errorf("gagal parse json response AI (HTTP %d): %w (Body: %s)", respStatusCode, err, string(respBody))
		}

		if respStatusCode != http.StatusOK {
			logger.AIRes(c.model, fmt.Sprintf("HTTP %d", respStatusCode), turnLatency, 0, 0, 0, "status_not_ok")
			errMsg := fmt.Sprintf("AI provider mengembalikan status HTTP %d", respStatusCode)
			if chatResp.Error != nil && chatResp.Error.Message != "" {
				errMsg += fmt.Sprintf(": %s (type: %s)", chatResp.Error.Message, chatResp.Error.Type)
			} else {
				errMsg += fmt.Sprintf(": %s", string(respBody))
			}
			return nil, fmt.Errorf("%s", errMsg)
		}

		if len(chatResp.Choices) == 0 {
			logger.AIRes(c.model, fmt.Sprintf("HTTP %d", respStatusCode), turnLatency, 0, 0, 0, "empty_choices")
			return nil, fmt.Errorf("respons AI tidak memiliki choices (kosong)")
		}

		// Accumulate token usage
		promptTok := chatResp.Usage.PromptTokens
		compTok := chatResp.Usage.CompletionTokens
		totTok := chatResp.Usage.TotalTokens
		if totTok == 0 && (promptTok > 0 || compTok > 0) {
			totTok = promptTok + compTok
		}
		totalPromptTokens += promptTok
		totalCompletionTokens += compTok
		totalTokens += totTok

		rawContent := chatResp.Choices[0].Message.Content
		goCode, err := ExtractGoCode(rawContent)
		if err != nil {
			logger.AIRes(c.model, fmt.Sprintf("HTTP %d", respStatusCode), turnLatency, promptTok, compTok, totTok, fmt.Sprintf("extract_err=%v", err))
			return nil, fmt.Errorf("gagal ekstraksi kode Go dari respons AI: %w\n(Teks: %s)", err, rawContent)
		}

		lastGoCode = goCode
		lastRawContent = rawContent

		lineCount := len(strings.Split(goCode, "\n"))
		logger.AIRes(c.model, "HTTP 200 OK", turnLatency, promptTok, compTok, totTok, fmt.Sprintf("attempt=%d code_lines=%d", attempt, lineCount))

		// If no verifier is provided, immediately return the result on the first attempt
		if verifier == nil {
			return &GenerationResult{
				RawResponse:      lastRawContent,
				GoCode:           lastGoCode,
				PromptTokens:     totalPromptTokens,
				CompletionTokens: totalCompletionTokens,
				TotalTokens:      totalTokens,
				Latency:          totalLatency,
				Model:            c.model,
				Attempts:         attempt,
			}, nil
		}

		// Perform ephemeral compilation verification
		passed, stderrMsg, verifierErr := verifier(goCode)
		if verifierErr != nil {
			logger.Warn("AI", "Verifier internal error: %v, melanjutkan evaluasi dengan kode saat ini", verifierErr)
			passed = true
		}

		if passed {
			if attempt > 1 {
				logger.Sys("AI", "Model %s berhasil memperbaiki kode pada attempt %d!", c.model, attempt)
			}
			return &GenerationResult{
				RawResponse:      lastRawContent,
				GoCode:           lastGoCode,
				PromptTokens:     totalPromptTokens,
				CompletionTokens: totalCompletionTokens,
				TotalTokens:      totalTokens,
				Latency:          totalLatency,
				Model:            c.model,
				Attempts:         attempt,
			}, nil
		}

		// Compilation failed on this attempt
		logger.Warn("AI", "Model %s gagal kompilasi pada attempt %d/%d: %s", c.model, attempt, maxAttempts, stderrMsg)

		if attempt >= maxAttempts {
			// Attempts exhausted, return the last code to be evaluated by the benchmark suite (will be scored 0 for compilation)
			return &GenerationResult{
				RawResponse:      lastRawContent,
				GoCode:           lastGoCode,
				PromptTokens:     totalPromptTokens,
				CompletionTokens: totalCompletionTokens,
				TotalTokens:      totalTokens,
				Latency:          totalLatency,
				Model:            c.model,
				Attempts:         attempt,
			}, nil
		}

		// Report progress if callback is available
		if progressFn != nil {
			progressFn(fmt.Sprintf("Kompilasi ke-%d gagal. Mengirim feedback error ke AI untuk perbaikan mandiri (Percobaan %d/%d)...", attempt, attempt+1, maxAttempts))
		}

		// Compose multi-turn history messages for the next repair round
		messages = append(messages, ChatMessage{
			Role:    "assistant",
			Content: rawContent,
		})

		cleanedErr := strings.TrimSpace(stderrMsg)
		if len(cleanedErr) > 2000 {
			cleanedErr = cleanedErr[:2000] + "\n...(truncated)"
		}

		repairPrompt := fmt.Sprintf("Your Go code failed to compile with the following compiler error (go build -race):\n\n```\n%s\n```\n\nPlease fix all compilation and syntax errors. Return the COMPLETE runnable Go code inside a single ```go ... ``` markdown code block. Do NOT omit any existing functions or logic.", cleanedErr)

		messages = append(messages, ChatMessage{
			Role:    "user",
			Content: repairPrompt,
		})
	}

	return &GenerationResult{
		RawResponse:      lastRawContent,
		GoCode:           lastGoCode,
		PromptTokens:     totalPromptTokens,
		CompletionTokens: totalCompletionTokens,
		TotalTokens:      totalTokens,
		Latency:          totalLatency,
		Model:            c.model,
		Attempts:         maxAttempts,
	}, nil
}

// GenerateCode sends a standard evaluation prompt to the AI model and extracts the generated Go code
func (c *Client) GenerateCode(ctx context.Context, customPrompt ...string) (*GenerationResult, error) {
	return c.GenerateCodeWithSelfHealing(ctx, 1, nil, nil, customPrompt...)
}
