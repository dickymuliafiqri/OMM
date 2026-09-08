package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"benchmark/internal/config"
	"benchmark/internal/metrics"
)

func newTestRouterConfig(benchSecret string, allowedOrigins []string, globalLimit, perSourceLimit int) RouterConfig {
	cfg := &config.Config{
		ListenAddr:         ":9090",
		BenchSecret:        benchSecret,
		AllowedOrigins:     allowedOrigins,
		MaxConcurrentJobs:  3,
		JobTimeoutSec:      300,
		GlobalRateLimit:    globalLimit,
		PerSourceRateLimit: perSourceLimit,
		LogLevel:           "info",
	}

	rl := NewRateLimiter(globalLimit, perSourceLimit)

	return RouterConfig{
		Config:      cfg,
		RateLimiter: rl,
	}
}

func TestRouter_PublicHealthAndReady(t *testing.T) {
	rc := newTestRouterConfig("test-secret", nil, 100, 100)
	defer rc.RateLimiter.Close()
	router := NewRouter(rc)

	// Test GET /healthz
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("expected application/json, got %s", ct)
	}

	var healthRes map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&healthRes); err != nil {
		t.Fatalf("failed to decode healthz response: %v", err)
	}
	if healthRes["status"] != "alive" {
		t.Errorf("expected status alive, got %v", healthRes["status"])
	}

	// Test GET /readyz
	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var readyRes map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&readyRes); err != nil {
		t.Fatalf("failed to decode readyz response: %v", err)
	}
	if readyRes["status"] != "ready" {
		t.Errorf("expected status ready, got %v", readyRes["status"])
	}
}

func TestRouter_RootEndpoint(t *testing.T) {
	rc := newTestRouterConfig("test-secret", nil, 100, 100)
	defer rc.RateLimiter.Close()
	router := NewRouter(rc)

	// 1. GET / should return 200 OK with server info
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on GET /, got %d (body: %s)", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("expected application/json, got %s", ct)
	}

	var rootRes map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&rootRes); err != nil {
		t.Fatalf("failed to decode root response: %v", err)
	}
	if rootRes["service"] != "omm-bench" {
		t.Errorf("expected service 'omm-bench', got %v", rootRes["service"])
	}
	if rootRes["status"] != "running" {
		t.Errorf("expected status 'running', got %v", rootRes["status"])
	}
	endpoints, ok := rootRes["endpoints"].(map[string]any)
	if !ok {
		t.Fatalf("expected endpoints map in response, got %T", rootRes["endpoints"])
	}
	if endpoints["bench"] != "POST /api/bench" {
		t.Errorf("expected endpoints.bench = 'POST /api/bench', got %v", endpoints["bench"])
	}

	// 2. POST / should return 405 Method Not Allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	reqPost.Header.Set("Content-Type", "application/json")
	recPost := httptest.NewRecorder()
	router.ServeHTTP(recPost, reqPost)

	if recPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 on POST /, got %d", recPost.Code)
	}
}

func TestRouter_Custom404JSON(t *testing.T) {
	rc := newTestRouterConfig("test-secret", nil, 100, 100)
	defer rc.RateLimiter.Close()
	router := NewRouter(rc)

	req := httptest.NewRequest(http.MethodGet, "/non-existent-endpoint", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("expected application/json, got %s", ct)
	}

	var res map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode 404 response: %v", err)
	}
	if res["error"] != "not found" {
		t.Errorf("expected error 'not found', got %s", res["error"])
	}
}

func TestRouter_Custom405JSON(t *testing.T) {
	rc := newTestRouterConfig("test-secret", nil, 100, 100)
	defer rc.RateLimiter.Close()
	router := NewRouter(rc)

	// /healthz only allows GET
	req := httptest.NewRequest(http.MethodPost, "/healthz", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("expected application/json, got %s", ct)
	}

	var res map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode 405 response: %v", err)
	}
	if res["error"] != "method not allowed" {
		t.Errorf("expected error 'method not allowed', got %s", res["error"])
	}
}

func TestAuthMiddleware(t *testing.T) {
	secret := "super-secret-hmac-token-12345678"
	rc := newTestRouterConfig(secret, nil, 100, 100)
	defer rc.RateLimiter.Close()
	// Set dummy bench handler to isolate auth middleware test
	rc.BenchHandler = func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
	}
	router := NewRouter(rc)

	// 1. Missing secret
	req := httptest.NewRequest(http.MethodPost, "/api/bench", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("expected 403 on missing secret, got %d", rec.Code)
	}

	// 2. Incorrect secret
	req = httptest.NewRequest(http.MethodPost, "/api/bench", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bench-Secret", "wrong-secret")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("expected 403 on invalid secret, got %d", rec.Code)
	}

	// 3. Correct secret
	req = httptest.NewRequest(http.MethodPost, "/api/bench", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bench-Secret", secret)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("expected 202 on valid secret, got %d (body: %s)", rec.Code, rec.Body.String())
	}
}

func TestRateLimiter(t *testing.T) {
	// 2 requests allowed per minute
	rc := newTestRouterConfig("test-secret", nil, 2, 2)
	defer rc.RateLimiter.Close()
	router := NewRouter(rc)

	for i := 1; i <= 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("request %d should succeed, got %d", i, rec.Code)
		}
	}

	// 3rd request exceeds limit
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", rec.Code)
	}
	if retryAfter := rec.Header().Get("Retry-After"); retryAfter == "" {
		t.Errorf("expected Retry-After header to be set")
	}
}

func TestCORSMiddleware(t *testing.T) {
	allowed := []string{"https://omm-web.com", "https://dashboard.example.com"}
	rc := newTestRouterConfig("test-secret", allowed, 100, 100)
	defer rc.RateLimiter.Close()
	router := NewRouter(rc)

	// 1. Whitelisted Origin
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "https://omm-web.com")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if acao := rec.Header().Get("Access-Control-Allow-Origin"); acao != "https://omm-web.com" {
		t.Errorf("expected Access-Control-Allow-Origin 'https://omm-web.com', got '%s'", acao)
	}

	// 2. Disallowed Origin
	req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "https://malicious.com")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if acao := rec.Header().Get("Access-Control-Allow-Origin"); acao != "" {
		t.Errorf("disallowed origin should not have Access-Control-Allow-Origin header, got '%s'", acao)
	}

	// 3. Preflight OPTIONS request
	req = httptest.NewRequest(http.MethodOptions, "/api/bench", nil)
	req.Header.Set("Origin", "https://omm-web.com")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected 204 No Content on OPTIONS, got %d", rec.Code)
	}
	if allowMethods := rec.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(allowMethods, "POST") {
		t.Errorf("expected Access-Control-Allow-Methods with POST, got %s", allowMethods)
	}

	// 4. Localhost Origins (automatically allowed over http:// without explicit configuration)
	localhostOrigins := []string{
		"http://localhost:3000",
		"http://127.0.0.1:5173",
		"http://[::1]:8080",
	}
	for _, origin := range localhostOrigins {
		req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
		req.Header.Set("Origin", origin)
		rec = httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if acao := rec.Header().Get("Access-Control-Allow-Origin"); acao != origin {
			t.Errorf("expected Access-Control-Allow-Origin '%s' for localhost, got '%s'", origin, acao)
		}
	}
}

func TestRequestValidationMiddleware(t *testing.T) {
	rc := newTestRouterConfig("test-secret", nil, 100, 100)
	defer rc.RateLimiter.Close()
	router := NewRouter(rc)

	// 1. Missing or non-json Content-Type on POST
	req := httptest.NewRequest(http.MethodPost, "/api/bench", strings.NewReader("hello"))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnsupportedMediaType {
		t.Errorf("expected 415 Unsupported Media Type, got %d", rec.Code)
	}

	// 2. Request ID generated
	req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	reqID := rec.Header().Get("X-Request-ID")
	if reqID == "" || !strings.HasPrefix(reqID, "req-") {
		t.Errorf("expected valid X-Request-ID, got '%s'", reqID)
	}
}

func TestSecurityHeaders(t *testing.T) {
	rc := newTestRouterConfig("test-secret", nil, 100, 100)
	defer rc.RateLimiter.Close()
	router := NewRouter(rc)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	headers := rec.Header()

	expectedHeaders := map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"X-XSS-Protection":       "0",
		"Cache-Control":          "no-store, no-cache, must-revalidate",
		"Referrer-Policy":        "no-referrer",
	}

	for k, v := range expectedHeaders {
		if val := headers.Get(k); val != v {
			t.Errorf("header %s: expected '%s', got '%s'", k, v, val)
		}
	}
}

func TestSSRFValidator(t *testing.T) {
	v := NewSSRFValidator(false)

	tests := []struct {
		url     string
		wantErr bool
	}{
		{"https://openrouter.ai/api/v1", false},
		{"https://api.openai.com/v1", false},
		{"http://127.0.0.1:8080/callback", true},       // Loopback IPv4
		{"http://localhost:3000/callback", true},        // Localhost
		{"https://10.0.0.1/callback", true},             // Private RFC 1918
		{"https://192.168.1.1/callback", true},          // Private RFC 1918
		{"https://172.16.0.1/callback", true},           // Private RFC 1918
		{"https://169.254.169.254/latest/meta-data", true}, // Link-local / AWS metadata
		{"https://user:pass@example.com/callback", true}, // User credentials
		{"ftp://example.com/callback", true},            // Invalid scheme
		{"", true},                                      // Empty URL
	}

	for _, tc := range tests {
		t.Run(tc.url, func(t *testing.T) {
			err := v.ValidateURL(tc.url)
			if (err != nil) != tc.wantErr {
				t.Errorf("ValidateURL(%q) err = %v, wantErr %v", tc.url, err, tc.wantErr)
			}
		})
	}
}

func TestMaxBodyLimit(t *testing.T) {
	rc := newTestRouterConfig("test-secret", nil, 100, 100)
	defer rc.RateLimiter.Close()

	// Handler that reads the full body
	rc.BenchHandler = func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		if err != nil {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "payload too large"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}

	router := NewRouter(rc)

	// Oversized body (> 1MB)
	oversizedData := bytes.Repeat([]byte("a"), int(MaxBodyBytes)+1024)
	req := httptest.NewRequest(http.MethodPost, "/api/bench", bytes.NewReader(oversizedData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bench-Secret", "test-secret")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("expected 413 Payload Too Large, got %d", rec.Code)
	}
}

type mockSubmitter struct {
	stats     PoolStats
	submitErr error
}

func (m *mockSubmitter) Submit(ctx context.Context, job *BenchJob) error {
	if m.submitErr != nil {
		return m.submitErr
	}
	return nil
}

func (m *mockSubmitter) Stats() PoolStats {
	return m.stats
}

func validBenchPayload() map[string]string {
	return map[string]string{
		"jobId":          "a0000000-0000-4000-8000-000000000001",
		"callbackUrl":    "https://8.8.8.8/callback",
		"callbackSecret": "very-secret-token-min-16-bytes",
		"baseUrl":        "https://8.8.8.8/v1",
		"apiKey":         "sk-test-mock-api-key",
		"model":          "openai/gpt-4o",
	}
}

func TestBenchHandler_Success(t *testing.T) {
	secret := "shared-secret-123456"
	rc := newTestRouterConfig(secret, nil, 100, 100)
	defer rc.RateLimiter.Close()
	router := NewRouter(rc)

	payload := validBenchPayload()
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/bench", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bench-Secret", secret)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}

	var res map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if res["jobId"] != payload["jobId"] {
		t.Errorf("expected jobId %s, got %s", payload["jobId"], res["jobId"])
	}
	if res["status"] != "ACCEPTED" {
		t.Errorf("expected status ACCEPTED, got %s", res["status"])
	}
	if res["message"] == "" {
		t.Errorf("expected non-empty message in response")
	}
}

func TestBenchHandler_ValidationErrors(t *testing.T) {
	secret := "shared-secret-123456"
	rc := newTestRouterConfig(secret, nil, 100, 100)
	defer rc.RateLimiter.Close()
	router := NewRouter(rc)

	tests := []struct {
		name    string
		modify  func(p map[string]string)
		rawBody string
	}{
		{
			name:    "Malformed JSON",
			rawBody: "{invalid-json",
		},
		{
			name: "Missing JobID",
			modify: func(p map[string]string) {
				delete(p, "jobId")
			},
		},
		{
			name: "Invalid JobID (non-UUID)",
			modify: func(p map[string]string) {
				p["jobId"] = "not-a-valid-uuid"
			},
		},
		{
			name: "Invalid JobID (UUID v1, not v4)",
			modify: func(p map[string]string) {
				p["jobId"] = "a0000000-0000-1000-8000-000000000001"
			},
		},
		{
			name: "Short CallbackSecret (<16 chars)",
			modify: func(p map[string]string) {
				p["callbackSecret"] = "too-short"
			},
		},
		{
			name: "Empty APIKey",
			modify: func(p map[string]string) {
				p["apiKey"] = ""
			},
		},
		{
			name: "APIKey too long (>512 chars)",
			modify: func(p map[string]string) {
				p["apiKey"] = strings.Repeat("k", 513)
			},
		},
		{
			name: "Empty Model",
			modify: func(p map[string]string) {
				p["model"] = ""
			},
		},
		{
			name: "Model too long (>128 chars)",
			modify: func(p map[string]string) {
				p["model"] = strings.Repeat("m", 129)
			},
		},
		{
			name: "Model with illegal characters (command injection attempt)",
			modify: func(p map[string]string) {
				p["model"] = "gpt-4; rm -rf /"
			},
		},
		{
			name: "Model with spaces",
			modify: func(p map[string]string) {
				p["model"] = "gpt 4 mini"
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var bodyReader io.Reader
			if tc.rawBody != "" {
				bodyReader = strings.NewReader(tc.rawBody)
			} else {
				payload := validBenchPayload()
				tc.modify(payload)
				b, _ := json.Marshal(payload)
				bodyReader = bytes.NewReader(b)
			}

			req := httptest.NewRequest(http.MethodPost, "/api/bench", bodyReader)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Bench-Secret", secret)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("expected 400 Bad Request, got %d (body: %s)", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestBenchHandler_SSRF(t *testing.T) {
	secret := "shared-secret-123456"
	rc := newTestRouterConfig(secret, nil, 100, 100)
	defer rc.RateLimiter.Close()
	router := NewRouter(rc)

	tests := []struct {
		name          string
		modify        func(p map[string]string)
		expectedField string
	}{
		{
			name: "CallbackURL non-HTTPS",
			modify: func(p map[string]string) {
				p["callbackUrl"] = "http://8.8.8.8/callback"
			},
			expectedField: "callbackUrl",
		},
		{
			name: "CallbackURL Loopback",
			modify: func(p map[string]string) {
				p["callbackUrl"] = "https://127.0.0.1/callback"
			},
			expectedField: "callbackUrl",
		},
		{
			name: "CallbackURL RFC1918 10.x",
			modify: func(p map[string]string) {
				p["callbackUrl"] = "https://10.0.0.1/callback"
			},
			expectedField: "callbackUrl",
		},
		{
			name: "CallbackURL RFC1918 192.168.x",
			modify: func(p map[string]string) {
				p["callbackUrl"] = "https://192.168.1.1/callback"
			},
			expectedField: "callbackUrl",
		},
		{
			name: "BaseURL Loopback",
			modify: func(p map[string]string) {
				p["baseUrl"] = "https://127.0.0.1/v1"
			},
			expectedField: "baseUrl",
		},
		{
			name: "BaseURL RFC1918 172.16.x",
			modify: func(p map[string]string) {
				p["baseUrl"] = "https://172.16.0.1/v1"
			},
			expectedField: "baseUrl",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := validBenchPayload()
			tc.modify(payload)
			b, _ := json.Marshal(payload)

			req := httptest.NewRequest(http.MethodPost, "/api/bench", bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Bench-Secret", secret)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("expected 422 Unprocessable Entity, got %d (body: %s)", rec.Code, rec.Body.String())
			}

			var res map[string]string
			if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}
			if res["field"] != tc.expectedField {
				t.Errorf("expected field '%s', got '%s'", tc.expectedField, res["field"])
			}
		})
	}
}

func TestBenchHandler_AllowedOrigins(t *testing.T) {
	secret := "shared-secret-123456"
	allowed := []string{"http://127.0.0.1:3000"}
	rc := newTestRouterConfig(secret, allowed, 100, 100)
	defer rc.RateLimiter.Close()
	rc.SSRF = NewSSRFValidator(true) // allow localhost for dev origin testing
	router := NewRouter(rc)

	// 1. Allowed callback origin
	payload := validBenchPayload()
	payload["callbackUrl"] = "http://127.0.0.1:3000/callback"
	b, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/bench", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bench-Secret", secret)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("expected 202 for allowed origin, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	// 2. Disallowed callback origin
	payload["callbackUrl"] = "http://127.0.0.1:4000/callback"
	b, _ = json.Marshal(payload)

	req = httptest.NewRequest(http.MethodPost, "/api/bench", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bench-Secret", secret)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected 422 for disallowed origin, got %d (body: %s)", rec.Code, rec.Body.String())
	}
}

func TestBenchHandler_AllowLocalhostOrigins(t *testing.T) {
	secret := "shared-secret-123456"
	allowed := []string{"https://1.1.1.1"}
	rc := newTestRouterConfig(secret, allowed, 100, 100)
	rc.Config.AllowLocalhost = true
	defer rc.RateLimiter.Close()
	router := NewRouter(rc)

	testCases := []struct {
		name        string
		callbackURL string
		expectCode  int
	}{
		{
			name:        "localhost port 3000 allowed",
			callbackURL: "http://localhost:3000/api/bench/callback/test",
			expectCode:  http.StatusAccepted,
		},
		{
			name:        "127.0.0.1 port 3000 allowed",
			callbackURL: "http://127.0.0.1:3000/api/bench/callback/test",
			expectCode:  http.StatusAccepted,
		},
		{
			name:        "whitelisted external domain allowed",
			callbackURL: "https://1.1.1.1/api/bench/callback/test",
			expectCode:  http.StatusAccepted,
		},
		{
			name:        "unwhitelisted external domain rejected",
			callbackURL: "https://8.8.8.8/api/bench/callback/test",
			expectCode:  http.StatusUnprocessableEntity,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			payload := validBenchPayload()
			payload["callbackUrl"] = tc.callbackURL
			b, _ := json.Marshal(payload)

			req := httptest.NewRequest(http.MethodPost, "/api/bench", bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Bench-Secret", secret)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tc.expectCode {
				t.Errorf("expected %d for %s, got %d (body: %s)", tc.expectCode, tc.callbackURL, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestBenchHandler_PoolFull(t *testing.T) {
	secret := "shared-secret-123456"
	rc := newTestRouterConfig(secret, nil, 100, 100)
	defer rc.RateLimiter.Close()

	// Mock submitter with 0 available slots
	rc.Pool = &mockSubmitter{
		stats: PoolStats{
			ActiveJobs:     3,
			MaxConcurrent:  3,
			AvailableSlots: 0,
		},
		submitErr: ErrPoolFull,
	}

	router := NewRouter(rc)

	payload := validBenchPayload()
	b, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/bench", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bench-Secret", secret)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d: %s", rec.Code, rec.Body.String())
	}

	var res map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if res["error"] != "server busy" {
		t.Errorf("expected error 'server busy', got %v", res["error"])
	}
	if qCap, ok := res["queueCapacity"].(float64); !ok || int(qCap) != 0 {
		t.Errorf("expected queueCapacity 0, got %v", res["queueCapacity"])
	}
}

func TestReadyHandler_CapacityStates(t *testing.T) {
	rc := newTestRouterConfig("test-secret", nil, 100, 100)
	defer rc.RateLimiter.Close()

	// 1. Ready state (slots available)
	rc.Pool = &mockSubmitter{
		stats: PoolStats{
			ActiveJobs:     1,
			MaxConcurrent:  3,
			AvailableSlots: 2,
		},
	}
	router := NewRouter(rc)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var res map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode readyz response: %v", err)
	}
	if res["status"] != "ready" {
		t.Errorf("expected status 'ready', got %v", res["status"])
	}

	// 2. Busy state (pool full)
	rc.Pool = &mockSubmitter{
		stats: PoolStats{
			ActiveJobs:     3,
			MaxConcurrent:  3,
			AvailableSlots: 0,
		},
	}
	router = NewRouter(rc)

	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d", rec.Code)
	}

	res = nil
	if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode readyz response: %v", err)
	}
	if res["status"] != "busy" {
		t.Errorf("expected status 'busy', got %v", res["status"])
	}
}

func TestStatusHandler(t *testing.T) {
	secret := "secret-status-token-1234"
	rc := newTestRouterConfig(secret, nil, 100, 100)
	defer rc.RateLimiter.Close()

	rc.Pool = &mockSubmitter{
		stats: PoolStats{
			ActiveJobs:     2,
			MaxConcurrent:  5,
			AvailableSlots: 3,
			TotalProcessed: 42,
			TotalFailed:    2,
		},
	}
	router := NewRouter(rc)

	// 1. Unauthenticated -> 403
	req := httptest.NewRequest(http.MethodGet, "/api/bench/status", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden without secret, got %d", rec.Code)
	}

	// 2. Authenticated -> 200 with metrics
	req = httptest.NewRequest(http.MethodGet, "/api/bench/status", nil)
	req.Header.Set("X-Bench-Secret", secret)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK with secret, got %d", rec.Code)
	}

	var stats map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil {
		t.Fatalf("failed to decode status response: %v", err)
	}

	if int(stats["activeJobs"].(float64)) != 2 {
		t.Errorf("expected activeJobs 2, got %v", stats["activeJobs"])
	}
	if int(stats["maxConcurrent"].(float64)) != 5 {
		t.Errorf("expected maxConcurrent 5, got %v", stats["maxConcurrent"])
	}
	if int(stats["availableSlots"].(float64)) != 3 {
		t.Errorf("expected availableSlots 3, got %v", stats["availableSlots"])
	}
	if int64(stats["totalProcessed"].(float64)) != 42 {
		t.Errorf("expected totalProcessed 42, got %v", stats["totalProcessed"])
	}
	if int64(stats["totalFailed"].(float64)) != 2 {
		t.Errorf("expected totalFailed 2, got %v", stats["totalFailed"])
	}
	if stats["uptime"] == "" {
		t.Errorf("expected non-empty uptime")
	}
}

func TestJob_ZeroAPIKey(t *testing.T) {
	job := &BenchJob{
		JobID:  "test-job-id",
		APIKey: "sk-super-secret-key-12345",
	}
	job.ZeroAPIKey()
	if job.APIKey != "" {
		t.Errorf("expected empty APIKey after ZeroAPIKey(), got %q", job.APIKey)
	}

	// Nil safety
	var nilJob *BenchJob
	nilJob.ZeroAPIKey() // Should not panic
}

func TestSSRFValidator_IPv6AndDevMode(t *testing.T) {
	strict := NewSSRFValidator(false)
	if err := strict.ValidateURL("https://[::1]/callback"); err == nil {
		t.Errorf("expected error for IPv6 loopback on strict validator")
	}

	dev := NewSSRFValidator(true)
	if err := dev.ValidateURL("http://127.0.0.1:3000/callback"); err != nil {
		t.Errorf("expected safe for 127.0.0.1 in dev mode, got err: %v", err)
	}
	if err := dev.ValidateURL("http://localhost:3000/callback"); err != nil {
		t.Errorf("expected safe for localhost in dev mode, got err: %v", err)
	}
	if err := dev.ValidateURL("http://[::1]:3000/callback"); err != nil {
		t.Errorf("expected safe for [::1] in dev mode, got err: %v", err)
	}
	if err := dev.ValidateURL("https://192.168.1.1/callback"); err == nil {
		t.Errorf("expected unsafe for private RFC1918 even in dev mode")
	}

	// dev validator allows localhost http:// endpoints
	if err := dev.ValidateURL("http://localhost:11434/v1"); err != nil {
		t.Errorf("expected dev validator to allow local Ollama http endpoint, got err: %v", err)
	}
	if err := dev.ValidateURL("http://localhost:3000/callback"); err != nil {
		t.Errorf("expected dev validator to allow local callback http endpoint, got err: %v", err)
	}
	if err := dev.ValidateURL("http://127.0.0.1:11434/v1"); err != nil {
		t.Errorf("expected dev validator to allow 127.0.0.1 http endpoint, got err: %v", err)
	}
}

func TestBenchHandler_AllowLocalhostConfig(t *testing.T) {
	secret := "shared-secret-123456"
	rc := newTestRouterConfig(secret, nil, 100, 100)
	rc.Config.AllowLocalhost = true
	defer rc.RateLimiter.Close()
	router := NewRouter(rc)

	payload := validBenchPayload()
	payload["baseUrl"] = "http://localhost:11434/v1"
	payload["callbackUrl"] = "http://localhost:3000/api/bench/callback"
	b, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/bench", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bench-Secret", secret)
	req.Header.Set("Origin", "http://localhost:3000")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted when AllowLocalhost is true, got %d (body: %s)", rec.Code, rec.Body.String())
	}
	if acao := rec.Header().Get("Access-Control-Allow-Origin"); acao != "http://localhost:3000" {
		t.Errorf("expected CORS header for http://localhost:3000, got '%s'", acao)
	}
}

func TestBenchHandler_CallbackDomainRateLimit(t *testing.T) {
	secret := "shared-secret-123456"
	// perSourceLimit = 2
	rc := newTestRouterConfig(secret, nil, 100, 2)
	defer rc.RateLimiter.Close()
	router := NewRouter(rc)

	// Send requests with different remote IPs (so IP rate limiting doesn't trigger)
	// but the SAME callback domain
	for i := 1; i <= 2; i++ {
		payload := validBenchPayload()
		payload["jobId"] = fmt.Sprintf("a0000000-0000-4000-8000-00000000000%d", i)
		b, _ := json.Marshal(payload)

		req := httptest.NewRequest(http.MethodPost, "/api/bench", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Bench-Secret", secret)
		req.Header.Set("X-Forwarded-For", fmt.Sprintf("203.0.113.%d", i))
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("request %d: expected 202 Accepted, got %d (body: %s)", i, rec.Code, rec.Body.String())
		}
	}

	// 3rd request to the same callback domain should trigger 429 Too Many Requests
	payload := validBenchPayload()
	payload["jobId"] = "a0000000-0000-4000-8000-000000000003"
	b, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/bench", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bench-Secret", secret)
	req.Header.Set("X-Forwarded-For", "203.0.113.99")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests on callback domain rate limit exceeded, got %d (body: %s)", rec.Code, rec.Body.String())
	}
}

func TestStatusHandler_MetricsCounters(t *testing.T) {
	secret := "secret-status-metrics-token-1234"
	met := metrics.New()
	rc := newTestRouterConfig(secret, nil, 100, 100)
	rc.Metrics = met
	defer rc.RateLimiter.Close()

	rc.Pool = &mockSubmitter{
		stats: PoolStats{
			ActiveJobs:     1,
			MaxConcurrent:  3,
			AvailableSlots: 2,
			TotalProcessed: 10,
			TotalFailed:    1,
		},
	}
	router := NewRouter(rc)

	// 1. Send public health request (200 OK)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// 2. Send bad bench request (400 Bad Request)
	req = httptest.NewRequest(http.MethodPost, "/api/bench", strings.NewReader("bad json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bench-Secret", secret)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	// 3. Send valid bench request (202 Accepted)
	payload := validBenchPayload()
	b, _ := json.Marshal(payload)
	req = httptest.NewRequest(http.MethodPost, "/api/bench", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bench-Secret", secret)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}

	// 4. Check status handler exposes all metrics
	req = httptest.NewRequest(http.MethodGet, "/api/bench/status", nil)
	req.Header.Set("X-Bench-Secret", secret)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var statusRes map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&statusRes); err != nil {
		t.Fatalf("failed to decode status response: %v", err)
	}

	// Verify metrics in status response
	// Total requests so far: healthz (1) + bad bench (2) + valid bench (3) + status (4) = 4
	reqTotal := int64(statusRes["requestsTotal"].(float64))
	if reqTotal != 4 {
		t.Errorf("expected requestsTotal 4, got %d", reqTotal)
	}
	reqAccepted := int64(statusRes["requestsAccepted"].(float64))
	if reqAccepted != 1 {
		t.Errorf("expected requestsAccepted 1, got %d", reqAccepted)
	}
	reqRejected := int64(statusRes["requestsRejected"].(float64))
	if reqRejected != 1 {
		t.Errorf("expected requestsRejected 1, got %d", reqRejected)
	}

	// Also verify nested metrics object
	metricsObj, ok := statusRes["metrics"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested 'metrics' object in status response")
	}
	if int64(metricsObj["requestsTotal"].(float64)) != 4 {
		t.Errorf("expected nested requestsTotal 4, got %v", metricsObj["requestsTotal"])
	}
}

type dynamicMockSubmitter struct {
	availableFunc func() int
}

func (d *dynamicMockSubmitter) Submit(ctx context.Context, job *BenchJob) error {
	if d.availableFunc != nil && d.availableFunc() <= 0 {
		return ErrPoolFull
	}
	return nil
}

func (d *dynamicMockSubmitter) Stats() PoolStats {
	avail := 3
	if d.availableFunc != nil {
		avail = d.availableFunc()
	}
	return PoolStats{
		ActiveJobs:     3 - avail,
		MaxConcurrent:  3,
		AvailableSlots: avail,
	}
}

func TestServer_EndToEndIntegration(t *testing.T) {
	secret := "shared-bench-integration-secret-1234"
	allowedOrigins := []string{"https://8.8.8.8"}
	globalLimit := 100
	perSourceLimit := 100

	cfg := &config.Config{
		ListenAddr:         ":0",
		BenchSecret:        secret,
		AllowedOrigins:     allowedOrigins,
		MaxConcurrentJobs:  3,
		JobTimeoutSec:      300,
		GlobalRateLimit:    globalLimit,
		PerSourceRateLimit: perSourceLimit,
		LogLevel:           "info",
	}

	rl := NewRateLimiter(globalLimit, perSourceLimit)
	defer rl.Close()

	var poolCapacity atomic.Int32
	poolCapacity.Store(3)

	mockPool := &dynamicMockSubmitter{
		availableFunc: func() int {
			return int(poolCapacity.Load())
		},
	}

	router := NewRouter(RouterConfig{
		Config:      cfg,
		RateLimiter: rl,
		Pool:        mockPool,
	})

	ts := httptest.NewServer(router)
	defer ts.Close()

	client := ts.Client()

	// 1. POST /api/bench tanpa auth → 403
	t.Run("1_Bench_NoAuth_403", func(t *testing.T) {
		resp, err := client.Post(ts.URL+"/api/bench", "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("expected 403, got %d", resp.StatusCode)
		}
	})

	// 2. POST /api/bench dengan auth valid + body valid → 202
	t.Run("2_Bench_Valid_202", func(t *testing.T) {
		payload := validBenchPayload()
		bodyBytes, _ := json.Marshal(payload)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/bench", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Bench-Secret", secret)

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Errorf("expected 202, got %d", resp.StatusCode)
		}
	})

	// 3. POST /api/bench dengan body tidak valid → 400
	t.Run("3_Bench_InvalidBody_400", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/bench", strings.NewReader(`{"jobId":"not-uuid"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Bench-Secret", secret)

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", resp.StatusCode)
		}
	})

	// 4. POST /api/bench dengan callbackUrl SSRF → 422
	t.Run("4_Bench_SSRF_422", func(t *testing.T) {
		payload := validBenchPayload()
		payload["callbackUrl"] = "http://127.0.0.1:8080/callback" // loopback
		bodyBytes, _ := json.Marshal(payload)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/bench", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Bench-Secret", secret)

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnprocessableEntity {
			t.Errorf("expected 422, got %d", resp.StatusCode)
		}
	})

	// 5. GET /healthz → 200
	t.Run("5_Healthz_200", func(t *testing.T) {
		resp, err := client.Get(ts.URL + "/healthz")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
	})

	// 6. GET /readyz saat ada kapasitas → 200
	t.Run("6_Readyz_Capacity_200", func(t *testing.T) {
		poolCapacity.Store(2)
		resp, err := client.Get(ts.URL + "/readyz")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
	})

	// 7. GET /readyz saat pool penuh → 503
	t.Run("7_Readyz_PoolFull_503", func(t *testing.T) {
		poolCapacity.Store(0)
		resp, err := client.Get(ts.URL + "/readyz")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("expected 503, got %d", resp.StatusCode)
		}
	})

	// 8. Rate limit enforcement → 429 setelah threshold
	t.Run("8_RateLimit_429", func(t *testing.T) {
		tightRL := NewRateLimiter(2, 2)
		defer tightRL.Close()
		tightRouter := NewRouter(RouterConfig{
			Config:      cfg,
			RateLimiter: tightRL,
			Pool:        mockPool,
		})
		tightTS := httptest.NewServer(tightRouter)
		defer tightTS.Close()

		tightClient := tightTS.Client()
		for i := 0; i < 2; i++ {
			r, err := tightClient.Get(tightTS.URL + "/healthz")
			if err != nil {
				t.Fatalf("request %d failed: %v", i, err)
			}
			r.Body.Close()
			if r.StatusCode != http.StatusOK {
				t.Fatalf("request %d: expected 200, got %d", i, r.StatusCode)
			}
		}

		r, err := tightClient.Get(tightTS.URL + "/healthz")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer r.Body.Close()
		if r.StatusCode != http.StatusTooManyRequests {
			t.Errorf("expected 429 Too Many Requests, got %d", r.StatusCode)
		}
	})

	// 9. CORS preflight → 204 dengan header benar
	t.Run("9_CORS_Preflight_204", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodOptions, ts.URL+"/api/bench", nil)
		req.Header.Set("Origin", "https://8.8.8.8")
		req.Header.Set("Access-Control-Request-Method", "POST")

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("expected 204 No Content, got %d", resp.StatusCode)
		}
		if acao := resp.Header.Get("Access-Control-Allow-Origin"); acao != "https://8.8.8.8" {
			t.Errorf("expected Access-Control-Allow-Origin 'https://8.8.8.8', got '%s'", acao)
		}
		if acam := resp.Header.Get("Access-Control-Allow-Methods"); !strings.Contains(acam, "POST") {
			t.Errorf("expected Access-Control-Allow-Methods containing POST, got '%s'", acam)
		}
	})
}


