package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func doGet(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func body(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("body not JSON: %v (%q)", err, rec.Body.String())
	}
	return m
}

func TestVisitCountsUp(t *testing.T) {
	h := http.HandlerFunc(handleVisit)
	first := doGet(t, h, "/visit")
	second := doGet(t, h, "/visit")
	if first.Code != 200 || second.Code != 200 {
		t.Fatalf("status: %d %d", first.Code, second.Code)
	}
	if body(t, second)["visits"].(float64) != body(t, first)["visits"].(float64)+1 {
		t.Fatal("counter did not increment by 1")
	}
}

func TestVisitConcurrent(t *testing.T) { // run with -race
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); doGet(t, http.HandlerFunc(handleVisit), "/visit") }()
	}
	wg.Wait()
}

func TestEcho(t *testing.T) {
	h := http.HandlerFunc(handleEcho)
	rec := doGet(t, h, `/echo?msg=%3Cscript%3Ealert(1)%3C%2Fscript%3E`)
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type %q", ct)
	}
	// Escaped output can never terminate the JSON string or form tags.
	if strings.ContainsAny(rec.Body.String(), "<>") {
		t.Fatalf("unescaped HTML in output: %q", rec.Body.String())
	}
	if got := body(t, rec)["msg"]; got != "<script>alert(1)</script>" {
		t.Fatalf("msg not echoed verbatim: %q", got)
	}
}

func TestEchoErrors(t *testing.T) {
	h := http.HandlerFunc(handleEcho)
	if rec := doGet(t, h, "/echo"); rec.Code != 400 {
		t.Fatalf("missing msg: got %d", rec.Code)
	}
	if rec := doGet(t, h, "/echo?msg="+strings.Repeat("x", maxMsgLen+1)); rec.Code != 413 {
		t.Fatalf("oversize msg: got %d", rec.Code)
	}
}

func TestLimiterBurstAndRefill(t *testing.T) {
	l := newLimiter()
	nOK := 0
	for i := 0; i < rateBurst+5; i++ {
		if l.allow("1.2.3.0/24") {
			nOK++
		}
	}
	if nOK != int(rateBurst) {
		t.Fatalf("burst: allowed %d, want %d", nOK, int(rateBurst))
	}
	if l.allow("1.2.3.0/24") {
		t.Fatal("bucket should be empty")
	}
	if !l.allow("4.5.6.0/24") {
		t.Fatal("independent bucket should be allowed")
	}
}

func TestBucketKeyAggregatesSubnets(t *testing.T) {
	if k := bucketKey("203.0.113.99"); k != "203.0.113.0" {
		t.Fatalf("ipv4 key %q", k)
	}
	if k := bucketKey("2001:db8:1234:5678::1"); k != "2001:db8:1234:5678::" {
		t.Fatalf("ipv6 key %q", k)
	}
}

func TestMethodGuard(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /visit", handleVisit)
	req := httptest.NewRequest(http.MethodPost, "/visit", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != 405 {
		t.Fatalf("POST /visit: got %d, want 405", rec.Code)
	}
}
