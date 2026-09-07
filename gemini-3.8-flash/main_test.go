package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestVisitConcurrent(t *testing.T) {
	atomic.StoreUint64(&totalVisitors, 0)
	handler := setupRoutes()

	const requests = 100
	var wg sync.WaitGroup
	wg.Add(requests)

	for i := 0; i < requests; i++ {
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/visit", nil)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
			}
		}()
	}
	wg.Wait()

	if totalVisitors != requests {
		t.Fatalf("totalVisitors = %d, want %d", totalVisitors, requests)
	}
}

func TestEcho(t *testing.T) {
	handler := setupRoutes()

	t.Run("safe text", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/echo?msg=halo", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w.Code)
		}
		if body := w.Body.String(); body != "halo" {
			t.Fatalf("body = %q, want 'halo'", body)
		}
		if ct := w.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
			t.Fatalf("content-type = %q", ct)
		}
		if nosniff := w.Header().Get("X-Content-Type-Options"); nosniff != "nosniff" {
			t.Fatalf("X-Content-Type-Options = %q", nosniff)
		}
	})

	t.Run("xss payload as plain text", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/echo?msg=%3Cscript%3Ealert(1)%3C/script%3E", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if ct := w.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
			t.Fatalf("content-type = %q, want text/plain", ct)
		}
		if body := w.Body.String(); body != "<script>alert(1)</script>" {
			t.Fatalf("body = %q", body)
		}
	})

	t.Run("msg too large", func(t *testing.T) {
		bigMsg := strings.Repeat("a", maxMsgLen+1)
		req := httptest.NewRequest(http.MethodGet, "/echo?msg="+bigMsg, nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", w.Code)
		}
	})
}

func TestMethodAndPathRestrictions(t *testing.T) {
	handler := setupRoutes()

	req := httptest.NewRequest(http.MethodPost, "/visit", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", w.Code)
	}

	req404 := httptest.NewRequest(http.MethodGet, "/unknown", nil)
	w404 := httptest.NewRecorder()
	handler.ServeHTTP(w404, req404)
	if w404.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w404.Code)
	}
}
