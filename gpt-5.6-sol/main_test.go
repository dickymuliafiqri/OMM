package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestVisitCountsConcurrentRequests(t *testing.T) {
	h := newApplication()
	const requests = 100

	var wg sync.WaitGroup
	wg.Add(requests)
	for range requests {
		go func() {
			defer wg.Done()
			r := httptest.NewRequest(http.MethodGet, "/visit", nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Errorf("status = %d, want 200", w.Code)
			}
		}()
	}
	wg.Wait()

	r := httptest.NewRequest(http.MethodGet, "/visit", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if got := strings.TrimSpace(w.Body.String()); got != `{"total":101}` {
		t.Fatalf("body = %q, want total 101", got)
	}
}

func TestEchoIsPlainTextAndValidatesInput(t *testing.T) {
	h := newApplication()

	tests := []struct {
		name, target string
		wantStatus   int
		wantBody     string
	}{
		{"normal", "/echo?msg=halo%20dunia", 200, "halo dunia"},
		{"html remains inert plain text", "/echo?msg=%3Cscript%3Ealert(1)%3C%2Fscript%3E", 200, "<script>alert(1)</script>"},
		{"missing", "/echo", 400, ""},
		{"duplicate", "/echo?msg=a&msg=b", 400, ""},
		{"too long", "/echo?msg=" + strings.Repeat("a", maxMessageBytes+1), 400, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, tt.target, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", w.Code, tt.wantStatus)
			}
			if tt.wantBody != "" && w.Body.String() != tt.wantBody {
				t.Fatalf("body = %q, want %q", w.Body.String(), tt.wantBody)
			}
			if got := w.Header().Get("X-Content-Type-Options"); got != "nosniff" {
				t.Fatalf("X-Content-Type-Options = %q", got)
			}
		})
	}
}

func TestRejectsUnsupportedMethodAndBody(t *testing.T) {
	h := newApplication()

	for _, tc := range []struct {
		method, target, body string
		want                 int
	}{
		{http.MethodPost, "/visit", "", http.StatusMethodNotAllowed},
		{http.MethodHead, "/visit", "", http.StatusMethodNotAllowed},
		{http.MethodGet, "/echo?msg=x", "unexpected", http.StatusBadRequest},
	} {
		r := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Errorf("%s %s: status = %d, want %d", tc.method, tc.target, w.Code, tc.want)
		}
	}
}
