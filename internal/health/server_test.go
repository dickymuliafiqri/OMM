package health

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"benchmark/internal/queue"
	"benchmark/internal/storage"
)

func TestHealthServer_Endpoints(t *testing.T) {
	db, err := storage.NewTursoDB(":memory:", "")
	if err != nil {
		t.Fatalf("Gagal init test db: %v", err)
	}
	repo := storage.NewRepository(db)
	defer repo.Close()

	if err := repo.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate db gagal: %v", err)
	}

	pool := queue.NewWorkerPool(1, 5, repo)
	defer func() { _ = pool.Stop(0) }()

	// Test default port when port <= 0
	server := NewServer(0, repo, pool)
	if server.httpServer.Addr != ":8080" {
		t.Errorf("Ekspektasi server addr ':8080', dapat: %s", server.httpServer.Addr)
	}

	// 1. Test / (Root overview endpoint)
	reqRoot := httptest.NewRequest(http.MethodGet, "/", nil)
	rrRoot := httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rrRoot, reqRoot)

	if rrRoot.Code != http.StatusOK {
		t.Errorf("Ekspektasi 200 OK untuk /, dapat: %d", rrRoot.Code)
	}

	var rootResp RootResponse
	if err := json.Unmarshal(rrRoot.Body.Bytes(), &rootResp); err != nil {
		t.Fatalf("Gagal unmarshal / response: %v", err)
	}
	if rootResp.Service == "" || rootResp.Status != "running" {
		t.Errorf("Data root response tidak valid: %+v", rootResp)
	}
	if len(rootResp.Endpoints) == 0 {
		t.Errorf("Endpoints overview tidak boleh kosong di /")
	}

	// 2. Test 404 on unhandled path via ServeMux
	req404 := httptest.NewRequest(http.MethodGet, "/random-unknown-route", nil)
	rr404 := httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rr404, req404)
	if rr404.Code != http.StatusNotFound {
		t.Errorf("Ekspektasi 404 NotFound untuk route tidak dikenal, dapat: %d", rr404.Code)
	}

	// 3. Test /healthz
	reqHealth := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rrHealth := httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rrHealth, reqHealth)

	if rrHealth.Code != http.StatusOK {
		t.Errorf("Ekspektasi 200 OK untuk /healthz, dapat: %d", rrHealth.Code)
	}

	var healthResp HealthResponse
	if err := json.Unmarshal(rrHealth.Body.Bytes(), &healthResp); err != nil {
		t.Fatalf("Gagal unmarshal /healthz response: %v", err)
	}
	if healthResp.Status != "healthy" || healthResp.Database != "connected" {
		t.Errorf("Data healthz tidak sesuai: %+v", healthResp)
	}

	// 4. Test /readyz
	reqReady := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rrReady := httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rrReady, reqReady)

	if rrReady.Code != http.StatusOK {
		t.Errorf("Ekspektasi 200 OK untuk /readyz, dapat: %d", rrReady.Code)
	}

	// 5. Test /stats
	reqStats := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rrStats := httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rrStats, reqStats)

	if rrStats.Code != http.StatusOK {
		t.Errorf("Ekspektasi 200 OK untuk /stats, dapat: %d", rrStats.Code)
	}

	var statsResp map[string]interface{}
	if err := json.Unmarshal(rrStats.Body.Bytes(), &statsResp); err != nil {
		t.Fatalf("Gagal unmarshal /stats response: %v", err)
	}
	if _, ok := statsResp["system_stats"]; !ok {
		t.Errorf("Key system_stats harus ada di /stats response")
	}
}
