package pricing

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"benchmark/internal/storage"
)

func TestGetModelPrice_Fallback(t *testing.T) {
	// DeepSeek
	price, ok := GetModelPrice("deepseek-chat")
	if !ok || price.PromptPricePerMillion != 0.14 {
		t.Errorf("Gagal mendapatkan harga deepseek-chat: %+v", price)
	}

	// GPT-4o
	price, ok = GetModelPrice("gpt-4o")
	if !ok || price.PromptPricePerMillion != 2.50 {
		t.Errorf("Gagal mendapatkan harga gpt-4o: %+v", price)
	}

	// Ollama (Local - calculated with market model rate or hardware compute cost, not free)
	price, ok = GetModelPrice("ollama/llama3")
	if !ok || price.PromptPricePerMillion <= 0 {
		t.Errorf("Model Ollama harus memiliki estimasi biaya non-nol: %+v", price)
	}

	// Unknown local models must use hardware compute baseline
	priceLocal, okLocal := GetModelPrice("ollama/unknown-model")
	if !okLocal || priceLocal.PromptPricePerMillion != 0.10 {
		t.Errorf("Model lokal tidak dikenal harus menggunakan baseline hardware: %+v", priceLocal)
	}
}

func TestCalculateCost(t *testing.T) {
	// DeepSeek-chat: 850 in, 1200 out
	// in = 850 * 0.14 / 1M = 0.000119
	// out = 1200 * 0.28 / 1M = 0.000336
	// total = 0.000455 USD -> Sangat Ekonomis
	est := CalculateCost("deepseek-chat", 850, 1200, 95)
	if !est.HasPricing {
		t.Errorf("Harus memiliki pricing data")
	}
	if est.Tier != "Sangat Ekonomis" {
		t.Errorf("Ekspektasi Sangat Ekonomis, dapat: %s", est.Tier)
	}
	if est.EstimatedCostUSD <= 0 {
		t.Errorf("Biaya estimasi harus > 0: %f", est.EstimatedCostUSD)
	}

	// Ollama: calculated based on compute/token load, not free (must be > 0)
	estOllama := CalculateCost("ollama/qwen", 500, 500, 80)
	if !estOllama.HasPricing || estOllama.EstimatedCostUSD <= 0 || estOllama.Tier == "Gratis (Lokal)" {
		t.Errorf("Ollama tidak boleh gratis dan harus memiliki estimasi biaya valid: %+v", estOllama)
	}
	if estOllama.Tier != "Sangat Ekonomis" {
		t.Errorf("Ekspektasi Sangat Ekonomis untuk ollama/qwen, dapat: %s", estOllama.Tier)
	}
}

func TestLoadPricingFromDB(t *testing.T) {
	db, err := storage.NewTursoDB(":memory:", "")
	if err != nil {
		t.Fatalf("Gagal inisialisasi test db: %v", err)
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	ctx := context.Background()
	if err := repo.Migrate(ctx); err != nil {
		t.Fatalf("Migrate gagal: %v", err)
	}

	// Save models to DB
	testModels := []storage.CachedModel{
		{
			ID:                  "provider/custom-test-model",
			Name:                "Custom Test Model",
			PromptPricePerM:     1.75,
			CompletionPricePerM: 3.50,
			ContextLength:       32000,
		},
	}
	if err := repo.SaveCachedModels(ctx, testModels); err != nil {
		t.Fatalf("SaveCachedModels gagal: %v", err)
	}

	// Load from DB into memory
	count, err := LoadPricingFromDB(ctx, repo)
	if err != nil {
		t.Fatalf("LoadPricingFromDB gagal: %v", err)
	}
	if count != 1 {
		t.Errorf("Ekspektasi 1 model termuat, dapat: %d", count)
	}

	// Check whether model can be found in memory cache
	price, ok := GetModelPrice("provider/custom-test-model")
	if !ok || price.PromptPricePerMillion != 1.75 {
		t.Errorf("Model dari cache DB gagal ditemukan: %+v", price)
	}

	// Also check short name
	priceShort, okShort := GetModelPrice("custom-test-model")
	if !okShort || priceShort.CompletionPricePerMillion != 3.50 {
		t.Errorf("Short name model dari cache DB gagal ditemukan: %+v", priceShort)
	}
}

func TestSyncOpenRouterPricing_WithDB(t *testing.T) {
	// Mock OpenRouter API server
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{
			"data": [
				{
					"id": "mock/mock-model-v1",
					"name": "Mock Model V1",
					"context_length": 64000,
					"pricing": {
						"prompt": "0.000001",
						"completion": "0.000002"
					}
				}
			]
		}`)
	}))
	defer mockServer.Close()

	origURL := OpenRouterModelsURL
	OpenRouterModelsURL = mockServer.URL
	defer func() { OpenRouterModelsURL = origURL }()

	db, err := storage.NewTursoDB(":memory:", "")
	if err != nil {
		t.Fatalf("Gagal inisialisasi test db: %v", err)
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	ctx := context.Background()
	if err := repo.Migrate(ctx); err != nil {
		t.Fatalf("Migrate gagal: %v", err)
	}

	// Run SyncOpenRouterPricing with DB repo
	if err := SyncOpenRouterPricing(ctx, repo); err != nil {
		t.Fatalf("SyncOpenRouterPricing gagal: %v", err)
	}

	// 1. Verify model is saved in Turso database
	all, err := repo.GetAllCachedModels(ctx)
	if err != nil || len(all) == 0 {
		t.Fatalf("Model hasil sync tidak tersimpan di database: %v", err)
	}
	if all[0].ID != "mock/mock-model-v1" || all[0].PromptPricePerM != 1.0 {
		t.Errorf("Data model di DB tidak cocok: %+v", all[0])
	}

	// 2. Verify model is immediately usable by GetModelPrice
	price, ok := GetModelPrice("mock/mock-model-v1")
	if !ok || price.PromptPricePerMillion != 1.0 || price.CompletionPricePerMillion != 2.0 {
		t.Errorf("GetModelPrice untuk mock model gagal: %+v", price)
	}
}

func TestGetModelPrice_ShortNameAndProviderResolution(t *testing.T) {
	db, err := storage.NewTursoDB(":memory:", "")
	if err != nil {
		t.Fatalf("Gagal inisialisasi test db: %v", err)
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	ctx := context.Background()
	_ = repo.Migrate(ctx)

	// Save models in PROVIDER/MODEL format to database
	models := []storage.CachedModel{
		{
			ID:                  "openai/gpt-oss-120b",
			Name:                "OpenAI: GPT-OSS 120B",
			PromptPricePerM:     0.50,
			CompletionPricePerM: 1.50,
			ContextLength:       128000,
		},
		{
			ID:                  "meta-llama/llama-3.3-70b-instruct:free",
			Name:                "Llama 3.3 70B (Free)",
			PromptPricePerM:     0.10,
			CompletionPricePerM: 0.20,
			ContextLength:       128000,
		},
	}
	_ = repo.SaveCachedModels(ctx, models)
	_, _ = LoadPricingFromDB(ctx, repo)

	// 1. Input model name only without provider ("gpt-oss-120b")
	p1, ok1 := GetModelPrice("gpt-oss-120b")
	if !ok1 || p1.PromptPricePerMillion != 0.50 {
		t.Errorf("Input short name 'gpt-oss-120b' harus valid dan mengembalikan harga: %+v", p1)
	}

	// 2. Input full canonical name ("openai/gpt-oss-120b")
	p2, ok2 := GetModelPrice("openai/gpt-oss-120b")
	if !ok2 || p2.PromptPricePerMillion != 0.50 {
		t.Errorf("Input canonical 'openai/gpt-oss-120b' harus valid: %+v", p2)
	}

	// 3. Input with version tag ("gpt-oss-120b:free")
	p3, ok3 := GetModelPrice("gpt-oss-120b:free")
	if !ok3 || p3.PromptPricePerMillion != 0.50 {
		t.Errorf("Input dengan tag versi 'gpt-oss-120b:free' harus valid: %+v", p3)
	}

	// 4. Model in DB has ':free' tag but user input is without tag ("llama-3.3-70b-instruct")
	p4, ok4 := GetModelPrice("llama-3.3-70b-instruct")
	if !ok4 || p4.PromptPricePerMillion != 0.10 {
		t.Errorf("Input tanpa tag versi saat di DB bertag harus valid: %+v", p4)
	}

	// 5. Calculate cost using short name
	est := CalculateCost("gpt-oss-120b", 1000, 2000, 85)
	if !est.HasPricing || est.EstimatedCostUSD <= 0 {
		t.Errorf("Kalkulasi biaya untuk short name 'gpt-oss-120b' gagal: %+v", est)
	}
}

func TestSyncOpenRouterPricingAsync(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{
			"data": [
				{
					"id": "mock/async-model-v1",
					"name": "Async Model V1",
					"context_length": 32000,
					"pricing": {
						"prompt": "0.000003",
						"completion": "0.000006"
					}
				}
			]
		}`)
	}))
	defer mockServer.Close()

	origURL := OpenRouterModelsURL
	OpenRouterModelsURL = mockServer.URL
	defer func() { OpenRouterModelsURL = origURL }()

	db, err := storage.NewTursoDB(":memory:", "")
	if err != nil {
		t.Fatalf("Gagal inisialisasi test db: %v", err)
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	ctx := context.Background()
	_ = repo.Migrate(ctx)

	// Run asynchronously
	SyncOpenRouterPricingAsync(ctx, repo)

	// Poll briefly until asynchronous process completes
	var found bool
	for i := 0; i < 20; i++ {
		time.Sleep(20 * time.Millisecond)
		if p, ok := GetModelPrice("async-model-v1"); ok && p.PromptPricePerMillion == 3.0 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Model dari SyncOpenRouterPricingAsync tidak ditemukan di cache memori")
	}
}

func TestSyncOpenRouterPricing_ConcurrencyGuard(t *testing.T) {
	// Set isSyncing flag manually
	isSyncing.Store(true)

	ctx := context.Background()
	// Synchronization call while isSyncing is active must return nil immediately without blocking
	err := SyncOpenRouterPricing(ctx, nil)
	if err != nil {
		t.Errorf("Ekspektasi nil saat isSyncing aktif, dapat: %v", err)
	}

	// Reset flag to false
	isSyncing.Store(false)
	if IsSyncing() {
		t.Errorf("IsSyncing harus false setelah di-store false")
	}
}

