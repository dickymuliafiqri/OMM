package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func setupTestDB(t *testing.T) Repository {
	db, err := NewTursoDB(":memory:", "")
	if err != nil {
		t.Fatalf("Gagal inisialisasi test database: %v", err)
	}

	repo := NewRepository(db)
	ctx := context.Background()

	if err := repo.Migrate(ctx); err != nil {
		t.Fatalf("Migrate gagal: %v", err)
	}

	return repo
}

func TestRepository_UserCRUD(t *testing.T) {
	repo := setupTestDB(t)
	defer repo.Close()
	ctx := context.Background()

	user := &User{
		TelegramID: 123456789,
		Username:   "johndoe",
		FirstName:  "John",
		Role:       "user",
		DailyQuota: 10,
	}

	// 1. Upsert
	if err := repo.UpsertUser(ctx, user); err != nil {
		t.Fatalf("UpsertUser gagal: %v", err)
	}

	// 2. Get
	fetched, err := repo.GetUser(ctx, user.TelegramID)
	if err != nil {
		t.Fatalf("GetUser gagal: %v", err)
	}
	if fetched.Username != "johndoe" || fetched.FirstName != "John" {
		t.Errorf("Data user tidak sesuai: %+v", fetched)
	}

	// 3. Update existing
	user.FirstName = "John Updated"
	if err := repo.UpsertUser(ctx, user); err != nil {
		t.Fatalf("UpsertUser update gagal: %v", err)
	}

	updated, err := repo.GetUser(ctx, user.TelegramID)
	if err != nil {
		t.Fatalf("GetUser setelah update gagal: %v", err)
	}
	if updated.FirstName != "John Updated" {
		t.Errorf("FirstName gagal terupdate: %s", updated.FirstName)
	}

	// 4. Test SaveUserCredentials
	if err := repo.SaveUserCredentials(ctx, user.TelegramID, "https://api.openai.com/v1", "sk-test-secret-key"); err != nil {
		t.Fatalf("SaveUserCredentials gagal: %v", err)
	}
	userWithCreds, err := repo.GetUser(ctx, user.TelegramID)
	if err != nil || userWithCreds.SavedBaseURL != "https://api.openai.com/v1" || userWithCreds.SavedAPIKey != "sk-test-secret-key" {
		t.Errorf("Kredensial gagal disimpan/dibaca: %+v", userWithCreds)
	}

	// 5. Test ResetUserCredentials
	if err := repo.ResetUserCredentials(ctx, user.TelegramID); err != nil {
		t.Fatalf("ResetUserCredentials gagal: %v", err)
	}
	userReset, err := repo.GetUser(ctx, user.TelegramID)
	if err != nil || userReset.SavedBaseURL != "" || userReset.SavedAPIKey != "" {
		t.Errorf("ResetUserCredentials gagal mengosongkan kredensial: %+v", userReset)
	}
}

func TestRepository_BenchmarkAndLeaderboard(t *testing.T) {
	repo := setupTestDB(t)
	defer repo.Close()
	ctx := context.Background()

	// Register user
	_ = repo.UpsertUser(ctx, &User{TelegramID: 999, Username: "tester", FirstName: "Test"})

	// Buat beberapa run untuk model berbeda
	run1 := &BenchmarkRun{
		ID:               "run-1",
		TelegramID:       999,
		ProviderBaseURL:  "https://api.openai.com/v1",
		ModelName:        "gpt-4o",
		TotalScore:       95,
		MaxScore:         100,
		ExecutionTimeMs:  12500,
		PromptTokens:     150,
		CompletionTokens: 500,
		TotalTokens:      650,
		EstimatedCostUSD: 0.005,
		CostTier:         "Standar",
		Status:           "SUCCESS",
	}
	sweTasks1 := []SWETaskRun{
		{TaskID: "swe-t1-01", Tier: "Junior", TaskTitle: "Order Validator", PointsAwarded: 20, MaxPoints: 20, Attempts: 1, Resolved: true},
	}
	if err := repo.SaveSWERun(ctx, run1, sweTasks1); err != nil {
		t.Fatalf("SaveSWERun run1 gagal: %v", err)
	}
	// Direct insert to benchmark_details for testing category leaderboard and GetRunDetails
	sqlRepo := repo.(*SQLiteRepository)
	_, _ = sqlRepo.db.ExecContext(ctx, `INSERT INTO benchmark_details (run_id, category_name, score, max_score, details_json) VALUES (?, ?, ?, ?, ?)`, run1.ID, "Kompilasi & Build", 15, 15, `["Build pass"]`)
	_, _ = sqlRepo.db.ExecContext(ctx, `INSERT INTO benchmark_details (run_id, category_name, score, max_score, details_json) VALUES (?, ?, ?, ?, ?)`, run1.ID, "XSS Protection", 15, 15, `["XSS safe"]`)

	run2 := &BenchmarkRun{
		ID:               "run-2",
		TelegramID:       999,
		ProviderBaseURL:  "https://api.deepseek.com",
		ModelName:        "deepseek-chat",
		TotalScore:       80,
		MaxScore:         100,
		ExecutionTimeMs:  15000,
		PromptTokens:     200,
		CompletionTokens: 600,
		TotalTokens:      800,
		EstimatedCostUSD: 0.0002,
		CostTier:         "Sangat Ekonomis",
		Status:           "SUCCESS",
	}
	if err := repo.SaveSWERun(ctx, run2, nil); err != nil {
		t.Fatalf("SaveSWERun run2 gagal: %v", err)
	}

	run3 := &BenchmarkRun{
		ID:               "run-3",
		TelegramID:       999,
		ProviderBaseURL:  "https://api.openai.com/v1",
		ModelName:        "gpt-4o",
		TotalScore:       85,
		MaxScore:         100,
		ExecutionTimeMs:  11000,
		EstimatedCostUSD: 0.005,
		CostTier:         "Standar",
		Status:           "SUCCESS",
	}
	if err := repo.SaveSWERun(ctx, run3, nil); err != nil {
		t.Fatalf("SaveSWERun run3 gagal: %v", err)
	}

	// 1. Leaderboard check
	lb, err := repo.GetLeaderboard(ctx, 10, 0)
	if err != nil {
		t.Fatalf("GetLeaderboard gagal: %v", err)
	}
	if len(lb) != 2 {
		t.Fatalf("Ekspektasi 2 model di leaderboard, dapat: %d", len(lb))
	}

	// gpt-4o rata-rata (95+85)/2 = 90.0, deepseek-chat = 80.0
	if lb[0].ModelName != "gpt-4o" || lb[0].AvgScore != 90.0 {
		t.Errorf("Leaderboard rank 1 tidak sesuai: %+v", lb[0])
	}
	if lb[1].ModelName != "deepseek-chat" || lb[1].AvgScore != 80.0 {
		t.Errorf("Leaderboard rank 2 tidak sesuai: %+v", lb[1])
	}

	// 1b. Value for Money Leaderboard check
	valLB, err := repo.GetValueLeaderboard(ctx, 10, 0)
	if err != nil {
		t.Fatalf("GetValueLeaderboard gagal: %v", err)
	}
	if len(valLB) != 2 {
		t.Fatalf("Ekspektasi 2 model di value leaderboard, dapat: %d", len(valLB))
	}
	// deepseek-chat memiliki rasio value score lebih tinggi (hemat biaya) sehingga menempati rank 1
	if valLB[0].ModelName != "deepseek-chat" {
		t.Errorf("Value leaderboard rank 1 harus deepseek-chat, dapat: %s", valLB[0].ModelName)
	}

	// 1b. Category Leaderboard check
	catLB, err := repo.GetCategoryLeaderboard(ctx, "XSS", 10, 0)
	if err != nil {
		t.Fatalf("GetCategoryLeaderboard gagal: %v", err)
	}
	if len(catLB) == 0 || catLB[0].ModelName != "gpt-4o" {
		t.Errorf("GetCategoryLeaderboard tidak sesuai: %+v", catLB)
	}

	// 2. User History check
	history, err := repo.GetUserHistory(ctx, 999, 10, 0)
	if err != nil {
		t.Fatalf("GetUserHistory gagal: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("Ekspektasi 3 riwayat pengujian, dapat: %d", len(history))
	}
	if history[0].PromptTokens != 150 && history[2].PromptTokens != 150 {
		t.Errorf("PromptTokens tidak tersimpan di riwayat: %+v", history)
	}

	// 3. Run Details check
	fetchedRun, fetchedDetails, err := repo.GetRunDetails(ctx, "run-1")
	if err != nil {
		t.Fatalf("GetRunDetails gagal: %v", err)
	}
	if fetchedRun.TotalScore != 95 {
		t.Errorf("Total score run-1 tidak cocok: %d", fetchedRun.TotalScore)
	}
	if fetchedRun.PromptTokens != 150 || fetchedRun.CompletionTokens != 500 || fetchedRun.TotalTokens != 650 {
		t.Errorf("Token metrics run-1 tidak cocok: prompt=%d, comp=%d, total=%d",
			fetchedRun.PromptTokens, fetchedRun.CompletionTokens, fetchedRun.TotalTokens)
	}
	if len(fetchedDetails) != 2 {
		t.Errorf("Ekspektasi 2 details pada run-1, dapat: %d", len(fetchedDetails))
	}
	if len(fetchedDetails[0].Details) == 0 || fetchedDetails[0].Details[0] != "Build pass" {
		t.Errorf("Deserialisasi details_json gagal: %+v", fetchedDetails[0])
	}
}

func TestRepository_RateLimit(t *testing.T) {
	repo := setupTestDB(t)
	defer repo.Close()
	ctx := context.Background()

	telegramID := int64(888777)
	limit := 3

	// Request 1: allow
	allowed, remaining, _, err := repo.CheckAndUpdateRateLimit(ctx, telegramID, limit)
	if err != nil || !allowed || remaining != 2 {
		t.Errorf("Request 1 salah: allowed=%v, remaining=%d, err=%v", allowed, remaining, err)
	}

	// Request 2: allow
	allowed, remaining, _, err = repo.CheckAndUpdateRateLimit(ctx, telegramID, limit)
	if err != nil || !allowed || remaining != 1 {
		t.Errorf("Request 2 salah: allowed=%v, remaining=%d, err=%v", allowed, remaining, err)
	}

	// Request 3: allow
	allowed, remaining, _, err = repo.CheckAndUpdateRateLimit(ctx, telegramID, limit)
	if err != nil || !allowed || remaining != 0 {
		t.Errorf("Request 3 salah: allowed=%v, remaining=%d, err=%v", allowed, remaining, err)
	}

	// Request 4: reject
	allowed, remaining, retryAfter, err := repo.CheckAndUpdateRateLimit(ctx, telegramID, limit)
	if err != nil {
		t.Fatalf("Request 4 error: %v", err)
	}
	if allowed {
		t.Errorf("Request 4 seharusnya ditolak karena melebihi kuota 3")
	}
	if retryAfter <= 0 {
		t.Errorf("retryAfter harus bernilai positif, dapat: %v", retryAfter)
	}
}

func TestRepository_AdminAndStats(t *testing.T) {
	repo := setupTestDB(t)
	defer repo.Close()
	ctx := context.Background()

	// 1. Test Ping
	if err := repo.Ping(ctx); err != nil {
		t.Fatalf("Ping gagal: %v", err)
	}

	targetID := int64(998877)

	// 2. Test Ban & IsUserBanned
	banned, err := repo.IsUserBanned(ctx, targetID)
	if err != nil || banned {
		t.Errorf("User awal harusnya tidak banned: %v", banned)
	}

	if err := repo.BanUser(ctx, targetID); err != nil {
		t.Fatalf("BanUser gagal: %v", err)
	}

	banned, err = repo.IsUserBanned(ctx, targetID)
	if err != nil || !banned {
		t.Errorf("User seharusnya banned setelah BanUser: %v", banned)
	}

	// 3. Test Unban
	if err := repo.UnbanUser(ctx, targetID); err != nil {
		t.Fatalf("UnbanUser gagal: %v", err)
	}

	banned, err = repo.IsUserBanned(ctx, targetID)
	if err != nil || banned {
		t.Errorf("User seharusnya tidak banned setelah UnbanUser: %v", banned)
	}

	// 4. Test ResetUserRateLimit
	_, _, _, _ = repo.CheckAndUpdateRateLimit(ctx, targetID, 1)
	allowed, _, _, _ := repo.CheckAndUpdateRateLimit(ctx, targetID, 1)
	if allowed {
		t.Errorf("Harusnya sudah habis kuota")
	}

	if err := repo.ResetUserRateLimit(ctx, targetID); err != nil {
		t.Fatalf("ResetUserRateLimit gagal: %v", err)
	}

	allowed, remaining, _, _ := repo.CheckAndUpdateRateLimit(ctx, targetID, 1)
	if !allowed || remaining != 0 {
		t.Errorf("Setelah reset kuota, request pertama harusnya diizinkan")
	}

	// 5. Test GetSystemStats
	_ = repo.SaveSWERun(ctx, &BenchmarkRun{
		ID:               "stats-cost-run",
		TelegramID:       targetID,
		ProviderBaseURL:  "https://api.openai.com/v1",
		ModelName:        "gpt-4o",
		TotalScore:       90,
		MaxScore:         100,
		ExecutionTimeMs:  5000,
		EstimatedCostUSD: 0.0042,
		CostTier:         "Ekonomis",
		Status:           "SUCCESS",
	}, nil)

	stats, err := repo.GetSystemStats(ctx)
	if err != nil {
		t.Fatalf("GetSystemStats gagal: %v", err)
	}
	if stats == nil {
		t.Fatalf("SystemStats tidak boleh nil")
	}
	if stats.TotalCostUSD <= 0 {
		t.Errorf("TotalCostUSD harus lebih dari 0, dapat: %f", stats.TotalCostUSD)
	}
}

func TestRepository_CachedModels(t *testing.T) {
	repo := setupTestDB(t)
	defer repo.Close()
	ctx := context.Background()

	models := []CachedModel{
		{
			ID:                  "openai/gpt-4o",
			Name:                "OpenAI: GPT-4o",
			PromptPricePerM:     2.50,
			CompletionPricePerM: 10.00,
			ContextLength:       128000,
		},
		{
			ID:                  "deepseek/deepseek-chat",
			Name:                "DeepSeek: DeepSeek V3",
			PromptPricePerM:     0.14,
			CompletionPricePerM: 0.28,
			ContextLength:       64000,
		},
	}

	// 1. Simpan cached models
	if err := repo.SaveCachedModels(ctx, models); err != nil {
		t.Fatalf("SaveCachedModels gagal: %v", err)
	}

	// 2. Ambil semua cached models
	all, err := repo.GetAllCachedModels(ctx)
	if err != nil {
		t.Fatalf("GetAllCachedModels gagal: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("Ekspektasi 2 model di cache, dapat: %d", len(all))
	}

	// 3. Get single model
	m1, err := repo.GetCachedModel(ctx, "openai/gpt-4o")
	if err != nil || m1 == nil {
		t.Fatalf("GetCachedModel openai/gpt-4o gagal: %v", err)
	}
	if m1.PromptPricePerM != 2.50 || m1.ContextLength != 128000 {
		t.Errorf("Data cached model tidak cocok: %+v", m1)
	}

	// 4. Update (upsert) model dengan harga baru
	updated := []CachedModel{
		{
			ID:                  "openai/gpt-4o",
			Name:                "OpenAI: GPT-4o Updated",
			PromptPricePerM:     2.00,
			CompletionPricePerM: 8.00,
			ContextLength:       128000,
		},
	}
	if err := repo.SaveCachedModels(ctx, updated); err != nil {
		t.Fatalf("SaveCachedModels update gagal: %v", err)
	}
	mUpdated, err := repo.GetCachedModel(ctx, "openai/gpt-4o")
	if err != nil || mUpdated.PromptPricePerM != 2.00 {
		t.Errorf("Upsert cached model gagal mengupdate harga: %+v", mUpdated)
	}
}

func TestRepository_SaveCachedModels_Batched(t *testing.T) {
	repo := setupTestDB(t)
	defer repo.Close()
	ctx := context.Background()

	// Buat 125 model dummy untuk menguji pemotongan batch berukuran 50 (50 + 50 + 25)
	const totalModels = 125
	models := make([]CachedModel, totalModels)
	for i := 0; i < totalModels; i++ {
		models[i] = CachedModel{
			ID:                  fmt.Sprintf("provider/model-%03d", i+1),
			Name:                fmt.Sprintf("Model %d", i+1),
			PromptPricePerM:     float64(i + 1),
			CompletionPricePerM: float64(i+1) * 2,
			ContextLength:       32000,
		}
	}

	if err := repo.SaveCachedModels(ctx, models); err != nil {
		t.Fatalf("SaveCachedModels batched gagal: %v", err)
	}

	all, err := repo.GetAllCachedModels(ctx)
	if err != nil {
		t.Fatalf("GetAllCachedModels gagal: %v", err)
	}
	if len(all) != totalModels {
		t.Fatalf("Ekspektasi %d model tersimpan, dapat: %d", totalModels, len(all))
	}

	// Verifikasi sampel data model pertama dan terakhir
	mFirst, err := repo.GetCachedModel(ctx, "provider/model-001")
	if err != nil || mFirst == nil || mFirst.PromptPricePerM != 1.0 {
		t.Errorf("Model pertama tidak sesuai: %+v", mFirst)
	}
	mLast, err := repo.GetCachedModel(ctx, "provider/model-125")
	if err != nil || mLast == nil || mLast.PromptPricePerM != 125.0 {
		t.Errorf("Model terakhir tidak sesuai: %+v", mLast)
	}
}

func TestRepository_SaveCachedModels_ContextCancel(t *testing.T) {
	repo := setupTestDB(t)
	defer repo.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // langsung batalkan

	models := []CachedModel{
		{
			ID:                  "test/canceled-model",
			Name:                "Canceled",
			PromptPricePerM:     1.0,
			CompletionPricePerM: 2.0,
		},
	}

	err := repo.SaveCachedModels(ctx, models)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Harus mengembalikan context.Canceled saat context dibatalkan, dapat: %v", err)
	}
}

func TestRepository_Leaderboard_PeakScoreVsAverage(t *testing.T) {
	repo := setupTestDB(t)
	defer repo.Close()
	ctx := context.Background()

	// Model A diuji 2 kali: skor 95 dan 65 (Rata-rata: 80.0, Peak: 95)
	_ = repo.SaveSWERun(ctx, &BenchmarkRun{
		ID: "run-a1", TelegramID: 100, ModelName: "model-frequent", TotalScore: 95, MaxScore: 100,
		ExecutionTimeMs: 1000, Status: "SUCCESS",
	}, nil)
	_ = repo.SaveSWERun(ctx, &BenchmarkRun{
		ID: "run-a2", TelegramID: 100, ModelName: "model-frequent", TotalScore: 65, MaxScore: 100,
		ExecutionTimeMs: 1000, Status: "SUCCESS",
	}, nil)

	// Model B hanya diuji 1 kali dengan skor 85 (Rata-rata: 85.0, Peak: 85)
	_ = repo.SaveSWERun(ctx, &BenchmarkRun{
		ID: "run-b1", TelegramID: 200, ModelName: "model-one-hit", TotalScore: 85, MaxScore: 100,
		ExecutionTimeMs: 1000, Status: "SUCCESS",
	}, nil)

	// Leaderboard harus mengurutkan berdasarkan PeakScore tertinggi, BUKAN rata-rata!
	// model-frequent (peak 95) HARUS menempati peringkat 1 di atas model-one-hit (peak 85)
	lb, err := repo.GetLeaderboard(ctx, 10, 0)
	if err != nil {
		t.Fatalf("GetLeaderboard gagal: %v", err)
	}
	if len(lb) != 2 {
		t.Fatalf("Ekspektasi 2 model, dapat: %d", len(lb))
	}

	if lb[0].ModelName != "model-frequent" || lb[0].PeakScore != 95 {
		t.Errorf("Rank 1 harus model-frequent dengan peak 95, dapat: %+v", lb[0])
	}
	if lb[1].ModelName != "model-one-hit" || lb[1].PeakScore != 85 {
		t.Errorf("Rank 2 harus model-one-hit dengan peak 85, dapat: %+v", lb[1])
	}
}

func TestRepository_Leaderboard_TieBreakerCheaperWins(t *testing.T) {
	repo := setupTestDB(t)
	defer repo.Close()
	ctx := context.Background()

	// 1. nemotron-3-super: skor 72, biaya lebih murah $0.0001, token 1800 (4 runs)
	for i := 1; i <= 4; i++ {
		_ = repo.SaveSWERun(ctx, &BenchmarkRun{
			ID:               fmt.Sprintf("run-nemotron-%d", i),
			TelegramID:       101,
			ModelName:        "nemotron-3-super",
			TotalScore:       72,
			MaxScore:         100,
			TotalTokens:      1800,
			EstimatedCostUSD: 0.0001,
			ExecutionTimeMs:  1200,
			Status:           "SUCCESS",
		}, nil)
	}

	// 2. gpt-oss:120b: skor 72, biaya lebih mahal $0.0002, token 2400 (5 runs)
	for i := 1; i <= 5; i++ {
		_ = repo.SaveSWERun(ctx, &BenchmarkRun{
			ID:               fmt.Sprintf("run-gpt-%d", i),
			TelegramID:       102,
			ModelName:        "gpt-oss:120b",
			TotalScore:       72,
			MaxScore:         100,
			TotalTokens:      2400,
			EstimatedCostUSD: 0.0002,
			ExecutionTimeMs:  1500,
			Status:           "SUCCESS",
		}, nil)
	}

	// 3. gemma4:31b: skor 72, biaya paling mahal $0.0003, token 2800 (1 run)
	_ = repo.SaveSWERun(ctx, &BenchmarkRun{
		ID:               "run-gemma-1",
		TelegramID:       103,
		ModelName:        "gemma4:31b",
		TotalScore:       72,
		MaxScore:         100,
		TotalTokens:      2800,
		EstimatedCostUSD: 0.0003,
		ExecutionTimeMs:  1100,
		Status:           "SUCCESS",
	}, nil)

	lb, err := repo.GetLeaderboard(ctx, 10, 0)
	if err != nil {
		t.Fatalf("GetLeaderboard gagal: %v", err)
	}
	if len(lb) != 3 {
		t.Fatalf("Ekspektasi 3 model di leaderboard, dapat: %d", len(lb))
	}

	// Saat skor puncak sama (72):
	// Peringkat 1: nemotron-3-super (karena biaya paling hemat $0.0001 & token 1800)
	if lb[0].ModelName != "nemotron-3-super" {
		t.Errorf("Rank 1 harus nemotron-3-super (biaya lebih murah), dapat: %s (biaya: %f)", lb[0].ModelName, lb[0].AvgCostUSD)
	}
	if lb[0].TotalTokens != 1800 {
		t.Errorf("TotalTokens nemotron harus 1800, dapat: %d", lb[0].TotalTokens)
	}

	// Peringkat 2: gpt-oss:120b (biaya $0.0002)
	if lb[1].ModelName != "gpt-oss:120b" {
		t.Errorf("Rank 2 harus gpt-oss:120b, dapat: %s", lb[1].ModelName)
	}

	// Peringkat 3: gemma4:31b (biaya $0.0003)
	if lb[2].ModelName != "gemma4:31b" {
		t.Errorf("Rank 3 harus gemma4:31b, dapat: %s", lb[2].ModelName)
	}
}

func TestRepository_SWETaskRuns(t *testing.T) {
	repo := setupTestDB(t)
	defer repo.Close()
	ctx := context.Background()

	_ = repo.UpsertUser(ctx, &User{TelegramID: 888, Username: "swe_tester", FirstName: "SWE"})

	run := &BenchmarkRun{
		ID:               "swe-run-123",
		TelegramID:       888,
		ProviderBaseURL:  "https://api.openai.com/v1",
		ModelName:        "claude-3-5-sonnet",
		TotalScore:       100,
		MaxScore:         100,
		ExecutionTimeMs:  18400,
		PromptTokens:     1200,
		CompletionTokens: 800,
		TotalTokens:      2000,
		EstimatedCostUSD: 0.0042,
		CostTier:         "Ekonomis",
		BenchmarkMode:    "swe",
		MaxTierAchieved:  "STAFF",
		Status:           "SUCCESS",
	}

	tasks := []SWETaskRun{
		{
			ID:            "task-run-1",
			RunID:         run.ID,
			TaskID:        "swe-t1-nil-pointer-01",
			TaskTitle:     "Fix Nil Pointer Dereference",
			Tier:          "JUNIOR",
			PointsAwarded: 20,
			MaxPoints:     20,
			Resolved:      true,
			HasRace:       false,
			Attempts:      1,
			TestOutput:    "PASS",
		},
		{
			ID:            "task-run-2",
			RunID:         run.ID,
			TaskID:        "swe-t2-goroutine-leak-01",
			TaskTitle:     "Goroutine Leak & Ticker Cleanup",
			Tier:          "MID",
			PointsAwarded: 25,
			MaxPoints:     25,
			Resolved:      true,
			HasRace:       false,
			Attempts:      1,
			TestOutput:    "PASS",
		},
	}

	if err := repo.SaveSWERun(ctx, run, tasks); err != nil {
		t.Fatalf("SaveSWERun failed: %v", err)
	}

	fetchedTasks, err := repo.GetSWETaskRuns(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetSWETaskRuns failed: %v", err)
	}

	if len(fetchedTasks) != 2 {
		t.Fatalf("expected 2 task runs, got %d", len(fetchedTasks))
	}

	if fetchedTasks[0].TaskID != "swe-t1-nil-pointer-01" || !fetchedTasks[0].Resolved {
		t.Errorf("task 1 data mismatch: %+v", fetchedTasks[0])
	}
	if fetchedTasks[1].Tier != "MID" || fetchedTasks[1].PointsAwarded != 25 {
		t.Errorf("task 2 data mismatch: %+v", fetchedTasks[1])
	}
}





