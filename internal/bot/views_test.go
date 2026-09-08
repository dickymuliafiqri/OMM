package bot

import (
	"strings"
	"testing"
	"time"

	"benchmark/internal/queue"
	"benchmark/internal/storage"
)

func TestMaskAPIKey(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{"sk-proj-1234567890abcdef", "sk-proj...cdef"},
		{"short", "********"},
		{"my-secret-key-1234", "my-s...1234"},
	}

	for _, tc := range testCases {
		res := MaskAPIKey(tc.input)
		if res != tc.expected {
			t.Errorf("MaskAPIKey(%s) = %s; ekspektasi %s", tc.input, res, tc.expected)
		}
	}
}

func TestFormatSWEScoreCard(t *testing.T) {
	run := &storage.BenchmarkRun{
		ID:               "swe-test-uuid",
		ModelName:        "claude-3-5-sonnet",
		TotalScore:       100,
		MaxScore:         100,
		ExecutionTimeMs:  18400,
		PromptTokens:     1200,
		CompletionTokens: 800,
		TotalTokens:      2000,
		BenchmarkMode:    "swe",
		MaxTierAchieved:  "STAFF",
	}

	tasks := []storage.SWETaskRun{
		{
			Tier:          "JUNIOR",
			TaskTitle:     "Fix Nil Pointer Dereference",
			PointsAwarded: 20,
			MaxPoints:     20,
			Resolved:      true,
			Attempts:      1,
		},
		{
			Tier:          "MID",
			TaskTitle:     "Goroutine Leak & Ticker Cleanup",
			PointsAwarded: 25,
			MaxPoints:     25,
			Resolved:      true,
			Attempts:      1,
		},
		{
			Tier:          "SENIOR",
			TaskTitle:     "Deadlock Elimination in Transfer",
			PointsAwarded: 24,
			MaxPoints:     30,
			Resolved:      true,
			Attempts:      2,
		},
		{
			Tier:          "STAFF",
			TaskTitle:     "Lock-Free Ring Buffer CAS",
			PointsAwarded: 25,
			MaxPoints:     25,
			Resolved:      true,
			Attempts:      1,
		},
	}

	card := FormatSWEScoreCard(run, tasks)

	if !strings.Contains(card, "ON MY MARK SWE-BENCH") {
		t.Errorf("expected header SWE-bench, got: %s", card)
	}
	if !strings.Contains(card, "claude-3-5-sonnet") {
		t.Errorf("expected model name in card: %s", card)
	}
	if !strings.Contains(card, "Grade S — Staff Engineer") {
		t.Errorf("expected Grade S Staff Engineer in card: %s", card)
	}
	if !strings.Contains(card, "Tier 1: Junior") || !strings.Contains(card, "Tier 4: Staff") {
		t.Errorf("expected multi-tier ladder lines in card: %s", card)
	}
	if !strings.Contains(card, "Lolos Turn 2 (Self-Healing)") {
		t.Errorf("expected self-healing note on turn 2 task: %s", card)
	}
}

// assertPreBlocksEscaped memindai semua <pre>...</pre> di dalam s dan
// memastikan tidak ada karakter '&', '<', '>' mentah di dalamnya.
func assertPreBlocksEscaped(t *testing.T, s string) {
	t.Helper()
	rest := s
	for {
		i := strings.Index(rest, "<pre>")
		if i < 0 {
			return
		}
		rest = rest[i+len("<pre>"):]
		j := strings.Index(rest, "</pre>")
		if j < 0 {
			t.Fatalf("<pre> tidak ditutup: %q", rest)
		}
		body := rest[:j]
		rest = rest[j+len("</pre>"):]
		// Ganti entity yang sudah benar dengan placeholder aman, lalu cek
		// sisa karakter khusus.
		safe := body
		safe = strings.ReplaceAll(safe, "&amp;", "")
		safe = strings.ReplaceAll(safe, "&lt;", "")
		safe = strings.ReplaceAll(safe, "&gt;", "")
		safe = strings.ReplaceAll(safe, "&quot;", "")
		safe = strings.ReplaceAll(safe, "&#39;", "")
		safe = strings.ReplaceAll(safe, "&#34;", "")
		if strings.ContainsAny(safe, "&<>") {
			t.Errorf("blok <pre> mengandung karakter HTML mentah tak-terescape (& < >):\n%q", body)
		}
	}
}

func TestFormatLeaderboardAndHistory(t *testing.T) {
	entries := []storage.LeaderboardEntry{
		{ModelName: "gpt-4o", AvgScore: 92.5, PeakScore: 95, TotalTokens: 2500, TotalRuns: 10, PassRate: 100.0},
		{ModelName: "deepseek-chat", AvgScore: 84.0, PeakScore: 90, TotalTokens: 1800, TotalRuns: 5, PassRate: 80.0},
	}

	lb := FormatLeaderboard(entries, 0, 10)
	if !strings.Contains(lb, "gpt-4o") || !strings.Contains(lb, "95 / 100") || !strings.Contains(lb, "2.500 token") {
		t.Errorf("Leaderboard format salah: %s", lb)
	}
	if strings.Contains(lb, "Rata-rata") {
		t.Errorf("Leaderboard tidak boleh memuat rata-rata skor: %s", lb)
	}

	valLB := FormatValueLeaderboard(entries, 0, 10)
	if !strings.Contains(valLB, "Efisiensi Biaya") || !strings.Contains(valLB, "gpt-4o") || !strings.Contains(valLB, "2.500 token") {
		t.Errorf("FormatValueLeaderboard salah: %s", valLB)
	}

	runs := []storage.BenchmarkRun{
		{ID: "run-abc-1234", ModelName: "gpt-4o", TotalScore: 95, MaxScore: 100, Status: "SUCCESS", CreatedAt: time.Now()},
	}

	hist := FormatHistory(runs)
	if !strings.Contains(hist, "gpt-4o") || !strings.Contains(hist, "95/100") {
		t.Errorf("History format salah: %s", hist)
	}
}

func TestFormatCategoryLeaderboard(t *testing.T) {
	catEntries := []storage.CategoryLeaderboardEntry{
		{ModelName: "gpt-4o", CategoryName: "Keamanan Aplikasi (XSS)", PeakScore: 15, AvgScore: 15.0, MaxScore: 15, TotalRuns: 3},
	}
	res := FormatCategoryLeaderboard("Anti-XSS", catEntries, 0, 10)
	if !strings.Contains(res, "gpt-4o") || !strings.Contains(res, "15 / 15 pts") {
		t.Errorf("FormatCategoryLeaderboard salah: %s", res)
	}
	if strings.Contains(res, "Rata-rata") {
		t.Errorf("FormatCategoryLeaderboard tidak boleh memuat rata-rata skor: %s", res)
	}
}

func TestFormatRunFullDetailsAndCSV(t *testing.T) {
	run := &storage.BenchmarkRun{
		ID:               "run-uuid-full",
		ModelName:        "claude-3-5-sonnet",
		ProviderBaseURL:  "https://api.anthropic.com",
		TotalScore:       90,
		MaxScore:         100,
		ExecutionTimeMs:  11200,
		PromptTokens:     1000,
		CompletionTokens: 1500,
		TotalTokens:      2500,
		EstimatedCostUSD: 0.0255,
		CostTier:         "Standar",
		CreatedAt:        time.Now(),
		Status:           "SUCCESS",
	}
	details := []storage.BenchmarkDetail{
		{CategoryName: "Server Hardening", Score: 15, MaxScore: 15, Details: []string{"Slowloris closed in 1.2s"}},
	}

	fullText := FormatRunFullDetails(run, details)
	if !strings.Contains(fullText, "claude-3-5-sonnet") || !strings.Contains(fullText, "Slowloris closed in 1.2s") {
		t.Errorf("FormatRunFullDetails salah: %s", fullText)
	}
	if !strings.Contains(fullText, "Total: 2500") {
		t.Errorf("FormatRunFullDetails tidak memuat total token: %s", fullText)
	}
	// Guard regresi HTML escaping — sama seperti pada scorecard.
	assertPreBlocksEscaped(t, fullText)

	csvData := GenerateRunCSV(run, details)
	csvStr := string(csvData)
	if !strings.Contains(csvStr, "Server Hardening") || !strings.Contains(csvStr, "\"TOTAL\",90,100,\"SUCCESS\"") {
		t.Errorf("GenerateRunCSV salah:\n%s", csvStr)
	}
	if !strings.Contains(csvStr, "\"TOKEN_TOTAL\",2500") {
		t.Errorf("GenerateRunCSV tidak memuat token total: %s", csvStr)
	}
	if !strings.Contains(csvStr, "\"ESTIMATED_COST_USD\",0.025500") || !strings.Contains(csvStr, "\"COST_TIER\",\"Standar\"") {
		t.Errorf("GenerateRunCSV tidak memuat metrik biaya: %s", csvStr)
	}
}

func TestNewKeyboards(t *testing.T) {
	savedKb := SavedCredsKeyboard()
	if len(savedKb.InlineKeyboard) != 3 {
		t.Errorf("SavedCredsKeyboard harus memiliki 3 baris: %d", len(savedKb.InlineKeyboard))
	}

	models := []string{"model-1", "model-2", "model-3", "model-4", "model-5", "model-6", "model-7"}
	kb := ModelListPaginationKeyboard(models, 0, 5)
	if len(kb.InlineKeyboard) < 6 { // 5 models + 1 nav + 1 extra actions
		t.Errorf("ModelListPaginationKeyboard baris kurang: %d", len(kb.InlineKeyboard))
	}
}

func TestFormatAdminViews(t *testing.T) {
	help := FormatAdminHelp()
	if !strings.Contains(help, "/admin stats") || !strings.Contains(help, "/admin ban") {
		t.Errorf("FormatAdminHelp salah: %s", help)
	}

	stats := &storage.SystemStats{
		TotalRuns:        20,
		SuccessRuns:      18,
		FailedRuns:       2,
		ViolationRuns:    1,
		ErrorRatePercent: 10.0,
		TotalUsers:       5,
		BannedUsers:      1,
		UniqueModels:     3,
		TotalCostUSD:     0.1234,
	}

	text := FormatAdminStats(stats, 2, 3, 1, 20, 2*time.Hour+15*time.Minute)
	if !strings.Contains(text, "Total Pengguna") || !strings.Contains(text, "2 / 3") || !strings.Contains(text, "10.0%") {
		t.Errorf("FormatAdminStats salah: %s", text)
	}
	if !strings.Contains(text, "Total Estimasi Biaya Token") || !strings.Contains(text, "0.1234") {
		t.Errorf("FormatAdminStats tidak memuat total estimasi biaya: %s", text)
	}
}

func TestFormatProgressMessage(t *testing.T) {
	// 1. Reasoning Phase
	info1 := queue.ProgressInfo{
		Step:        2,
		Total:       4,
		TaskTitle:   "Worker Pool Leak",
		Tier:        "Mid-Level",
		TierNumber:  2,
		Attempt:     1,
		MaxAttempts: 3,
		Phase:       "REASONING",
		Snippet:     "Analyzing ticker stop channel <select>",
		Elapsed:     75 * time.Second,
		Timeout:     5 * time.Minute,
	}

	msg1 := FormatProgressMessage(info1)
	if !strings.Contains(msg1, "[2/4] Tier 2 (Mid-Level)") {
		t.Errorf("expected tier display, got: %s", msg1)
	}
	if !strings.Contains(msg1, "Worker Pool Leak") {
		t.Errorf("expected task title, got: %s", msg1)
	}
	if !strings.Contains(msg1, "01:15 / 05:00") {
		t.Errorf("expected elapsed/timeout 01:15 / 05:00, got: %s", msg1)
	}
	if !strings.Contains(msg1, "Sedang bernalar (reasoning)...") {
		t.Errorf("expected reasoning activity header, got: %s", msg1)
	}
	if !strings.Contains(msg1, "&lt;select&gt;") {
		t.Errorf("expected escaped snippet &lt;select&gt;, got: %s", msg1)
	}

	// 2. Coding Phase
	info2 := queue.ProgressInfo{
		Step:        3,
		Total:       4,
		TaskTitle:   "Deadlock Transfer",
		Tier:        "Senior",
		TierNumber:  3,
		Attempt:     2,
		MaxAttempts: 2,
		Phase:       "CODING",
		Snippet:     "func (p *Pool) Acquire()",
		Elapsed:     120 * time.Second,
		Timeout:     5 * time.Minute,
	}

	msg2 := FormatProgressMessage(info2)
	if !strings.Contains(msg2, "Sedang menulis kode Go...") {
		t.Errorf("expected coding activity header, got: %s", msg2)
	}
	if !strings.Contains(msg2, "<pre>func (p *Pool) Acquire()</pre>") {
		t.Errorf("expected code inside <pre>, got: %s", msg2)
	}
	if !strings.Contains(msg2, "Turn 2/2") {
		t.Errorf("expected Turn 2/2, got: %s", msg2)
	}

	// 3. Evaluating Phase
	info3 := queue.ProgressInfo{
		Step:       4,
		Total:      4,
		TaskTitle:  "Atomic CAS Queue",
		Tier:       "Staff",
		TierNumber: 4,
		Attempt:    1,
		Phase:      "EVALUATING",
		StatusText: "Menjalankan sandbox evaluator (go test -race)...",
	}

	msg3 := FormatProgressMessage(info3)
	if !strings.Contains(msg3, "Aktivitas Sandbox") {
		t.Errorf("expected sandbox activity header, got: %s", msg3)
	}
}


