package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"

	"benchmark/internal/ai"
	"benchmark/internal/config"
	"benchmark/internal/pricing"
	"benchmark/internal/storage"
	"benchmark/internal/swe"
	"benchmark/internal/swe/tasks"
)

func main() {
	taskID := flag.String("task", "", "ID Task SWE tertentu (misal swe-t1-order-validator-01)")
	sourceFile := flag.String("file", "", "Path ke file kode Go yang akan diuji")
	verifyAll := flag.Bool("verify-all", false, "Verifikasi pre-flight F2P pada seluruh 8 task SWE")
	runLadder := flag.Bool("ladder", false, "Jalankan evaluasi 4-Tier Ladder lokal menggunakan solusi referensi")

	// AI inference flags
	modelName := flag.String("model", "", "Nama model AI (misal gpt-4o, claude-3-5-sonnet, deepseek-chat)")
	baseURL := flag.String("base-url", "", "Base URL AI provider (misal https://openrouter.ai/api/v1)")
	apiKey := flag.String("api-key", "", "API Key AI provider")

	flag.Parse()

	// SWE-bench mode
	fmt.Println("=====================================================")
	fmt.Println("⚡ ON MY MARK NATIVE GO SWE-BENCH (100 PTS) ⚡")
	fmt.Println("=====================================================")

	evaluator := swe.NewEvaluator(true)
	ctx := context.Background()

	// 1. Verify all 8 tasks
	if *verifyAll {
		runVerifyAll(ctx, evaluator)
		return
	}

	// 2. Evaluate using AI Model via API
	if *modelName != "" && *baseURL != "" {
		runAIModelSWE(ctx, evaluator, *modelName, *baseURL, *apiKey)
		return
	}

	// 3. Evaluate specific file for a single task
	if *taskID != "" && *sourceFile != "" {
		runSingleTaskFile(ctx, evaluator, *taskID, *sourceFile)
		return
	}

	// 4. Evaluate ladder using local reference solutions
	if *runLadder || (*taskID == "" && *sourceFile == "" && *modelName == "") {
		runLocalLadder(ctx, evaluator)
		return
	}

	printUsage()
}

func runVerifyAll(ctx context.Context, evaluator *swe.Evaluator) {
	fmt.Println("🔍 Menjalankan Pre-Flight Fail-to-Pass (F2P) Validation pada seluruh 8 task...")
	allTasks := tasks.AllTasksList()

	passCount := 0
	for _, task := range allTasks {
		fmt.Printf("\n▶ [%s] %s (%s - %d pts)\n", task.Tier, task.Title, task.ID, task.Points)

		// Test BrokenCode (must fail)
		brokenRes, err := evaluator.Evaluate(ctx, task, task.BrokenCode, 1)
		if err != nil {
			fmt.Printf("  ❌ Error mengevaluasi broken code: %v\n", err)
			continue
		}
		if brokenRes.Resolved && !brokenRes.HasRace {
			fmt.Printf("  ❌ F2P GAGAL: Broken code lolos verifikasi tanpa error!\n")
			continue
		}
		fmt.Printf("  ✅ Broken Code: Gagal seperti yang diharapkan (F2P confirmed)\n")

		// Test ReferenceSolution (must pass)
		refRes, err := evaluator.Evaluate(ctx, task, task.ReferenceSolution, 1)
		if err != nil {
			fmt.Printf("  ❌ Error mengevaluasi reference solution: %v\n", err)
			continue
		}
		if !refRes.Resolved || refRes.HasRace {
			fmt.Printf("  ❌ Solusi referensi gagal menyelesaikan task:\n%s\n", refRes.TestOutput)
			continue
		}
		fmt.Printf("  ✅ Reference Solution: Lolos 100%% (%d/%d pts, Durasi: %dms, Bebas Race)\n",
			refRes.Points, task.Points, refRes.DurationMs)
		passCount++
	}

	fmt.Println("\n=====================================================")
	fmt.Printf("🏆 F2P VERIFIKASI SELESAI: %d / %d Task Valid & Siap Digunakan\n", passCount, len(allTasks))
	fmt.Println("=====================================================")
}

func runLocalLadder(ctx context.Context, evaluator *swe.Evaluator) {
	fmt.Println("🪜 Menjalankan Evaluasi 4-Tier Ladder (Solusi Referensi):")
	ladder, err := swe.DefaultLadder()
	if err != nil {
		fmt.Printf("❌ Gagal memuat ladder: %v\n", err)
		return
	}

	totalScore := 0
	maxScore := 0
	start := time.Now()

	for i, task := range ladder {
		fmt.Printf("\n[%d/4] Menguji Tier %s: %s (%d pts)...\n", i+1, task.Tier, task.Title, task.Points)
		res, err := evaluator.Evaluate(ctx, task, task.ReferenceSolution, 1)
		if err != nil {
			fmt.Printf("  ❌ Error evaluasi: %v\n", err)
			continue
		}

		status := "Gagal"
		if res.Resolved && !res.HasRace {
			status = "Lolos Turn 1"
		}
		fmt.Printf("  ↳ Hasil: %s (%d/%d pts, Durasi: %dms)\n", status, res.Points, task.Points, res.DurationMs)
		totalScore += res.Points
		maxScore += task.Points
	}

	grade, title := swe.CalculateGrade(totalScore)
	fmt.Println("\n=====================================================")
	fmt.Printf("🏅 TOTAL SKOR LADDER: %d / %d [Grade %s — %s]\n", totalScore, maxScore, grade, title)
	fmt.Printf("⏱️ Total Waktu: %v\n", time.Since(start).Round(10*time.Millisecond))
	fmt.Println("=====================================================")
}

func runSingleTaskFile(ctx context.Context, evaluator *swe.Evaluator, taskID, filePath string) {
	task, found := swe.GetTask(taskID)
	if !found {
		fmt.Printf("❌ Task dengan ID '%s' tidak ditemukan di registry.\n", taskID)
		return
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Printf("❌ Gagal membaca file %s: %v\n", filePath, err)
		return
	}

	fmt.Printf("Menguji task [%s] %s dengan file: %s\n", task.Tier, task.Title, filePath)
	res, err := evaluator.Evaluate(ctx, task, string(data), 1)
	if err != nil {
		fmt.Printf("❌ Terjadi kesalahan saat evaluasi: %v\n", err)
		return
	}

	fmt.Println("\n=====================================================")
	fmt.Println("📊 HASIL EVALUASI TASK")
	fmt.Println("=====================================================")
	fmt.Printf("Status      : %v\n", map[bool]string{true: "RESOLVED (Lolos)", false: "FAILED (Gagal)"}[res.Resolved])
	fmt.Printf("Skor        : %d / %d pts\n", res.Points, res.MaxPoints)
	fmt.Printf("Data Race   : %v\n", map[bool]string{true: "TERDETEKSI (0 pts)", false: "Bebas Data Race"}[res.HasRace])
	fmt.Printf("Durasi Uji  : %d ms\n", res.DurationMs)
	if res.CompileErr != "" {
		fmt.Printf("\nCompile / Syntax Error:\n%s\n", res.CompileErr)
	}
	fmt.Println("\nTest Output:")
	fmt.Println(res.TestOutput)
}

func runAIModelSWE(ctx context.Context, evaluator *swe.Evaluator, model, baseURL, apiKey string) {
	fmt.Printf("🤖 Menghubungkan ke model AI '%s' di %s...\n", model, baseURL)
	aiClient, err := ai.NewClient(ai.Config{
		BaseURL: baseURL,
		APIKey:  apiKey,
		Model:   model,
		Timeout: 3 * time.Minute,
	})
	if err != nil {
		fmt.Printf("❌ Gagal inisialisasi AI client: %v\n", err)
		return
	}

	ladder, err := swe.DefaultLadder()
	if err != nil {
		fmt.Printf("❌ Gagal memuat ladder: %v\n", err)
		return
	}

	totalScore := 0
	maxScore := 100
	totalPromptTokens := 0
	totalCompletionTokens := 0
	start := time.Now()

	var lastFixedCode string
	var maxTierAchieved swe.Tier
	var sweTaskRuns []storage.SWETaskRun

	for taskIdx, task := range ladder {
		step := taskIdx + 1
		fmt.Printf("\n[%d/4] Menguji Tier %s (%d pts): %s...\n", step, task.Tier, task.Points, task.Title)

		messages := []ai.ChatMessage{
			{Role: "system", Content: swe.SWESystemPrompt},
			{Role: "user", Content: swe.BuildTaskPrompt(task)},
		}

		var lastEvalRes *swe.EvalResult

		for attempt := 1; attempt <= 2; attempt++ {
			if attempt > 1 {
				fmt.Printf("  ↳ Turn %d: Mengirim feedback error/race ke AI untuk perbaikan mandiri...\n", attempt)
			} else {
				fmt.Printf("  ↳ Turn 1: Mengirim prompt perbaikan bug ke AI...\n")
			}

			chatRes, chatErr := aiClient.Chat(ctx, messages)
			if chatErr != nil {
				fmt.Printf("  ❌ Error inferensi AI: %v\n", chatErr)
				break
			}

			totalPromptTokens += chatRes.PromptTokens
			totalCompletionTokens += chatRes.CompletionTokens

			code, extractErr := swe.ExtractSWECode(chatRes.Content)
			if extractErr != nil {
				fmt.Printf("  ⚠️ Gagal mengekstrak kode Go: %v\n", extractErr)
				if attempt < 2 {
					messages = append(messages, ai.ChatMessage{Role: "assistant", Content: chatRes.Content})
					messages = append(messages, ai.ChatMessage{Role: "user", Content: "Please return the COMPLETE runnable Go code inside a single ```go ... ``` code block."})
					continue
				}
				break
			}

			lastFixedCode = code

			evalRes, evalErr := evaluator.Evaluate(ctx, task, code, attempt)
			if evalErr != nil {
				fmt.Printf("  ❌ Error evaluator: %v\n", evalErr)
				break
			}

			lastEvalRes = evalRes

			if evalRes.Resolved && !evalRes.HasRace {
				fmt.Printf("  ✅ LOLOS pada Turn %d! (%d/%d pts, Durasi: %dms, Bebas Data Race)\n",
					attempt, evalRes.Points, task.Points, evalRes.DurationMs)
				break
			}

			if evalRes.HasRace {
				fmt.Printf("  ⚠️ Gagal: Terdeteksi DATA RACE pada pengujian (-race).\n")
			} else {
				fmt.Printf("  ⚠️ Gagal: Verifikasi unit test tidak lolos.\n")
			}

			if attempt < 2 {
				failureMsg := evalRes.TestOutput
				if evalRes.CompileErr != "" {
					failureMsg = evalRes.CompileErr
				}
				messages = append(messages, ai.ChatMessage{Role: "assistant", Content: chatRes.Content})
				messages = append(messages, ai.ChatMessage{Role: "user", Content: swe.BuildSelfHealingPrompt(task, failureMsg)})
			}
		}

		if lastEvalRes == nil {
			lastEvalRes = &swe.EvalResult{
				TaskID:    task.ID,
				Tier:      task.Tier,
				MaxPoints: task.Points,
				Attempts:  0,
			}
		} else {
			totalScore += lastEvalRes.Points
			if lastEvalRes.Resolved && !lastEvalRes.HasRace {
				if swe.TierOrder(task.Tier) > swe.TierOrder(maxTierAchieved) {
					maxTierAchieved = task.Tier
				}
			}
		}

		sweTaskRuns = append(sweTaskRuns, storage.SWETaskRun{
			ID:            uuid.New().String(),
			TaskID:        task.ID,
			TaskTitle:     task.Title,
			Tier:          string(task.Tier),
			PointsAwarded: lastEvalRes.Points,
			MaxPoints:     task.Points,
			Resolved:      lastEvalRes.Resolved,
			HasRace:       lastEvalRes.HasRace,
			Attempts:      lastEvalRes.Attempts,
			TestOutput:    lastEvalRes.TestOutput,
		})

		if !lastEvalRes.Resolved || lastEvalRes.HasRace {
			fmt.Printf("\n❌ Tahap %s (%s) gagal diselesaikan. Menghentikan benchmark ladder: tahap berikutnya tidak akan diuji.\n", task.Tier, task.Title)
			for remainingIdx := taskIdx + 1; remainingIdx < len(ladder); remainingIdx++ {
				remTask := ladder[remainingIdx]
				sweTaskRuns = append(sweTaskRuns, storage.SWETaskRun{
					ID:            uuid.New().String(),
					TaskID:        remTask.ID,
					TaskTitle:     remTask.Title,
					Tier:          string(remTask.Tier),
					PointsAwarded: 0,
					MaxPoints:     remTask.Points,
					Resolved:      false,
					HasRace:       false,
					Attempts:      0,
					TestOutput:    fmt.Sprintf("Dibatalkan: Gagal pada Tier %s (%s)", task.Tier, task.Title),
				})
			}
			break
		}
	}

	grade, title := swe.CalculateGrade(totalScore)
	fmt.Println("\n=====================================================")
	fmt.Printf("🏅 HASIL EVALUASI SWE-BENCH: %s\n", model)
	fmt.Printf("🎯 Total Skor : %d / %d [Grade %s — %s]\n", totalScore, maxScore, grade, title)
	fmt.Printf("⏱️ Total Waktu: %v\n", time.Since(start).Round(100*time.Millisecond))
	fmt.Printf("🪙 Total Token: %d (in=%d, out=%d)\n", totalPromptTokens+totalCompletionTokens, totalPromptTokens, totalCompletionTokens)

	// Persist run results to Turso DB if configuration is available
	cfg, cfgErr := config.Load()
	if cfgErr == nil && cfg.TursoDatabaseURL != "" {
		db, dbErr := storage.NewTursoDB(cfg.TursoDatabaseURL, cfg.TursoAuthToken)
		if dbErr == nil {
			defer db.Close()
			repo := storage.NewRepository(db)
			_ = repo.Migrate(ctx)

			var codeHash string
			if lastFixedCode != "" {
				h := sha256.Sum256([]byte(lastFixedCode))
				codeHash = hex.EncodeToString(h[:])
			}

			costEst := pricing.CalculateCost(model, totalPromptTokens, totalCompletionTokens, totalScore)
			runStatus := "SUCCESS"
			if totalScore == 0 {
				runStatus = "FAILED"
			} else if totalScore < maxScore {
				runStatus = "PARTIAL_FAILED"
			}

			runID := uuid.New().String()
			run := &storage.BenchmarkRun{
				ID:               runID,
				TelegramID:       0,
				ProviderBaseURL:  baseURL,
				ModelName:        model,
				TotalScore:       totalScore,
				MaxScore:         maxScore,
				ExecutionTimeMs:  time.Since(start).Milliseconds(),
				PromptTokens:     totalPromptTokens,
				CompletionTokens: totalCompletionTokens,
				TotalTokens:      totalPromptTokens + totalCompletionTokens,
				EstimatedCostUSD: costEst.EstimatedCostUSD,
				CostTier:         costEst.Tier,
				CodeHash:         codeHash,
				CodeSnippet:      lastFixedCode,
				Status:           runStatus,
				BenchmarkMode:    "swe",
				MaxTierAchieved:  string(maxTierAchieved),
			}

			if err := repo.SaveSWERun(ctx, run, sweTaskRuns); err != nil {
				fmt.Printf("⚠️ Gagal menyimpan ke basis data: %v\n", err)
			} else {
				fmt.Printf("💾 Hasil benchmark berhasil disimpan ke database (Run ID: %s)\n", runID)
			}
		}
	}
	fmt.Println("=====================================================")
}

func printUsage() {
	fmt.Println(`Penggunaan On My Mark CLI:
  1. Jalankan simulasi 4-Tier Ladder lokal:
     go run ./cmd/cli -ladder

  2. Verifikasi F2P seluruh 8 task:
     go run ./cmd/cli -verify-all

  3. Uji solusi file tertentu terhadap task:
     go run ./cmd/cli -task=swe-t1-order-validator-01 -file=path/to/solution.go

  4. Evaluasi model AI langsung via API:
     go run ./cmd/cli -model=gpt-4o -base-url=https://openrouter.ai/api/v1 -api-key=sk-...`)
}
