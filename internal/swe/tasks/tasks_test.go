package tasks

import (
	"context"
	"testing"
	"time"

	"benchmark/internal/swe"
)

func TestAllTasks_F2PValidation(t *testing.T) {
	evaluator := swe.NewEvaluator(true)
	evaluator.Timeout = 30 * time.Second

	allTasks := AllTasksList()
	if len(allTasks) < 8 {
		t.Fatalf("expected at least 8 tasks, got %d", len(allTasks))
	}

	for _, task := range allTasks {
		task := task
		t.Run(task.ID, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()

			// 1. Verify that initial code MUST FAIL (BrokenCode)
			brokenRes, err := evaluator.Evaluate(ctx, task, task.BrokenCode, 1)
			if err != nil {
				t.Fatalf("[%s] Evaluasi BrokenCode gagal dijalankan: %v", task.ID, err)
			}
			if brokenRes.Resolved && !brokenRes.HasRace {
				t.Fatalf("[%s] Pelanggaran F2P: BrokenCode lolos verifikasi tanpa error/race!", task.ID)
			}

			// 2. Verify that reference solution MUST PASS 100% without data races (ReferenceSolution)
			refRes, err := evaluator.Evaluate(ctx, task, task.ReferenceSolution, 1)
			if err != nil {
				t.Fatalf("[%s] Evaluasi ReferenceSolution gagal dijalankan: %v", task.ID, err)
			}
			if !refRes.Resolved {
				t.Fatalf("[%s] ReferenceSolution gagal menyelesaikan task:\n%s", task.ID, refRes.TestOutput)
			}
			if refRes.HasRace {
				t.Fatalf("[%s] ReferenceSolution memicu DATA RACE:\n%s", task.ID, refRes.TestOutput)
			}
			if refRes.Points != task.Points {
				t.Fatalf("[%s] Expected %d points, got %d", task.ID, task.Points, refRes.Points)
			}
		})
	}
}

func TestRegistry_DefaultLadder(t *testing.T) {
	ladder, err := swe.DefaultLadder()
	if err != nil {
		t.Fatalf("DefaultLadder error: %v", err)
	}

	if len(ladder) != 4 {
		t.Fatalf("expected 4 tasks in default ladder, got %d", len(ladder))
	}

	expectedTiers := []swe.Tier{swe.TierJunior, swe.TierMid, swe.TierSenior, swe.TierStaff}
	totalPoints := 0
	for i, task := range ladder {
		if task.Tier != expectedTiers[i] {
			t.Errorf("ladder[%d] tier mismatch: got %s, expected %s", i, task.Tier, expectedTiers[i])
		}
		totalPoints += task.Points
	}

	if totalPoints != 100 {
		t.Fatalf("expected 100 total ladder points, got %d", totalPoints)
	}
}

func TestAllTasks_BrokenCodeLineCount(t *testing.T) {
	minLines := map[string]int{
		TaskT1OrderValidator.ID:    200,
		TaskT1InventoryCache.ID:    200,
		TaskT2RateLimiter.ID:       1000,
		TaskT2WorkerPool.ID:        1000,
		TaskT3DistributedCache.ID:  1500,
		TaskT3ConnectionPool.ID:    1500,
		TaskT4SagaOrchestrator.ID:  2000,
		TaskT4MPMCRing.ID:          2000,
	}

	for _, task := range AllTasksList() {
		expectedMin, ok := minLines[task.ID]
		if !ok {
			t.Errorf("[%s] no minLines defined", task.ID)
			continue
		}

		lines := 0
		for _, b := range task.BrokenCode {
			if b == '\n' {
				lines++
			}
		}
		if len(task.BrokenCode) > 0 && task.BrokenCode[len(task.BrokenCode)-1] != '\n' {
			lines++
		}

		t.Logf("[%s] BrokenCode line count: %d (expected >= %d)", task.ID, lines, expectedMin)
		if lines < expectedMin {
			t.Errorf("[%s] BrokenCode has %d lines, expected >= %d", task.ID, lines, expectedMin)
		}
	}
}
