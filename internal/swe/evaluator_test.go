package swe

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCalculatePoints(t *testing.T) {
	tests := []struct {
		name      string
		maxPoints int
		attempt   int
		resolved  bool
		hasRace   bool
		expected  int
	}{
		{"Turn 1 Junior Success", 20, 1, true, false, 20},
		{"Turn 2 Junior Success (-20%)", 20, 2, true, false, 16},
		{"Turn 3 Junior Fail (>2 attempts = 0)", 20, 3, true, false, 0},
		{"Turn 4 Junior Fail (>2 attempts = 0)", 20, 4, true, false, 0},
		{"Turn 1 Senior Success", 30, 1, true, false, 30},
		{"Turn 2 Senior Success (-20%)", 30, 2, true, false, 24},
		{"Turn 3 Senior Fail (>2 attempts = 0)", 30, 3, true, false, 0},
		{"Resolved but Has Data Race (0 pts)", 30, 1, true, true, 0},
		{"Not Resolved (0 pts)", 30, 1, false, false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculatePoints(tt.maxPoints, tt.attempt, tt.resolved, tt.hasRace)
			if got != tt.expected {
				t.Errorf("CalculatePoints(%d, %d, %v, %v) = %d, expected %d",
					tt.maxPoints, tt.attempt, tt.resolved, tt.hasRace, got, tt.expected)
			}
		})
	}
}

func TestCalculateGrade(t *testing.T) {
	tests := []struct {
		score         int
		expectedGrade string
		expectedTitle string
	}{
		{100, "S", "Staff Engineer"},
		{90, "S", "Staff Engineer"},
		{89, "A", "Senior Engineer"},
		{75, "A", "Senior Engineer"},
		{74, "B", "Mid-level Engineer"},
		{50, "B", "Mid-level Engineer"},
		{49, "C", "Junior Engineer"},
		{20, "C", "Junior Engineer"},
		{19, "F", "Untrained / Hallucinating"},
		{0, "F", "Untrained / Hallucinating"},
	}

	for _, tt := range tests {
		g, title := CalculateGrade(tt.score)
		if g != tt.expectedGrade || title != tt.expectedTitle {
			t.Errorf("CalculateGrade(%d) = (%s, %s), expected (%s, %s)",
				tt.score, g, title, tt.expectedGrade, tt.expectedTitle)
		}
	}
}

func TestExtractSWECode(t *testing.T) {
	t.Run("Markdown Go Fence", func(t *testing.T) {
		raw := "Here is the fix:\n```go\npackage main\n\nfunc Add(a, b int) int { return a + b }\n```\nDone."
		code, err := ExtractSWECode(raw)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(code, "func Add") || !strings.HasPrefix(code, "package main") {
			t.Errorf("unexpected extracted code: %s", code)
		}
	})

	t.Run("No Markdown Fence Fallback", func(t *testing.T) {
		raw := "package main\n\nfunc Add(a, b int) int { return a + b }"
		code, err := ExtractSWECode(raw)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.HasPrefix(code, "package main") {
			t.Errorf("unexpected code: %s", code)
		}
	})

	t.Run("Empty Response Error", func(t *testing.T) {
		_, err := ExtractSWECode("   ")
		if err == nil {
			t.Errorf("expected error for empty response")
		}
	})
}

func TestEvaluator_Evaluate(t *testing.T) {
	evaluator := NewEvaluator(true)
	evaluator.Timeout = 15 * time.Second
	ctx := context.Background()

	testTask := &Task{
		ID:         "test-task-add",
		Title:      "Simple Add Function",
		Tier:       TierJunior,
		Points:     20,
		Category:   "math",
		IssueBody:  "Add returns wrong value",
		BrokenCode: "package main\n\nfunc Add(a, b int) int { return a - b }\n",
		TestCode: `package main

import "testing"

func TestAdd(t *testing.T) {
	if Add(2, 3) != 5 {
		t.Fatalf("expected 5")
	}
}
`,
		TotalTests:        1,
		ReferenceSolution: "package main\n\nfunc Add(a, b int) int { return a + b }\n",
	}

	t.Run("Passing Solution", func(t *testing.T) {
		res, err := evaluator.Evaluate(ctx, testTask, testTask.ReferenceSolution, 1)
		if err != nil {
			t.Fatalf("evaluation error: %v", err)
		}
		if !res.Resolved {
			t.Fatalf("expected resolved, got output: %s", res.TestOutput)
		}
		if res.Points != 20 {
			t.Errorf("expected 20 points, got %d", res.Points)
		}
		if res.HasRace {
			t.Errorf("unexpected data race")
		}
	})

	t.Run("Failing Broken Solution", func(t *testing.T) {
		res, err := evaluator.Evaluate(ctx, testTask, testTask.BrokenCode, 1)
		if err != nil {
			t.Fatalf("evaluation error: %v", err)
		}
		if res.Resolved {
			t.Errorf("expected broken solution to fail")
		}
		if res.Points != 0 {
			t.Errorf("expected 0 points, got %d", res.Points)
		}
	})

	t.Run("Compilation Error", func(t *testing.T) {
		badSyntaxCode := "package main\n\nfunc Add(a, b int) int { return invalid_syntax }"
		res, err := evaluator.Evaluate(ctx, testTask, badSyntaxCode, 1)
		if err != nil {
			t.Fatalf("evaluation error: %v", err)
		}
		if res.Resolved {
			t.Errorf("expected compile error to fail")
		}
		if res.CompileErr == "" {
			t.Errorf("expected CompileErr to be populated")
		}
	})

	t.Run("AST Violation Rejection", func(t *testing.T) {
		unsafeCode := "package main\n\nimport \"os/exec\"\n\nfunc Add(a, b int) int { _ = exec.Command(\"ls\"); return a + b }"
		res, err := evaluator.Evaluate(ctx, testTask, unsafeCode, 1)
		if err != nil {
			t.Fatalf("evaluation error: %v", err)
		}
		if res.Resolved {
			t.Errorf("expected AST guard to block forbidden package")
		}
		if !strings.Contains(res.TestOutput, "AST Guard") {
			t.Errorf("expected AST Guard violation message, got: %s", res.TestOutput)
		}
	})

	t.Run("F2P ValidateTask", func(t *testing.T) {
		err := evaluator.ValidateTask(ctx, testTask, testTask.ReferenceSolution)
		if err != nil {
			t.Fatalf("ValidateTask failed: %v", err)
		}
	})
}
