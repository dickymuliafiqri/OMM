package swe

import (
	"fmt"
	"strings"
)

// Tier defines software engineering difficulty levels
type Tier string

const (
	TierLow   Tier = "LOW"   // 20 Pts
	TierMid   Tier = "MID"   // 25 Pts
	TierHigh  Tier = "HIGH"  // 30 Pts
	TierUltra Tier = "ULTRA" // 25 Pts

	// Backward compatibility aliases
	TierJunior Tier = TierLow
	TierSenior Tier = TierHigh
	TierStaff  Tier = TierUltra
)

// Task defines a single unit of code fixing problem (SWE-bench instance)
type Task struct {
	ID                string `json:"id"`                 // Example: "swe-t3-deadlock-01"
	Title             string `json:"title"`              // Example: "Deadlock on Concurrent Balance Transfer"
	Tier              Tier   `json:"tier"`               // Junior, Mid, Senior, Staff
	Points            int    `json:"points"`             // Points if task is fully resolved
	Category          string `json:"category"`           // "concurrency", "memory", "deadlock", "atomic"
	IssueBody         string `json:"issue_body"`         // Bug report markdown in GitHub issue style
	BrokenCode        string `json:"broken_code"`        // Initial Go code containing logic flaws/bugs
	TestCode          string `json:"test_code"`          // *_test.go file validating the fix
	TotalTests        int    `json:"total_tests"`        // Number of Test functions in TestCode
	ReferenceSolution string `json:"reference_solution"` // Reference solution code for F2P verification
}

// EvalResult stores task testing status
type EvalResult struct {
	TaskID     string `json:"task_id"`
	Tier       Tier   `json:"tier"`
	Points     int    `json:"points"`
	MaxPoints  int    `json:"max_points"`
	Resolved   bool   `json:"resolved"`
	HasRace    bool   `json:"has_race"`
	CompileErr string `json:"compile_err,omitempty"`
	TestOutput string `json:"test_output"`
	DurationMs int64  `json:"duration_ms"`
	Attempts   int    `json:"attempts"`
}

// LadderReport summarizes evaluation results across all ladder tiers in a single SWE-bench session
type LadderReport struct {
	TotalScore      int           `json:"total_score"`
	MaxScore        int           `json:"max_score"`
	Grade           string        `json:"grade"`
	MaxTierAchieved Tier          `json:"max_tier_achieved"`
	TaskResults     []*EvalResult `json:"task_results"`
	TotalDurationMs int64         `json:"total_duration_ms"`
}

// CalculateGrade determines engineering title based on total score
func CalculateGrade(score int) (grade string, title string) {
	switch {
	case score >= 90:
		return "S", "Staff Engineer"
	case score >= 75:
		return "A", "Senior Engineer"
	case score >= 50:
		return "B", "Mid-level Engineer"
	case score >= 20:
		return "C", "Junior Engineer"
	default:
		return "F", "Untrained / Hallucinating"
	}
}

// NormalizeTier converts any legacy tier string (JUNIOR, SENIOR, STAFF) into canonical LOW, MID, HIGH, ULTRA.
func NormalizeTier(t string) string {
	switch strings.ToUpper(strings.TrimSpace(t)) {
	case "LOW", "JUNIOR":
		return "LOW"
	case "MID":
		return "MID"
	case "HIGH", "SENIOR":
		return "HIGH"
	case "ULTRA", "STAFF":
		return "ULTRA"
	default:
		return strings.ToUpper(strings.TrimSpace(t))
	}
}

// TierOrder returns the numeric order of a tier for comparison
func TierOrder(t Tier) int {
	switch NormalizeTier(string(t)) {
	case "LOW":
		return 1
	case "MID":
		return 2
	case "HIGH":
		return 3
	case "ULTRA":
		return 4
	default:
		return 0
	}
}

// FormatGradeDescription returns a brief description text for a grade
func FormatGradeDescription(grade string) string {
	switch grade {
	case "S":
		return "AI menunjukkan pemahaman arsitektur sistem tingkat tinggi dan mampu menyelesaikan seluruh tantangan bug-fixing konkurensi."
	case "A":
		return "AI menguasai pola konkurensi lanjutan dan mampu mengisolasi masalah deadlock multi-domain secara presisi."
	case "B":
		return "AI memahami pengelolaan resource dan lifecycle goroutine, namun masih kesulitan pada arsitektur sinkronisasi kompleks."
	case "C":
		return "AI hanya mampu memperbaiki bug logika dasar; penalaran konkurensi dan pencegahan race condition belum memadai."
	default:
		return "AI gagal menyelesaikan perbaikan dasar atau kode yang dihasilkan memicu fatal data race / error kompilasi."
	}
}

// String returns the string representation of Task
func (t *Task) String() string {
	return fmt.Sprintf("[%s] %s (%s - %d pts)", t.Tier, t.Title, t.ID, t.Points)
}
