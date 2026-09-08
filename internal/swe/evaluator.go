package swe

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"benchmark/internal/sandbox"
	"benchmark/pkg/logger"
)

// Evaluator runs unit tests and -race in an ephemeral sandbox
type Evaluator struct {
	StrictAST bool
	Timeout   time.Duration
}

// NewEvaluator initializes a new SWE-bench evaluator
func NewEvaluator(strictAST bool) *Evaluator {
	return &Evaluator{
		StrictAST: strictAST,
		Timeout:   30 * time.Second,
	}
}

// CalculatePoints calculates points earned based on attempt number and race status
func CalculatePoints(maxPoints int, attempt int, resolved bool, hasRace bool) int {
	if !resolved || hasRace {
		return 0
	}
	switch attempt {
	case 1:
		return maxPoints
	case 2:
		// -20% penalty (1 feedback received)
		return (maxPoints * 80) / 100
	default:
		return 0
	}
}

// Evaluate tests a Go code solution against the task test suite in an ephemeral sandbox
func (e *Evaluator) Evaluate(ctx context.Context, task *Task, solutionCode string, attempt int) (*EvalResult, error) {
	start := time.Now()

	logger.Debug("swe.eval_start", "task=%s tier=%s attempt=%d max_points=%d timeout=%v bytes=%d", task.ID, task.Tier, attempt, task.Points, e.Timeout, len(solutionCode))

	result := &EvalResult{
		TaskID:    task.ID,
		Tier:      task.Tier,
		MaxPoints: task.Points,
		Attempts:  attempt,
	}

	// 1. Static security audit via AST Guard (with strict network blocking for SWE AI solutions)
	inspection, err := sandbox.InspectAST(solutionCode, true)
	if err != nil {
		result.CompileErr = fmt.Sprintf("Gagal parse sintaks AST: %v", err)
		result.TestOutput = result.CompileErr
		result.DurationMs = time.Since(start).Milliseconds()
		logger.Debug("swe.ast_parse_fail", "task=%s err=%v", task.ID, err)
		return result, nil
	}

	if e.StrictAST && !inspection.Passed {
		result.CompileErr = inspection.Error()
		result.TestOutput = fmt.Sprintf("Pelanggaran keamanan terdeteksi (AST Guard):\n%s", inspection.Error())
		result.DurationMs = time.Since(start).Milliseconds()
		logger.Debug("swe.ast_check_fail", "task=%s violations=%d", task.ID, len(inspection.Violations))
		return result, nil
	}

	// 2. Prepare unique ephemeral directory
	tempDir, err := os.MkdirTemp("", "omm-swe-*")
	if err != nil {
		return nil, fmt.Errorf("gagal membuat direktori sandbox swe: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// 3. Write go.mod, implementation file (main.go), and test suite (main_test.go)
	goModContent := "module omm/swetask\n\ngo 1.24\n"
	if err := os.WriteFile(filepath.Join(tempDir, "go.mod"), []byte(goModContent), 0600); err != nil {
		return nil, fmt.Errorf("gagal menulis go.mod: %w", err)
	}

	if err := os.WriteFile(filepath.Join(tempDir, "main.go"), []byte(solutionCode), 0600); err != nil {
		return nil, fmt.Errorf("gagal menulis main.go di sandbox: %w", err)
	}

	if err := os.WriteFile(filepath.Join(tempDir, "main_test.go"), []byte(task.TestCode), 0600); err != nil {
		return nil, fmt.Errorf("gagal menulis main_test.go di sandbox: %w", err)
	}

	logger.Debug("swe.sandbox_ready", "task=%s dir=%s", task.ID, filepath.Base(tempDir))

	// 4. Execute: go test -race -v -count=1 -timeout=30s ./...
	evalTimeout := e.Timeout
	if evalTimeout <= 0 {
		evalTimeout = 30 * time.Second
	}

	cmdCtx, cancel := context.WithTimeout(ctx, evalTimeout)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, "go", "test", "-race", "-v", "-count=1", fmt.Sprintf("-timeout=%ds", int(evalTimeout.Seconds())), "./...")
	cmd.Dir = tempDir

	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf

	runErr := cmd.Run()
	output := outBuf.String()
	result.TestOutput = output
	result.DurationMs = time.Since(start).Milliseconds()

	// 5. Output analysis
	hasRace := strings.Contains(output, "WARNING: DATA RACE")
	result.HasRace = hasRace

	isBuildFailure := strings.Contains(output, "[build failed]") ||
		strings.Contains(output, "syntax error") ||
		strings.Contains(output, "undefined:") ||
		strings.Contains(output, "cannot use") ||
		strings.Contains(output, "imported and not used")

	if isBuildFailure {
		result.CompileErr = extractCompileError(output)
	}

	// Check pass status
	if runErr == nil && !hasRace && !isBuildFailure && (strings.Contains(output, "PASS\n") || strings.Contains(output, "\nPASS") || strings.HasSuffix(strings.TrimSpace(output), "PASS")) {
		result.Resolved = true
		result.Points = CalculatePoints(task.Points, attempt, true, false)
	} else {
		result.Resolved = false
		result.Points = 0
	}

	logger.Debug("swe.eval_result", "task=%s resolved=%t has_race=%t build_fail=%t points=%d/%d duration_ms=%d", task.ID, result.Resolved, hasRace, isBuildFailure, result.Points, task.Points, result.DurationMs)

	return result, nil
}

// ValidateTask verifies the Fail-to-Pass (F2P) principle:
// 1. BrokenCode + TestCode MUST FAIL.
// 2. ReferenceSolution + TestCode MUST PASS without data races.
func (e *Evaluator) ValidateTask(ctx context.Context, task *Task, refSolution string) error {
	logger.Debug("swe.validate_f2p", "task=%s tier=%s", task.ID, task.Tier)

	// Test broken code
	brokenRes, err := e.Evaluate(ctx, task, task.BrokenCode, 1)
	if err != nil {
		return fmt.Errorf("evaluasi broken code gagal: %w", err)
	}
	if brokenRes.Resolved && !brokenRes.HasRace {
		return fmt.Errorf("task %s invalid: broken code lolos verifikasi (diharapkan gagal sesuai prinsip F2P)", task.ID)
	}

	// Test reference solution if present
	if strings.TrimSpace(refSolution) != "" {
		refRes, err := e.Evaluate(ctx, task, refSolution, 1)
		if err != nil {
			return fmt.Errorf("evaluasi solusi referensi gagal: %w", err)
		}
		if !refRes.Resolved || refRes.HasRace {
			return fmt.Errorf("task %s reference solution gagal diverifikasi:\n%s", task.ID, refRes.TestOutput)
		}
	}

	return nil
}

func extractCompileError(output string) string {
	lines := strings.Split(output, "\n")
	var errLines []string
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		if strings.HasPrefix(trimmed, "./") ||
			strings.Contains(trimmed, ": syntax error") ||
			strings.Contains(trimmed, "undefined:") ||
			strings.Contains(trimmed, "cannot use") ||
			strings.Contains(trimmed, "imported and not used") ||
			strings.HasPrefix(trimmed, "# ") {
			errLines = append(errLines, trimmed)
		}
	}
	if len(errLines) > 0 {
		return strings.Join(errLines, "\n")
	}
	return output
}
