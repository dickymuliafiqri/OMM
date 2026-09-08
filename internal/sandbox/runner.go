package sandbox

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"benchmark/pkg/logger"
)

// RunEnvironment represents an isolated sandbox execution environment for a single evaluation
type RunEnvironment struct {
	TempDir    string
	SourceFile string
	BinaryPath string
	Port       int
	TargetURL  string
}

// Cleanup removes all temporary sandbox files and directories
func (env *RunEnvironment) Cleanup() error {
	if env.TempDir != "" && strings.Contains(env.TempDir, "omm-sandbox-") {
		return os.RemoveAll(env.TempDir)
	}
	return nil
}

// Runner manages the sandbox lifecycle: AST scan, directory isolation, dynamic ports, and execution
type Runner struct {
	StrictMode bool // If true, AST violations immediately fail the evaluation
}

// NewRunner creates a new Runner instance
func NewRunner(strictMode bool) *Runner {
	return &Runner{
		StrictMode: strictMode,
	}
}

// Prepare sets up an ephemeral sandbox environment and Go code ready for testing
func (r *Runner) Prepare(sourceCode string, assignedPort int) (*RunEnvironment, error) {
	// 1. Static Security Audit via AST Guard
	inspection, err := InspectAST(sourceCode)
	if err != nil {
		return nil, fmt.Errorf("audit AST gagal: %w", err)
	}
	if r.StrictMode && !inspection.Passed {
		return nil, fmt.Errorf("keamanan kode ditolak oleh AST Guard:\n%s", inspection.Error())
	}

	// 2. Allocate dynamic port if not specified
	port := assignedPort
	if port <= 0 {
		freePort, portErr := GetFreePort()
		if portErr != nil {
			return nil, fmt.Errorf("gagal mengalokasikan port bebas: %w", portErr)
		}
		port = freePort
	}

	// 3. Create a unique ephemeral temporary directory
	tempDir, err := os.MkdirTemp("", "omm-sandbox-*")
	if err != nil {
		return nil, fmt.Errorf("gagal membuat direktori sandbox sementara: %w", err)
	}

	// 4. Port Adaptation: If the AI hardcoded :8080, adjust it to the allocated dynamic port
	adaptedCode := adaptPort(sourceCode, port)

	// 5. Write source code (main.go) and ephemeral go.mod inside the sandbox
	sourcePath := filepath.Join(tempDir, "main.go")
	if err := os.WriteFile(sourcePath, []byte(adaptedCode), 0600); err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, fmt.Errorf("gagal menulis source code di sandbox: %w", err)
	}

	goModContent := "module sandbox_run\n\ngo 1.24\n"
	goModPath := filepath.Join(tempDir, "go.mod")
	if err := os.WriteFile(goModPath, []byte(goModContent), 0600); err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, fmt.Errorf("gagal menulis go.mod di sandbox: %w", err)
	}

	binaryName := "server_bin"
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	binaryPath := filepath.Join(tempDir, binaryName)

	logger.Debug("sandbox.prepare", "assigned_port=%d port=%d dir=%s", assignedPort, port, filepath.Base(tempDir))

	return &RunEnvironment{
		TempDir:    tempDir,
		SourceFile: sourcePath,
		BinaryPath: binaryPath,
		Port:       port,
		TargetURL:  fmt.Sprintf("http://127.0.0.1:%d", port),
	}, nil
}

// VerifyCompile quickly tests whether Go code passes AST audit and compiles successfully (go build -race).
// It returns passed (bool), compiler error message (string), and internal system error (error).
func (r *Runner) VerifyCompile(sourceCode string) (bool, string, error) {
	// 1. Static Security Audit via AST Guard
	inspection, err := InspectAST(sourceCode)
	if err != nil {
		return false, fmt.Sprintf("Audit AST gagal: %v", err), nil
	}
	if !inspection.Passed {
		return false, fmt.Sprintf("AST Security Guard Flagged:\n%s", inspection.Error()), nil
	}

	// 2. Create a unique ephemeral temporary directory for compilation check
	tempDir, err := os.MkdirTemp("", "omm-compile-check-*")
	if err != nil {
		return false, "", fmt.Errorf("gagal membuat direktori compile check: %w", err)
	}
	defer os.RemoveAll(tempDir)

	sourcePath := filepath.Join(tempDir, "main.go")
	if err := os.WriteFile(sourcePath, []byte(sourceCode), 0600); err != nil {
		return false, "", fmt.Errorf("gagal menulis source compile check: %w", err)
	}

	goModContent := "module compile_check\n\ngo 1.24\n"
	goModPath := filepath.Join(tempDir, "go.mod")
	if err := os.WriteFile(goModPath, []byte(goModContent), 0600); err != nil {
		return false, "", fmt.Errorf("gagal menulis go.mod compile check: %w", err)
	}

	binOutput := filepath.Join(tempDir, "test_check_bin")
	if runtime.GOOS == "windows" {
		binOutput += ".exe"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()

	logger.Debug("sandbox.compile_start", "dir=%s code_len=%d", filepath.Base(tempDir), len(sourceCode))

	cmd := exec.CommandContext(ctx, "go", "build", "-race", "-o", binOutput, sourcePath)
	cmd.Dir = tempDir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errMsg := strings.TrimSpace(stderr.String())
		if errMsg == "" {
			errMsg = err.Error()
		}
		logger.Debug("sandbox.compile_done", "passed=false err=%s", errMsg)
		return false, errMsg, nil
	}

	logger.Debug("sandbox.compile_done", "passed=true")
	return true, "", nil
}

// commonPorts lists common ports frequently hardcoded by AI models
var commonPorts = []string{"8080", "8000", "3000", "5000", "9090", "8888", "4000", "8081", "80"}

// adaptPort replaces hardcoded port references with the allocated dynamic port.
// It supports various common ports frequently used by AI models, not just :8080.
func adaptPort(code string, port int) string {
	newPortStr := fmt.Sprintf(":%d", port)

	for _, commonPort := range commonPorts {
		// Pattern 1: ":PORT" (listen address format)
		reAddr := regexp.MustCompile(fmt.Sprintf(`":%s"`, commonPort))
		code = reAddr.ReplaceAllString(code, fmt.Sprintf(`"%s"`, newPortStr))

		// Pattern 2: "PORT" (port number string format only)
		reNum := regexp.MustCompile(fmt.Sprintf(`"%s"`, commonPort))
		code = reNum.ReplaceAllString(code, fmt.Sprintf(`"%d"`, port))
	}

	return code
}
