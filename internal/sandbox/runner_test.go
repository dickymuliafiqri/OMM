package sandbox

import (
	"os"
	"strings"
	"testing"
)

func TestAdaptPort(t *testing.T) {
	code := `
package main
import "net/http"
func main() {
	http.ListenAndServe(":8080", nil)
}
`
	adapted := adaptPort(code, 19283)
	if !strings.Contains(adapted, `":19283"`) {
		t.Errorf("adaptPort gagal mengganti :8080 menjadi :19283, hasil:\n%s", adapted)
	}
}

func TestRunner_PrepareAndCleanup(t *testing.T) {
	runner := NewRunner(true)
	safeCode := `
package main
import "net/http"
func main() {
	http.ListenAndServe(":8080", nil)
}
`
	env, err := runner.Prepare(safeCode, 18456)
	if err != nil {
		t.Fatalf("runner.Prepare gagal: %v", err)
	}

	if env.Port != 18456 {
		t.Errorf("Ekspektasi port 18456, dapat: %d", env.Port)
	}
	if env.TargetURL != "http://127.0.0.1:18456" {
		t.Errorf("TargetURL tidak sesuai: %s", env.TargetURL)
	}

	// Ensure main.go and go.mod exist in tempDir
	if _, err := os.Stat(env.SourceFile); os.IsNotExist(err) {
		t.Errorf("SourceFile tidak dibuat: %s", env.SourceFile)
	}

	content, err := os.ReadFile(env.SourceFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), ":18456") {
		t.Errorf("Source file tidak memuat port dinamis teradaptasi: %s", string(content))
	}

	// Test Cleanup
	tempDir := env.TempDir
	if err := env.Cleanup(); err != nil {
		t.Fatalf("Cleanup gagal: %v", err)
	}

	if _, err := os.Stat(tempDir); !os.IsNotExist(err) {
		t.Errorf("TempDir masih ada setelah Cleanup(): %s", tempDir)
	}
}

func TestRunner_RejectMaliciousCode(t *testing.T) {
	runner := NewRunner(true)
	maliciousCode := `
package main
import "os/exec"
func main() {
	exec.Command("calc.exe").Run()
}
`
	_, err := runner.Prepare(maliciousCode, 18456)
	if err == nil {
		t.Errorf("Runner dengan StrictMode seharusnya menolak kode yang mengimpor os/exec")
	}
	if !strings.Contains(err.Error(), "AST Guard") {
		t.Errorf("Pesan error harus menyebutkan AST Guard: %v", err)
	}
}

func TestRunner_VerifyCompile(t *testing.T) {
	runner := NewRunner(true)

	// 1. Valid code
	validCode := `
package main
import "fmt"
func main() {
	fmt.Println("hello world")
}
`
	passed, stderr, err := runner.VerifyCompile(validCode)
	if err != nil {
		t.Fatalf("VerifyCompile internal error: %v", err)
	}
	if !passed {
		t.Errorf("Kode valid harusnya passed=true, dapat passed=false (stderr: %s)", stderr)
	}

	// 2. Syntax/compiler error
	syntaxErrCode := `
package main
func main() {
	var a int = "bukan int"
}
`
	passed, stderr, err = runner.VerifyCompile(syntaxErrCode)
	if err != nil {
		t.Fatalf("VerifyCompile internal error: %v", err)
	}
	if passed {
		t.Errorf("Kode dengan error tipe harusnya passed=false")
	}
	if !strings.Contains(stderr, "cannot use") && !strings.Contains(stderr, "cannot initialize") {
		t.Errorf("Stderr harus memuat pesan error kompilasi: %s", stderr)
	}

	// 3. Security violation (AST rejected)
	maliciousCode := `
package main
import "os/exec"
func main() {
	exec.Command("ls").Run()
}
`
	passed, stderr, err = runner.VerifyCompile(maliciousCode)
	if err != nil {
		t.Fatalf("VerifyCompile internal error: %v", err)
	}
	if passed {
		t.Errorf("Kode berbahaya harusnya ditolak oleh AST")
	}
	if !strings.Contains(stderr, "AST Security Guard") {
		t.Errorf("Stderr harus memuat indikasi AST Security Guard: %s", stderr)
	}
}

