package sandbox

import (
	"strings"
	"testing"
)

func TestInspectAST_SafeCode(t *testing.T) {
	safeCode := `
package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/visit", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Visited"))
	})
	server := &http.Server{Addr: ":8080", Handler: mux}
	_ = server.ListenAndServe()
}
`
	result, err := InspectAST(safeCode)
	if err != nil {
		t.Fatalf("InspectAST gagal pada kode aman: %v", err)
	}

	if !result.Passed {
		t.Errorf("Kode aman seharusnya lulus verifikasi, namun didapatkan pelanggaran: %v", result.Violations)
	}
	if len(result.Violations) != 0 {
		t.Errorf("Jumlah pelanggaran seharusnya 0, dapat: %d", len(result.Violations))
	}
}

func TestInspectAST_ForbiddenImports(t *testing.T) {
	testCases := []struct {
		name       string
		code       string
		expectPkg  string
		expectPass bool
	}{
		{
			name: "Import os/exec for RCE",
			code: `
package main
import (
	"os/exec"
	"net/http"
)
func main() {
	exec.Command("sh", "-c", "rm -rf /")
}
`,
			expectPkg:  "os/exec",
			expectPass: false,
		},
		{
			name: "Import syscall",
			code: `
package main
import (
	"syscall"
)
func main() {
	_ = syscall.Getpid()
}
`,
			expectPkg:  "syscall",
			expectPass: false,
		},
		{
			name: "Import unsafe",
			code: `
package main
import (
	"unsafe"
)
func main() {
	var x int
	_ = unsafe.Pointer(&x)
}
`,
			expectPkg:  "unsafe",
			expectPass: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := InspectAST(tc.code)
			if err != nil {
				t.Fatalf("InspectAST error: %v", err)
			}
			if result.Passed != tc.expectPass {
				t.Errorf("Ekspektasi Passed=%v, didapatkan: %v", tc.expectPass, result.Passed)
			}

			found := false
			for _, v := range result.Violations {
				if v.Target == tc.expectPkg {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Pelanggaran import %s tidak terdeteksi dalam violations: %v", tc.expectPkg, result.Violations)
			}
		})
	}
}

func TestInspectAST_DangerousCalls(t *testing.T) {
	dangerousCode := `
package main

import (
	"os"
)

func main() {
	_ = os.RemoveAll("/var/data")
}
`
	result, err := InspectAST(dangerousCode)
	if err != nil {
		t.Fatalf("InspectAST error: %v", err)
	}

	if result.Passed {
		t.Errorf("Kode dengan os.RemoveAll seharusnya ditolak")
	}

	foundCall := false
	for _, v := range result.Violations {
		if v.Type == "DANGEROUS_CALL" && strings.Contains(v.Target, "RemoveAll") {
			foundCall = true
			break
		}
	}
	if !foundCall {
		t.Errorf("Pemanggilan os.RemoveAll tidak terdeteksi: %v", result.Violations)
	}
}

func TestInspectAST_SyntaxError(t *testing.T) {
	brokenCode := `package main func broken syntax {`
	_, err := InspectAST(brokenCode)
	if err == nil {
		t.Errorf("Kode rusak sintaks seharusnya menghasilkan error parsing AST")
	}
}
