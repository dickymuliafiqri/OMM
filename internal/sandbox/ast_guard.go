package sandbox

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
)

// Violation stores details of security violations detected in the code AST
type Violation struct {
	Type        string `json:"type"`        // Example: "FORBIDDEN_IMPORT", "DANGEROUS_CALL"
	Target      string `json:"target"`      // Violating package or function name
	Line        int    `json:"line"`        // Line number
	Description string `json:"description"` // Security risk description
}

// SecurityInspectionResult stores overall AST inspection results
type SecurityInspectionResult struct {
	Passed     bool        `json:"passed"`
	Violations []Violation `json:"violations"`
}

// Error implements the error interface when violations are present
func (r *SecurityInspectionResult) Error() string {
	if r.Passed {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("pelanggaran keamanan kode terdeteksi:\n")
	for _, v := range r.Violations {
		sb.WriteString(fmt.Sprintf("- [L%d] %s: %s (%s)\n", v.Line, v.Type, v.Target, v.Description))
	}
	return sb.String()
}

// forbiddenImports lists prohibited imports that could lead to RCE or host system damage
var forbiddenImports = map[string]string{
	"os/exec":        "Dilarang: Eksekusi command eksternal / shell berisiko RCE",
	"syscall":        "Dilarang: Pemanggilan syscall langsung berisiko bypass sandbox OS",
	"unsafe":         "Dilarang: Manipulasi memori langsung / type evasion",
	"plugin":         "Dilarang: Dynamic loading library eksternal .so/.dll",
	"runtime/cgo":    "Dilarang: Eksekusi kode native C / Cgo",
	"C":              "Dilarang: Eksekusi kode native C / Cgo",
	"golang.org/x/sys": "Dilarang: Low-level OS syscall manipulation",
}

// InspectAST inspects the Go syntax and abstract syntax tree (AST).
// It validates the absence of forbidden packages and high-risk function calls.
func InspectAST(sourceCode string) (*SecurityInspectionResult, error) {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, "server.go", sourceCode, parser.AllErrors)
	if err != nil {
		return nil, fmt.Errorf("sintaks Go tidak valid (gagal parse AST): %w", err)
	}

	result := &SecurityInspectionResult{
		Passed:     true,
		Violations: make([]Violation, 0),
	}

	// 1. Check Package Imports
	for _, imp := range node.Imports {
		importPath := strings.Trim(imp.Path.Value, `"`)
		for forbidden, reason := range forbiddenImports {
			if importPath == forbidden || strings.HasPrefix(importPath, forbidden+"/") {
				pos := fset.Position(imp.Pos())
				result.Passed = false
				result.Violations = append(result.Violations, Violation{
					Type:        "FORBIDDEN_IMPORT",
					Target:      importPath,
					Line:        pos.Line,
					Description: reason,
				})
			}
		}
	}

	// 2. Traverse AST nodes for sensitive function calls
	ast.Inspect(node, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		// Check selector call, e.g. os.RemoveAll("/")
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
			if ident, ok := sel.X.(*ast.Ident); ok {
				pkgName := ident.Name
				funcName := sel.Sel.Name

				// Disallow os.RemoveAll or os.Remove on root/parent directory
				if pkgName == "os" && (funcName == "RemoveAll" || funcName == "Remove") {
					pos := fset.Position(call.Pos())
					result.Passed = false
					result.Violations = append(result.Violations, Violation{
						Type:        "DANGEROUS_CALL",
						Target:      fmt.Sprintf("%s.%s", pkgName, funcName),
						Line:        pos.Line,
						Description: "Dilarang: Operasi penghapusan file di luar direktori kerja sementara",
					})
				}
			}
		}

		return true
	})

	return result, nil
}

// ValidateSource is a concise helper that returns an error if security violations exist
func ValidateSource(sourceCode string) error {
	res, err := InspectAST(sourceCode)
	if err != nil {
		return err
	}
	if !res.Passed {
		return res
	}
	return nil
}
