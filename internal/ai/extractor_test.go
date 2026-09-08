package ai

import (
	"errors"
	"strings"
	"testing"
)

func TestExtractGoCode_StandardMarkdown(t *testing.T) {
	raw := `Here is the requested web server implementation:

` + "```go" + `
package main

import (
	"fmt"
	"net/http"
)

func main() {
	fmt.Println("Starting server...")
	http.ListenAndServe(":8080", nil)
}
` + "```" + `

Make sure to run it with go run.`

	code, err := ExtractGoCode(raw)
	if err != nil {
		t.Fatalf("Ekstraksi gagal: %v", err)
	}

	if !strings.HasPrefix(code, "package main") {
		t.Errorf("Kode harus diawali dengan 'package main', dapat: %s", code)
	}
	if !strings.Contains(code, "func main()") {
		t.Errorf("Kode harus memuat 'func main()'")
	}
}

func TestExtractGoCode_MultipleCodeBlocks(t *testing.T) {
	raw := `First, install Go:
` + "```bash" + `
sudo apt install golang
` + "```" + `

Then save this code to main.go:
` + "```go" + `
package main

import "net/http"

func main() {
	http.ListenAndServe(":8080", nil)
}
` + "```" + `

Run it with:
` + "```bash" + `
go run main.go
` + "```"

	code, err := ExtractGoCode(raw)
	if err != nil {
		t.Fatalf("Ekstraksi gagal pada multiple code blocks: %v", err)
	}

	if !strings.Contains(code, "package main") || !strings.Contains(code, "func main()") {
		t.Errorf("Ekstraktor salah memilih blok kode non-Go: %s", code)
	}
	if strings.Contains(code, "sudo apt install") {
		t.Errorf("Ekstraktor menyertakan blok bash: %s", code)
	}
}

func TestExtractGoCode_NoMarkdownFences(t *testing.T) {
	raw := `
package main

import (
	"net/http"
)

func main() {
	http.ListenAndServe(":8080", nil)
}
`
	code, err := ExtractGoCode(raw)
	if err != nil {
		t.Fatalf("Ekstraksi fallback tanpa markdown gagal: %v", err)
	}

	if !strings.Contains(code, "package main") || !strings.Contains(code, "func main()") {
		t.Errorf("Hasil fallback tidak lengkap: %s", code)
	}
}

func TestExtractGoCode_MissingMainErrors(t *testing.T) {
	t.Run("Missing package main", func(t *testing.T) {
		raw := "```go\npackage server\nfunc main() {}\n```"
		_, err := ExtractGoCode(raw)
		if !errors.Is(err, ErrNoMainPackage) {
			t.Errorf("Ekspektasi ErrNoMainPackage, dapat: %v", err)
		}
	})

	t.Run("Missing func main", func(t *testing.T) {
		raw := "```go\npackage main\nfunc helper() {}\n```"
		_, err := ExtractGoCode(raw)
		if !errors.Is(err, ErrNoMainFunction) {
			t.Errorf("Ekspektasi ErrNoMainFunction, dapat: %v", err)
		}
	})

	t.Run("Empty response", func(t *testing.T) {
		_, err := ExtractGoCode("   ")
		if !errors.Is(err, ErrNoGoCode) {
			t.Errorf("Ekspektasi ErrNoGoCode, dapat: %v", err)
		}
	})
}
