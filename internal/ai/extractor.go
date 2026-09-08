package ai

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	ErrNoGoCode       = errors.New("tidak ditemukan kode Go yang valid pada respons AI")
	ErrNoMainPackage  = errors.New("kode tidak memiliki 'package main'")
	ErrNoMainFunction = errors.New("kode tidak memiliki fungsi 'func main()'")
)

// Regex to detect markdown code blocks (```go ... ```, ```golang ... ```, or ``` ... ```)
var codeBlockRegex = regexp.MustCompile("(?s)```(?:go|golang)?\\s*\\n?(.*?)```")

// ExtractGoCode extracts pure Go source code from AI model text responses (which are typically wrapped in markdown).
func ExtractGoCode(rawResponse string) (string, error) {
	trimmed := strings.TrimSpace(rawResponse)
	if trimmed == "" {
		return "", ErrNoGoCode
	}

	// 1. Find all markdown code blocks
	matches := codeBlockRegex.FindAllStringSubmatch(trimmed, -1)
	var candidates []string

	for _, match := range matches {
		if len(match) > 1 {
			codeCandidate := strings.TrimSpace(match[1])
			if codeCandidate != "" {
				candidates = append(candidates, codeCandidate)
			}
		}
	}

	// 2. Prioritize code block candidates containing 'package main' and 'func main()'
	for _, cand := range candidates {
		if strings.Contains(cand, "package main") && strings.Contains(cand, "func main") {
			return cleanCode(cand), nil
		}
	}

	// If there are code block candidates without a full func main, take the one containing package main
	for _, cand := range candidates {
		if strings.Contains(cand, "package main") {
			return cleanCode(cand), validateCodeStructure(cand)
		}
	}

	// 3. Fallback: If the model does not use markdown fences ```go ... ```
	// Check if there is 'package main' text in the response
	pkgIdx := strings.Index(trimmed, "package main")
	if pkgIdx != -1 {
		rawCode := trimmed[pkgIdx:]
		// Clean up any trailing backticks at the end
		rawCode = strings.TrimRight(rawCode, "` \r\n\t")
		if err := validateCodeStructure(rawCode); err != nil {
			return cleanCode(rawCode), err
		}
		return cleanCode(rawCode), nil
	}

	// If there is a first candidate even without package main
	if len(candidates) > 0 {
		return cleanCode(candidates[0]), validateCodeStructure(candidates[0])
	}

	return "", fmt.Errorf("%w: respons model tidak memuat definisi Go yang dapat dikenali", ErrNoGoCode)
}

// validateCodeStructure ensures the code meets the requirements for a standalone server executable
func validateCodeStructure(code string) error {
	if !strings.Contains(code, "package main") {
		return ErrNoMainPackage
	}
	if !strings.Contains(code, "func main") {
		return ErrNoMainFunction
	}
	return nil
}

// cleanCode removes leading and trailing whitespace and invisible characters
func cleanCode(code string) string {
	code = strings.TrimSpace(code)
	// Normalize line endings (CRLF -> LF)
	code = strings.ReplaceAll(code, "\r\n", "\n")
	return code
}
