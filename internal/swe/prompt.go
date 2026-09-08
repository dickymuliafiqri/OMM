package swe

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// SWESystemPrompt is a dedicated system prompt for brownfield SWE-bench bug fixing
const SWESystemPrompt = `You are an expert Go systems engineer and technical lead.
You are tasked with fixing production bugs, race conditions, goroutine leaks, deadlocks, and memory issues in existing Go codebases.

Guidelines:
1. Maintain existing API signatures, structs, and interfaces unless explicitly instructed otherwise.
2. Write clean, idiomatic, and thread-safe Go 1.24+ code.
3. Completely eliminate data races, race windows, goroutine leaks, and deadlocks.
4. Your response must contain the COMPLETE runnable Go code inside a single ` + "```go ... ```" + ` markdown block.
5. Do NOT use placeholder comments like "// ... keep existing code ...". Provide the entire file content so it can be compiled directly.`

// BuildTaskPrompt builds the initial task prompt to send to the AI model
func BuildTaskPrompt(task *Task) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("# GitHub Issue: %s\n\n", task.Title))
	sb.WriteString(fmt.Sprintf("- **Task ID:** `%s`\n", task.ID))
	sb.WriteString(fmt.Sprintf("- **Tier:** %s (%d Points)\n", task.Tier, task.Points))
	sb.WriteString(fmt.Sprintf("- **Category:** %s\n\n", task.Category))

	sb.WriteString("## Issue Description\n\n")
	sb.WriteString(strings.TrimSpace(task.IssueBody))
	sb.WriteString("\n\n")

	sb.WriteString("## Broken Go Code (`main.go`)\n\n")
	sb.WriteString("```go\n")
	sb.WriteString(strings.TrimSpace(task.BrokenCode))
	sb.WriteString("\n```\n\n")

	sb.WriteString("## Instructions\n")
	sb.WriteString("1. Analyze the issue description and identify the root cause in the broken Go code.\n")
	sb.WriteString("2. Fix the bug cleanly. Ensure strict memory safety, concurrency protection (zero race detector warnings), and correct lifecycle management.\n")
	sb.WriteString("3. Return the COMPLETE fixed Go code inside a single ```go ... ``` code block. Do NOT truncate or omit any unchanged functions.\n")

	return sb.String()
}

// BuildSelfHealingPrompt builds a self-healing prompt when verification testing fails
func BuildSelfHealingPrompt(task *Task, failureOutput string) string {
	cleanedOutput := strings.TrimSpace(failureOutput)
	if len(cleanedOutput) > 3000 {
		cleanedOutput = cleanedOutput[:3000] + "\n...(log truncated)"
	}

	return fmt.Sprintf(`Your fix for task "%s" (%s) failed the verification test suite with the following output:

`+"```"+`
%s
`+"```"+`

Please analyze the failure and error output carefully. Fix the issue completely.
Return the COMPLETE updated Go code inside a single `+"```go ... ```"+` markdown code block. Do NOT omit any functions.`,
		task.Title, task.ID, cleanedOutput)
}

var sweCodeBlockRegex = regexp.MustCompile("(?s)```(?:go|golang)?\\s*\\n?(.*?)```")

var (
	ErrNoCodeInResponse = errors.New("tidak ditemukan blok kode Go pada respons AI")
	ErrMissingPackage   = errors.New("kode yang dihasilkan tidak memuat deklarasi 'package'")
)

// ExtractSWECode extracts pure Go code from a markdown response for SWE tasks
func ExtractSWECode(rawResponse string) (string, error) {
	trimmed := strings.TrimSpace(rawResponse)
	if trimmed == "" {
		return "", ErrNoCodeInResponse
	}

	matches := sweCodeBlockRegex.FindAllStringSubmatch(trimmed, -1)
	var candidates []string

	for _, match := range matches {
		if len(match) > 1 {
			cand := strings.TrimSpace(match[1])
			if cand != "" {
				candidates = append(candidates, cand)
			}
		}
	}

	// Prioritize candidates containing a package declaration
	for _, cand := range candidates {
		if strings.HasPrefix(cand, "package ") || strings.Contains(cand, "\npackage ") {
			return normalizeCode(cand), nil
		}
	}

	// Fallback if there is no markdown fence
	pkgIdx := strings.Index(trimmed, "package ")
	if pkgIdx != -1 {
		rawCode := trimmed[pkgIdx:]
		rawCode = strings.TrimRight(rawCode, "` \r\n\t")
		return normalizeCode(rawCode), nil
	}

	if len(candidates) > 0 {
		cand := candidates[0]
		if !strings.Contains(cand, "package ") {
			return "", ErrMissingPackage
		}
		return normalizeCode(cand), nil
	}

	return "", ErrNoCodeInResponse
}

func normalizeCode(code string) string {
	code = strings.TrimSpace(code)
	code = strings.ReplaceAll(code, "\r\n", "\n")
	return code
}
