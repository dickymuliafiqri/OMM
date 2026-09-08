package logger

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Level defines log severity levels
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

// ANSI color codes for elegant terminal display
const (
	colorReset   = "\033[0m"
	colorDim     = "\033[2m"
	colorBold    = "\033[1m"
	colorCyan    = "\033[36m"
	colorBlue    = "\033[34m"
	colorGreen   = "\033[32m"
	colorYellow  = "\033[33m"
	colorRed     = "\033[31m"
	colorMagenta = "\033[35m"
	colorWhite   = "\033[37m"
)

// Logger provides elegant, structured single-line logging
type Logger struct {
	mu        sync.Mutex
	out       io.Writer
	level     Level
	useColors bool
}

var (
	defaultLogger = New(os.Stdout, LevelInfo)
	secretRegex   = regexp.MustCompile(`(sk-[a-zA-Z0-9_-]{6})[a-zA-Z0-9_-]+([a-zA-Z0-9_-]{4})`)
	bearerRegex   = regexp.MustCompile(`(?i)(bearer\s+)([a-zA-Z0-9_\-\.]{6})[a-zA-Z0-9_\-\.]+([a-zA-Z0-9_\-\.]{4})`)
)

// New creates a new Logger instance
func New(out io.Writer, level Level) *Logger {
	useColors := true
	if os.Getenv("NO_COLOR") != "" || strings.ToLower(os.Getenv("TERM")) == "dumb" {
		useColors = false
	}
	return &Logger{
		out:       out,
		level:     level,
		useColors: useColors,
	}
}

// SetOutput changes the logger output writer
func SetOutput(w io.Writer) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.out = w
}

// SetLevel changes the log level threshold
func SetLevel(lvl Level) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.level = lvl
}

// CleanSingleLine cleans newlines and trims text into a single line
func CleanSingleLine(s string, maxLen int) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\t", " ")

	// Normalize multiple consecutive spaces
	fields := strings.Fields(s)
	s = strings.Join(fields, " ")

	s = MaskSecrets(s)

	if maxLen > 0 && len(s) > maxLen {
		return s[:maxLen-3] + "..."
	}
	return s
}

// MaskSecrets obscures secret API keys or tokens to prevent leakage in logs
func MaskSecrets(s string) string {
	s = secretRegex.ReplaceAllString(s, "${1}...${2}")
	s = bearerRegex.ReplaceAllString(s, "${1}${2}...${3}")
	return s
}

func (l *Logger) logLine(colorTag, tag string, content string) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.useColors && colorTag != "" {
		fmt.Fprintf(l.out, "%s%s%s %s[%-7s]%s %s\n",
			colorDim, timestamp, colorReset,
			colorTag, tag, colorReset,
			content,
		)
	} else {
		fmt.Fprintf(l.out, "%s [%-7s] %s\n", timestamp, tag, content)
	}
}

// --- Telegram Logging ---

// TGIn logs incoming messages/commands from user Telegram
func TGIn(username string, userID int64, chatID int64, text string) {
	userDisplay := "@" + username
	if username == "" {
		userDisplay = fmt.Sprintf("ID:%d", userID)
	}
	cleanText := CleanSingleLine(text, 80)
	defaultLogger.logLine(colorCyan, "TG:IN",
		fmt.Sprintf("user=%s (%d) chat=%d | text=\"%s\"", userDisplay, userID, chatID, cleanText))
}

// TGOut logs outgoing messages sent to Telegram
func TGOut(chatID int64, action string, preview string) {
	cleanPreview := CleanSingleLine(preview, 75)
	defaultLogger.logLine(colorBlue, "TG:OUT",
		fmt.Sprintf("chat=%d action=%s | \"%s\"", chatID, action, cleanPreview))
}

// TGCB logs inline keyboard callback query button presses
func TGCB(username string, userID int64, chatID int64, data string) {
	userDisplay := "@" + username
	if username == "" {
		userDisplay = fmt.Sprintf("ID:%d", userID)
	}
	defaultLogger.logLine(colorCyan, "TG:CB",
		fmt.Sprintf("user=%s (%d) chat=%d | data=\"%s\"", userDisplay, userID, chatID, data))
}

// --- AI Interaction Logging ---

// AIReq logs request dispatch to AI provider endpoints
func AIReq(target string, endpoint string, action string, extra string) {
	content := fmt.Sprintf("target=%s endpoint=%s action=%s", target, endpoint, action)
	if extra != "" {
		content += " | " + extra
	}
	defaultLogger.logLine(colorMagenta, "AI:REQ", content)
}

// AIRes logs response back from AI provider (status, latency, tokens)
func AIRes(target string, status string, latency time.Duration, inTokens, outTokens, totTokens int, extra string) {
	tokenStr := ""
	if totTokens > 0 {
		tokenStr = fmt.Sprintf("tokens(in=%d out=%d tot=%d)", inTokens, outTokens, totTokens)
	}
	parts := []string{
		fmt.Sprintf("target=%s", target),
		fmt.Sprintf("status=%s", status),
		fmt.Sprintf("latency=%.2fs", latency.Seconds()),
	}
	if tokenStr != "" {
		parts = append(parts, tokenStr)
	}
	if extra != "" {
		parts = append(parts, extra)
	}
	defaultLogger.logLine(colorMagenta, "AI:RES", strings.Join(parts, " | "))
}

// --- Benchmark & Sandbox Logging ---

// BenchProgress logs test suite completion progress
func BenchProgress(jobID string, step, total int, suite string, score, maxScore int, status string, dur time.Duration) {
	shortID := jobID
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}
	defaultLogger.logLine(colorGreen, "BENCH",
		fmt.Sprintf("job=%s [%d/%d] %s | score=%d/%d pts | status=%s | dur=%.2fs",
			shortID, step, total, suite, score, maxScore, status, dur.Seconds()))
}

// BenchFinished logs the completion of a full benchmark session
func BenchFinished(jobID string, model string, score, maxScore int, grade string, dur time.Duration, tokens int) {
	shortID := jobID
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}
	tokenStr := ""
	if tokens > 0 {
		tokenStr = fmt.Sprintf(" | tokens=%d", tokens)
	}
	defaultLogger.logLine(colorBold+colorGreen, "BENCH",
		fmt.Sprintf("job=%s FINISHED model=%s | score=%d/%d [%s] | time=%.1fs%s",
			shortID, model, score, maxScore, grade, dur.Seconds(), tokenStr))
}

// Sandbox logs sandbox environment activities (compilation, port allocation, AST)
func Sandbox(jobID string, port int, action string, result string) {
	shortID := jobID
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}
	defaultLogger.logLine(colorGreen, "SANDBOX",
		fmt.Sprintf("job=%s port=:%d action=%s | %s", shortID, port, action, result))
}

// --- Queue & Worker Pool Logging ---

// Queue logs queue status transitions and workers
func Queue(jobID string, action string, detail string) {
	shortID := jobID
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}
	defaultLogger.logLine(colorYellow, "QUEUE",
		fmt.Sprintf("job=%s action=%s | %s", shortID, action, detail))
}

// --- Database Logging ---

// DB logs data persistence operations
func DB(action string, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	defaultLogger.logLine(colorWhite, "DB",
		fmt.Sprintf("action=%s | %s", action, msg))
}

// --- System, Warning, & Error Logging ---

// Sys logs system lifecycle events (startup, shutdown, health)
func Sys(action string, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	defaultLogger.logLine(colorBold+colorWhite, "SYS",
		fmt.Sprintf("action=%s | %s", action, msg))
}

// Warn logs system warnings
func Warn(tag string, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	defaultLogger.logLine(colorYellow, tag, msg)
}

// Error logs single-line errors
func Error(tag string, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	defaultLogger.logLine(colorRed, tag, msg)
}

// ErrorLong logs multiline long errors when strictly necessary (e.g. stack trace)
func ErrorLong(tag string, title string, err error, details ...string) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()

	fmt.Fprintf(defaultLogger.out, "\n%s%s [%-7s] ⚠️  KESALAHAN SISTEM: %s%s\n",
		colorBold+colorRed, timestamp, tag, title, colorReset)
	if err != nil {
		fmt.Fprintf(defaultLogger.out, "   Detail Error : %v\n", err)
	}
	for _, d := range details {
		fmt.Fprintf(defaultLogger.out, "   %s\n", d)
	}
	fmt.Fprintf(defaultLogger.out, "%s━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━%s\n\n", colorDim, colorReset)
}
