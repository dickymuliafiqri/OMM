package logger

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestCleanSingleLine(t *testing.T) {
	input := "Baris 1\nBaris 2\r\nBaris 3\t\tSpasi   banyak"
	res := CleanSingleLine(input, 100)
	if strings.Contains(res, "\n") || strings.Contains(res, "\r") || strings.Contains(res, "\t") {
		t.Errorf("CleanSingleLine masih mengandung karakter newline/tab: %s", res)
	}
	expected := "Baris 1 Baris 2 Baris 3 Spasi banyak"
	if res != expected {
		t.Errorf("CleanSingleLine salah. Got '%s', expected '%s'", res, expected)
	}

	longInput := "12345678901234567890"
	resTrunc := CleanSingleLine(longInput, 10)
	if len(resTrunc) != 10 || !strings.HasSuffix(resTrunc, "...") {
		t.Errorf("Truncate salah: %s", resTrunc)
	}
}

func TestMaskSecrets(t *testing.T) {
	input := "Bearer secret_jwt_token_1234567890_abcdef dan sk-proj-1234567890abcdef1234567890"
	masked := MaskSecrets(input)

	if strings.Contains(masked, "secret_jwt_token_1234567890_abcdef") {
		t.Errorf("Bearer token tidak tersamarkan: %s", masked)
	}
	if strings.Contains(masked, "1234567890abcdef1234567890") {
		t.Errorf("API key tidak tersamarkan: %s", masked)
	}
	if !strings.Contains(masked, "sk-proj-1...7890") {
		t.Errorf("Format masking salah: %s", masked)
	}
}

func TestLogOutput_SingleLine(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)

	TGIn("dickymulia", 1001, 1001, "/start\nwith newline")
	TGOut(1001, "SEND", "Halo selamat datang!\nSilakan pilih menu.")
	TGCB("dickymulia", 1001, 1001, "use_saved_creds")
	AIReq("https://api.openai.com/v1", "/models", "ListModels", "")
	AIRes("gpt-4o", "200 OK", 1250*time.Millisecond, 100, 200, 300, "code_lines=150")
	BenchProgress("job-12345678", 1, 8, "Executable", 15, 15, "PASS", 500*time.Millisecond)
	BenchFinished("job-12345678", "gpt-4o", 95, 100, "S", 15*time.Second, 300)
	Queue("job-12345678", "SUBMIT", "queue_len=1/20")
	DB("SAVE", "target=%s id=%d", "users", 1001)
	Sys("STARTUP", "Server started")

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	// Harus ada tepat 10 baris, masing-masing 1 baris
	if len(lines) != 10 {
		t.Fatalf("Ekspektasi 10 baris log, dapat: %d baris\nOutput:\n%s", len(lines), output)
	}

	for i, line := range lines {
		if strings.Contains(line, "\r") {
			t.Errorf("Baris %d mengandung carriage return: %s", i+1, line)
		}
	}
}

func TestLogger_LevelsAndStructuredLogging(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)

	// 1. Level Warn - Debug and Info should be dropped
	SetLevel(LevelWarn)
	if GetLevel() != LevelWarn {
		t.Errorf("expected LevelWarn, got %v", GetLevel())
	}

	Debug("debug.event", "detail=1")
	Info("info.event", "detail=2")
	Warn("auth.rejected", "ip=1.2.3.4 reason=invalid_secret")
	Error("bench.error", "jobId=123 error=%q", "timeout")

	output := buf.String()
	if strings.Contains(output, "debug.event") {
		t.Errorf("Debug log should have been filtered out")
	}
	if strings.Contains(output, "info.event") {
		t.Errorf("Info log should have been filtered out")
	}
	if !strings.Contains(output, "auth.rejected ip=1.2.3.4 reason=invalid_secret") {
		t.Errorf("Warn log missing: %s", output)
	}
	if !strings.Contains(output, "bench.error jobId=123 error=\"timeout\"") {
		t.Errorf("Error log missing: %s", output)
	}

	// 2. Reset back to LevelDebug - all should appear
	buf.Reset()
	SetLevel(LevelDebug)
	Debug("test.debug", "val=123")
	Info("server.start", "listen=:9090 workers=3")

	outDebug := buf.String()
	if !strings.Contains(outDebug, "test.debug val=123") {
		t.Errorf("expected debug log, got: %s", outDebug)
	}
	if !strings.Contains(outDebug, "server.start listen=:9090 workers=3") {
		t.Errorf("expected info log, got: %s", outDebug)
	}

	// 3. Test ParseLevel
	if ParseLevel("debug") != LevelDebug {
		t.Errorf("ParseLevel('debug') failed")
	}
	if ParseLevel("info") != LevelInfo {
		t.Errorf("ParseLevel('info') failed")
	}
	if ParseLevel("warn") != LevelWarn || ParseLevel("warning") != LevelWarn {
		t.Errorf("ParseLevel('warn') failed")
	}
	if ParseLevel("error") != LevelError {
		t.Errorf("ParseLevel('error') failed")
	}
	if ParseLevel("unknown") != LevelInfo {
		t.Errorf("ParseLevel('unknown') expected default LevelInfo")
	}
}

