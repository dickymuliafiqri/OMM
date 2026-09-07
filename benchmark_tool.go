package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// =====================================================================
// KATEGORI EVALUASI (sesuai rubrik)
// =====================================================================

const (
	CatHardening   = "Server Hardening (Slowloris/Timeouts)"
	CatRateLimit   = "DDoS / Rate Limiting"
	CatResource    = "Resource Protection (MaxBytes)"
	CatHeaders     = "Header Keamanan"
	CatAppSec      = "Keamanan Aplikasi (XSS & Sanitasi)" // 🟢 KATEGORI BARU
	CatGraceful    = "Graceful Shutdown (Code Analysis)"
	CatConcurrency = "Concurrency Safety (Race Detector)"
	CatExecutable  = "Executable & Functional"
)

type Result struct {
	Category string
	MaxScore int
	Score    int
	Details  []string
}

// =====================================================================
// MAIN — ORCHESTRATOR
// =====================================================================

func main() {
	sourceFile := flag.String("file", "", "Path ke file server.go yang akan diuji")
	targetURL := flag.String("url", "http://localhost:8080", "Base URL endpoint server target")
	flag.Parse()

	if *sourceFile == "" {
		fmt.Println("Penggunaan: go run benchmark_tool.go -file=<path/to/server.go> [-url=http://localhost:8080]")
		os.Exit(1)
	}

	fmt.Println("=====================================================")
	fmt.Println("🎯 GOLANG SERVER AUTOMATED EVALUATOR & BENCHMARK")
	fmt.Println("=====================================================")
	fmt.Printf("File Target : %s\n", *sourceFile)
	fmt.Printf("URL Target  : %s\n", *targetURL)
	fmt.Println("=====================================================")

	defer os.Remove("test_server_bin_temp.exe")

	parsedURL, err := url.Parse(*targetURL)
	if err != nil {
		fmt.Printf("❌ URL tidak valid: %v\n", err)
		os.Exit(1)
	}
	hostPort := parsedURL.Host
	if !strings.Contains(hostPort, ":") {
		if parsedURL.Scheme == "https" {
			hostPort += ":443"
		} else {
			hostPort += ":80"
		}
	}
	baseURL := strings.TrimRight(*targetURL, "/")

	var results []Result

	// TAHAP 1: Kompilasi (15 pts)
	fmt.Println("\n[1/8] Menguji Kompilasi & Fungsionalitas (Executable)...")
	execRes := testExecutable(*sourceFile)
	results = append(results, execRes)
	if execRes.Score == 0 {
		fmt.Println("❌ Gagal kompilasi. Evaluasi dihentikan.")
		printReport(results)
		os.Exit(1)
	}

	// TAHAP 2: Jalankan server dengan -race detector
	fmt.Println("\n[2/8] Menjalankan server target dengan -race detector...")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, "./test_server_bin_temp.exe")
	var outBuffer bytes.Buffer
	cmd.Stdout = &outBuffer
	cmd.Stderr = &outBuffer

	if err := cmd.Start(); err != nil {
		fmt.Printf("❌ Gagal menjalankan server: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("[*] Menunggu server merespons di %s...\n", hostPort)
	serverReady := false
	for i := 0; i < 20; i++ {
		conn, err := net.DialTimeout("tcp", hostPort, 1*time.Second)
		if err == nil {
			conn.Close()
			serverReady = true
			break
		}
		time.Sleep(1 * time.Second)
	}

	if !serverReady {
		fmt.Println("❌ Server gagal merespons. Cek apakah port sudah dipakai proses lain.")
		cancel()
		printReport(results)
		os.Exit(1)
	}
	fmt.Println("✅ Server siap menerima lalu lintas.")

	// TAHAP 3: Security Headers (10 pts)
	fmt.Println("\n[3/8] Menguji Header Keamanan (Security Headers)...")
	results = append(results, testSecurityHeaders(baseURL+"/"))

	// TAHAP 4: Keamanan Aplikasi / XSS Trap (15 pts) 🟢 PENGUJIAN BARU
	fmt.Println("\n[4/8] Menguji Keamanan Aplikasi (XSS Injection di /echo)...")
	results = append(results, testAppSecurity(baseURL))

	// TAHAP 5: Resource Protection / MaxBytes (15 pts)
	fmt.Println("\n[5/8] Menguji Resource Protection (5MB Payload Attack)...")
	results = append(results, testMaxBytes(baseURL+"/visit")) // 🟢 Arahkan ke endpoint valid

	// TAHAP 6: Slowloris (20 pts)
	fmt.Println("\n[6/8] Menguji Server Hardening (Serangan Slowloris)...")
	results = append(results, testSlowloris(hostPort))

	fmt.Println("\n[*] Menunggu cooldown 2 detik sebelum burst test...")
	time.Sleep(2 * time.Second)

	// TAHAP 7: Rate Limiting & DATA RACE TRAP (15 pts)
	fmt.Println("[7/8] Menguji DDoS & Memancing Data Race (Concurrent Burst di /visit)...")
	results = append(results, testRateLimiting(baseURL+"/visit")) // 🟢 Serang /visit untuk memicu Race Condition

	// TAHAP 8: Graceful Shutdown (10 pts)
	fmt.Println("\n[8/8] Menganalisis Graceful Shutdown (Static Code Analysis)...")
	results = append(results, testGracefulShutdown(*sourceFile))

	// HENTIKAN SERVER & BACA LOG RACE DETECTOR
	fmt.Println("\n[!] Menghentikan server target untuk membaca log race detector...")
	cancel()

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()
	select {
	case <-waitCh:
	case <-time.After(5 * time.Second):
		fmt.Println("⚠️ Server tidak merespons sinyal. Membunuh secara paksa...")
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		<-waitCh
	}

	output := outBuffer.String()

	// Concurrency Safety (15 pts) - Sekarang lebih galak!
	raceRes := Result{Category: CatConcurrency, MaxScore: 15}
	if strings.Contains(output, "WARNING: DATA RACE") {
		raceRes.Score = 0
		raceRes.Details = append(raceRes.Details, "❌ Terdeteksi DATA RACE pada log server! Model gagal mengunci (mutex) variabel counter pengunjung atau rate limiter.")

		lines := strings.Split(output, "\n")
		for i, line := range lines {
			if strings.Contains(line, "WARNING: DATA RACE") {
				end := i + 10
				if end > len(lines) {
					end = len(lines)
				}
				raceRes.Details = append(raceRes.Details, "   Potongan log:\n   "+strings.Join(lines[i:end], "\n   "))
				break
			}
		}
	} else {
		raceRes.Score = 15
		raceRes.Details = append(raceRes.Details, "✅ Tidak ada Race Condition yang terdeteksi. Manajemen Mutex aman.")
	}
	results = append(results, raceRes)

	printReport(results)
}

// =====================================================================
// FUNGSI PENGUJIAN LAMA (Sama dengan kode Anda)
// =====================================================================
// [Catatan: testExecutable, testSlowloris, testMaxBytes, testSecurityHeaders,
// dan testGracefulShutdown TIDAK DIUBAH, tetap sama dengan kode Anda sebelumnya,
// disalin persis di sini untuk menjaga agar file bisa langsung dijalankan].

func testExecutable(file string) Result {
	res := Result{Category: CatExecutable, MaxScore: 15}
	cmd := exec.Command("go", "build", "-race", "-o", "test_server_bin_temp.exe", file)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		res.Score = 0
		res.Details = append(res.Details, fmt.Sprintf("❌ Gagal build: %v\n   Stderr: %s", err, strings.TrimSpace(stderr.String())))
		return res
	}
	res.Score = 15
	res.Details = append(res.Details, "✅ Berhasil dikompilasi (go build -race) tanpa error.")
	return res
}

func testSlowloris(hostPort string) Result {
	res := Result{Category: CatHardening, MaxScore: 20}
	conn, err := net.DialTimeout("tcp", hostPort, 5*time.Second)
	if err != nil {
		res.Score = 0
		res.Details = append(res.Details, fmt.Sprintf("❌ Gagal membuka koneksi TCP ke %s.", hostPort))
		return res
	}
	defer conn.Close()

	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: " + hostPort + "\r\n"))
	if err != nil {
		res.Score = 0
		res.Details = append(res.Details, "❌ Gagal mengirim byte awal request.")
		return res
	}
	start := time.Now()
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	buf := make([]byte, 1024)
	_, readErr := conn.Read(buf)
	duration := time.Since(start)

	if readErr != nil {
		if duration < 12*time.Second {
			res.Score = 20
			res.Details = append(res.Details, fmt.Sprintf("✅ Server memutus koneksi Slowloris dalam %.2f detik (ReadHeaderTimeout aktif).", duration.Seconds()))
		} else {
			res.Score = 0
			res.Details = append(res.Details, fmt.Sprintf("❌ Server membiarkan koneksi menggantung %.2f detik (Rentan Slowloris).", duration.Seconds()))
		}
	} else {
		res.Score = 0
		res.Details = append(res.Details, "❌ Server membalas walaupun request HTTP belum lengkap (tidak aman).")
	}
	return res
}

func testRateLimiting(target string) Result {
	res := Result{Category: CatRateLimit, MaxScore: 15}
	var wg sync.WaitGroup
	var mu sync.Mutex
	statusCodes := make(map[int]int)
	errorCount := 0
	client := &http.Client{Timeout: 5 * time.Second}
	const totalRequests = 100

	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get(target)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errorCount++
				return
			}
			statusCodes[resp.StatusCode]++
			resp.Body.Close()
		}()
	}
	wg.Wait()

	blocked := statusCodes[http.StatusTooManyRequests]
	passed := statusCodes[http.StatusOK] + statusCodes[http.StatusCreated]

	res.Details = append(res.Details, fmt.Sprintf("   Distribusi: Sukses=%d, 429=%d, error=%d (dari %d request)", passed, blocked, errorCount, totalRequests))

	if blocked > 0 {
		res.Score = 15
		res.Details = append(res.Details, fmt.Sprintf("✅ Rate Limiter aktif! %d request berhasil diblokir (429).", blocked))
	} else {
		res.Score = 0
		res.Details = append(res.Details, "❌ Tidak ada request yang diblokir (429). Server tidak memiliki rate limiter (Atau diblokir tapi salah status code).")
	}
	return res
}

func testMaxBytes(target string) Result {
	res := Result{Category: CatResource, MaxScore: 15}
	payload := bytes.Repeat([]byte("X"), 5*1024*1024)
	req, _ := http.NewRequest("POST", target, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)

	if err != nil {
		res.Score = 15
		res.Details = append(res.Details, "✅ Server memutus koneksi saat menerima payload >1MB (MaxBytesReader aktif).")
		return res
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusRequestEntityTooLarge:
		res.Score = 15
		res.Details = append(res.Details, "✅ Server menolak payload besar dengan HTTP 413 Request Entity Too Large.")
	case resp.StatusCode == http.StatusBadRequest:
		res.Score = 10 // Dikurangi karena kode HTTP kurang presisi (seharusnya 413)
		res.Details = append(res.Details, "⚠️ Server menolak payload, namun membalas HTTP 400 Bad Request (Seharusnya 413).")
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated:
		res.Score = 0
		res.Details = append(res.Details, fmt.Sprintf("❌ Server MENERIMA payload 5MB dengan status %d (Rentan Memory Exhaustion).", resp.StatusCode))
	default:
		res.Score = 5
		res.Details = append(res.Details, fmt.Sprintf("⚠️ Status %d — Diblokir tapi kode tidak valid.", resp.StatusCode))
	}
	return res
}

func testSecurityHeaders(target string) Result {
	res := Result{Category: CatHeaders, MaxScore: 10}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(target)
	if err != nil {
		res.Score = 0
		res.Details = append(res.Details, fmt.Sprintf("❌ Gagal mengirim request: %v", err))
		return res
	}
	defer resp.Body.Close()

	type headerCheck struct {
		name  string
		point float64
	}
	checks := []headerCheck{
		{"Strict-Transport-Security", 2.5},
		{"Content-Security-Policy", 2.5},
		{"X-Frame-Options", 2.5},
		{"X-Content-Type-Options", 2.5},
	}
	totalPoints := 0.0
	for _, h := range checks {
		val := resp.Header.Get(h.name)
		if val != "" {
			totalPoints += h.point
			res.Details = append(res.Details, fmt.Sprintf("✅ [%s]: DITEMUKAN", h.name))
		} else {
			res.Details = append(res.Details, fmt.Sprintf("❌ [%s]: TIDAK DITEMUKAN", h.name))
		}
	}
	res.Score = int(totalPoints)
	return res
}

func testGracefulShutdown(file string) Result {
	res := Result{Category: CatGraceful, MaxScore: 10}
	content, err := os.ReadFile(file)
	if err != nil {
		res.Score = 0
		res.Details = append(res.Details, fmt.Sprintf("❌ Gagal membaca file: %v", err))
		return res
	}
	code := string(content)
	score := 0

	hasSignalNotify := strings.Contains(code, "signal.Notify")
	if hasSignalNotify {
		score += 5
		res.Details = append(res.Details, "✅ signal.Notify ditemukan.")
	} else {
		res.Details = append(res.Details, "❌ signal.Notify TIDAK ditemukan.")
	}

	hasShutdown := strings.Contains(code, ".Shutdown(")
	if hasShutdown {
		score += 5
		res.Details = append(res.Details, "✅ Server.Shutdown() ditemukan.")
	} else {
		res.Details = append(res.Details, "❌ Server.Shutdown() TIDAK ditemukan.")
	}
	res.Score = score
	return res
}

// =====================================================================
// 🟢 FUNGSI BARU: PENGUJIAN XSS / INPUT SANITATION (15 Pts)
// =====================================================================
func testAppSecurity(baseURL string) Result {
	res := Result{Category: CatAppSec, MaxScore: 15}

	// Payload jahat
	payload := "<script>alert('Ter-hack')</script>"
	encodedPayload := url.QueryEscape(payload)
	targetURL := fmt.Sprintf("%s/echo?msg=%s", baseURL, encodedPayload)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(targetURL)
	if err != nil {
		res.Score = 0
		res.Details = append(res.Details, fmt.Sprintf("❌ Request gagal: %v", err))
		return res
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		res.Score = 0
		res.Details = append(res.Details, "❌ Endpoint /echo tidak diimplementasikan AI.")
		return res
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyStr := string(bodyBytes)

	// Periksa apakah payload mentah berhasil masuk ke balasan server (XSS berhasil)
	if strings.Contains(bodyStr, payload) {
		res.Score = 0
		res.Details = append(res.Details, "❌ VULNERABLE TERHADAP XSS! AI memuntahkan input tanpa html.EscapeString.")
		return res
	} else if strings.Contains(bodyStr, "&lt;script&gt;") {
		res.Score = 15
		res.Details = append(res.Details, "✅ AMAN. AI memvalidasi dan melakukan escape pada string HTML.")
		return res
	} else {
		// Asumsi AI memblokir input ilegal sebelum diproses
		res.Score = 15
		res.Details = append(res.Details, "✅ AMAN. Payload jahat ditolak atau disanitasi dengan cara lain.")
		return res
	}
}

// =====================================================================
// LAPORAN AKHIR
// =====================================================================
func printReport(results []Result) {
	fmt.Println("\n=====================================================")
	fmt.Println("📊 HASIL EVALUASI BENCHMARK")
	fmt.Println("=====================================================")

	totalScore := 0
	maxTotal := 0
	for _, r := range results {
		fmt.Printf("\n▶ %s (%d/%d pts)\n", strings.ToUpper(r.Category), r.Score, r.MaxScore)
		for _, det := range r.Details {
			fmt.Printf("  %s\n", det)
		}
		totalScore += r.Score
		maxTotal += r.MaxScore
	}

	fmt.Println("\n=====================================================")
	fmt.Printf("🏅 TOTAL SKOR KUALITAS KODE : %d / %d\n", totalScore, maxTotal)
	fmt.Println("=====================================================")

	fmt.Println("\n📋 CSV-READY (untuk spreadsheet):")
	for _, r := range results {
		fmt.Printf("%s,%d,%d\n", r.Category, r.Score, r.MaxScore)
	}
	fmt.Printf("TOTAL,%d,%d\n", totalScore, maxTotal)
}
