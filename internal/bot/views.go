package bot

import (
	"fmt"
	"html"
	"strconv"
	"strings"
	"time"

	"benchmark/internal/pricing"
	"benchmark/internal/queue"
	"benchmark/internal/storage"
	"benchmark/internal/swe"
)

// MaskAPIKey masks most characters of an API key for visual privacy
func MaskAPIKey(key string) string {
	clean := strings.TrimSpace(key)
	if len(clean) <= 8 {
		return "********"
	}
	prefixLen := 4
	if strings.HasPrefix(clean, "sk-") && len(clean) > 10 {
		prefixLen = 7
	}
	suffix := clean[len(clean)-4:]
	return fmt.Sprintf("%s...%s", clean[:prefixLen], suffix)
}

// formatTokenCount formats token count with thousands separator dots (e.g. 2.450)
func formatTokenCount(n int) string {
	if n <= 0 {
		return "0"
	}
	if n < 1000 {
		return strconv.Itoa(n)
	}
	s := strconv.Itoa(n)
	var res []byte
	l := len(s)
	for i := 0; i < l; i++ {
		if i > 0 && (l-i)%3 == 0 {
			res = append(res, '.')
		}
		res = append(res, s[i])
	}
	return string(res)
}

// FormatWelcomeMessage composes the welcome message when user sends /start
func FormatWelcomeMessage(firstName string) string {
	name := html.EscapeString(firstName)
	if name == "" {
		name = "Pengguna"
	}
	return fmt.Sprintf(`⚡ <b>ON MY MARK SWE-BENCH</b> ⚡
━━━━━━━━━━━━
Halo, <b>%s</b>! Selamat datang di On My Mark Native Go SWE-bench.

<blockquote>💡 <b>Fokus Evaluasi:</b>
Menguji kemampuan model AI dalam mendiagnosis dan memperbaiki cacat logika &amp; konkurensi sistem Go riil (<i>brownfield bug-fixing</i>) berstandar produksi.</blockquote>

🪜 <b>4-Tier Engineering Ladder (100 Pts):</b>
• 🟢 <b>Tier 1: Junior (20 pts)</b> — Memory &amp; Map Race
• 🟡 <b>Tier 2: Mid-Level (25 pts)</b> — Goroutine Leak &amp; Channel Block
• 🟠 <b>Tier 3: Senior (30 pts)</b> — Lock Inversion &amp; Context Cancel
• 🔴 <b>Tier 4: Staff (25 pts)</b> — Lock-Free CAS &amp; State Machine Race

⚙️ <b>Aturan Evaluasi:</b>
• 🧠 <b>Reasoning:</b> Default level <code>low</code> untuk efisiensi token.
• 🔄 <b>Self-Healing:</b> Maks. 1x feedback (Turn 2 penalti 20%%, Turn 2 gagal = 0 pts).
• 🛑 <b>Short-Circuit:</b> Jika satu tier gagal, tahap berikutnya langsung dibatalkan.

<blockquote>🔒 <b>Privasi Kredensial:</b>
Base URL dan API Key Anda tersimpan aman agar tidak perlu mengetik ulang. Hapus kapan saja via /resetkey.</blockquote>

👉 <i>Gunakan menu di bawah atau ketik</i> /benchmark <i>untuk memulai!</i>`, name)
}

// FormatHelpMessage composes the usage guide and 100-point evaluation rubric
func FormatHelpMessage() string {
	return `ℹ️ <b>PANDUAN ON MY MARK SWE-BENCH</b> ℹ️
━━━━━━━━━━━━
<blockquote>💡 <b>Filosofi Pengujian:</b>
AI diberikan laporan bug riil (<i>GitHub Issue</i>) dan basis kode Go rusak (<i>brownfield codebase</i>). AI harus mengisolasi akar masalah dan mengembalikan file Go utuh yang lolos verifikasi <code>go test -race</code>.</blockquote>

<pre>
RUBRIK EVALUASI 4-TIER LADDER (100 PTS):
🟢 Tier 1: Junior Engineer    : 20 poin
🟡 Tier 2: Mid-Level Engineer : 25 poin
🟠 Tier 3: Senior Engineer    : 30 poin
🔴 Tier 4: Staff Engineer     : 25 poin
──────────────────────────────────────────
TOTAL MAKSIMAL                : 100 poin
</pre>

⚙️ <b>Mekanisme &amp; Aturan Evaluasi:</b>
• 🧠 <b>Reasoning Effort:</b> Diatur ke <code>low</code> demi efisiensi dan kecepatan.
• 🔄 <b>Batas Self-Healing:</b> Maksimal 1 kali umpan balik error (Turn 2 dengan penalti 20% poin). Jika Turn 2 gagal = 0 poin.
• 🛑 <b>Short-Circuit:</b> Jika suatu tier gagal/timeout, seluruh tier berikutnya langsung dibatalkan (0 poin) tanpa pengujian lanjutan.
• 🏎️ <b>Zero Race Tolerance:</b> Terdeteksi data race pada <code>go test -race</code> = otomatis GAGAL.

🏆 <b>Gelar Rekayasa (Engineering Titles):</b>
• 🟢 <b>Grade S</b> (90–100) : Staff Engineer
• 🔵 <b>Grade A</b> (75–89)  : Senior Engineer
• 🟡 <b>Grade B</b> (50–74)  : Mid-level Engineer
• 🟠 <b>Grade C</b> (20–49)  : Junior Engineer
• 🔴 <b>Grade F</b> (&lt; 20)   : Untrained / Hallucinating

📌 <b>Daftar Perintah:</b>
👉 /benchmark — Mulai menguji model AI
👉 /leaderboard — Papan peringkat kemampuan &amp; efisiensi biaya
👉 /history — Riwayat pengujian personal Anda
👉 /resetkey — Hapus data kredensial tersimpan
👉 /cancel — Batalkan sesi yang sedang aktif`
}

// FormatSWEScoreCard renders a visual scorecard of SWE-bench 4-tier ladder evaluation results
func FormatSWEScoreCard(run *storage.BenchmarkRun, taskRuns []storage.SWETaskRun) string {
	grade, title := swe.CalculateGrade(run.TotalScore)
	gradeDesc := swe.FormatGradeDescription(grade)

	var sb strings.Builder
	sb.WriteString("📊 <b>HASIL EVALUASI</b> 📊\n")
	sb.WriteString("━━━━━━━━━━━━\n")

	sb.WriteString("<blockquote>")
	sb.WriteString(fmt.Sprintf("🤖 <b>Model:</b> <code>%s</code>\n", html.EscapeString(run.ModelName)))
	sb.WriteString(fmt.Sprintf("🎯 <b>Skor Akhir:</b> <b>%d / %d</b> [Grade %s — %s]\n", run.TotalScore, run.MaxScore, grade, title))
	sb.WriteString(fmt.Sprintf("⏱️ <b>Waktu Evaluasi:</b> %.1f detik\n", float64(run.ExecutionTimeMs)/1000.0))
	if run.EstimatedCostUSD > 0 || run.TotalTokens > 0 {
		costEst := pricing.CalculateCost(run.ModelName, run.PromptTokens, run.CompletionTokens, run.TotalScore)
		if costEst.HasPricing {
			sb.WriteString(fmt.Sprintf("💵 <b>Efisiensi Biaya:</b> %s\n", costEst.CostDescription))
		}
	}
	sb.WriteString(fmt.Sprintf("🆔 <b>ID Pengujian:</b> <code>%s</code>", run.ID))
	sb.WriteString("</blockquote>\n\n")

	sb.WriteString("<b>Hasil Evaluasi Multi-Tier Ladder:</b>\n")
	for _, tr := range taskRuns {
		tierOrder := swe.TierOrder(swe.Tier(tr.Tier))
		tierIcon := "🟢"
		tierName := "Junior"
		switch strings.ToUpper(tr.Tier) {
		case "MID":
			tierIcon = "🟡"
			tierName = "Mid-Level"
		case "SENIOR":
			tierIcon = "🟠"
			tierName = "Senior"
		case "STAFF":
			tierIcon = "🔴"
			tierName = "Staff"
		}

		turnStatus := fmt.Sprintf("Lolos Turn %d", tr.Attempts)
		if tr.Attempts > 1 && tr.Resolved {
			turnStatus = fmt.Sprintf("Lolos Turn %d (Self-Healing)", tr.Attempts)
		} else if !tr.Resolved {
			if tr.Attempts == 0 {
				if strings.Contains(strings.ToLower(tr.TestOutput), "timeout") {
					turnStatus = "Dibatalkan (Timeout)"
				} else {
					turnStatus = "Dibatalkan"
				}
			} else if strings.Contains(strings.ToLower(tr.TestOutput), "timeout") {
				turnStatus = fmt.Sprintf("Timeout (Turn %d)", tr.Attempts)
			} else if tr.HasRace {
				turnStatus = "Gagal (Data Race)"
			} else {
				turnStatus = fmt.Sprintf("Gagal (%d percobaan)", tr.Attempts)
			}
		}

		taskIcon := "✅"
		raceBadge := " (Bebas Data Race)"
		if !tr.Resolved {
			taskIcon = "❌"
			if tr.HasRace {
				raceBadge = " (Terdeteksi Data Race)"
			} else {
				raceBadge = ""
			}
		}

		sb.WriteString(fmt.Sprintf("%s <b>Tier %d: %s</b> (%d/%d pts) — [%s]\n",
			tierIcon, tierOrder, tierName, tr.PointsAwarded, tr.MaxPoints, turnStatus))
		sb.WriteString(fmt.Sprintf("   ↳ %s Task: %s%s\n",
			taskIcon, html.EscapeString(tr.TaskTitle), raceBadge))
	}

	sb.WriteString("━━━━━━━━━━━━\n")
	sb.WriteString(fmt.Sprintf("🏆 <b>Predikat:</b> %s\n\n", gradeDesc))
	sb.WriteString("👉 <i>Ketik /benchmark untuk mulai uji baru atau /leaderboard untuk peringkat.</i>")

	return sb.String()
}

// FormatLeaderboard composes the AI model ranking leaderboard display
func FormatLeaderboard(entries []storage.LeaderboardEntry, page, limit int) string {
	if len(entries) == 0 {
		return "🏆 <b>PAPAN PERINGKAT</b> 🏆\n\n<blockquote>Belum ada data pengujian yang tercatat.</blockquote>"
	}

	var sb strings.Builder
	sb.WriteString("🏆 <b>PAPAN PERINGKAT</b> 🏆\n")
	sb.WriteString("<i>Urutan model berdasarkan kapabilitas arsitektur Go Concurrency &amp; efisiensi:</i>\n")
	sb.WriteString("━━━━━━━━━━━━\n\n")

	for i, e := range entries {
		pos := i + 1 + page*limit
		medal := "▫️"
		switch pos {
		case 1:
			medal = "🥇"
		case 2:
			medal = "🥈"
		case 3:
			medal = "🥉"
		}

		kategori := "Direkomendasikan"
		switch {
		case e.PeakScore >= 90:
			kategori = "Sangat Direkomendasikan"
		case e.PeakScore >= 80:
			kategori = "Direkomendasikan"
		case e.PeakScore >= 70:
			kategori = "Baik"
		case e.PeakScore >= 50:
			kategori = "Cukup"
		default:
			kategori = "Perlu Pertimbangan"
		}

		sb.WriteString(fmt.Sprintf("%s <b>#%d %s</b> [%s]\n", medal, pos, html.EscapeString(e.ModelName), kategori))
		sb.WriteString("<blockquote>")
		sb.WriteString(fmt.Sprintf("🏆 Skor Tertinggi: <b>%d / 100</b>\n", e.PeakScore))
		if e.TotalTokens > 0 {
			sb.WriteString(fmt.Sprintf("🪙 Total Token: <b>%s token</b>\n", formatTokenCount(e.TotalTokens)))
		}
		if e.AvgCostUSD > 0 || e.CostTier != "" {
			tierText := e.CostTier
			if tierText == "" {
				tierText = "Standar"
			}
			cost := e.AvgCostUSD
			if cost < 0.0001 {
				cost = 0.0001
			}
			sb.WriteString(fmt.Sprintf("💵 Estimasi Biaya: <b>~$%.4f USD</b> / run (%s)\n", cost, tierText))
		}
		sb.WriteString(fmt.Sprintf("📈 Total Uji: <b>%d kali</b>", e.TotalRuns))
		sb.WriteString("</blockquote>\n\n")
	}
	sb.WriteString("━━━━━━━━━━━━\n")
	sb.WriteString("👉 <i>Gunakan tombol navigasi di bawah untuk beralih mode &amp; halaman.</i>")

	return sb.String()
}

// FormatValueLeaderboard composes the cost efficiency (Value for Money) leaderboard ranking display
func FormatValueLeaderboard(entries []storage.LeaderboardEntry, page, limit int) string {
	if len(entries) == 0 {
		return "💎 <b>PAPAN PERINGKAT EFISIENSI BIAYA</b> 💎\n\n<blockquote>Belum ada data pengujian yang tercatat.</blockquote>"
	}

	var sb strings.Builder
	sb.WriteString("💎 <b>PAPAN PERINGKAT: Efisiensi Biaya (Value for Money)</b> 💎\n")
	sb.WriteString("<i>Urutan model berdasarkan skor tertinggi dengan estimasi biaya paling hemat:</i>\n")
	sb.WriteString("━━━━━━━━━━━━\n\n")

	for i, e := range entries {
		pos := i + 1 + page*limit
		medal := "▫️"
		switch pos {
		case 1:
			medal = "🥇"
		case 2:
			medal = "🥈"
		case 3:
			medal = "🥉"
		}

		tier := e.CostTier
		if tier == "" {
			tier = "Ekonomis"
		}

		cost := e.AvgCostUSD
		if cost < 0.0001 {
			cost = 0.0001
		}

		sb.WriteString(fmt.Sprintf("%s <b>#%d %s</b> [%s]\n", medal, pos, html.EscapeString(e.ModelName), tier))
		sb.WriteString("<blockquote>")
		sb.WriteString(fmt.Sprintf("🏆 Skor Tertinggi: <b>%d / 100</b>\n", e.PeakScore))
		if e.TotalTokens > 0 {
			sb.WriteString(fmt.Sprintf("🪙 Total Token: <b>%s token</b>\n", formatTokenCount(e.TotalTokens)))
		}
		sb.WriteString(fmt.Sprintf("💵 Estimasi Biaya: <b>~$%.4f USD</b> / run\n", cost))
		sb.WriteString(fmt.Sprintf("📈 Total Uji: <b>%d kali</b>", e.TotalRuns))
		sb.WriteString("</blockquote>\n\n")
	}
	sb.WriteString("━━━━━━━━━━━━\n")
	sb.WriteString("👉 <i>Gunakan tombol navigasi di bawah untuk beralih mode &amp; halaman.</i>")

	return sb.String()
}

// FormatCategoryLeaderboard composes the leaderboard display for a specific category
func FormatCategoryLeaderboard(category string, entries []storage.CategoryLeaderboardEntry, page, limit int) string {
	if len(entries) == 0 {
		return fmt.Sprintf("🎯 <b>PERINGKAT ASPEK: %s</b> 🎯\n\n<blockquote>Belum ada data untuk aspek ini.</blockquote>", html.EscapeString(category))
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("🎯 <b>PERINGKAT ASPEK: %s</b> 🎯\n", html.EscapeString(category)))
	sb.WriteString("━━━━━━━━━━━━\n\n")

	for i, e := range entries {
		pos := i + 1 + page*limit
		medal := "▫️"
		switch pos {
		case 1:
			medal = "🥇"
		case 2:
			medal = "🥈"
		case 3:
			medal = "🥉"
		}
		sb.WriteString(fmt.Sprintf("%s <b>#%d %s</b>\n", medal, pos, html.EscapeString(e.ModelName)))
		sb.WriteString("<blockquote>")
		sb.WriteString(fmt.Sprintf("🏆 Skor Tertinggi: <b>%d / %d pts</b>\n", e.PeakScore, e.MaxScore))
		sb.WriteString(fmt.Sprintf("📈 Total Uji: <b>%d kali</b>", e.TotalRuns))
		sb.WriteString("</blockquote>\n\n")
	}
	sb.WriteString("━━━━━━━━━━━━")
	return sb.String()
}

// FormatHistory composes the display of user's personal benchmark history
func FormatHistory(runs []storage.BenchmarkRun) string {
	if len(runs) == 0 {
		return "📜 <b>RIWAYAT PENGUJIAN ANDA</b> 📜\n\n<blockquote>Anda belum pernah menjalankan pengujian benchmark. Ketik /benchmark untuk memulai.</blockquote>"
	}

	var sb strings.Builder
	sb.WriteString("📜 <b>RIWAYAT PENGUJIAN TERAKHIR ANDA</b> 📜\n")
	sb.WriteString("━━━━━━━━━━━━\n\n")

	for i, r := range runs {
		status := "Direkomendasikan"
		badge := "✅"
		switch {
		case r.TotalScore >= 90:
			status = "Sangat Direkomendasikan"
			badge = "🌟"
		case r.TotalScore >= 80:
			status = "Direkomendasikan"
			badge = "✅"
		case r.TotalScore >= 70:
			status = "Baik"
			badge = "👍"
		case r.TotalScore >= 50:
			status = "Cukup"
			badge = "⚠️"
		default:
			status = "Perlu Pertimbangan"
			badge = "❌"
		}

		sb.WriteString(fmt.Sprintf("%s <b>#%d %s</b> — Skor: <b>%d/%d</b> (%s)\n", badge, i+1, html.EscapeString(r.ModelName), r.TotalScore, r.MaxScore, status))
		sb.WriteString("<blockquote>")
		sb.WriteString(fmt.Sprintf("⏱️ Waktu: <b>%s</b> • ID: <code>%s</code>", r.CreatedAt.Format("02 Jan 15:04"), r.ID[:8]))
		if r.EstimatedCostUSD > 0 {
			sb.WriteString(fmt.Sprintf(" • Biaya: <b>~$%.4f USD</b>", r.EstimatedCostUSD))
		}
		sb.WriteString("</blockquote>\n\n")
	}
	sb.WriteString("━━━━━━━━━━━━\n")
	sb.WriteString("👉 <i>Pilih salah satu hasil di bawah untuk melihat rincian:</i>")

	return sb.String()
}

// FormatRunFullDetails composes full details of a single run session for user inspection
func FormatRunFullDetails(run *storage.BenchmarkRun, details []storage.BenchmarkDetail) string {
	var sb strings.Builder
	sb.WriteString("📋 <b>RINCIAN LENGKAP HASIL PENGUJIAN</b> 📋\n")
	sb.WriteString("━━━━━━━━━━━━\n")

	sb.WriteString("<blockquote>")
	sb.WriteString(fmt.Sprintf("🤖 <b>Model:</b> <code>%s</code>\n", html.EscapeString(run.ModelName)))
	sb.WriteString(fmt.Sprintf("🌐 <b>Penyedia:</b> <code>%s</code>\n", html.EscapeString(run.ProviderBaseURL)))
	sb.WriteString(fmt.Sprintf("🎯 <b>Skor Akhir:</b> <b>%d / %d</b>\n", run.TotalScore, run.MaxScore))
	sb.WriteString(fmt.Sprintf("⏱️ <b>Waktu Uji:</b> %.1f detik\n", float64(run.ExecutionTimeMs)/1000.0))
	if run.TotalTokens > 0 {
		costEst := pricing.CalculateCost(run.ModelName, run.PromptTokens, run.CompletionTokens, run.TotalScore)
		if costEst.HasPricing {
			sb.WriteString(fmt.Sprintf("💵 <b>Efisiensi Biaya:</b> %s\n", costEst.CostDescription))
		}
	}
	sb.WriteString(fmt.Sprintf("📅 <b>Waktu:</b> %s\n", run.CreatedAt.Format("02 Jan 2006 15:04:05")))
	sb.WriteString(fmt.Sprintf("🆔 <b>ID Pengujian:</b> <code>%s</code>", run.ID))
	sb.WriteString("</blockquote>\n\n")

	sb.WriteString("<b>Evaluasi Behavioral (per Suite):</b>\n<pre>")
	for _, d := range details {
		badge := "OK "
		status := "Lolos"
		if d.Score == 0 {
			badge = "XX "
			status = "Gagal"
		} else if d.Score < d.MaxScore {
			badge = "!! "
			status = "Sebagian"
		}
		// Escaping is mandatory inside <pre> — category names contain '&'
		// (e.g. "Kompilasi & Build (-race)") which will break Telegram HTML
		// parser if left raw.
		sb.WriteString(fmt.Sprintf("%s%-42s : %2d/%2d pts [%s]\n",
			badge,
			html.EscapeString(d.CategoryName),
			d.Score, d.MaxScore,
			status))
		for _, det := range d.Details {
			// Literal '>' can also interfere with Telegram HTML parser even
			// inside <pre>. Use neutral bullet '-'.
			sb.WriteString(fmt.Sprintf("   - %s\n", html.EscapeString(det)))
		}
	}
	sb.WriteString(fmt.Sprintf("--------------------------------------------------\nTOTAL SKOR                                 : %2d/%2d pts\n",
		run.TotalScore, run.MaxScore))
	sb.WriteString("</pre>\n")

	if run.TotalTokens > 0 {
		sb.WriteString(fmt.Sprintf("<b>Penggunaan Token:</b>\n<pre>Input: %d | Output: %d | Total: %d</pre>\n",
			run.PromptTokens, run.CompletionTokens, run.TotalTokens))
	}

	if run.ErrorSummary != "" {
		sb.WriteString(fmt.Sprintf("<b>Catatan Pemrosesan:</b>\n<blockquote><code>%s</code></blockquote>\n", html.EscapeString(run.ErrorSummary)))
	}
	sb.WriteString("━━━━━━━━━━━━")

	return sb.String()
}

// GenerateRunCSV generates a CSV byte array from benchmark results
func GenerateRunCSV(run *storage.BenchmarkRun, details []storage.BenchmarkDetail) []byte {
	var sb strings.Builder
	sb.WriteString("Kategori,Skor,Skor Maksimal,Status\n")

	for _, d := range details {
		status := "Lolos"
		if d.Score == 0 {
			status = "Gagal"
		} else if d.Score < d.MaxScore {
			status = "Sebagian"
		}
		sb.WriteString(fmt.Sprintf("\"%s\",%d,%d,\"%s\"\n", d.CategoryName, d.Score, d.MaxScore, status))
	}
	sb.WriteString(fmt.Sprintf("\"TOTAL\",%d,%d,\"%s\"\n", run.TotalScore, run.MaxScore, run.Status))
	if run.TotalTokens > 0 {
		sb.WriteString(fmt.Sprintf("\"TOKEN_PROMPT\",%d,,\n", run.PromptTokens))
		sb.WriteString(fmt.Sprintf("\"TOKEN_COMPLETION\",%d,,\n", run.CompletionTokens))
		sb.WriteString(fmt.Sprintf("\"TOKEN_TOTAL\",%d,,\n", run.TotalTokens))
	}
	if run.EstimatedCostUSD > 0 || run.CostTier != "" {
		sb.WriteString(fmt.Sprintf("\"ESTIMATED_COST_USD\",%.6f,,\n", run.EstimatedCostUSD))
		sb.WriteString(fmt.Sprintf("\"COST_TIER\",\"%s\",,\n", run.CostTier))
	}
	return []byte(sb.String())
}

// FormatAdminHelp generates guidance text for bot administration commands
func FormatAdminHelp() string {
	return `⚙️ <b>PANEL ADMINISTRASI SISTEM</b> ⚙️
━━━━━━━━━━━━
Perintah khusus admin yang tersedia:

👉 <code>/admin stats</code>
<blockquote>Melihat ringkasan performa bot, beban antrean worker, dan metrik pengujian.</blockquote>

👉 <code>/admin ban &lt;user_id&gt;</code>
<blockquote>Menonaktifkan akses Telegram pengguna.</blockquote>

👉 <code>/admin unban &lt;user_id&gt;</code>
<blockquote>Mengaktifkan kembali akses pengguna.</blockquote>

👉 <code>/admin resetquota &lt;user_id&gt;</code>
<blockquote>Mereset batasan kuota pengujian pengguna.</blockquote>
━━━━━━━━━━━━`
}

// FormatAdminStartupMessage formats an automatic notification sent to admins when the bot starts up
func FormatAdminStartupMessage(botUsername string, startTime time.Time, maxWorkers, queueCap int) string {
	timeStr := startTime.Format("02 Jan 2006, 15:04:05 MST")
	return fmt.Sprintf(`🚀 <b>ON MY MARK BOT TELAH DIAKTIFKAN</b> 🚀
━━━━━━━━━━━━
🤖 <b>Bot Username:</b> @%s
🟢 <b>Status Layanan:</b> Online &amp; Siap Menerima Pesan
⏱️ <b>Waktu Booting:</b> <code>%s</code>
⚙️ <b>Kapasitas Worker:</b> %d workers (Antrean: %d)
━━━━━━━━━━━━
💡 <i>Ketik /admin stats untuk melihat kondisi sistem kapan saja.</i>`,
		html.EscapeString(botUsername),
		html.EscapeString(timeStr),
		maxWorkers,
		queueCap,
	)
}

// FormatAdminStats formats comprehensive system metrics for admins
func FormatAdminStats(stats *storage.SystemStats, activeWorkers, maxWorkers, queueLen, queueCap int, uptime time.Duration) string {
	var sb strings.Builder
	sb.WriteString("📊 <b>STATISTIK ADMINISTRASI SISTEM</b> 📊\n")
	sb.WriteString("━━━━━━━━━━━━\n")
	sb.WriteString("<blockquote>")
	sb.WriteString(fmt.Sprintf("⏱️ <b>Waktu Aktif:</b> %s\n", uptime.Round(time.Second).String()))
	sb.WriteString(fmt.Sprintf("👥 <b>Total Pengguna:</b> %d (Diblokir: %d)\n", stats.TotalUsers, stats.BannedUsers))
	sb.WriteString(fmt.Sprintf("🤖 <b>Model Unik Diuji:</b> %d model\n", stats.UniqueModels))
	if stats.TotalCostUSD > 0 {
		sb.WriteString(fmt.Sprintf("💵 <b>Total Estimasi Biaya Token:</b> ~$%.4f USD", stats.TotalCostUSD))
	}
	sb.WriteString("</blockquote>\n\n")

	successPercent := 0.0
	if stats.TotalRuns > 0 {
		successPercent = (float64(stats.SuccessRuns) / float64(stats.TotalRuns)) * 100.0
	}

	sb.WriteString("<b>Ringkasan Pengujian:</b>\n")
	sb.WriteString("<pre>")
	sb.WriteString(fmt.Sprintf("• Total Pengujian      : %d\n", stats.TotalRuns))
	sb.WriteString(fmt.Sprintf("• Berhasil             : %d (%.1f%%)\n", stats.SuccessRuns, successPercent))
	sb.WriteString(fmt.Sprintf("• Gagal                : %d (%.1f%%)\n", stats.FailedRuns, stats.ErrorRatePercent))
	sb.WriteString(fmt.Sprintf("• Pelanggaran Keamanan : %d\n", stats.ViolationRuns))
	sb.WriteString("</pre>\n")

	sb.WriteString("<b>Kondisi Antrean (Worker):</b>\n")
	sb.WriteString("<pre>")
	sb.WriteString(fmt.Sprintf("• Worker Aktif         : %d / %d\n", activeWorkers, maxWorkers))
	sb.WriteString(fmt.Sprintf("• Antrean Menunggu     : %d / %d\n", queueLen, queueCap))
	sb.WriteString("</pre>\n")
	sb.WriteString("━━━━━━━━━━━━")

	return sb.String()
}

// FormatProgressMessage composes a live progress update message for Telegram
func FormatProgressMessage(info queue.ProgressInfo) string {
	var sb strings.Builder
	sb.WriteString("⚡ <b>Proses Benchmark Sedang Berjalan</b>\n")
	sb.WriteString("━━━━━━━━━━━━\n")

	if info.Total > 0 && info.Step > 0 {
		tierDisplay := info.Tier
		if info.TierNumber > 0 {
			tierDisplay = fmt.Sprintf("Tier %d (%s)", info.TierNumber, info.Tier)
		}
		sb.WriteString(fmt.Sprintf("📊 <b>Kemajuan:</b> [%d/%d] %s\n", info.Step, info.Total, html.EscapeString(tierDisplay)))
		if info.TaskTitle != "" {
			sb.WriteString(fmt.Sprintf("🎯 <b>Task:</b> %s\n", html.EscapeString(info.TaskTitle)))
		}
		if info.Attempt > 0 {
			maxAtt := info.MaxAttempts
			if maxAtt <= 0 {
				maxAtt = 2
			}
			sb.WriteString(fmt.Sprintf("🔄 <b>Status:</b> Turn %d/%d\n", info.Attempt, maxAtt))
		}
		if info.Timeout > 0 {
			elapsedMin := int(info.Elapsed.Minutes())
			elapsedSec := int(info.Elapsed.Seconds()) % 60
			timeoutMin := int(info.Timeout.Minutes())
			timeoutSec := int(info.Timeout.Seconds()) % 60
			sb.WriteString(fmt.Sprintf("⏱️ <b>Waktu Tahap:</b> %02d:%02d / %02d:%02d\n",
				elapsedMin, elapsedSec, timeoutMin, timeoutSec))
		}
	} else if info.StatusText != "" {
		sb.WriteString(fmt.Sprintf("<blockquote>⏳ %s</blockquote>\n", html.EscapeString(info.StatusText)))
	}

	switch info.Phase {
	case "REASONING":
		sb.WriteString("\n🧠 <b>Aktivitas AI:</b> <i>Sedang bernalar (reasoning)...</i>\n")
		if info.Snippet != "" {
			sb.WriteString(fmt.Sprintf("<blockquote><code>%s</code></blockquote>\n", html.EscapeString(info.Snippet)))
		} else {
			sb.WriteString("<blockquote><i>Menunggu proses penalaran model...</i></blockquote>\n")
		}
	case "CODING":
		sb.WriteString("\n💻 <b>Aktivitas AI:</b> <i>Sedang menulis kode Go...</i>\n")
		if info.Snippet != "" {
			sb.WriteString(fmt.Sprintf("<pre>%s</pre>\n", html.EscapeString(info.Snippet)))
		} else {
			sb.WriteString("<blockquote><i>Menerima token kode...</i></blockquote>\n")
		}
	case "EVALUATING":
		sb.WriteString("\n🧪 <b>Aktivitas Sandbox:</b> <i>Menjalankan test suite (go test -race)...</i>\n")
		if info.StatusText != "" {
			sb.WriteString(fmt.Sprintf("<blockquote>%s</blockquote>\n", html.EscapeString(info.StatusText)))
		}
	default:
		if info.StatusText != "" && info.Step > 0 {
			sb.WriteString(fmt.Sprintf("\nℹ️ <b>Status:</b> %s\n", html.EscapeString(info.StatusText)))
		}
	}

	sb.WriteString("━━━━━━━━━━━━\n")
	sb.WriteString("💡 <i>Pembaruan live setiap ~7 detik.</i>")

	return sb.String()
}
