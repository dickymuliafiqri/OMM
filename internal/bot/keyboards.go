package bot

import (
	"fmt"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"benchmark/internal/storage"
)

// MainMenuKeyboard creates reply keyboard at the bottom navigation of the chat
func MainMenuKeyboard() tgbotapi.ReplyKeyboardMarkup {
	kb := tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("🚀 Mulai Benchmark"),
			tgbotapi.NewKeyboardButton("🏆 Leaderboard"),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("📜 Riwayat Saya"),
			tgbotapi.NewKeyboardButton("ℹ️ Panduan & Rubrik"),
		),
	)
	kb.ResizeKeyboard = true
	kb.InputFieldPlaceholder = "Pilih menu atau ketik /benchmark"
	return kb
}

// BaseURLKeyboard presents preset buttons for the most popular AI providers
func BaseURLKeyboard() tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🟢 OpenAI", "preset_url_openai"),
			tgbotapi.NewInlineKeyboardButtonData("🟣 OpenRouter", "preset_url_openrouter"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⚡ Groq", "preset_url_groq"),
			tgbotapi.NewInlineKeyboardButtonData("🐋 DeepSeek", "preset_url_deepseek"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🦙 Ollama (Lokal)", "preset_url_ollama"),
			tgbotapi.NewInlineKeyboardButtonData("✏️ Ketik URL Sendiri", "preset_url_custom"),
		),
	)
}

// SavedCredsKeyboard presents options to use saved credentials or input new ones
func SavedCredsKeyboard() tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✅ Gunakan Kredensial Tersimpan", "use_saved_creds"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✏️ Input Kredensial Baru", "use_new_creds"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🗑️ Hapus Kredensial (/resetkey)", "clear_saved_creds"),
		),
	)
}

// ModelListPaginationKeyboard creates an inline keyboard for model list from /models endpoint with pagination
func ModelListPaginationKeyboard(models []string, page, pageSize int) tgbotapi.InlineKeyboardMarkup {
	if pageSize <= 0 {
		pageSize = 5
	}
	totalModels := len(models)
	totalPages := (totalModels + pageSize - 1) / pageSize
	if totalPages == 0 {
		totalPages = 1
	}
	if page < 0 {
		page = 0
	}
	if page >= totalPages {
		page = totalPages - 1
	}

	start := page * pageSize
	end := start + pageSize
	if end > totalModels {
		end = totalModels
	}

	var rows [][]tgbotapi.InlineKeyboardButton

	// Model buttons (1 model per row so long model names fit neatly on mobile)
	for i := start; i < end; i++ {
		modelName := models[i]
		displayName := modelName
		if len(displayName) > 36 {
			displayName = displayName[:33] + "..."
		}
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(displayName, fmt.Sprintf("model_sel_%d", i)),
		))
	}

	// Pagination navigation row
	var navRow []tgbotapi.InlineKeyboardButton
	if page > 0 {
		navRow = append(navRow, tgbotapi.NewInlineKeyboardButtonData("« Prev", fmt.Sprintf("model_page_%d", page-1)))
	}
	navRow = append(navRow, tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("%d/%d (%d model)", page+1, totalPages, totalModels), "noop"))
	if page < totalPages-1 {
		navRow = append(navRow, tgbotapi.NewInlineKeyboardButtonData("Next »", fmt.Sprintf("model_page_%d", page+1)))
	}
	rows = append(rows, navRow)

	// Manual typing option & cancel
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("✏️ Ketik Nama Manual", "model_custom_type"),
		tgbotapi.NewInlineKeyboardButtonData("❌ Batal", "bench_cancel"),
	))

	return tgbotapi.NewInlineKeyboardMarkup(rows...)
}

// ModelPresetKeyboard presents quick model choices as fallback
func ModelPresetKeyboard() tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("gpt-4o", "preset_model_gpt-4o"),
			tgbotapi.NewInlineKeyboardButtonData("gpt-4o-mini", "preset_model_gpt-4o-mini"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("deepseek-chat", "preset_model_deepseek-chat"),
			tgbotapi.NewInlineKeyboardButtonData("claude-3-5-sonnet", "preset_model_claude-3-5-sonnet"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("llama-3.3-70b", "preset_model_llama-3.3-70b-versatile"),
			tgbotapi.NewInlineKeyboardButtonData("✏️ Ketik Nama Model", "preset_model_custom"),
		),
	)
}

// ConfirmationKeyboard presents final execution or cancellation buttons
func ConfirmationKeyboard() tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🚀 Mulai Pengujian Sekarang", "bench_confirm"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("❌ Batalkan", "bench_cancel"),
		),
	)
}

// LeaderboardPaginationKeyboard creates page navigation buttons and category filters for the leaderboard
func LeaderboardPaginationKeyboard(page int, category string) tgbotapi.InlineKeyboardMarkup {
	var navRow []tgbotapi.InlineKeyboardButton

	if page > 0 {
		navRow = append(navRow, tgbotapi.NewInlineKeyboardButtonData("« Prev", fmt.Sprintf("lb_page_%d_%s", page-1, category)))
	}
	navRow = append(navRow, tgbotapi.NewInlineKeyboardButtonData("🔄 Refresh", fmt.Sprintf("lb_page_%d_%s", page, category)))
	navRow = append(navRow, tgbotapi.NewInlineKeyboardButtonData("Next »", fmt.Sprintf("lb_page_%d_%s", page+1, category)))

	filterRow := tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("🏆 Skor Tertinggi", "lb_filter_all"),
		tgbotapi.NewInlineKeyboardButtonData("💎 Efisiensi Biaya", "lb_filter_value"),
	)

	return tgbotapi.NewInlineKeyboardMarkup(
		navRow,
		filterRow,
	)
}

// HistoryItemsKeyboard creates detail buttons for each history item
func HistoryItemsKeyboard(runs []storage.BenchmarkRun) tgbotapi.InlineKeyboardMarkup {
	var rows [][]tgbotapi.InlineKeyboardButton

	for i, r := range runs {
		label := fmt.Sprintf("#%d %s (Skor %d)", i+1, r.ModelName, r.TotalScore)
		data := fmt.Sprintf("hist_detail_%s", r.ID)
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(label, data),
		))
	}

	return tgbotapi.NewInlineKeyboardMarkup(rows...)
}

// RunDetailKeyboard creates action buttons on run detail page (download CSV and source code)
func RunDetailKeyboard(runID string) tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("📊 Unduh CSV", fmt.Sprintf("export_csv_%s", runID)),
			tgbotapi.NewInlineKeyboardButtonData("💻 Unduh Kode Go", fmt.Sprintf("export_code_%s", runID)),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("« Kembali ke Riwayat", "hist_back"),
		),
	)
}

