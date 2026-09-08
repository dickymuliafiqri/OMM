package bot

import (
	"context"
	"errors"
	"fmt"
	"html"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/google/uuid"

	"benchmark/internal/ai"
	"benchmark/internal/queue"
	"benchmark/internal/storage"
	"benchmark/pkg/logger"
)

// BotConfig holds bot initialization parameters
type BotConfig struct {
	Token         string
	HourlyLimit   int
	AdminIDs      []int64
	WhitelistMode bool
	WhitelistIDs  []int64
}

// Bot manages Telegram integration, session state machine, and job queue orchestration
type Bot struct {
	api           *tgbotapi.BotAPI
	pool          *queue.WorkerPool
	repo          storage.Repository
	sessions      *SessionManager
	hourlyLimit   int
	adminIDs      map[int64]bool
	whitelistMode bool
	whitelistIDs  map[int64]bool
	startTime     time.Time
}

// NewBot creates a new Bot instance
func NewBot(cfg BotConfig, pool *queue.WorkerPool, repo storage.Repository) (*Bot, error) {
	if cfg.Token == "" {
		return nil, fmt.Errorf("telegram bot token tidak boleh kosong")
	}
	if cfg.HourlyLimit <= 0 {
		cfg.HourlyLimit = 5 // Default 5 runs per hour
	}

	api, err := tgbotapi.NewBotAPI(cfg.Token)
	if err != nil {
		return nil, fmt.Errorf("gagal inisialisasi telegram bot api: %w", err)
	}

	admins := make(map[int64]bool)
	for _, id := range cfg.AdminIDs {
		admins[id] = true
	}

	whitelists := make(map[int64]bool)
	for _, id := range cfg.WhitelistIDs {
		whitelists[id] = true
	}

	return &Bot{
		api:           api,
		pool:          pool,
		repo:          repo,
		sessions:      NewSessionManager(15 * time.Minute),
		hourlyLimit:   cfg.HourlyLimit,
		adminIDs:      admins,
		whitelistMode: cfg.WhitelistMode,
		whitelistIDs:  whitelists,
		startTime:     time.Now(),
	}, nil
}

// UserName returns the currently active Telegram bot username
func (b *Bot) UserName() string {
	if b.api != nil {
		return b.api.Self.UserName
	}
	return ""
}

// send executes sending a Telegram message/action and logs an elegant 1-line output
func (b *Bot) send(chatID int64, action string, c tgbotapi.Chattable, preview string) (tgbotapi.Message, error) {
	if b.api == nil {
		return tgbotapi.Message{}, nil
	}

	// For actions that return a boolean (such as answerCallbackQuery or deleteMessage),
	// use b.api.Request so telegram-bot-api does not try to unmarshal boolean into tgbotapi.Message struct
	switch c.(type) {
	case tgbotapi.CallbackConfig, tgbotapi.DeleteMessageConfig:
		_, err := b.api.Request(c)
		if err != nil {
			logger.Error("TG:OUT", "chat=%d action=%s failed: %v", chatID, action, err)
			return tgbotapi.Message{}, err
		}
		if action != "ACK_CB" && preview != "" {
			logger.TGOut(chatID, action, preview)
		}
		return tgbotapi.Message{}, nil
	}

	// Use HTML mode by default for all messages and edit text messages
	switch v := c.(type) {
	case tgbotapi.MessageConfig:
		if v.ParseMode == "" || v.ParseMode == tgbotapi.ModeMarkdown {
			v.ParseMode = tgbotapi.ModeHTML
			c = v
		}
	case tgbotapi.EditMessageTextConfig:
		if v.ParseMode == "" || v.ParseMode == tgbotapi.ModeMarkdown {
			v.ParseMode = tgbotapi.ModeHTML
			c = v
		}
	}

	msg, err := b.api.Send(c)
	if err != nil {
		// If Telegram returns "message is not modified", it's benign — return gracefully without error
		if strings.Contains(strings.ToLower(err.Error()), "message is not modified") {
			return tgbotapi.Message{}, nil
		}

		// If Telegram fails to parse entities (HTML), fall back to plain text without formatting
		if strings.Contains(strings.ToLower(err.Error()), "can't parse entities") {
			switch v := c.(type) {
			case tgbotapi.EditMessageTextConfig:
				v.ParseMode = ""
				msg, err = b.api.Send(v)
			case tgbotapi.MessageConfig:
				v.ParseMode = ""
				msg, err = b.api.Send(v)
			}
			if err == nil {
				logger.Warn("TG:OUT", "chat=%d action=%s entity parse error, delivered as plain text", chatID, action)
				return msg, nil
			}
		}
		logger.Error("TG:OUT", "chat=%d action=%s failed: %v", chatID, action, err)
		return msg, err
	}
	if action != "ACK_CB" {
		logger.TGOut(chatID, action, preview)
	}
	return msg, nil
}

// Start begins listening for messages and updates from Telegram
func (b *Bot) Start(ctx context.Context) error {
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := b.api.GetUpdatesChan(u)
	logger.Sys("TELEGRAM", "Bot @%s berhasil terhubung dan siap menerima pesan", b.api.Self.UserName)

	// Send automated startup notification to all configured administrators
	go b.notifyAdminsStartup()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case update, ok := <-updates:
			if !ok {
				return nil
			}
			go b.handleUpdate(ctx, update)
		}
	}
}

// notifyAdminsStartup sends an automated system online notification to all configured admin IDs
func (b *Bot) notifyAdminsStartup() {
	if len(b.adminIDs) == 0 {
		return
	}

	workers := 0
	queueCap := 0
	if b.pool != nil {
		workers = b.pool.MaxWorkers()
		queueCap = b.pool.QueueCapacity()
	}

	text := FormatAdminStartupMessage(b.UserName(), b.startTime, workers, queueCap)

	for adminID := range b.adminIDs {
		go func(id int64) {
			msg := tgbotapi.NewMessage(id, text)
			msg.ParseMode = tgbotapi.ModeHTML
			_, err := b.send(id, "NOTIFY_STARTUP", msg, "Notifikasi bot startup ke admin")
			if err != nil {
				logger.Warn("ADMIN", "Gagal kirim notifikasi startup ke admin ID %d: %v", id, err)
			}
		}(adminID)
	}
}

// handleUpdate routes updates to the appropriate handler
func (b *Bot) handleUpdate(ctx context.Context, update tgbotapi.Update) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("UPDATE", "Panic in handleUpdate: %v", r)
		}
	}()

	if update.Message != nil {
		b.handleMessage(ctx, update.Message)
		return
	}
	if update.CallbackQuery != nil {
		b.handleCallback(ctx, update.CallbackQuery)
		return
	}
}

// checkAccess validates user access rights (whitelist & blacklist/banned)
func (b *Bot) checkAccess(ctx context.Context, userID int64, chatID int64) bool {
	// 1. Check private Whitelist Mode if enabled
	if b.whitelistMode {
		if !b.whitelistIDs[userID] && !b.adminIDs[userID] {
			reply := tgbotapi.NewMessage(chatID, "🚫 <b>Akses Terbatas (Whitelist)</b>\n\n<blockquote>Bot ini sedang dalam mode privat khusus pengguna terdaftar. Hubungi administrator untuk meminta izin akses.</blockquote>")
			reply.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "DENY_ACCESS", reply, "Akses Terbatas (Whitelist)")
			logger.Warn("ACCESS", "user=%d chat=%d rejected (not in whitelist)", userID, chatID)
			return false
		}
	}

	// 2. Check if user is blocked (Blacklist / Banned)
	if b.repo != nil {
		banned, err := b.repo.IsUserBanned(ctx, userID)
		if err != nil {
			logger.Error("ACCESS", "user=%d check banned error: %v", userID, err)
		} else if banned {
			reply := tgbotapi.NewMessage(chatID, "🚫 <b>Akses Dinonaktifkan</b>\n\n<blockquote>Akun Telegram Anda telah dinonaktifkan oleh administrator sistem.</blockquote>")
			reply.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "DENY_ACCESS", reply, "Akses Ditolak (Banned)")
			logger.Warn("ACCESS", "user=%d chat=%d rejected (user is banned)", userID, chatID)
			return false
		}
	}

	return true
}

// handleMessage processes incoming text messages and command routing
func (b *Bot) handleMessage(ctx context.Context, msg *tgbotapi.Message) {
	if msg == nil || msg.From == nil {
		return
	}

	userID := msg.From.ID
	chatID := msg.Chat.ID
	text := strings.TrimSpace(msg.Text)

	// Security Middleware: Whitelist & Banned Check (with 3-second timeout)
	authCtx, authCancel := context.WithTimeout(ctx, 3*time.Second)
	defer authCancel()
	if !b.checkAccess(authCtx, userID, chatID) {
		return
	}

	// Register or update user profile asynchronously in background so DB latency never delays response
	if b.repo != nil {
		go func(uID int64, username, firstName string) {
			dbCtx, dbCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer dbCancel()
			_ = b.repo.UpsertUser(dbCtx, &storage.User{
				TelegramID: uID,
				Username:   username,
				FirstName:  firstName,
			})
		}(userID, msg.From.UserName, msg.From.FirstName)
	}

	session := b.sessions.Get(userID, chatID)

	// Log incoming Telegram message (hide if waiting for API key input)
	if session.State == StateWaitKey && !strings.HasPrefix(text, "/") {
		logger.TGIn(msg.From.UserName, userID, chatID, "[API_KEY_HIDDEN]")
	} else {
		logger.TGIn(msg.From.UserName, userID, chatID, text)
	}

	// Main Command Routing
	lowerText := strings.ToLower(text)
	switch {
	case strings.HasPrefix(text, "/start") || lowerText == "start":
		b.sessions.Clear(userID)
		reply := tgbotapi.NewMessage(chatID, FormatWelcomeMessage(msg.From.FirstName))
		reply.ParseMode = tgbotapi.ModeHTML
		reply.ReplyMarkup = MainMenuKeyboard()
		_, _ = b.send(chatID, "SEND_MSG", reply, "FormatWelcomeMessage")
		return

	case strings.HasPrefix(text, "/help") || lowerText == "help" || text == "Panduan & Rubrik" || text == "ℹ️ Panduan & Rubrik" || lowerText == "bantuan":
		reply := tgbotapi.NewMessage(chatID, FormatHelpMessage())
		reply.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "SEND_MSG", reply, "FormatHelpMessage")
		return

	case strings.HasPrefix(text, "/admin"):
		b.handleAdminCommand(ctx, msg)
		return

	case strings.HasPrefix(text, "/resetkey") || strings.HasPrefix(text, "/reset_key") || text == "Reset API Key" || text == "🗑️ Reset API Key" || lowerText == "reset key" || lowerText == "reset":
		if b.repo != nil {
			_ = b.repo.ResetUserCredentials(ctx, userID)
		}
		b.sessions.Clear(userID)
		reply := tgbotapi.NewMessage(chatID, "🗑️ <b>Kredensial Berhasil Dihapus</b>\n\n<blockquote>Kredensial tersimpan (Base URL dan API Key) telah dihapus dari sistem.</blockquote>\n\n👉 <i>Untuk memulai pengujian baru, ketik /benchmark.</i>")
		reply.ParseMode = tgbotapi.ModeHTML
		reply.ReplyMarkup = MainMenuKeyboard()
		_, _ = b.send(chatID, "SEND_MSG", reply, "Kredensial berhasil dihapus")
		return

	case strings.HasPrefix(text, "/cancel") || lowerText == "cancel" || lowerText == "batal":
		b.sessions.Clear(userID)
		reply := tgbotapi.NewMessage(chatID, "❌ <b>Sesi Dibatalkan</b>\n\n<blockquote>Sesi konfigurasi pengujian telah dibatalkan. Anda dapat memulainya kembali kapan saja dengan /benchmark.</blockquote>")
		reply.ParseMode = tgbotapi.ModeHTML
		reply.ReplyMarkup = MainMenuKeyboard()
		_, _ = b.send(chatID, "SEND_MSG", reply, "Sesi pengujian dibatalkan")
		return

	case strings.HasPrefix(text, "/benchmark") || lowerText == "benchmark" || text == "Mulai Benchmark" || text == "🚀 Mulai Benchmark" || lowerText == "mulai":
		if b.pool != nil && b.pool.IsUserActive(userID) {
			reply := tgbotapi.NewMessage(chatID, "⏳ <b>Pengujian Sedang Berjalan</b>\n\n<blockquote>Anda sudah memiliki proses pengujian yang sedang aktif di antrean. Harap tunggu hingga selesai sebelum memulai yang baru.</blockquote>")
			reply.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "SEND_MSG", reply, "Pengujian sedang berjalan")
			return
		}

		b.sessions.Clear(userID)

		// Check if the user has credentials stored in the database
		if b.repo != nil {
			user, err := b.repo.GetUser(ctx, userID)
			if err == nil && user != nil && user.SavedBaseURL != "" && user.SavedAPIKey != "" {
				msgText := fmt.Sprintf("🔐 <b>Kredensial Tersimpan Ditemukan</b>\n\n<blockquote>🌐 <b>Base URL:</b> <code>%s</code>\n🔑 <b>API Key:</b> <code>%s</code></blockquote>\n\nApakah Anda ingin menggunakan kredensial ini untuk pengujian, atau memasukkan kredensial baru?",
					html.EscapeString(user.SavedBaseURL), MaskAPIKey(user.SavedAPIKey))
				reply := tgbotapi.NewMessage(chatID, msgText)
				reply.ParseMode = tgbotapi.ModeHTML
				reply.ReplyMarkup = SavedCredsKeyboard()
				_, _ = b.send(chatID, "SEND_MSG", reply, "Kredensial Tersimpan Ditemukan")
				return
			}
		}

		session.State = StateWaitURL

		reply := tgbotapi.NewMessage(chatID, "⚙️ <b>Pilih Penyedia AI (Base URL)</b>\n\n<blockquote>Silakan pilih opsi penyedia di bawah atau gunakan <b>Ketik URL Sendiri</b> (OpenAI-compatible):</blockquote>")
		reply.ParseMode = tgbotapi.ModeHTML
		reply.ReplyMarkup = BaseURLKeyboard()
		_, _ = b.send(chatID, "SEND_MSG", reply, "Pilih AI Provider atau masukkan Base URL")
		return

	case strings.HasPrefix(text, "/leaderboard") || lowerText == "leaderboard" || text == "Leaderboard" || text == "🏆 Leaderboard":
		b.showLeaderboard(ctx, chatID, 0, 0, "all")
		return

	case strings.HasPrefix(text, "/history") || lowerText == "history" || text == "Riwayat Saya" || text == "📜 Riwayat Saya" || lowerText == "riwayat":
		b.showHistory(ctx, chatID, userID, 0)
		return

	case strings.HasPrefix(text, "/ping") || lowerText == "ping" || lowerText == "p":
		reply := tgbotapi.NewMessage(chatID, "🏓 <b>Pong!</b> Bot On My Mark aktif dan siap menerima perintah.\n\n👉 Ketik /benchmark untuk mulai pengujian, atau /help untuk panduan.")
		reply.ParseMode = tgbotapi.ModeHTML
		reply.ReplyMarkup = MainMenuKeyboard()
		_, _ = b.send(chatID, "SEND_MSG", reply, "Pong response")
		return

	case strings.HasPrefix(text, "/menu") || lowerText == "menu":
		reply := tgbotapi.NewMessage(chatID, "📋 <b>Menu Utama On My Mark SWE-Bench</b>\n\nSilakan pilih opsi di bawah atau ketik /benchmark untuk memulai pengujian:")
		reply.ParseMode = tgbotapi.ModeHTML
		reply.ReplyMarkup = MainMenuKeyboard()
		_, _ = b.send(chatID, "SEND_MSG", reply, "Menu Utama")
		return
	}

	// Handle text input based on Wizard State
	switch session.State {
	case StateWaitURL:
		cleanURL := strings.TrimRight(strings.ToLower(strings.TrimSpace(text)), "/")
		if cleanURL == "https://ollama.com" || cleanURL == "http://ollama.com" || cleanURL == "https://www.ollama.com" || cleanURL == "http://www.ollama.com" {
			reply := tgbotapi.NewMessage(chatID, "⚠️ <b>Perhatian: Alamat Website Resmi</b>\n\n<blockquote><code>https://ollama.com</code> adalah alamat website resmi, bukan endpoint API.</blockquote>\n\nJika Ollama berjalan di komputer lokal Anda, gunakan:\n<pre>http://localhost:11434/v1</pre>\n\n👉 <i>Silakan ketikkan kembali Base URL yang benar:</i>")
			reply.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "SEND_MSG", reply, "Peringatan URL ollama.com")
			return
		}
		// Automatically append /v1 if user enters default Ollama port without /v1
		if strings.HasSuffix(cleanURL, ":11434") {
			text = strings.TrimRight(text, "/") + "/v1"
		}

		session.BaseURL = text
		session.State = StateWaitKey
		reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("🌐 <b>Base URL Disimpan:</b>\n<blockquote><code>%s</code></blockquote>\n\n🔑 Sekarang masukkan <b>API Key</b> Anda:\n<i>(Pesan berisi API Key akan langsung dihapus otomatis demi keamanan visual)</i>", html.EscapeString(text)))
		reply.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "SEND_MSG", reply, fmt.Sprintf("Base URL diset: %s", text))

	case StateWaitKey:
		session.APIKey = text

		// Instantly delete user message containing API key for visual security
		deleteMsg := tgbotapi.NewDeleteMessage(chatID, msg.MessageID)
		_, _ = b.send(chatID, "DELETE_MSG", deleteMsg, fmt.Sprintf("Hapus pesan key ID: %d", msg.MessageID))

		// Save credentials to database
		if b.repo != nil {
			_ = b.repo.SaveUserCredentials(ctx, userID, session.BaseURL, session.APIKey)
		}

		// Automatically fetch model list from /models
		b.fetchAndShowModels(ctx, chatID, session, 0)

	case StateWaitModel:
		b.testAndSelectModel(ctx, chatID, session, text, 0)

	case StateConfirm:
		reply := tgbotapi.NewMessage(chatID, "👉 Silakan klik tombol <b>Mulai Pengujian Sekarang</b> atau ketik /cancel untuk membatalkan.")
		reply.ParseMode = tgbotapi.ModeHTML
		reply.ReplyMarkup = ConfirmationKeyboard()
		_, _ = b.send(chatID, "SEND_MSG", reply, "Menunggu konfirmasi eksekusi benchmark")

	default:
		// Fallback when message is received in StateIdle and does not match any command
		reply := tgbotapi.NewMessage(chatID, "👋 Halo! Saya adalah bot evaluasi <b>On My Mark Native Go SWE-Bench</b>.\n\n👉 Ketik /benchmark untuk mulai pengujian model AI, atau gunakan tombol menu di bawah:")
		reply.ParseMode = tgbotapi.ModeHTML
		reply.ReplyMarkup = MainMenuKeyboard()
		_, _ = b.send(chatID, "SEND_MSG", reply, "Fallback unknown message")
	}
}

// handleCallback processes InlineKeyboard button clicks
func (b *Bot) handleCallback(ctx context.Context, cb *tgbotapi.CallbackQuery) {
	if cb == nil || cb.From == nil {
		return
	}

	userID := cb.From.ID
	var chatID int64
	if cb.Message != nil {
		chatID = cb.Message.Chat.ID
	} else {
		chatID = userID
	}
	data := cb.Data

	logger.TGCB(cb.From.UserName, userID, chatID, data)

	// Acknowledge callback immediately
	ack := tgbotapi.NewCallback(cb.ID, "")
	_, _ = b.send(chatID, "ACK_CB", ack, "")

	if !b.checkAccess(ctx, userID, chatID) {
		return
	}

	session := b.sessions.Get(userID, chatID)

	switch {
	// Saved Credentials
	case data == "use_saved_creds":
		if b.repo == nil {
			reply := tgbotapi.NewMessage(chatID, "Basis data belum terhubung.")
			_, _ = b.send(chatID, "SEND_MSG", reply, "Basis data belum terhubung")
			return
		}
		user, err := b.repo.GetUser(ctx, userID)
		if err != nil || user == nil || user.SavedBaseURL == "" || user.SavedAPIKey == "" {
			edit := tgbotapi.NewEditMessageText(chatID, cb.Message.MessageID, "⚠️ <b>Kredensial Tidak Ditemukan</b>\n\n<blockquote>Kredensial belum tersimpan di database. Silakan masukkan secara manual:</blockquote>")
			edit.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "EDIT_MSG", edit, "Kredensial tidak ditemukan")
			session.State = StateWaitURL
			reply := tgbotapi.NewMessage(chatID, "⚙️ <b>Pilih Penyedia AI atau masukkan Base URL:</b>")
			reply.ParseMode = tgbotapi.ModeHTML
			reply.ReplyMarkup = BaseURLKeyboard()
			_, _ = b.send(chatID, "SEND_MSG", reply, "Pilih AI Provider")
			return
		}
		session.BaseURL = user.SavedBaseURL
		session.APIKey = user.SavedAPIKey
		b.fetchAndShowModels(ctx, chatID, session, cb.Message.MessageID)

	case data == "use_new_creds":
		b.sessions.Clear(userID)
		session.State = StateWaitURL
		edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, cb.Message.MessageID,
			"⚙️ <b>Pilih Penyedia AI (Base URL)</b>\n\n<blockquote>Pilih opsi di bawah atau gunakan <b>Ketik URL Sendiri</b> (OpenAI-compatible):</blockquote>",
			BaseURLKeyboard())
		edit.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "EDIT_MSG", edit, "Pilih AI Provider atau masukkan Base URL")

	case data == "clear_saved_creds":
		if b.repo != nil {
			_ = b.repo.ResetUserCredentials(ctx, userID)
		}
		b.sessions.Clear(userID)
		edit := tgbotapi.NewEditMessageText(chatID, cb.Message.MessageID, "🗑️ <b>Kredensial Dihapus</b>\n\n<blockquote>Kredensial Anda berhasil dihapus dari sistem.</blockquote>\n\n👉 <i>Ketik /benchmark untuk memulai kembali.</i>")
		edit.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "EDIT_MSG", edit, "Kredensial berhasil dihapus dari database")

	// Preset Base URL
	case strings.HasPrefix(data, "preset_url_"):
		target := strings.TrimPrefix(data, "preset_url_")
		var keyHint string
		switch target {
		case "openai":
			session.BaseURL = "https://api.openai.com/v1"
			keyHint = "🔑 Masukkan <b>API Key OpenAI</b> Anda:\n<i>(Pesan berisi API Key akan langsung dihapus otomatis demi keamanan visual)</i>"
		case "openrouter":
			session.BaseURL = "https://openrouter.ai/api/v1"
			keyHint = "🔑 Masukkan <b>API Key OpenRouter</b> Anda:\n<i>(Pesan berisi API Key akan langsung dihapus otomatis demi keamanan visual)</i>"
		case "groq":
			session.BaseURL = "https://api.groq.com/openai/v1"
			keyHint = "🔑 Masukkan <b>API Key Groq</b> Anda:\n<i>(Pesan berisi API Key akan langsung dihapus otomatis demi keamanan visual)</i>"
		case "deepseek":
			session.BaseURL = "https://api.deepseek.com"
			keyHint = "🔑 Masukkan <b>API Key DeepSeek</b> Anda:\n<i>(Pesan berisi API Key akan langsung dihapus otomatis demi keamanan visual)</i>"
		case "ollama":
			session.BaseURL = "http://localhost:11434/v1"
			keyHint = "🔑 Ketikkan sembarang teks untuk API Key Ollama (misal: <code>ollama</code>):"
		case "custom":
			session.State = StateWaitURL
			reply := tgbotapi.NewMessage(chatID, "🌐 <b>Ketikkan Base URL Penyedia Anda:</b>\n\n<blockquote>Contoh:\n• <code>http://localhost:11434/v1</code> (Ollama)\n• <code>https://api.openai.com/v1</code> (OpenAI)</blockquote>")
			reply.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "SEND_MSG", reply, "Ketikkan Base URL provider")
			return
		}

		session.State = StateWaitKey
		edit := tgbotapi.NewEditMessageText(chatID, cb.Message.MessageID, fmt.Sprintf("🌐 <b>Base URL Diset:</b>\n<blockquote><code>%s</code></blockquote>\n\n%s", session.BaseURL, keyHint))
		edit.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "EDIT_MSG", edit, fmt.Sprintf("Base URL diset: %s", session.BaseURL))

	// Model selection from /models list
	case strings.HasPrefix(data, "model_sel_"):
		idxStr := strings.TrimPrefix(data, "model_sel_")
		idx, err := strconv.Atoi(idxStr)
		if err != nil || idx < 0 || idx >= len(session.AvailableModels) {
			reply := tgbotapi.NewMessage(chatID, "⚠️ Pilihan model tidak valid atau sesi telah kedaluwarsa. Silakan ulangi dengan /benchmark.")
			_, _ = b.send(chatID, "SEND_MSG", reply, "Model tidak valid / sesi expired")
			return
		}
		selectedModel := session.AvailableModels[idx]
		b.testAndSelectModel(ctx, chatID, session, selectedModel, cb.Message.MessageID)

	// Model list pagination
	case strings.HasPrefix(data, "model_page_"):
		pageStr := strings.TrimPrefix(data, "model_page_")
		page, err := strconv.Atoi(pageStr)
		if err == nil && len(session.AvailableModels) > 0 {
			session.ModelPage = page
			markup := ModelListPaginationKeyboard(session.AvailableModels, page, 5)
			edit := tgbotapi.NewEditMessageReplyMarkup(chatID, cb.Message.MessageID, markup)
			_, _ = b.send(chatID, "EDIT_MARKUP", edit, fmt.Sprintf("Paginasi model hal %d", page+1))
		}

	// Manual typing option from pagination keyboard
	case data == "model_custom_type":
		session.State = StateWaitModel
		reply := tgbotapi.NewMessage(chatID, "🤖 <b>Ketikkan Nama Model Target:</b>\n\n<blockquote>Contoh:\n• <code>qwen-2.5-coder-32b-instruct</code>\n• <code>deepseek-chat</code>\n• <code>gpt-4o-mini</code></blockquote>")
		reply.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "SEND_MSG", reply, "Ketikkan nama model target")

	case data == "noop":
		// Pagination indicator button that performs no action
		return

	// Preset Model (Fallback)
	case strings.HasPrefix(data, "preset_model_"):
		target := strings.TrimPrefix(data, "preset_model_")
		if target == "custom" {
			session.State = StateWaitModel
			reply := tgbotapi.NewMessage(chatID, "🤖 <b>Ketikkan Nama Model Target:</b>\n\n<blockquote>Contoh:\n• <code>qwen-2.5-coder-32b-instruct</code>\n• <code>deepseek-chat</code>\n• <code>gpt-4o-mini</code></blockquote>")
			reply.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "SEND_MSG", reply, "Ketikkan nama model target")
			return
		}

		b.testAndSelectModel(ctx, chatID, session, target, cb.Message.MessageID)

	// Benchmark Execution Confirmation
	case data == "bench_confirm":
		// Remove inline keyboard from confirmation message so it cannot be pressed repeatedly
		rmMarkup := tgbotapi.NewEditMessageReplyMarkup(chatID, cb.Message.MessageID, tgbotapi.InlineKeyboardMarkup{
			InlineKeyboard: [][]tgbotapi.InlineKeyboardButton{},
		})
		_, _ = b.send(chatID, "REMOVE_KEYBOARD", rmMarkup, "Hapus tombol konfirmasi benchmark")

		if session.BaseURL == "" || session.Model == "" {
			reply := tgbotapi.NewMessage(chatID, "⚠️ Parameter belum lengkap. Silakan ketik /benchmark untuk mengulang.")
			_, _ = b.send(chatID, "SEND_MSG", reply, "Parameter belum lengkap")
			return
		}

		// Deduplication: Ensure user does not have an active job
		if b.pool != nil && b.pool.IsUserActive(userID) {
			edit := tgbotapi.NewEditMessageText(chatID, cb.Message.MessageID, "⏳ <b>Pengujian Sedang Berjalan</b>\n\n<blockquote>Anda sudah memiliki proses pengujian yang sedang aktif di antrean. Harap tunggu hingga selesai.</blockquote>")
			edit.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "EDIT_MSG", edit, "Job sedang berjalan di antrean")
			return
		}

		// Check user rate limit (Admin is exempt)
		if b.repo != nil && !b.adminIDs[userID] {
			allowed, remaining, retryAfter, err := b.repo.CheckAndUpdateRateLimit(ctx, userID, b.hourlyLimit)
			if err != nil {
				logger.Error("RATELIMIT", "user=%d check error: %v", userID, err)
			} else if !allowed {
				reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("⚠️ <b>Batas Kuota Pengujian Tercapai</b>\n\n<blockquote>Batas kuota pengujian per jam telah tercapai.\nAnda dapat menjalankan benchmark berikutnya dalam <b>%v</b>.\n(Sisa kuota: <b>%d</b>)</blockquote>", retryAfter.Round(time.Minute), remaining))
				reply.ParseMode = tgbotapi.ModeHTML
				_, _ = b.send(chatID, "SEND_MSG", reply, "Batas kuota per jam tercapai")
				return
			}
		}

		// Send initial status
		statusMsg, err := b.send(chatID, "SEND_STATUS", tgbotapi.NewMessage(chatID, "⏳ <b>Menyiapkan Pengujian...</b>\n<blockquote>[0/4] Mengalokasikan worker sandbox &amp; antrean...</blockquote>"), "Menyiapkan antrean pengujian")
		if err != nil {
			logger.Error("TG:OUT", "Gagal mengirim status message: %v", err)
			return
		}

		// Setup throttler for live message edit updates (7 seconds interval)
		throttler := queue.NewProgressThrottler(7*time.Second, func(text string) {
			edit := tgbotapi.NewEditMessageText(chatID, statusMsg.MessageID, text)
			edit.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "EDIT_PROGRESS", edit, "Live progress update")
		})

		jobID := uuid.New().String()
		job := &queue.BenchmarkJob{
			ID:         jobID,
			TelegramID: userID,
			ChatID:     chatID,
			MessageID:  statusMsg.MessageID,
			BaseURL:    session.BaseURL,
			APIKey:     session.APIKey,
			Model:      session.Model,
			CreatedAt:  time.Now(),
			Ctx:        ctx,
			ResultChan: make(chan *queue.JobResult, 1),
			OnProgress: func(info queue.ProgressInfo) {
				throttler.Update(FormatProgressMessage(info))
			},
		}

		// Reset session state
		b.sessions.Clear(userID)

		// Submit to Worker Pool
		if err := b.pool.Submit(job); err != nil {
			throttler.Close()
			errMsg := fmt.Sprintf("Gagal memulai pengujian: %v", err)
			if errors.Is(err, queue.ErrUserJobActive) {
				errMsg = "Anda sudah memiliki proses pengujian yang sedang berjalan di antrean."
			}
			edit := tgbotapi.NewEditMessageText(chatID, statusMsg.MessageID, fmt.Sprintf("❌ <b>Gagal Memulai Pengujian:</b>\n<blockquote>%s</blockquote>", html.EscapeString(errMsg)))
			edit.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "EDIT_MSG", edit, errMsg)
			return
		}

		// Goroutine waiting for results
		go func() {
			res := <-job.ResultChan
			throttler.Flush()
			throttler.Close()

			if res.Error != nil {
				errMsg := fmt.Sprintf("❌ <b>Evaluasi Pengujian Tidak Berhasil</b>\n\n<blockquote>🤖 <b>Model:</b> <code>%s</code></blockquote>\n\n<b>Detail Kesalahan:</b>\n<pre>%s</pre>\n\n👉 <i>Silakan periksa koneksi endpoint, kuota saldo API Key, atau ejaan nama model.</i>", html.EscapeString(job.Model), html.EscapeString(res.Error.Error()))
				edit := tgbotapi.NewEditMessageText(chatID, statusMsg.MessageID, errMsg)
				edit.ParseMode = tgbotapi.ModeHTML
				_, _ = b.send(chatID, "EDIT_MSG", edit, fmt.Sprintf("Evaluasi gagal: %s", job.Model))
				return
			}

			// Render aesthetic final scorecard
			card := FormatSWEScoreCard(res.Run, res.SWETasks)
			edit := tgbotapi.NewEditMessageText(chatID, statusMsg.MessageID, card)
			edit.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "EDIT_SCORE", edit, fmt.Sprintf("Scorecard %s: %d/%d", res.Run.ModelName, res.Run.TotalScore, res.Run.MaxScore))
		}()

	case data == "bench_cancel":
		b.sessions.Clear(userID)
		edit := tgbotapi.NewEditMessageTextAndMarkup(
			chatID,
			cb.Message.MessageID,
			"❌ <b>Sesi Dibatalkan</b>\n\n<blockquote>Sesi pengujian telah dibatalkan. Anda dapat memulainya kembali kapan saja dengan /benchmark.</blockquote>",
			tgbotapi.InlineKeyboardMarkup{InlineKeyboard: [][]tgbotapi.InlineKeyboardButton{}},
		)
		edit.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "EDIT_MSG", edit, "Sesi pengujian telah dibatalkan")

	// Filter Leaderboard Category
	case strings.HasPrefix(data, "lb_filter_"):
		category := strings.TrimPrefix(data, "lb_filter_")
		b.showLeaderboard(ctx, chatID, cb.Message.MessageID, 0, category)

	// Leaderboard Page Navigation
	case strings.HasPrefix(data, "lb_page_"):
		parts := strings.Split(strings.TrimPrefix(data, "lb_page_"), "_")
		page := 0
		category := "all"
		if len(parts) >= 1 {
			page, _ = strconv.Atoi(parts[0])
		}
		if len(parts) >= 2 {
			category = parts[1]
		}
		b.showLeaderboard(ctx, chatID, cb.Message.MessageID, page, category)

	// Show full details of a specific run result
	case strings.HasPrefix(data, "hist_detail_"):
		runID := strings.TrimPrefix(data, "hist_detail_")
		b.showRunDetail(ctx, chatID, cb.Message.MessageID, runID)

	// Export CSV
	case strings.HasPrefix(data, "export_csv_"):
		runID := strings.TrimPrefix(data, "export_csv_")
		b.exportCSV(ctx, chatID, runID)

	// Export Go source code
	case strings.HasPrefix(data, "export_code_"):
		runID := strings.TrimPrefix(data, "export_code_")
		b.exportCode(ctx, chatID, runID)

	// Back to History List
	case data == "hist_back":
		b.showHistory(ctx, chatID, userID, 0)
	}
}

// fetchAndShowModels dynamically fetches model list from provider via /models endpoint
func (b *Bot) fetchAndShowModels(ctx context.Context, chatID int64, session *UserSession, editMsgID int) {
	loadingText := "🔍 <b>Memeriksa Daftar Model...</b>\n<blockquote>Mengambil daftar model yang tersedia dari penyedia...\nMohon tunggu sebentar...</blockquote>"
	var currentMsgID int

	logger.Sys("MODELS", "user=%d fetching /models from %s", session.TelegramID, session.BaseURL)

	if editMsgID > 0 {
		currentMsgID = editMsgID
		edit := tgbotapi.NewEditMessageText(chatID, currentMsgID, loadingText)
		edit.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "EDIT_MSG", edit, "Memeriksa daftar /models...")
	} else {
		sent, err := b.send(chatID, "SEND_MSG", tgbotapi.NewMessage(chatID, loadingText), "Memeriksa daftar /models...")
		if err == nil {
			currentMsgID = sent.MessageID
		}
	}

	client, err := ai.NewClient(ai.Config{
		BaseURL: session.BaseURL,
		APIKey:  session.APIKey,
		Timeout: 20 * time.Second,
	})
	if err != nil {
		session.State = StateWaitModel
		msgText := fmt.Sprintf("❌ <b>Gagal Menghubungkan ke Penyedia</b>\n<blockquote>%s</blockquote>\n\n👉 <i>Silakan pilih dari model umum atau ketik nama model secara manual:</i>", html.EscapeString(err.Error()))
		if currentMsgID > 0 {
			edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, currentMsgID, msgText, ModelPresetKeyboard())
			edit.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "EDIT_MSG", edit, "Gagal menghubungkan ke provider")
		} else {
			reply := tgbotapi.NewMessage(chatID, msgText)
			reply.ParseMode = tgbotapi.ModeHTML
			reply.ReplyMarkup = ModelPresetKeyboard()
			_, _ = b.send(chatID, "SEND_MSG", reply, "Gagal menghubungkan ke provider")
		}
		return
	}

	fetchCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	models, err := client.ListModels(fetchCtx)
	if err != nil || len(models) == 0 {
		session.State = StateWaitModel
		errMsg := "Daftar model kosong"
		if err != nil {
			errMsg = err.Error()
		}
		msgText := fmt.Sprintf("⚠️ <b>Daftar Model Otomatis Tidak Tersedia</b>\n<blockquote>Keterangan: %s</blockquote>\n\n👉 <i>Silakan pilih model dari daftar umum atau ketik manual:</i>", html.EscapeString(errMsg))
		if currentMsgID > 0 {
			edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, currentMsgID, msgText, ModelPresetKeyboard())
			edit.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "EDIT_MSG", edit, "Endpoint /models tidak terdeteksi")
		} else {
			reply := tgbotapi.NewMessage(chatID, msgText)
			reply.ParseMode = tgbotapi.ModeHTML
			reply.ReplyMarkup = ModelPresetKeyboard()
			_, _ = b.send(chatID, "SEND_MSG", reply, "Endpoint /models tidak terdeteksi")
		}
		return
	}

	session.AvailableModels = models
	session.ModelPage = 0
	session.State = StateWaitModel

	msgText := fmt.Sprintf("🤖 <b>Daftar Model Ditemukan (%d model)</b>\n\n<blockquote>Silakan pilih model yang ingin diuji dari tombol di bawah:</blockquote>", len(models))
	markup := ModelListPaginationKeyboard(models, 0, 5)

	if currentMsgID > 0 {
		edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, currentMsgID, msgText, markup)
		edit.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "EDIT_MSG", edit, fmt.Sprintf("Ditemukan %d model dari provider", len(models)))
	} else {
		reply := tgbotapi.NewMessage(chatID, msgText)
		reply.ParseMode = tgbotapi.ModeHTML
		reply.ReplyMarkup = markup
		_, _ = b.send(chatID, "SEND_MSG", reply, fmt.Sprintf("Ditemukan %d model dari provider", len(models)))
	}
}

// testAndSelectModel tests model responsiveness with a ping request before proceeding to confirmation
func (b *Bot) testAndSelectModel(ctx context.Context, chatID int64, session *UserSession, modelName string, editMsgID int) {
	testingText := fmt.Sprintf("⚡ <b>Memeriksa Kesiapan Model...</b>\n<blockquote>Menguji ketersediaan model <code>%s</code>...\nMohon tunggu sebentar...</blockquote>", html.EscapeString(modelName))
	var currentMsgID int

	logger.Sys("PING", "user=%d model=%s test started", session.TelegramID, modelName)

	if editMsgID > 0 {
		currentMsgID = editMsgID
		edit := tgbotapi.NewEditMessageText(chatID, currentMsgID, testingText)
		edit.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "EDIT_MSG", edit, "Menguji ketersediaan model "+modelName)
	} else {
		sent, err := b.send(chatID, "SEND_MSG", tgbotapi.NewMessage(chatID, testingText), "Menguji ketersediaan model "+modelName)
		if err == nil {
			currentMsgID = sent.MessageID
		}
	}

	client, err := ai.NewClient(ai.Config{
		BaseURL: session.BaseURL,
		APIKey:  session.APIKey,
		Timeout: 30 * time.Second,
	})
	if err != nil {
		errMsg := fmt.Sprintf("❌ <b>Konfigurasi AI Client Tidak Valid:</b>\n<blockquote>%s</blockquote>", html.EscapeString(err.Error()))
		if currentMsgID > 0 {
			edit := tgbotapi.NewEditMessageText(chatID, currentMsgID, errMsg)
			edit.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "EDIT_MSG", edit, "Konfigurasi AI invalid")
		} else {
			reply := tgbotapi.NewMessage(chatID, errMsg)
			reply.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "SEND_MSG", reply, "Konfigurasi AI invalid")
		}
		return
	}

	testCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	if err := client.TestModel(testCtx, modelName); err != nil {
		errMsg := fmt.Sprintf("❌ <b>Model Tidak Merespons</b>\n\n<blockquote>Model <code>%s</code> tidak merespons atau tidak aktif.\nKeterangan: <code>%s</code></blockquote>\n\n👉 <i>Silakan pilih model lain yang aktif atau periksa ejaan nama model:</i>", html.EscapeString(modelName), html.EscapeString(err.Error()))
		var markup tgbotapi.InlineKeyboardMarkup
		if len(session.AvailableModels) > 0 {
			markup = ModelListPaginationKeyboard(session.AvailableModels, session.ModelPage, 5)
		} else {
			markup = ModelPresetKeyboard()
		}

		if currentMsgID > 0 {
			edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, currentMsgID, errMsg, markup)
			edit.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "EDIT_MSG", edit, "Model gagal merespons: "+modelName)
		} else {
			reply := tgbotapi.NewMessage(chatID, errMsg)
			reply.ParseMode = tgbotapi.ModeHTML
			reply.ReplyMarkup = markup
			_, _ = b.send(chatID, "SEND_MSG", reply, "Model gagal merespons: "+modelName)
		}
		return
	}

	// Test successful: save selected model name and set state to StateConfirm
	session.Model = modelName
	session.State = StateConfirm

	// Delete testing status message to keep conversation clean
	if currentMsgID > 0 {
		del := tgbotapi.NewDeleteMessage(chatID, currentMsgID)
		_, _ = b.send(chatID, "DELETE_MSG", del, fmt.Sprintf("Hapus status testing msg_id=%d", currentMsgID))
	}

	b.showConfirmation(chatID, session)
}

// showConfirmation displays data summary prior to execution
func (b *Bot) showConfirmation(chatID int64, session *UserSession) {
	msgText := fmt.Sprintf(`🚀 <b>KONFIRMASI EVALUASI SWE-BENCH</b> 🚀
━━━━━━━━━━━━
<blockquote>🌐 <b>Base URL:</b> <code>%s</code>
🔑 <b>API Key:</b> <code>%s</code>
🤖 <b>Model AI:</b> <code>%s</code>
🧠 <b>Reasoning:</b> <code>low</code>
⏱️ <b>Batas Waktu:</b> 5 Menit / Tier</blockquote>

<blockquote>💡 <b>Aturan Evaluasi 4-Tier Ladder:</b>
• Menguji 4 tier: Junior (20), Mid (25), Senior (30), Staff (25) = 100 pts.
• Maks. 1x feedback (Turn 2 penalti 20%%, Turn 2 gagal = 0 pts).
• <b>Short-Circuit:</b> Jika satu tier gagal, tahap berikutnya langsung dibatalkan.
• Solusi wajib lolos 100%%%% unit test &amp; bebas race condition (<code>-race</code>).</blockquote>
━━━━━━━━━━━━
👉 <i>Klik tombol di bawah untuk memulai pengujian:</i>`,
		html.EscapeString(session.BaseURL),
		MaskAPIKey(session.APIKey),
		html.EscapeString(session.Model),
	)

	reply := tgbotapi.NewMessage(chatID, msgText)
	reply.ParseMode = tgbotapi.ModeHTML
	reply.ReplyMarkup = ConfirmationKeyboard()
	_, _ = b.send(chatID, "SEND_CONFIRM", reply, "Konfirmasi Parameter: "+session.Model)
}

// showLeaderboard presents the top model rankings (overall or per category)
func (b *Bot) showLeaderboard(ctx context.Context, chatID int64, messageID int, page int, category string) {
	if b.repo == nil {
		reply := tgbotapi.NewMessage(chatID, "Basis data belum terhubung.")
		_, _ = b.send(chatID, "SEND_MSG", reply, "Basis data belum terhubung")
		return
	}

	limit := 10
	offset := page * limit
	var text string

	if category == "" || category == "all" {
		entries, err := b.repo.GetLeaderboard(ctx, limit, offset)
		if err != nil {
			reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("Gagal mengambil leaderboard: %v", err))
			_, _ = b.send(chatID, "SEND_MSG", reply, "Gagal mengambil leaderboard")
			return
		}
		text = FormatLeaderboard(entries, page, limit)
	} else if category == "value" {
		entries, err := b.repo.GetValueLeaderboard(ctx, limit, offset)
		if err != nil {
			reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("Gagal mengambil leaderboard efisiensi biaya: %v", err))
			_, _ = b.send(chatID, "SEND_MSG", reply, "Gagal mengambil leaderboard efisiensi biaya")
			return
		}
		text = FormatValueLeaderboard(entries, page, limit)
	} else {
		entries, err := b.repo.GetCategoryLeaderboard(ctx, category, limit, offset)
		if err != nil {
			reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("Gagal mengambil leaderboard kategori: %v", err))
			_, _ = b.send(chatID, "SEND_MSG", reply, "Gagal mengambil leaderboard kategori")
			return
		}
		text = FormatCategoryLeaderboard(category, entries, page, limit)
	}

	markup := LeaderboardPaginationKeyboard(page, category)

	// If called from an inline button click, edit existing message
	if messageID > 0 {
		edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, messageID, text, markup)
		edit.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "EDIT_LB", edit, fmt.Sprintf("Leaderboard cat=%s page=%d", category, page))
	} else {
		reply := tgbotapi.NewMessage(chatID, text)
		reply.ParseMode = tgbotapi.ModeHTML
		reply.ReplyMarkup = markup
		_, _ = b.send(chatID, "SEND_LB", reply, fmt.Sprintf("Leaderboard cat=%s page=%d", category, page))
	}
}

// showHistory presents user's benchmark history along with detail buttons
func (b *Bot) showHistory(ctx context.Context, chatID int64, userID int64, page int) {
	if b.repo == nil {
		reply := tgbotapi.NewMessage(chatID, "Basis data belum terhubung.")
		_, _ = b.send(chatID, "SEND_MSG", reply, "Basis data belum terhubung")
		return
	}

	runs, err := b.repo.GetUserHistory(ctx, userID, 5, page*5)
	if err != nil {
		reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("Gagal mengambil riwayat: %v", err))
		_, _ = b.send(chatID, "SEND_MSG", reply, "Gagal mengambil riwayat")
		return
	}

	text := FormatHistory(runs)
	reply := tgbotapi.NewMessage(chatID, text)
	reply.ParseMode = tgbotapi.ModeHTML
	if len(runs) > 0 {
		reply.ReplyMarkup = HistoryItemsKeyboard(runs)
	}
	_, _ = b.send(chatID, "SEND_HIST", reply, fmt.Sprintf("Riwayat pengujian user=%d page=%d", userID, page))
}

// showRunDetail displays full details of a single benchmark session
func (b *Bot) showRunDetail(ctx context.Context, chatID int64, messageID int, runID string) {
	if b.repo == nil {
		return
	}

	run, details, err := b.repo.GetRunDetails(ctx, runID)
	if err != nil {
		reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("Gagal membaca detail run: %v", err))
		_, _ = b.send(chatID, "SEND_MSG", reply, "Gagal membaca detail run: "+runID)
		return
	}

	var text string
	if run.BenchmarkMode == "swe" || len(details) == 0 {
		sweTasks, err := b.repo.GetSWETaskRuns(ctx, runID)
		if err == nil && len(sweTasks) > 0 {
			text = FormatSWEScoreCard(run, sweTasks)
		} else {
			text = FormatRunFullDetails(run, details)
		}
	} else {
		text = FormatRunFullDetails(run, details)
	}
	markup := RunDetailKeyboard(runID)

	if messageID > 0 {
		edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, messageID, text, markup)
		edit.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "EDIT_DETAIL", edit, "Detail run: "+runID)
	} else {
		reply := tgbotapi.NewMessage(chatID, text)
		reply.ParseMode = tgbotapi.ModeHTML
		reply.ReplyMarkup = markup
		_, _ = b.send(chatID, "SEND_DETAIL", reply, "Detail run: "+runID)
	}
}

// exportCSV sends the benchmark report CSV file to the user
func (b *Bot) exportCSV(ctx context.Context, chatID int64, runID string) {
	if b.repo == nil {
		return
	}

	run, details, err := b.repo.GetRunDetails(ctx, runID)
	if err != nil {
		reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("Gagal generate CSV: %v", err))
		_, _ = b.send(chatID, "SEND_MSG", reply, "Gagal generate CSV: "+runID)
		return
	}

	csvBytes := GenerateRunCSV(run, details)
	fileName := fmt.Sprintf("benchmark_%s_%s.csv", run.ModelName, run.ID[:8])
	doc := tgbotapi.NewDocument(chatID, tgbotapi.FileBytes{
		Name:  fileName,
		Bytes: csvBytes,
	})
	doc.Caption = fmt.Sprintf("📊 <b>Laporan Benchmark:</b> %s (Skor: %d/%d)", html.EscapeString(run.ModelName), run.TotalScore, run.MaxScore)
	doc.ParseMode = tgbotapi.ModeHTML
	_, _ = b.send(chatID, "SEND_DOC", doc, "CSV Report: "+fileName)
}

// exportCode sends the AI-generated Go source code file to the user
func (b *Bot) exportCode(ctx context.Context, chatID int64, runID string) {
	if b.repo == nil {
		return
	}

	run, _, err := b.repo.GetRunDetails(ctx, runID)
	if err != nil {
		reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("Gagal mengambil source code: %v", err))
		_, _ = b.send(chatID, "SEND_MSG", reply, "Gagal mengambil source code: "+runID)
		return
	}

	if run.CodeSnippet == "" {
		reply := tgbotapi.NewMessage(chatID, "⚠️ Source code tidak tersedia untuk sesi ini.")
		_, _ = b.send(chatID, "SEND_MSG", reply, "Source code tidak tersedia")
		return
	}

	fileName := fmt.Sprintf("server_%s_%s.go", run.ModelName, run.ID[:8])
	doc := tgbotapi.NewDocument(chatID, tgbotapi.FileBytes{
		Name:  fileName,
		Bytes: []byte(run.CodeSnippet),
	})
	doc.Caption = fmt.Sprintf("💻 <b>Source Code Go:</b> %s", html.EscapeString(run.ModelName))
	doc.ParseMode = tgbotapi.ModeHTML
	_, _ = b.send(chatID, "SEND_DOC", doc, "Go Code: "+fileName)
}

// handleAdminCommand processes operational commands specific to administrators
func (b *Bot) handleAdminCommand(ctx context.Context, msg *tgbotapi.Message) {
	userID := msg.From.ID
	chatID := msg.Chat.ID

	if !b.adminIDs[userID] {
		reply := tgbotapi.NewMessage(chatID, "🚫 <b>Akses Ditolak</b>\n\n<blockquote>Perintah ini hanya dapat diakses oleh administrator sistem.</blockquote>")
		reply.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "ADMIN_REPLY", reply, "Akses ditolak")
		return
	}

	parts := strings.Fields(msg.Text)
	if len(parts) == 1 || strings.ToLower(parts[1]) == "help" {
		reply := tgbotapi.NewMessage(chatID, FormatAdminHelp())
		reply.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "ADMIN_REPLY", reply, "Admin Help")
		return
	}

	subcmd := strings.ToLower(parts[1])
	switch subcmd {
	case "stats":
		var stats *storage.SystemStats
		var err error
		if b.repo != nil {
			stats, err = b.repo.GetSystemStats(ctx)
		}
		if err != nil || stats == nil {
			reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("Gagal mengambil statistik: %v", err))
			_, _ = b.send(chatID, "ADMIN_REPLY", reply, "Gagal statistik")
			return
		}

		activeJobs := 0
		queueLen := 0
		maxWorkers := 0
		queueCap := 0
		if b.pool != nil {
			activeJobs = b.pool.ActiveJobs()
			queueLen = b.pool.QueueLength()
			maxWorkers = b.pool.MaxWorkers()
			queueCap = b.pool.QueueCapacity()
		}

		uptime := time.Since(b.startTime)
		text := FormatAdminStats(stats, activeJobs, maxWorkers, queueLen, queueCap, uptime)
		reply := tgbotapi.NewMessage(chatID, text)
		reply.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "ADMIN_REPLY", reply, "Admin Stats")

	case "ban":
		if len(parts) < 3 {
			reply := tgbotapi.NewMessage(chatID, "⚠️ Format salah. Gunakan: <code>/admin ban &lt;telegram_user_id&gt;</code>")
			reply.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "ADMIN_REPLY", reply, "Format ban salah")
			return
		}
		targetID, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			reply := tgbotapi.NewMessage(chatID, "User ID harus berupa angka Telegram ID.")
			_, _ = b.send(chatID, "ADMIN_REPLY", reply, "User ID invalid")
			return
		}
		if b.adminIDs[targetID] {
			reply := tgbotapi.NewMessage(chatID, "⚠️ Tidak dapat memblokir sesama administrator.")
			_, _ = b.send(chatID, "ADMIN_REPLY", reply, "Tidak dapat memblokir admin")
			return
		}
		if b.repo != nil {
			if err := b.repo.BanUser(ctx, targetID); err != nil {
				reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("Gagal memblokir user: %v", err))
				_, _ = b.send(chatID, "ADMIN_REPLY", reply, "Gagal memblokir user")
				return
			}
		}
		reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ <b>Pengguna Diblokir</b>\n\n<blockquote>Pengguna dengan Telegram ID <code>%d</code> berhasil dinonaktifkan.</blockquote>", targetID))
		reply.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "ADMIN_REPLY", reply, fmt.Sprintf("User %d diblokir", targetID))

	case "unban":
		if len(parts) < 3 {
			reply := tgbotapi.NewMessage(chatID, "⚠️ Format salah. Gunakan: <code>/admin unban &lt;telegram_user_id&gt;</code>")
			reply.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "ADMIN_REPLY", reply, "Format unban salah")
			return
		}
		targetID, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			reply := tgbotapi.NewMessage(chatID, "User ID harus berupa angka Telegram ID.")
			_, _ = b.send(chatID, "ADMIN_REPLY", reply, "User ID invalid")
			return
		}
		if b.repo != nil {
			if err := b.repo.UnbanUser(ctx, targetID); err != nil {
				reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("Gagal membuka blokir user: %v", err))
				_, _ = b.send(chatID, "ADMIN_REPLY", reply, "Gagal membuka blokir")
				return
			}
		}
		reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ <b>Blokir Dicabut</b>\n\n<blockquote>Akses pengguna dengan Telegram ID <code>%d</code> telah diaktifkan kembali.</blockquote>", targetID))
		reply.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "ADMIN_REPLY", reply, fmt.Sprintf("Blokir user %d dicabut", targetID))

	case "resetquota":
		if len(parts) < 3 {
			reply := tgbotapi.NewMessage(chatID, "⚠️ Format salah. Gunakan: <code>/admin resetquota &lt;telegram_user_id&gt;</code>")
			reply.ParseMode = tgbotapi.ModeHTML
			_, _ = b.send(chatID, "ADMIN_REPLY", reply, "Format resetquota salah")
			return
		}
		targetID, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			reply := tgbotapi.NewMessage(chatID, "User ID harus berupa angka Telegram ID.")
			_, _ = b.send(chatID, "ADMIN_REPLY", reply, "User ID invalid")
			return
		}
		if b.repo != nil {
			if err := b.repo.ResetUserRateLimit(ctx, targetID); err != nil {
				reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("Gagal mereset kuota user: %v", err))
				_, _ = b.send(chatID, "ADMIN_REPLY", reply, "Gagal reset quota")
				return
			}
		}
		reply := tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ <b>Kuota Direset</b>\n\n<blockquote>Batasan kuota pengguna ID <code>%d</code> berhasil direset.</blockquote>", targetID))
		reply.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "ADMIN_REPLY", reply, fmt.Sprintf("Quota user %d direset", targetID))

	default:
		reply := tgbotapi.NewMessage(chatID, "⚠️ Perintah admin tidak dikenal. Ketik <code>/admin help</code> untuk panduan.")
		reply.ParseMode = tgbotapi.ModeHTML
		_, _ = b.send(chatID, "ADMIN_REPLY", reply, "Perintah admin tidak dikenal")
	}
}


