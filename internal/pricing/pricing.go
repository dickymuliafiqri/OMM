package pricing

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"benchmark/internal/storage"
	"benchmark/pkg/logger"
)

// ModelPrice stores pricing rates per 1 million tokens (USD)
type ModelPrice struct {
	PromptPricePerMillion     float64 `json:"prompt_per_m"`
	CompletionPricePerMillion float64 `json:"completion_per_m"`
}

// CostEstimate stores cost calculation and efficiency results
type CostEstimate struct {
	EstimatedCostUSD float64
	HasPricing       bool
	Tier             string // "Sangat Ekonomis", "Ekonomis", "Standar", "Relatif Mahal"
	CostDescription  string
}

var (
	cacheMu     sync.RWMutex
	remoteCache = make(map[string]ModelPrice)
	isSyncing   atomic.Bool // Protection flag to prevent concurrent overlapping synchronization

	// fallbackPrices contains baseline market rates (USD per 1M tokens)
	// for popular models when disconnected from OpenRouter API
	fallbackPrices = map[string]ModelPrice{
		// DeepSeek
		"deepseek-chat":     {PromptPricePerMillion: 0.14, CompletionPricePerMillion: 0.28},
		"deepseek-v3":       {PromptPricePerMillion: 0.14, CompletionPricePerMillion: 0.28},
		"deepseek-reasoner": {PromptPricePerMillion: 0.55, CompletionPricePerMillion: 2.19},
		"deepseek-r1":       {PromptPricePerMillion: 0.55, CompletionPricePerMillion: 2.19},

		// OpenAI
		"gpt-4o-mini": {PromptPricePerMillion: 0.15, CompletionPricePerMillion: 0.60},
		"gpt-4o":      {PromptPricePerMillion: 2.50, CompletionPricePerMillion: 10.00},
		"o1-mini":     {PromptPricePerMillion: 1.10, CompletionPricePerMillion: 4.40},
		"o3-mini":     {PromptPricePerMillion: 1.10, CompletionPricePerMillion: 4.40},
		"o1":          {PromptPricePerMillion: 15.00, CompletionPricePerMillion: 60.00},

		// Anthropic
		"claude-3-5-sonnet": {PromptPricePerMillion: 3.00, CompletionPricePerMillion: 15.00},
		"claude-3-7-sonnet": {PromptPricePerMillion: 3.00, CompletionPricePerMillion: 15.00},
		"claude-3-5-haiku":  {PromptPricePerMillion: 0.80, CompletionPricePerMillion: 4.00},
		"claude-3-opus":     {PromptPricePerMillion: 15.00, CompletionPricePerMillion: 75.00},

		// Google
		"gemini-2.0-flash": {PromptPricePerMillion: 0.10, CompletionPricePerMillion: 0.40},
		"gemini-1.5-flash": {PromptPricePerMillion: 0.075, CompletionPricePerMillion: 0.30},
		"gemini-1.5-pro":   {PromptPricePerMillion: 1.25, CompletionPricePerMillion: 5.00},

		// Meta / Open Weights
		"llama-3.3-70b": {PromptPricePerMillion: 0.59, CompletionPricePerMillion: 0.79},
		"llama-3.1-8b":  {PromptPricePerMillion: 0.05, CompletionPricePerMillion: 0.08},
		"llama3":        {PromptPricePerMillion: 0.05, CompletionPricePerMillion: 0.08},
		"llama-3":       {PromptPricePerMillion: 0.05, CompletionPricePerMillion: 0.08},
		"qwen-2.5":      {PromptPricePerMillion: 0.20, CompletionPricePerMillion: 0.60},
		"qwen":          {PromptPricePerMillion: 0.15, CompletionPricePerMillion: 0.30},
		"mistral":       {PromptPricePerMillion: 0.15, CompletionPricePerMillion: 0.30},
		"phi4":          {PromptPricePerMillion: 0.07, CompletionPricePerMillion: 0.14},
		"phi-4":         {PromptPricePerMillion: 0.07, CompletionPricePerMillion: 0.14},
		"gemma":         {PromptPricePerMillion: 0.10, CompletionPricePerMillion: 0.20},

		// Baseline compute cost for local / self-hosted models (hardware & electricity baseline)
		"local-compute": {PromptPricePerMillion: 0.10, CompletionPricePerMillion: 0.20},
		"ollama":        {PromptPricePerMillion: 0.10, CompletionPricePerMillion: 0.20},
		"localhost":     {PromptPricePerMillion: 0.10, CompletionPricePerMillion: 0.20},
	}
)

// IsSyncing returns true if the price synchronization process is running in the background
func IsSyncing() bool {
	return isSyncing.Load()
}

// ensureNonZeroPrice ensures no zero rate (e.g. from local models or OpenRouter :free endpoints)
func ensureNonZeroPrice(p ModelPrice) ModelPrice {
	if p.PromptPricePerMillion <= 0 {
		p.PromptPricePerMillion = 0.10
	}
	if p.CompletionPricePerMillion <= 0 {
		p.CompletionPricePerMillion = 0.20
	}
	return p
}

// GetModelPrice looks up token pricing per 1M tokens based on model name.
// Local models (Ollama/localhost) are calculated based on equivalent model rates or hardware compute load, not free.
func GetModelPrice(modelName string) (ModelPrice, bool) {
	name := strings.ToLower(strings.TrimSpace(modelName))
	if name == "" {
		return ModelPrice{}, false
	}

	// Detect and strip local prefixes (e.g. "ollama/llama3" -> "llama3")
	cleanName := name
	isLocal := false
	if strings.HasPrefix(cleanName, "ollama/") {
		cleanName = strings.TrimPrefix(cleanName, "ollama/")
		isLocal = true
	} else if strings.HasPrefix(cleanName, "localhost/") {
		cleanName = strings.TrimPrefix(cleanName, "localhost/")
		isLocal = true
	} else if strings.Contains(name, "ollama") || strings.Contains(name, "localhost") {
		isLocal = true
	}

	baseName := cleanName
	if idx := strings.LastIndex(cleanName, "/"); idx != -1 {
		baseName = cleanName[idx+1:]
	}
	noTag := strings.Split(baseName, ":")[0]

	// 1. Check remote cache from OpenRouter / Turso DB
	cacheMu.RLock()
	var foundPrice ModelPrice
	var found bool

	if p, ok := remoteCache[cleanName]; ok && (p.PromptPricePerMillion > 0 || p.CompletionPricePerMillion > 0) {
		foundPrice, found = p, true
	} else if p, ok := remoteCache[baseName]; ok && (p.PromptPricePerMillion > 0 || p.CompletionPricePerMillion > 0) {
		foundPrice, found = p, true
	} else if p, ok := remoteCache[noTag]; ok && (p.PromptPricePerMillion > 0 || p.CompletionPricePerMillion > 0) {
		foundPrice, found = p, true
	} else {
		for k, p := range remoteCache {
			kBase := k
			if idx := strings.LastIndex(k, "/"); idx != -1 {
				kBase = k[idx+1:]
			}
			kNoTag := strings.Split(kBase, ":")[0]
			if (kNoTag == noTag || strings.Contains(kNoTag, noTag) || strings.Contains(noTag, kNoTag)) &&
				(p.PromptPricePerMillion > 0 || p.CompletionPricePerMillion > 0) {
				foundPrice, found = p, true
				break
			}
		}
	}
	cacheMu.RUnlock()

	if found {
		return ensureNonZeroPrice(foundPrice), true
	}

	// 2. Check built-in fallback prices
	if p, ok := fallbackPrices[cleanName]; ok {
		return ensureNonZeroPrice(p), true
	}
	if p, ok := fallbackPrices[baseName]; ok {
		return ensureNonZeroPrice(p), true
	}
	if p, ok := fallbackPrices[noTag]; ok {
		return ensureNonZeroPrice(p), true
	}

	// 3. Check fuzzy substring match in fallback
	for key, p := range fallbackPrices {
		if strings.Contains(cleanName, key) || strings.Contains(key, cleanName) ||
			strings.Contains(noTag, key) || strings.Contains(key, noTag) {
			return ensureNonZeroPrice(p), true
		}
	}

	// 4. If local model (Ollama / Localhost), use self-hosted hardware compute baseline rate
	if isLocal {
		return ModelPrice{PromptPricePerMillion: 0.10, CompletionPricePerMillion: 0.20}, true
	}

	return ModelPrice{}, false
}

// CalculateCost calculates estimated cost and model efficiency
func CalculateCost(modelName string, promptTokens, completionTokens, totalScore int) CostEstimate {
	price, found := GetModelPrice(modelName)
	if !found {
		return CostEstimate{
			HasPricing:      false,
			Tier:            "Biaya Belum Diketahui",
			CostDescription: "Tarif token belum terdaftar",
		}
	}

	price = ensureNonZeroPrice(price)

	costIn := (float64(promptTokens) * price.PromptPricePerMillion) / 1_000_000.0
	costOut := (float64(completionTokens) * price.CompletionPricePerMillion) / 1_000_000.0
	totalCost := costIn + costOut
	if totalCost < 0.00001 {
		totalCost = 0.00001
	}

	// Efficiency classification (Score vs Cost)
	var tier string
	switch {
	case totalCost <= 0.0008:
		tier = "Sangat Ekonomis"
	case totalCost <= 0.003:
		if totalScore >= 70 {
			tier = "Ekonomis"
		} else {
			tier = "Cukup"
		}
	case totalCost <= 0.015:
		if totalScore >= 80 {
			tier = "Standar"
		} else {
			tier = "Kurang Efisien"
		}
	default:
		tier = "Relatif Mahal"
	}

	desc := fmt.Sprintf("~$%.4f USD (%s)", totalCost, tier)
	if totalCost < 0.0001 {
		desc = fmt.Sprintf("~$%.5f USD (%s)", totalCost, tier)
	}

	return CostEstimate{
		EstimatedCostUSD: totalCost,
		HasPricing:       true,
		Tier:             tier,
		CostDescription:  desc,
	}
}

// OpenRouterModelsURL is the OpenRouter model metadata endpoint URL (can be overridden in unit tests)
var OpenRouterModelsURL = "https://openrouter.ai/api/v1/models"

// LoadPricingFromDB loads cached model pricing data from Turso database into local memory
func LoadPricingFromDB(ctx context.Context, repo storage.Repository) (int, error) {
	if repo == nil {
		return 0, nil
	}

	models, err := repo.GetAllCachedModels(ctx)
	if err != nil {
		return 0, fmt.Errorf("gagal memuat cache model dari database: %w", err)
	}

	if len(models) == 0 {
		return 0, nil
	}

	cacheMu.Lock()
	defer cacheMu.Unlock()

	for _, m := range models {
		price := ModelPrice{
			PromptPricePerMillion:     m.PromptPricePerM,
			CompletionPricePerMillion: m.CompletionPricePerM,
		}
		price = ensureNonZeroPrice(price)
		cleanID := strings.ToLower(m.ID)
		remoteCache[cleanID] = price

		baseName := cleanID
		if idx := strings.LastIndex(cleanID, "/"); idx != -1 {
			baseName = cleanID[idx+1:]
		}
		remoteCache[baseName] = price

		noTag := strings.Split(baseName, ":")[0]
		if noTag != "" && noTag != baseName {
			remoteCache[noTag] = price
		}
	}

	logger.Sys("PRICING", "Memuat %d model dari cache database Turso ke memori", len(models))
	return len(models), nil
}

// SyncOpenRouterPricing downloads official pricing metadata from OpenRouter and saves it to the database and memory cache.
// This operation is protected by an atomic guard to prevent overlapping concurrent synchronizations.
func SyncOpenRouterPricing(ctx context.Context, repo storage.Repository) error {
	if !isSyncing.CompareAndSwap(false, true) {
		logger.Sys("PRICING", "Sinkronisasi tarif token sedang berjalan di latar belakang, melewati eksekusi baru")
		return nil
	}
	defer isSyncing.Store(false)

	// Use a context with a maximum timeout (45 seconds) to prevent hanging indefinitely
	syncCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	client := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequestWithContext(syncCtx, http.MethodGet, OpenRouterModelsURL, nil)
	if err != nil {
		return fmt.Errorf("gagal membuat request OpenRouter: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("openrouter returned status %d", resp.StatusCode)
	}

	var res struct {
		Data []struct {
			ID            string `json:"id"`
			Name          string `json:"name"`
			ContextLength int    `json:"context_length"`
			Pricing       struct {
				Prompt     string `json:"prompt"`
				Completion string `json:"completion"`
			} `json:"pricing"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return err
	}

	cacheMu.Lock()
	cachedList := make([]storage.CachedModel, 0, len(res.Data))

	for _, m := range res.Data {
		pIn, _ := strconv.ParseFloat(m.Pricing.Prompt, 64)
		pOut, _ := strconv.ParseFloat(m.Pricing.Completion, 64)
		if pIn < 0 {
			pIn = 0
		}
		if pOut < 0 {
			pOut = 0
		}

		price := ModelPrice{
			PromptPricePerMillion:     pIn * 1_000_000.0,
			CompletionPricePerMillion: pOut * 1_000_000.0,
		}
		price = ensureNonZeroPrice(price)

		cleanID := strings.ToLower(m.ID)
		remoteCache[cleanID] = price

		baseName := cleanID
		if idx := strings.LastIndex(cleanID, "/"); idx != -1 {
			baseName = cleanID[idx+1:]
		}
		remoteCache[baseName] = price

		noTag := strings.Split(baseName, ":")[0]
		if noTag != "" && noTag != baseName {
			remoteCache[noTag] = price
		}

		cachedList = append(cachedList, storage.CachedModel{
			ID:                  cleanID,
			Name:                m.Name,
			PromptPricePerM:     price.PromptPricePerMillion,
			CompletionPricePerM: price.CompletionPricePerMillion,
			ContextLength:       m.ContextLength,
		})
	}
	cacheMu.Unlock()

	logger.Sys("PRICING", "Metadata tarif token diperbarui dari OpenRouter (%d model)", len(cachedList))

	// Save to Turso database in batches to persist & allow use as local/offline cache
	if repo != nil && len(cachedList) > 0 {
		if err := repo.SaveCachedModels(syncCtx, cachedList); err != nil {
			logger.Warn("PRICING", "Gagal menyimpan cache model ke database: %v", err)
			return err
		}
		logger.Sys("PRICING", "Berhasil menyimpan %d model ke database Turso", len(cachedList))
	}

	return nil
}

// SyncOpenRouterPricingAsync runs price synchronization asynchronously in a separate goroutine
// without blocking the main bot flow. Equipped with panic recovery for maximum reliability.
func SyncOpenRouterPricingAsync(ctx context.Context, repo storage.Repository) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("PRICING", "Panic tertangkap saat sinkronisasi harga asinkron: %v", r)
			}
		}()
		if err := SyncOpenRouterPricing(ctx, repo); err != nil {
			logger.Warn("PRICING", "Gagal sinkronisasi asinkron harga token: %v", err)
		}
	}()
}

// StartPeriodicSync runs periodic token price synchronization from OpenRouter in the background
// and loads cache from the database on startup
func StartPeriodicSync(ctx context.Context, repo storage.Repository, interval time.Duration) {
	// 1. Load cache from database instantly to populate memory without waiting for network requests
	if repo != nil {
		if count, err := LoadPricingFromDB(ctx, repo); err != nil {
			logger.Warn("PRICING", "Peringatan saat memuat cache model dari database: %v", err)
		} else if count > 0 {
			logger.Sys("PRICING", "Cache model awal aktif dari database Turso (%d model)", count)
		}
	}

	// 2. Add a brief delay (2 seconds) before the first online synchronization
	// so the bot finishes network initialization and is ready to accept user requests
	select {
	case <-ctx.Done():
		return
	case <-time.After(2 * time.Second):
	}

	// 3. Initial online synchronization asynchronously in the background
	SyncOpenRouterPricingAsync(ctx, repo)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Sys("PRICING", "Penghentian sinkronisasi berkala tarif token")
			return
		case <-ticker.C:
			SyncOpenRouterPricingAsync(ctx, repo)
		}
	}
}

