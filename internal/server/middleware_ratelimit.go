package server

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"benchmark/pkg/logger"
)

type rateEntry struct {
	count       int
	windowStart time.Time
}

// RateLimiter manages in-memory sliding window rate limits globally and per-source.
type RateLimiter struct {
	mu             sync.Mutex
	globalLimit    int
	perSourceLimit int
	windowDuration time.Duration
	entries        map[string]*rateEntry
	stopChan       chan struct{}
	closed         bool
}

// NewRateLimiter creates an in-memory RateLimiter with automated window cleanup.
func NewRateLimiter(globalLimit, perSourceLimit int) *RateLimiter {
	rl := &RateLimiter{
		globalLimit:    globalLimit,
		perSourceLimit: perSourceLimit,
		windowDuration: time.Minute,
		entries:        make(map[string]*rateEntry),
		stopChan:       make(chan struct{}),
	}

	// Background routine to evict expired entries periodically
	go rl.cleanupLoop()

	return rl
}

// Close terminates the rate limiter cleanup loop.
func (rl *RateLimiter) Close() {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if !rl.closed {
		rl.closed = true
		close(rl.stopChan)
	}
}

func (rl *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rl.mu.Lock()
			now := time.Now()
			for k, entry := range rl.entries {
				if now.Sub(entry.windowStart) > 2*rl.windowDuration {
					delete(rl.entries, k)
				}
			}
			rl.mu.Unlock()
		case <-rl.stopChan:
			return
		}
	}
}

// Check evaluates whether a request from the given source IP/identifier is permitted.
func (rl *RateLimiter) Check(source string) (allowed bool, remaining int, retryAfter int, resetSec int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()

	// 1. Check Global Limit
	globalKey := "__global__"
	globalEntry := rl.getOrCreateEntry(globalKey, now)
	if globalEntry.count >= rl.globalLimit {
		retryAfter = int(rl.windowDuration.Seconds() - now.Sub(globalEntry.windowStart).Seconds())
		if retryAfter <= 0 {
			retryAfter = 1
		}
		return false, 0, retryAfter, retryAfter
	}

	// 2. Check Per-Source Limit
	sourceKey := "src:" + source
	srcEntry := rl.getOrCreateEntry(sourceKey, now)
	if srcEntry.count >= rl.perSourceLimit {
		retryAfter = int(rl.windowDuration.Seconds() - now.Sub(srcEntry.windowStart).Seconds())
		if retryAfter <= 0 {
			retryAfter = 1
		}
		return false, 0, retryAfter, retryAfter
	}

	// 3. Both allowed - record consumption
	globalEntry.count++
	srcEntry.count++

	globalRemaining := rl.globalLimit - globalEntry.count
	srcRemaining := rl.perSourceLimit - srcEntry.count
	remaining = globalRemaining
	if srcRemaining < remaining {
		remaining = srcRemaining
	}
	if remaining < 0 {
		remaining = 0
	}

	resetSec = int(rl.windowDuration.Seconds() - now.Sub(globalEntry.windowStart).Seconds())
	if resetSec <= 0 {
		resetSec = 1
	}

	return true, remaining, 0, resetSec
}

// CheckSource evaluates whether a request for a specific source key (e.g. callback domain) is permitted without consuming global quota.
func (rl *RateLimiter) CheckSource(source string) (allowed bool, remaining int, retryAfter int, resetSec int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	sourceKey := "src:" + source
	srcEntry := rl.getOrCreateEntry(sourceKey, now)
	if srcEntry.count >= rl.perSourceLimit {
		retryAfter = int(rl.windowDuration.Seconds() - now.Sub(srcEntry.windowStart).Seconds())
		if retryAfter <= 0 {
			retryAfter = 1
		}
		return false, 0, retryAfter, retryAfter
	}

	srcEntry.count++
	remaining = rl.perSourceLimit - srcEntry.count
	if remaining < 0 {
		remaining = 0
	}

	resetSec = int(rl.windowDuration.Seconds() - now.Sub(srcEntry.windowStart).Seconds())
	if resetSec <= 0 {
		resetSec = 1
	}

	return true, remaining, 0, resetSec
}

func (rl *RateLimiter) getOrCreateEntry(key string, now time.Time) *rateEntry {
	entry, exists := rl.entries[key]
	if !exists || now.Sub(entry.windowStart) >= rl.windowDuration {
		entry = &rateEntry{
			count:       0,
			windowStart: now,
		}
		rl.entries[key] = entry
	}
	return entry
}

// RateLimitMiddleware creates an HTTP middleware that enforces global & per-source limits.
func RateLimitMiddleware(rl *RateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			clientIP := GetClientIP(r)

			allowed, remaining, retryAfter, resetSec := rl.Check(clientIP)

			w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", remaining))
			w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", resetSec))

			if !allowed {
				logger.Warn("ratelimit.exceeded", "ip=%s remaining=%d", clientIP, remaining)
				w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfter))
				writeJSON(w, http.StatusTooManyRequests, map[string]any{
					"error":      "rate limit exceeded",
					"retryAfter": retryAfter,
				})
				return
			}

			logger.Debug("ratelimit.pass", "ip=%s remaining=%d resetIn=%ds path=%s", clientIP, remaining, resetSec, r.URL.Path)
			next.ServeHTTP(w, r)
		})
	}
}

// GetClientIP extracts the real client IP, factoring in trusted proxy headers.
func GetClientIP(r *http.Request) string {
	// Check X-Forwarded-For header
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		ips := strings.Split(xff, ",")
		if len(ips) > 0 {
			ip := strings.TrimSpace(ips[0])
			if net.ParseIP(ip) != nil {
				return ip
			}
		}
	}

	// Check X-Real-IP header
	if xri := strings.TrimSpace(r.Header.Get("X-Real-IP")); xri != "" {
		if net.ParseIP(xri) != nil {
			return xri
		}
	}

	// Fallback to RemoteAddr
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return host
	}

	return r.RemoteAddr
}
