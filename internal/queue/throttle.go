package queue

import (
	"sync"
	"time"
)

// ProgressThrottler limits the frequency of Telegram message updates to avoid HTTP 429 Too Many Requests.
// Telegram recommends a minimum interval of 1-1.5 seconds per edited message.
type ProgressThrottler struct {
	mu           sync.Mutex
	minInterval  time.Duration
	updateFn     func(text string)
	lastUpdate   time.Time
	lastSentText string
	pendingText  string
	hasPending   bool
	timer        *time.Timer
	closed       bool
}

// NewProgressThrottler creates a new message update rate-limiter instance
func NewProgressThrottler(minInterval time.Duration, updateFn func(text string)) *ProgressThrottler {
	if minInterval <= 0 {
		minInterval = 1500 * time.Millisecond // Default 1.5 seconds
	}
	return &ProgressThrottler{
		minInterval: minInterval,
		updateFn:    updateFn,
	}
}

// Update schedules or immediately sends the update text if the minimum interval is satisfied
func (t *ProgressThrottler) Update(text string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed || text == t.lastSentText {
		return
	}

	now := time.Now()
	elapsed := now.Sub(t.lastUpdate)

	if elapsed >= t.minInterval {
		// Send immediately
		t.lastUpdate = now
		t.lastSentText = text
		t.hasPending = false
		if t.timer != nil {
			t.timer.Stop()
			t.timer = nil
		}
		go t.updateFn(text)
		return
	}

	// Save as pending update
	t.pendingText = text
	t.hasPending = true

	if t.timer == nil {
		remaining := t.minInterval - elapsed
		t.timer = time.AfterFunc(remaining, func() {
			t.mu.Lock()
			defer t.mu.Unlock()

			if t.closed || !t.hasPending {
				t.timer = nil
				return
			}

			textToSend := t.pendingText
			if textToSend == t.lastSentText {
				t.hasPending = false
				t.timer = nil
				return
			}

			t.hasPending = false
			t.lastUpdate = time.Now()
			t.lastSentText = textToSend
			t.timer = nil

			go t.updateFn(textToSend)
		})
	}
}

// Flush ensures the latest pending update is sent immediately
func (t *ProgressThrottler) Flush() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}

	if t.hasPending && !t.closed {
		textToSend := t.pendingText
		t.hasPending = false
		if textToSend != t.lastSentText {
			t.lastUpdate = time.Now()
			t.lastSentText = textToSend
			t.updateFn(textToSend)
		}
	}
}

// Close stops the throttler and cancels any running timer
func (t *ProgressThrottler) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.closed = true
	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
}
