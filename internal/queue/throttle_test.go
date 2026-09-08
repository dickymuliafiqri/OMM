package queue

import (
	"sync"
	"testing"
	"time"
)

func TestProgressThrottler_ImmediateAndThrottle(t *testing.T) {
	var mu sync.Mutex
	var updates []string

	throttler := NewProgressThrottler(50*time.Millisecond, func(text string) {
		mu.Lock()
		defer mu.Unlock()
		updates = append(updates, text)
	})
	defer throttler.Close()

	// First update must be sent immediately
	throttler.Update("Update 1")
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	if len(updates) != 1 || updates[0] != "Update 1" {
		t.Fatalf("Update 1 gagal dikirim langsung: %v", updates)
	}
	mu.Unlock()

	// Second and third updates are called quickly before 50ms
	throttler.Update("Update 2")
	throttler.Update("Update 3")

	// Before 50ms elapses, Update 3 must not be sent yet
	mu.Lock()
	countBefore := len(updates)
	mu.Unlock()
	if countBefore > 2 {
		t.Errorf("Throttler bocor, mengirim lebih dari interval: %v", updates)
	}

	// Wait for interval to elapse (70ms)
	time.Sleep(70 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(updates) < 2 {
		t.Fatalf("Ekspektasi minimal 2 update setelah jeda: %v", updates)
	}
	// Ensure the last update sent is the latest value ("Update 3")
	last := updates[len(updates)-1]
	if last != "Update 3" {
		t.Errorf("Update terakhir harusnya 'Update 3', didapatkan: %s", last)
	}
}

func TestProgressThrottler_Flush(t *testing.T) {
	var mu sync.Mutex
	var updates []string

	throttler := NewProgressThrottler(500*time.Millisecond, func(text string) {
		mu.Lock()
		defer mu.Unlock()
		updates = append(updates, text)
	})
	defer throttler.Close()

	throttler.Update("Msg 1")
	time.Sleep(10 * time.Millisecond)

	throttler.Update("Msg Pending")
	// Call Flush immediately without waiting 500ms
	throttler.Flush()

	mu.Lock()
	defer mu.Unlock()
	if len(updates) != 2 || updates[1] != "Msg Pending" {
		t.Errorf("Flush gagal memaksa pengiriman pembaruan tertunda: %v", updates)
	}
}
