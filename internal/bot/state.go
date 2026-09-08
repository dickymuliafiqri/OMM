package bot

import (
	"sync"
	"time"
)

// State represents the step status in the Telegram wizard interaction
type State string

const (
	StateIdle      State = "IDLE"
	StateWaitURL   State = "WAIT_URL"
	StateWaitKey   State = "WAIT_KEY"
	StateWaitModel State = "WAIT_MODEL"
	StateConfirm   State = "CONFIRM"
)

// UserSession stores temporary data inputted by the user during the wizard
type UserSession struct {
	TelegramID      int64
	ChatID          int64
	State           State
	BaseURL         string
	APIKey          string
	Model           string
	AvailableModels []string
	ModelPage       int
	LastActive      time.Time
}

// SessionManager manages in-memory (RAM) user conversation sessions in a thread-safe manner
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[int64]*UserSession
	ttl      time.Duration
}

// NewSessionManager creates a SessionManager instance with a specified TTL
func NewSessionManager(ttl time.Duration) *SessionManager {
	if ttl <= 0 {
		ttl = 15 * time.Minute // Default timeout 15 minutes
	}

	sm := &SessionManager{
		sessions: make(map[int64]*UserSession),
		ttl:      ttl,
	}

	// Background cleanup routine for expired sessions
	go sm.cleanupRoutine()

	return sm
}

// Get retrieves a user session or returns a new IDLE session if none exists
func (sm *SessionManager) Get(userID int64, chatID int64) *UserSession {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	s, exists := sm.sessions[userID]
	if !exists || time.Since(s.LastActive) > sm.ttl {
		s = &UserSession{
			TelegramID: userID,
			ChatID:     chatID,
			State:      StateIdle,
			LastActive: time.Now(),
		}
		sm.sessions[userID] = s
	} else {
		s.LastActive = time.Now()
		if chatID != 0 {
			s.ChatID = chatID
		}
	}

	return s
}

// SetState updates the user's wizard state
func (sm *SessionManager) SetState(userID int64, state State) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if s, exists := sm.sessions[userID]; exists {
		s.State = state
		s.LastActive = time.Now()
	}
}

// Clear resets user session to IDLE and cleans up secret credentials
func (sm *SessionManager) Clear(userID int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if s, exists := sm.sessions[userID]; exists {
		s.State = StateIdle
		s.APIKey = ""
		s.BaseURL = ""
		s.Model = ""
		s.AvailableModels = nil
		s.ModelPage = 0
		s.LastActive = time.Now()
	}
}

// cleanupRoutine removes sessions inactive beyond TTL every 5 minutes
func (sm *SessionManager) cleanupRoutine() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		sm.mu.Lock()
		now := time.Now()
		for id, s := range sm.sessions {
			if now.Sub(s.LastActive) > sm.ttl {
				delete(sm.sessions, id)
			}
		}
		sm.mu.Unlock()
	}
}
