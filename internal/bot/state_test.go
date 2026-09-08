package bot

import (
	"testing"
	"time"
)

func TestSessionManager_Lifecycle(t *testing.T) {
	sm := NewSessionManager(500 * time.Millisecond)

	userID := int64(1001)
	chatID := int64(2001)

	// 1. Initial initialization
	sess := sm.Get(userID, chatID)
	if sess.State != StateIdle {
		t.Errorf("Awalnya state harus IDLE, dapat: %s", sess.State)
	}

	// 2. Set state & data
	sm.SetState(userID, StateWaitURL)
	sess.BaseURL = "https://api.openai.com/v1"
	sess.APIKey = "sk-secret-token"
	sess.Model = "gpt-4o"

	sessAfter := sm.Get(userID, chatID)
	if sessAfter.State != StateWaitURL || sessAfter.Model != "gpt-4o" {
		t.Errorf("Data sesi tidak tersimpan dengan benar: %+v", sessAfter)
	}

	// 3. Clear data
	sm.Clear(userID)
	sessCleared := sm.Get(userID, chatID)
	if sessCleared.State != StateIdle || sessCleared.APIKey != "" {
		t.Errorf("Sesi setelah Clear() harus bersih dan IDLE: %+v", sessCleared)
	}

	// 4. Test TTL expiry
	sm.SetState(userID, StateWaitKey)
	time.Sleep(600 * time.Millisecond) // Exceeds 500ms TTL

	sessExpired := sm.Get(userID, chatID)
	if sessExpired.State != StateIdle {
		t.Errorf("Sesi yang melebihi TTL harus di-reset ke IDLE, dapat: %s", sessExpired.State)
	}
}
