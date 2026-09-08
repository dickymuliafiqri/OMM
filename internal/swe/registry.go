package swe

import (
	"fmt"
	"sync"
)

var (
	registryMu sync.RWMutex
	taskMap    = make(map[string]*Task)
	taskList   []*Task
)

// RegisterTask registers a task into the global registry
func RegisterTask(t *Task) {
	registryMu.Lock()
	defer registryMu.Unlock()

	if _, exists := taskMap[t.ID]; !exists {
		taskList = append(taskList, t)
	}
	taskMap[t.ID] = t
}

// GetTask retrieves a task by its unique ID
func GetTask(id string) (*Task, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	t, ok := taskMap[id]
	return t, ok
}

// AllTasks returns the list of all registered tasks
func AllTasks() []*Task {
	registryMu.RLock()
	defer registryMu.RUnlock()

	copied := make([]*Task, len(taskList))
	copy(copied, taskList)
	return copied
}

// GetTasksByTier returns tasks in a specific tier
func GetTasksByTier(tier Tier) []*Task {
	registryMu.RLock()
	defer registryMu.RUnlock()

	var result []*Task
	for _, t := range taskList {
		if t.Tier == tier {
			result = append(result, t)
		}
	}
	return result
}

// DefaultLadder returns 4 standard curated tasks (1 task per tier: Junior 20, Mid 25, Senior 30, Staff 25 = 100 Points)
func DefaultLadder() ([]*Task, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	tiers := []Tier{TierJunior, TierMid, TierSenior, TierStaff}
	var ladder []*Task

	for _, tier := range tiers {
		found := false
		for _, t := range taskList {
			if t.Tier == tier {
				ladder = append(ladder, t)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("task untuk tier %s belum terdaftar di registry", tier)
		}
	}

	return ladder, nil
}

// FullLadder returns all 8 curated tasks ordered strictly by tier (Junior -> Mid -> Senior -> Staff)
func FullLadder() ([]*Task, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	tiers := []Tier{TierJunior, TierMid, TierSenior, TierStaff}
	var ladder []*Task

	for _, tier := range tiers {
		tierFound := false
		for _, t := range taskList {
			if t.Tier == tier {
				ladder = append(ladder, t)
				tierFound = true
			}
		}
		if !tierFound {
			return nil, fmt.Errorf("task untuk tier %s belum terdaftar di registry", tier)
		}
	}

	return ladder, nil
}
