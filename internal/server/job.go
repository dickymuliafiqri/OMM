package server

import (
	"context"
	"sync/atomic"
	"time"

	"benchmark/internal/worker"
)

// Aliases for worker types to preserve clean API surface and backward compatibility.
type BenchJob = worker.BenchJob
type PoolStats = worker.PoolStats

var (
	ErrPoolFull   = worker.ErrPoolFull
	ErrPoolClosed = worker.ErrPoolClosed
)

// JobSubmitter defines the interface for submitting benchmark jobs and querying status.
type JobSubmitter interface {
	Submit(ctx context.Context, job *BenchJob) error
	Stats() PoolStats
}

// MemoryJobSubmitter is a lightweight in-memory JobSubmitter implementation for testing & fallback.
type MemoryJobSubmitter struct {
	maxConcurrent  int
	activeJobs     atomic.Int32
	totalProcessed atomic.Int64
	totalFailed    atomic.Int64
}

// NewMemoryJobSubmitter creates an in-memory submitter.
func NewMemoryJobSubmitter(maxConcurrent int) *MemoryJobSubmitter {
	if maxConcurrent <= 0 {
		maxConcurrent = 3
	}
	return &MemoryJobSubmitter{
		maxConcurrent: maxConcurrent,
	}
}

func (m *MemoryJobSubmitter) Submit(ctx context.Context, job *BenchJob) error {
	current := m.activeJobs.Load()
	if int(current) >= m.maxConcurrent {
		return ErrPoolFull
	}

	m.activeJobs.Add(1)
	m.totalProcessed.Add(1)

	// Simulate async execution completion
	go func() {
		defer m.activeJobs.Add(-1)
		defer job.ZeroAPIKey()

		select {
		case <-ctx.Done():
			m.totalFailed.Add(1)
		case <-time.After(50 * time.Millisecond):
			// Simulated job finished
		}
	}()

	return nil
}

func (m *MemoryJobSubmitter) Stats() PoolStats {
	active := int(m.activeJobs.Load())
	available := m.maxConcurrent - active
	if available < 0 {
		available = 0
	}

	return PoolStats{
		ActiveJobs:     active,
		MaxConcurrent:  m.maxConcurrent,
		AvailableSlots: available,
		TotalProcessed: m.totalProcessed.Load(),
		TotalFailed:    m.totalFailed.Load(),
	}
}
