package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"benchmark/internal/metrics"
	"benchmark/pkg/logger"
)

// Pool manages concurrent benchmark job execution with strict concurrency bounds and graceful shutdown.
type Pool struct {
	sem        chan struct{}
	active     atomic.Int32
	total      atomic.Int64
	failed     atomic.Int64
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	closed     atomic.Bool
	maxSlots   int
	jobTimeout time.Duration
	executor   JobExecutor
	metrics    *metrics.Metrics
}

// NewPool creates and initializes a worker pool with optional metrics registry.
func NewPool(parentCtx context.Context, maxSlots int, jobTimeout time.Duration, executor JobExecutor, m ...*metrics.Metrics) *Pool {
	if maxSlots <= 0 {
		maxSlots = 3
	}
	if jobTimeout <= 0 {
		jobTimeout = 5 * time.Minute
	}
	if parentCtx == nil {
		parentCtx = context.Background()
	}

	ctx, cancel := context.WithCancel(parentCtx)

	if executor == nil {
		executor = NewDefaultExecutor(nil, nil)
	}

	met := metrics.Default
	if len(m) > 0 && m[0] != nil {
		met = m[0]
	}

	return &Pool{
		sem:        make(chan struct{}, maxSlots),
		ctx:        ctx,
		cancel:     cancel,
		maxSlots:   maxSlots,
		jobTimeout: jobTimeout,
		executor:   executor,
		metrics:    met,
	}
}

// Submit enqueues a benchmark job if there is available capacity in a non-blocking manner.
func (p *Pool) Submit(ctx context.Context, job *BenchJob) error {
	if p.closed.Load() {
		return ErrPoolClosed
	}

	logger.Debug("pool.submit", "jobId=%s active=%d maxSlots=%d", job.JobID, p.active.Load(), p.maxSlots)

	// Non-blocking semaphore acquisition
	select {
	case p.sem <- struct{}{}:
		logger.Debug("pool.slot_acquired", "jobId=%s active=%d available=%d", job.JobID, p.active.Load()+1, p.maxSlots-int(p.active.Load()+1))
	default:
		logger.Warn("pool.full", "jobId=%s active=%d maxSlots=%d", job.JobID, p.active.Load(), p.maxSlots)
		return ErrPoolFull
	}

	p.active.Add(1)
	p.total.Add(1)
	p.wg.Add(1)
	if p.metrics != nil {
		p.metrics.ActiveJobs.Add(1)
	}

	go func() {
		logger.Debug("worker.goroutine_started", "jobId=%s model=%s timeout=%v", job.JobID, job.Model, p.jobTimeout)
		defer func() {
			<-p.sem
			p.active.Add(-1)
			if p.metrics != nil {
				p.metrics.ActiveJobs.Add(-1)
			}
			logger.Debug("worker.goroutine_finished", "jobId=%s activeNow=%d", job.JobID, p.active.Load())
			p.wg.Done()
		}()

		// Derive job context bounded by parent pool context and job timeout
		jobCtx, cancel := context.WithTimeout(p.ctx, p.jobTimeout)
		defer cancel()

		p.executor.Execute(jobCtx, job)

		if jobCtx.Err() == context.DeadlineExceeded {
			logger.Warn("worker.timeout", "jobId=%s exceeded jobTimeout=%v", job.JobID, p.jobTimeout)
			p.failed.Add(1)
			if p.metrics != nil {
				p.metrics.JobsTimeout.Add(1)
			}
		}
	}()

	return nil
}

// Drain waits for active jobs to complete up to the specified timeout.
// When the timeout expires, remaining jobs are cancelled via context.
func (p *Pool) Drain(timeout time.Duration) error {
	p.closed.Store(true)
	logger.Debug("pool.drain", "activeJobs=%d timeout=%v", p.active.Load(), timeout)

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	if timeout <= 0 {
		<-done
		return nil
	}

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		p.cancel() // Cancel running jobs
		return errors.New("timeout waiting for worker pool to drain")
	}
}

// Cancel terminates all active jobs immediately.
func (p *Pool) Cancel() {
	p.closed.Store(true)
	p.cancel()
}

// Stats returns a snapshot of worker pool metrics.
func (p *Pool) Stats() PoolStats {
	active := int(p.active.Load())
	available := p.maxSlots - active
	if available < 0 {
		available = 0
	}

	return PoolStats{
		ActiveJobs:     active,
		MaxConcurrent:  p.maxSlots,
		AvailableSlots: available,
		TotalProcessed: p.total.Load(),
		TotalFailed:    p.failed.Load(),
	}
}
