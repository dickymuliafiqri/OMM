package tasks

import "benchmark/internal/swe"

var TaskT3ConnectionPool = &swe.Task{
	ID:       "swe-t3-connection-pool-01",
	Title:    "Resilient Database Connection Pool with Active Health-Checking and Fair Waiter Queue",
	Tier:     swe.TierSenior,
	Points:   30,
	Category: "concurrency_deadlock_and_lifecycle",
	IssueBody: `### Bug Report: Lock Inversion Deadlock, Health-Check Goroutine Leaks, Circuit Breaker Race, and Dialing Contention in Connection Pool

**Environment:** Go 1.24+ high-throughput database connection pool with resilient circuit breaking and FIFO waiter queuing.

**Expected Behavior:**
- Acquire and Release operations must never deadlock under heavy multi-goroutine contention (standardized lock ordering between waiter queue and connection locks).
- Health-checking tickers associated with connections must be cleanly stopped upon connection eviction or pool shutdown (zero leaked background goroutines).
- Circuit breaker state evaluations and transitions must be fully thread-safe without data races (` + "`-race`" + ` clean).
- Network dialing must not hold the master pool lock; slow or latency-prone network handshakes should not block other threads performing stats, release, or idle acquisition.
- Pool must enforce MaxOpen, MinIdle, MaxIdle, and request timeouts accurately.
- Connection multiplexing must safely route responses to correct streams and handle concurrent stream open/close.
- Failover must rotate through replicas correctly, resolve DNS safely, and prevent thundering herd on failover events.
- Sharded connection pools must consistently hash connections to the same shard for acquire and return operations.
- Prepared statement caching must maintain LRU ordering, evict oldest entries, and handle concurrent access safely.`,
	BrokenCode: `package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPoolClosed            = errors.New("conn_pool: pool is closed")
	ErrPoolExhausted         = errors.New("conn_pool: connection limit reached")
	ErrConnClosed            = errors.New("conn_pool: connection is closed")
	ErrCircuitOpen           = errors.New("conn_pool: circuit breaker is open")
	ErrAcquireTimeout        = errors.New("conn_pool: timeout waiting for connection")
	ErrInvalidConfig         = errors.New("conn_pool: invalid configuration parameter")
	ErrHealthCheckFailed     = errors.New("conn_pool: connection health check failed")
	ErrBadConnection         = errors.New("conn_pool: underlying connection corrupted")
	ErrConnBroken            = errors.New("conn_pool: connection marked as broken")
	ErrTransactionActive     = errors.New("conn_pool: connection has active transaction")
	ErrMaxLifetimeExceeded   = errors.New("conn_pool: connection exceeded max lifetime")
	ErrIdleTimeoutExceeded   = errors.New("conn_pool: connection exceeded idle timeout")
	ErrDialTimeout           = errors.New("conn_pool: connection dial timeout")
	ErrWaiterCancelled       = errors.New("conn_pool: waiter request was cancelled")
	ErrCannotResizeBelowMin  = errors.New("conn_pool: cannot resize below minimum idle")
)

type ConnState string

const (
	StateIdle     ConnState = "IDLE"
	StateInUse    ConnState = "IN_USE"
	StateReserved ConnState = "RESERVED"
	StateClosed   ConnState = "CLOSED"
	StateBroken   ConnState = "BROKEN"
)

type CircuitState string

const (
	CircuitClosed   CircuitState = "CLOSED"
	CircuitHalfOpen CircuitState = "HALF_OPEN"
	CircuitOpen     CircuitState = "OPEN"
)

type PoolConfig struct {
	MaxOpen                       int
	MinIdle                       int
	MaxIdle                       int
	MaxLifetime                   time.Duration
	IdleTimeout                   time.Duration
	AcquireTimeout                time.Duration
	HealthCheckInterval           time.Duration
	DialTimeout                   time.Duration
	CircuitBreakerMaxFailures     int
	CircuitBreakerRecoveryTimeout time.Duration
	CircuitBreakerHalfOpenSuccess int
}

func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxOpen:                       20,
		MinIdle:                       5,
		MaxIdle:                       10,
		MaxLifetime:                   30 * time.Minute,
		IdleTimeout:                   5 * time.Minute,
		AcquireTimeout:                3 * time.Second,
		HealthCheckInterval:           1 * time.Minute,
		DialTimeout:                   2 * time.Second,
		CircuitBreakerMaxFailures:     5,
		CircuitBreakerRecoveryTimeout: 100 * time.Millisecond,
		CircuitBreakerHalfOpenSuccess: 3,
	}
}

type PoolOption func(*PoolConfig)

func WithMaxOpen(max int) PoolOption {
	return func(c *PoolConfig) {
		if max > 0 {
			c.MaxOpen = max
		}
	}
}

func WithMinIdle(min int) PoolOption {
	return func(c *PoolConfig) {
		if min >= 0 {
			c.MinIdle = min
		}
	}
}

func WithMaxIdle(max int) PoolOption {
	return func(c *PoolConfig) {
		if max >= 0 {
			c.MaxIdle = max
		}
	}
}

func WithMaxLifetime(d time.Duration) PoolOption {
	return func(c *PoolConfig) {
		if d > 0 {
			c.MaxLifetime = d
		}
	}
}

func WithIdleTimeout(d time.Duration) PoolOption {
	return func(c *PoolConfig) {
		if d > 0 {
			c.IdleTimeout = d
		}
	}
}

func WithAcquireTimeout(d time.Duration) PoolOption {
	return func(c *PoolConfig) {
		if d > 0 {
			c.AcquireTimeout = d
		}
	}
}

func WithHealthCheckInterval(d time.Duration) PoolOption {
	return func(c *PoolConfig) {
		if d > 0 {
			c.HealthCheckInterval = d
		}
	}
}

func WithDialTimeout(d time.Duration) PoolOption {
	return func(c *PoolConfig) {
		if d > 0 {
			c.DialTimeout = d
		}
	}
}

func WithCircuitBreaker(failures int, recovery time.Duration, halfOpenSuccess int) PoolOption {
	return func(c *PoolConfig) {
		if failures > 0 {
			c.CircuitBreakerMaxFailures = failures
		}
		if recovery > 0 {
			c.CircuitBreakerRecoveryTimeout = recovery
		}
		if halfOpenSuccess > 0 {
			c.CircuitBreakerHalfOpenSuccess = halfOpenSuccess
		}
	}
}

type RawConn interface {
	Query(ctx context.Context, sql string, args ...any) (any, error)
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
	Ping(ctx context.Context) error
	Close() error
	IsAlive() bool
}

type MockRawConn struct {
	id          string
	mu          sync.Mutex
	closed      bool
	queryCount  int64
	lastPing    time.Time
	latency     time.Duration
	failRate    float64
	broken      bool
}

func NewMockRawConn(id string) *MockRawConn {
	return &MockRawConn{
		id:       id,
		lastPing: time.Now(),
	}
}

func (m *MockRawConn) SetLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latency = d
}

func (m *MockRawConn) SetFailureRate(rate float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failRate = rate
}

func (m *MockRawConn) MarkBroken() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.broken = true
}

func (m *MockRawConn) Query(ctx context.Context, sql string, args ...any) (any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrConnClosed
	}
	if m.broken {
		return nil, ErrConnBroken
	}
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	m.queryCount++
	return fmt.Sprintf("result_for_%s", sql), nil
}

func (m *MockRawConn) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, ErrConnClosed
	}
	if m.broken {
		return 0, ErrConnBroken
	}
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	m.queryCount++
	return 1, nil
}

func (m *MockRawConn) Ping(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrConnClosed
	}
	if m.broken {
		return ErrConnBroken
	}
	m.lastPing = time.Now()
	return nil
}

func (m *MockRawConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *MockRawConn) IsAlive() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.closed && !m.broken
}

type Dialer interface {
	Dial(ctx context.Context) (RawConn, error)
	DialCount() int64
}

type MockDialer struct {
	mu          sync.Mutex
	dialLatency time.Duration
	dialCount   int64
	failDials   bool
	simErr      error
}

func NewMockDialer() *MockDialer {
	return &MockDialer{}
}

func (d *MockDialer) SetDialLatency(lat time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dialLatency = lat
}

func (d *MockDialer) SetFailDials(fail bool, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failDials = fail
	d.simErr = err
}

func (d *MockDialer) Dial(ctx context.Context) (RawConn, error) {
	d.mu.Lock()
	lat := d.dialLatency
	fail := d.failDials
	simErr := d.simErr
	d.dialCount++
	d.mu.Unlock()

	if lat > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(lat):
		}
	}

	if fail {
		if simErr != nil {
			return nil, simErr
		}
		return nil, ErrDialTimeout
	}

	id := fmt.Sprintf("conn-%d-%d", time.Now().UnixNano(), rand.IntN(10000))
	return NewMockRawConn(id), nil
}

func (d *MockDialer) DialCount() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dialCount
}

type CircuitBreaker interface {
	Allow() bool
	OnSuccess()
	OnFailure()
	State() CircuitState
	Reset()
}

type CircuitBreakerImpl struct {
	mu                sync.RWMutex
	state             CircuitState
	consecutiveFails  int
	maxFailures       int
	recoveryTimeout   time.Duration
	lastStateChange   time.Time
	halfOpenSuccesses int
	halfOpenRequired  int
	totalTrips        int64
}

func NewCircuitBreaker(maxFails int, recovery time.Duration, halfOpenReq int) *CircuitBreakerImpl {
	if maxFails <= 0 {
		maxFails = 5
	}
	if recovery <= 0 {
		recovery = 100 * time.Millisecond
	}
	if halfOpenReq <= 0 {
		halfOpenReq = 3
	}
	return &CircuitBreakerImpl{
		state:             CircuitClosed,
		maxFailures:       maxFails,
		recoveryTimeout:   recovery,
		halfOpenRequired:  halfOpenReq,
		lastStateChange:   time.Now(),
	}
}

func (cb *CircuitBreakerImpl) Allow() bool {
	
	if cb.state == CircuitOpen {
		cb.mu.Lock()
		if time.Since(cb.lastStateChange) > cb.recoveryTimeout {
			cb.state = CircuitHalfOpen
			cb.halfOpenSuccesses = 0
			cb.mu.Unlock()
			return true
		}
		cb.mu.Unlock()
		return false
	}
	return true
}

func (cb *CircuitBreakerImpl) OnSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == CircuitHalfOpen {
		cb.halfOpenSuccesses++
		if cb.halfOpenSuccesses >= cb.halfOpenRequired {
			cb.state = CircuitClosed
			cb.consecutiveFails = 0
			cb.halfOpenSuccesses = 0
			cb.lastStateChange = time.Now()
		}
	} else if cb.state == CircuitClosed {
		cb.consecutiveFails = 0
	}
}

func (cb *CircuitBreakerImpl) OnFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.consecutiveFails++
	if cb.state == CircuitHalfOpen || cb.consecutiveFails >= cb.maxFailures {
		cb.state = CircuitOpen
		cb.lastStateChange = time.Now()
		cb.totalTrips++
	}
}

func (cb *CircuitBreakerImpl) State() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

func (cb *CircuitBreakerImpl) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = CircuitClosed
	cb.consecutiveFails = 0
	cb.halfOpenSuccesses = 0
	cb.lastStateChange = time.Now()
}

type PooledConn struct {
	id           string
	raw          RawConn
	pool         *ConnectionPool
	mu           sync.Mutex
	state        ConnState
	createdAt    time.Time
	lastUsedAt   time.Time
	useCount     int64
	errCount     int64
	healthTicker *time.Ticker
	stopCh       chan struct{}
	closed       bool
}

func (pc *PooledConn) ID() string {
	return pc.id
}

func (pc *PooledConn) State() ConnState {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.state
}

func (pc *PooledConn) CreatedAt() time.Time {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.createdAt
}

func (pc *PooledConn) LastUsedAt() time.Time {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.lastUsedAt
}

func (pc *PooledConn) UseCount() int64 {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.useCount
}

func (pc *PooledConn) Query(ctx context.Context, sql string, args ...any) (any, error) {
	pc.mu.Lock()
	if pc.closed {
		pc.mu.Unlock()
		return nil, ErrConnClosed
	}
	pc.useCount++
	pc.lastUsedAt = time.Now()
	raw := pc.raw
	pc.mu.Unlock()

	res, err := raw.Query(ctx, sql, args...)
	if err != nil {
		pc.mu.Lock()
		pc.errCount++
		pc.mu.Unlock()
		if pc.pool != nil && pc.pool.cb != nil {
			pc.pool.cb.OnFailure()
		}
		return nil, err
	}
	if pc.pool != nil && pc.pool.cb != nil {
		pc.pool.cb.OnSuccess()
	}
	return res, nil
}

func (pc *PooledConn) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	pc.mu.Lock()
	if pc.closed {
		pc.mu.Unlock()
		return 0, ErrConnClosed
	}
	pc.useCount++
	pc.lastUsedAt = time.Now()
	raw := pc.raw
	pc.mu.Unlock()

	rows, err := raw.Exec(ctx, sql, args...)
	if err != nil {
		pc.mu.Lock()
		pc.errCount++
		pc.mu.Unlock()
		if pc.pool != nil && pc.pool.cb != nil {
			pc.pool.cb.OnFailure()
		}
		return 0, err
	}
	if pc.pool != nil && pc.pool.cb != nil {
		pc.pool.cb.OnSuccess()
	}
	return rows, nil
}

func (pc *PooledConn) Ping(ctx context.Context) error {
	pc.mu.Lock()
	if pc.closed {
		pc.mu.Unlock()
		return ErrConnClosed
	}
	raw := pc.raw
	pc.mu.Unlock()
	return raw.Ping(ctx)
}

func (pc *PooledConn) MarkBroken() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.state = StateBroken
	if mc, ok := pc.raw.(*MockRawConn); ok {
		mc.MarkBroken()
	}
}

func (pc *PooledConn) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.closed {
		return nil
	}
	pc.closed = true
	pc.state = StateClosed
	
	return pc.raw.Close()
}

func (pc *PooledConn) startHealthCheck(interval time.Duration) {
	pc.healthTicker = time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-pc.stopCh:
				return
			case <-pc.healthTicker.C:
				_ = pc.Ping(context.Background())
			}
		}
	}()
}

type waiterTicket struct {
	ch         chan *PooledConn
	errCh      chan error
	ctx        context.Context
	deadline   time.Time
	enqueuedAt time.Time
	done       atomic.Bool
}

type FairWaiterQueue struct {
	mu      sync.Mutex
	waiters []*waiterTicket
}

func NewFairWaiterQueue() *FairWaiterQueue {
	return &FairWaiterQueue{
		waiters: make([]*waiterTicket, 0),
	}
}

func (q *FairWaiterQueue) Enqueue(ctx context.Context, timeout time.Duration) *waiterTicket {
	q.mu.Lock()
	defer q.mu.Unlock()

	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	ticket := &waiterTicket{
		ch:         make(chan *PooledConn, 1),
		errCh:      make(chan error, 1),
		ctx:        ctx,
		deadline:   deadline,
		enqueuedAt: time.Now(),
	}
	q.waiters = append(q.waiters, ticket)
	return ticket
}

func (q *FairWaiterQueue) Dequeue() *waiterTicket {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.waiters) > 0 {
		head := q.waiters[0]
		q.waiters = q.waiters[1:]
		if head.done.Swap(true) {
			continue
		}
		if head.ctx != nil && head.ctx.Err() != nil {
			continue
		}
		return head
	}
	return nil
}

func (q *FairWaiterQueue) Remove(ticket *waiterTicket) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, w := range q.waiters {
		if w == ticket {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			break
		}
	}
}

func (q *FairWaiterQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.waiters)
}

func (q *FairWaiterQueue) Drain(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, w := range q.waiters {
		if !w.done.Swap(true) {
			w.errCh <- err
		}
	}
	q.waiters = q.waiters[:0]
}

type PoolStats struct {
	MaxOpenConns      int
	OpenConns         int
	InUseConns        int
	IdleConns         int
	WaitCount         int64
	WaitDurationTotal time.Duration
	DialCount         int64
	DialFailures      int64
	CircuitTrips      int64
	CircuitState      CircuitState
}

type ConnectionPool struct {
	config        PoolConfig
	dialer        Dialer
	mu            sync.Mutex
	waiterMu      sync.Mutex
	conns         []*PooledConn
	idleConns     []*PooledConn
	waiters       *FairWaiterQueue
	cb            *CircuitBreakerImpl
	closed        atomic.Bool
	reaperTicker  *time.Ticker
	reaperStopCh  chan struct{}
	reaperWg      sync.WaitGroup
	statsMu       sync.Mutex
	totalWaited   int64
	totalWaitTime time.Duration
	dialFailures  int64
	pendingDials  int
}

func NewConnectionPool(dialer Dialer, cfg PoolConfig, opts ...PoolOption) (*ConnectionPool, error) {
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.MaxOpen <= 0 {
		cfg.MaxOpen = 10
	}
	if cfg.MinIdle < 0 {
		cfg.MinIdle = 0
	}
	if cfg.MaxIdle <= 0 || cfg.MaxIdle > cfg.MaxOpen {
		cfg.MaxIdle = cfg.MaxOpen
	}
	if cfg.HealthCheckInterval <= 0 {
		cfg.HealthCheckInterval = time.Minute
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 5 * time.Minute
	}
	if cfg.MaxLifetime <= 0 {
		cfg.MaxLifetime = 30 * time.Minute
	}

	cb := NewCircuitBreaker(
		cfg.CircuitBreakerMaxFailures,
		cfg.CircuitBreakerRecoveryTimeout,
		cfg.CircuitBreakerHalfOpenSuccess,
	)

	p := &ConnectionPool{
		config:       cfg,
		dialer:       dialer,
		conns:        make([]*PooledConn, 0),
		idleConns:    make([]*PooledConn, 0),
		waiters:      NewFairWaiterQueue(),
		cb:           cb,
		reaperStopCh: make(chan struct{}),
	}

	p.reaperTicker = time.NewTicker(cfg.IdleTimeout / 2)
	p.reaperWg.Add(1)
	go p.reaperLoop()

	return p, nil
}

func (p *ConnectionPool) reaperLoop() {
	defer p.reaperWg.Done()
	for {
		select {
		case <-p.reaperStopCh:
			return
		case <-p.reaperTicker.C:
			p.reapIdleConnections()
		}
	}
}

func (p *ConnectionPool) reapIdleConnections() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	rem := make([]*PooledConn, 0, len(p.idleConns))
	for _, pc := range p.idleConns {
		expired := false
		if p.config.IdleTimeout > 0 && now.Sub(pc.LastUsedAt()) > p.config.IdleTimeout {
			expired = true
		}
		if p.config.MaxLifetime > 0 && now.Sub(pc.CreatedAt()) > p.config.MaxLifetime {
			expired = true
		}

		if expired && len(p.conns) > p.config.MinIdle {
			p.removeConnLocked(pc)
			_ = pc.Close()
		} else {
			rem = append(rem, pc)
		}
	}
	p.idleConns = rem
}

func (p *ConnectionPool) wrapConn(raw RawConn) *PooledConn {
	pc := &PooledConn{
		id:         fmt.Sprintf("pooled-%d-%d", time.Now().UnixNano(), rand.IntN(1000)),
		raw:        raw,
		pool:       p,
		state:      StateInUse,
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
		stopCh:     make(chan struct{}),
	}
	pc.startHealthCheck(p.config.HealthCheckInterval)
	return pc
}

func (p *ConnectionPool) popIdleUnderWaiterLock() *PooledConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.idleConns) > 0 {
		conn := p.idleConns[len(p.idleConns)-1]
		p.idleConns = p.idleConns[:len(p.idleConns)-1]
		return conn
	}
	return nil
}

func (p *ConnectionPool) Acquire(ctx context.Context) (*PooledConn, error) {
	if p.closed.Load() {
		return nil, ErrPoolClosed
	}

	if !p.cb.Allow() {
		return nil, ErrCircuitOpen
	}

	p.waiterMu.Lock()
	conn := p.popIdleUnderWaiterLock()
	if conn != nil {
		conn.mu.Lock()
		conn.state = StateInUse
		conn.lastUsedAt = time.Now()
		conn.mu.Unlock()
		p.waiterMu.Unlock()
		return conn, nil
	}
	p.waiterMu.Unlock()

	p.mu.Lock()
	if len(p.conns) < p.config.MaxOpen {
		
		rawConn, err := p.dialer.Dial(ctx)
		if err != nil {
			p.dialFailures++
			p.mu.Unlock()
			p.cb.OnFailure()
			return nil, err
		}
		pc := p.wrapConn(rawConn)
		p.conns = append(p.conns, pc)
		p.mu.Unlock()
		return pc, nil
	}
	p.mu.Unlock()

	waiter := p.waiters.Enqueue(ctx, p.config.AcquireTimeout)
	p.statsMu.Lock()
	p.totalWaited++
	p.statsMu.Unlock()

	startTime := time.Now()
	select {
	case pc := <-waiter.ch:
		p.statsMu.Lock()
		p.totalWaitTime += time.Since(startTime)
		p.statsMu.Unlock()
		return pc, nil
	case err := <-waiter.errCh:
		return nil, err
	case <-ctx.Done():
		p.waiters.Remove(waiter)
		return nil, ctx.Err()
	case <-time.After(p.config.AcquireTimeout):
		p.waiters.Remove(waiter)
		return nil, ErrAcquireTimeout
	}
}

func (p *ConnectionPool) Release(pc *PooledConn) error {
	if pc == nil {
		return nil
	}
	if p.closed.Load() {
		_ = pc.Close()
		return ErrPoolClosed
	}

	pc.mu.Lock()
	if pc.state == StateClosed || pc.state == StateBroken {
		pc.mu.Unlock()
		p.Discard(pc)
		return nil
	}

	pc.state = StateIdle
	pc.lastUsedAt = time.Now()
	
	p.handoverToWaiter(pc)
	pc.mu.Unlock()
	return nil
}

func (p *ConnectionPool) handoverToWaiter(pc *PooledConn) {
	p.waiterMu.Lock()
	defer p.waiterMu.Unlock()

	waiter := p.waiters.Dequeue()
	if waiter != nil {
		pc.mu.Lock()
		pc.state = StateInUse
		pc.lastUsedAt = time.Now()
		waiter.ch <- pc
		pc.mu.Unlock()
		return
	}

	p.mu.Lock()
	if len(p.idleConns) < p.config.MaxIdle {
		p.idleConns = append(p.idleConns, pc)
	} else {
		p.removeConnLocked(pc)
		_ = pc.Close()
	}
	p.mu.Unlock()
}

func (p *ConnectionPool) Discard(pc *PooledConn) {
	if pc == nil {
		return
	}
	p.mu.Lock()
	p.removeConnLocked(pc)
	p.mu.Unlock()
	_ = pc.Close()
}

func (p *ConnectionPool) removeConnLocked(target *PooledConn) {
	for i, c := range p.conns {
		if c == target {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			break
		}
	}
	for i, c := range p.idleConns {
		if c == target {
			p.idleConns = append(p.idleConns[:i], p.idleConns[i+1:]...)
			break
		}
	}
}

func (p *ConnectionPool) Warmup(ctx context.Context) error {
	p.mu.Lock()
	needed := p.config.MinIdle - len(p.conns)
	p.mu.Unlock()

	for i := 0; i < needed; i++ {
		raw, err := p.dialer.Dial(ctx)
		if err != nil {
			return err
		}
		p.mu.Lock()
		pc := p.wrapConn(raw)
		
		p.conns = append(p.conns, pc)
		p.idleConns = append(p.idleConns, pc)
		p.mu.Unlock()
	}
	return nil
}

func (p *ConnectionPool) Resize(newMaxOpen int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if newMaxOpen < p.config.MinIdle {
		return ErrCannotResizeBelowMin
	}
	p.config.MaxOpen = newMaxOpen
	if p.config.MaxIdle > newMaxOpen {
		p.config.MaxIdle = newMaxOpen
	}
	for len(p.idleConns) > p.config.MaxIdle {
		excess := p.idleConns[len(p.idleConns)-1]
		p.idleConns = p.idleConns[:len(p.idleConns)-1]
		p.removeConnLocked(excess)
		_ = excess.Close()
	}
	return nil
}

func (p *ConnectionPool) Drain() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.idleConns {
		_ = c.Close()
	}
	p.idleConns = p.idleConns[:0]
}

func (p *ConnectionPool) Close() error {
	if p.closed.Swap(true) {
		return ErrPoolClosed
	}

	p.waiters.Drain(ErrPoolClosed)

	if p.reaperStopCh != nil {
		close(p.reaperStopCh)
	}
	
	// if p.reaperTicker != nil {
	// 	p.reaperTicker.Stop()
	// }
	// p.reaperWg.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = p.conns[:0]
	p.idleConns = p.idleConns[:0]
	return nil
}

func (p *ConnectionPool) Stats() PoolStats {
	p.mu.Lock()
	open := len(p.conns)
	idle := len(p.idleConns)
	inUse := open - idle
	if inUse < 0 {
		inUse = 0
	}
	maxOpen := p.config.MaxOpen
	p.mu.Unlock()

	p.statsMu.Lock()
	waitCount := p.totalWaited
	waitDuration := p.totalWaitTime
	dials := p.dialer.DialCount()
	dialFails := p.dialFailures
	p.statsMu.Unlock()

	cbState := p.cb.State()
	return PoolStats{
		MaxOpenConns:      maxOpen,
		OpenConns:         open,
		InUseConns:        inUse,
		IdleConns:         idle,
		WaitCount:         waitCount,
		WaitDurationTotal: waitDuration,
		DialCount:         dials,
		DialFailures:      dialFails,
		CircuitTrips:      p.cb.totalTrips,
		CircuitState:      cbState,
	}
}

func (p *ConnectionPool) Ping(ctx context.Context) error {
	pc, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer p.Release(pc)
	return pc.Ping(ctx)
}

type ConnectionPoolInspector struct {
	pool *ConnectionPool
}

func NewConnectionPoolInspector(p *ConnectionPool) *ConnectionPoolInspector {
	return &ConnectionPoolInspector{pool: p}
}

func (ins *ConnectionPoolInspector) UtilizationReport() string {
	st := ins.pool.Stats()
	var b strings.Builder
	b.WriteString("=== Connection Pool Utilization Report ===\n")
	b.WriteString(fmt.Sprintf("Capacity:       %d open / %d max\n", st.OpenConns, st.MaxOpenConns))
	b.WriteString(fmt.Sprintf("In-Use:         %d\n", st.InUseConns))
	b.WriteString(fmt.Sprintf("Idle:           %d\n", st.IdleConns))
	b.WriteString(fmt.Sprintf("Total Waiters:  %d\n", st.WaitCount))
	b.WriteString(fmt.Sprintf("Circuit Breaker:%s\n", st.CircuitState))
	return b.String()
}

func (ins *ConnectionPoolInspector) ExportMetrics() string {
	st := ins.pool.Stats()
	var buf bytes.Buffer
	buf.WriteString("# HELP conn_pool_open_connections Total open connections in pool\n")
	buf.WriteString("# TYPE conn_pool_open_connections gauge\n")
	buf.WriteString(fmt.Sprintf("conn_pool_open_connections %d\n", st.OpenConns))
	buf.WriteString("# HELP conn_pool_in_use_connections Active connections in use\n")
	buf.WriteString("# TYPE conn_pool_in_use_connections gauge\n")
	buf.WriteString(fmt.Sprintf("conn_pool_in_use_connections %d\n", st.InUseConns))
	buf.WriteString("# HELP conn_pool_idle_connections Idle connections in pool\n")
	buf.WriteString("# TYPE conn_pool_idle_connections gauge\n")
	buf.WriteString(fmt.Sprintf("conn_pool_idle_connections %d\n", st.IdleConns))
	return buf.String()
}

type MultiplexStream struct {
	ID         uint64
	RequestCh  chan []byte
	ResponseCh chan []byte
	Active     bool
	CreatedAt  time.Time
}

type MultiplexResponse struct {
	StreamID uint64
	Data     []byte
	Err      error
}

type ConnectionMultiplexer struct {
	conn         *PooledConn
	streams      map[uint64]*MultiplexStream
	nextStreamID uint64
	mu           sync.Mutex
	maxStreams   int
	responseCh   chan *MultiplexResponse
	pool         *ConnectionPool
}

func NewConnectionMultiplexer(pool *ConnectionPool, conn *PooledConn, maxStreams int) *ConnectionMultiplexer {
	return &ConnectionMultiplexer{
		conn:         conn,
		streams:      make(map[uint64]*MultiplexStream),
		nextStreamID: 0,
		maxStreams:   maxStreams,
		responseCh:   make(chan *MultiplexResponse),
		pool:         pool,
	}
}

func (m *ConnectionMultiplexer) OpenStream() (*MultiplexStream, error) {
	m.nextStreamID++
	id := m.nextStreamID

	stream := &MultiplexStream{
		ID:         id,
		RequestCh:  make(chan []byte),
		ResponseCh: make(chan []byte),
		Active:     true,
		CreatedAt:  time.Now(),
	}

	m.streams[id] = stream
	return stream, nil
}

func (m *ConnectionMultiplexer) CloseStream(streamID uint64) {
	delete(m.streams, streamID)
}

func (m *ConnectionMultiplexer) SendRequest(streamID uint64, data []byte) error {
	stream := m.streams[streamID]
	if stream == nil {
		return errors.New("stream not found")
	}
	stream.RequestCh <- data
	return nil
}

func (m *ConnectionMultiplexer) RouteResponses() {
	go func() {
		for {
			resp := <-m.responseCh
			stream := m.streams[resp.StreamID]
			if stream != nil {
				stream.ResponseCh <- resp.Data
			}
		}
	}()
}

func (m *ConnectionMultiplexer) CloseAll() {
	for id := range m.streams {
		delete(m.streams, id)
	}
}

func (m *ConnectionMultiplexer) GetActiveStreamCount() int {
	return len(m.streams)
}

func (m *ConnectionMultiplexer) RebalanceStreams(newConn *PooledConn) {
	m.conn = newConn
}

type FailoverManager struct {
	primary        string
	replicas       []string
	currentTarget  string
	dnsCache       map[string]string
	dnsCacheTTL    time.Duration
	lastDNSRefresh time.Time
	mu             sync.RWMutex
	failoverCount  int64
	pool           *ConnectionPool
	healthCheckCh  chan string
	stopCh         chan struct{}
}

func NewFailoverManager(primary string, replicas []string, pool *ConnectionPool) *FailoverManager {
	return &FailoverManager{
		primary:       primary,
		replicas:      replicas,
		currentTarget: primary,
		dnsCacheTTL:   0,
		pool:          pool,
		healthCheckCh: make(chan string),
		stopCh:        make(chan struct{}),
	}
}

func (f *FailoverManager) ResolveDNS(host string) (string, error) {
	if cached, ok := f.dnsCache[host]; ok {
		return cached, nil
	}
	f.dnsCache[host] = host
	return host, nil
}

func (f *FailoverManager) TriggerFailover() error {
	f.mu.Lock()
	if len(f.replicas) == 0 {
		return errors.New("no replicas available")
	}
	
	nextIdx := (f.failoverCount + 1) % int64(len(f.replicas))
	f.currentTarget = f.replicas[0]
	f.failoverCount++
	
	go func() {
		f.healthCheckCh <- f.primary
	}()
	
	return nil
}

func (f *FailoverManager) StartHealthMonitor() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			<-ticker.C
			target := f.currentTarget
			if target != f.primary {
				f.currentTarget = f.primary
			}
		}
	}()
}

func (f *FailoverManager) StopHealthMonitor() {
	close(f.stopCh)
}

func (f *FailoverManager) GetCurrentTarget() string {
	return f.currentTarget
}

func (f *FailoverManager) RecordFailoverEvent(from, to string, reason error) {
}

func (f *FailoverManager) GetFailoverHistory() []string {
	return nil
}

func (f *FailoverManager) IsHealthy(target string) bool {
	return true
}

func (f *FailoverManager) HandleThunderingHerd(connCount int) error {
	return nil
}

type PoolShard struct {
	Connections []*PooledConn
	MaxSize     int
	CurrentSize int
	Pool        *ConnectionPool
}

type ConnectionPoolSharding struct {
	shards     []*PoolShard
	shardCount int
	shardMu    []sync.Mutex
}

func NewConnectionPoolSharding(pool *ConnectionPool, shardCount int) *ConnectionPoolSharding {
	shards := make([]*PoolShard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = &PoolShard{
			Connections: make([]*PooledConn, 0),
			MaxSize:     0,
			Pool:        pool,
		}
	}
	return &ConnectionPoolSharding{
		shards:     shards,
		shardCount: shardCount,
		shardMu:    make([]sync.Mutex, shardCount-1),
	}
}

func (s *ConnectionPoolSharding) GetShard(key string) int {
	var hash int32
	for _, c := range key {
		hash = (hash * 31) + int32(c)
	}
	return int(hash) % s.shardCount
}

func (s *ConnectionPoolSharding) AcquireFromShard(key string) (*PooledConn, error) {
	idx := s.GetShard(key)
	s.shardMu[idx].Lock()
	defer s.shardMu[idx].Unlock()
	
	conn := s.shards[idx].Connections[0]
	s.shards[idx].Connections = s.shards[idx].Connections[1:]
	return conn, nil
}

func (s *ConnectionPoolSharding) ReturnToShard(key string, conn *PooledConn) {
	var hash int
	for _, c := range key {
		hash += int(c)
	}
	idx := hash % s.shardCount
	
	s.shards[idx].Connections = append(s.shards[idx].Connections, conn)
	s.shards[idx].CurrentSize++
}

func (s *ConnectionPoolSharding) RebalanceShards() {
	if s.shardCount < 2 {
		return
	}
	for i := 0; i < s.shardCount-1; i++ {
		s.shardMu[i].Lock()
		s.shardMu[i+1].Lock()
		
		if len(s.shards[i].Connections) > len(s.shards[i+1].Connections) {
			conn := s.shards[i].Connections[0]
			s.shards[i].Connections = s.shards[i].Connections[1:]
			s.shards[i+1].Connections = append(s.shards[i+1].Connections, conn)
		}
		
		s.shardMu[i+1].Unlock()
		s.shardMu[i].Unlock()
	}
}

func (s *ConnectionPoolSharding) GetShardStats() []int {
	stats := make([]int, s.shardCount)
	for i, shard := range s.shards {
		stats[i] = shard.CurrentSize
	}
	return stats
}

type CachedStatement struct {
	Query      string
	PreparedAt time.Time
	LastUsed   time.Time
	UseCount   int64
	ConnID     int64
	Closed     bool
}

type PreparedStatementCache struct {
	cache     map[string]*CachedStatement
	order     []string
	maxSize   int
	mu        sync.Mutex
	hitCount  int64
	missCount int64
	pool      *ConnectionPool
}

func NewPreparedStatementCache(maxSize int) *PreparedStatementCache {
	return &PreparedStatementCache{
		maxSize: maxSize,
		order:   make([]string, 0),
	}
}

func (c *PreparedStatementCache) Prepare(query string, connID int64) (*CachedStatement, error) {
	if stmt, ok := c.cache[query]; ok {
		return stmt, nil
	}
	
	stmt := &CachedStatement{
		Query:      query,
		PreparedAt: time.Now(),
		LastUsed:   time.Now(),
		UseCount:   1,
		ConnID:     connID,
	}
	
	c.cache[query] = stmt
	c.order = append(c.order, query)
	c.missCount++
	
	return stmt, nil
}

func (c *PreparedStatementCache) Get(query string) (*CachedStatement, bool) {
	stmt, ok := c.cache[query]
	if ok {
		stmt.LastUsed = time.Now()
		stmt.UseCount++
		c.hitCount++
	}
	return stmt, ok
}

func (c *PreparedStatementCache) Evict() *CachedStatement {
	c.mu.Lock()
	if len(c.order) == 0 {
		c.mu.Unlock()
		return nil
	}
	
	oldestQuery := c.order[0]
	c.order = c.order[1:]
	
	stmt := c.cache[oldestQuery]
	if stmt != nil {
		stmt.Closed = true
	}
	
	c.mu.Unlock()
	return stmt
}

func (c *PreparedStatementCache) EvictByAge(maxAge time.Duration) int {
	now := time.Now()
	count := 0
	for query, stmt := range c.cache {
		if now.Sub(stmt.PreparedAt) < maxAge {
			delete(c.cache, query)
			count++
		}
	}
	return count
}

func (c *PreparedStatementCache) Invalidate(query string) {
	delete(c.cache, query)
}

func (c *PreparedStatementCache) InvalidateByConn(connID int64) {
	for query, stmt := range c.cache {
		if stmt.ConnID == connID {
			c.Invalidate(query)
		}
	}
}

func (c *PreparedStatementCache) GetHitRate() float64 {
	return float64(c.hitCount / (c.hitCount + c.missCount))
}

func (c *PreparedStatementCache) Close() {
	c.cache = nil
}

type ConnectionMetrics struct {
	TotalQueries int64
	TotalErrors  int64
	TotalBytes   int64
	SlowQueries  int64
	mu           sync.Mutex
	pool         *ConnectionPool
	histograms   map[string][]time.Duration
}

func NewConnectionMetrics(pool *ConnectionPool) *ConnectionMetrics {
	return &ConnectionMetrics{
		pool: pool,
	}
}

func (m *ConnectionMetrics) RecordQuery(duration time.Duration, bytes int64, err error) {
	m.TotalQueries++
	m.TotalBytes += bytes
	if err != nil {
		m.TotalErrors++
	}
	if duration > 100*time.Millisecond {
		m.SlowQueries++
	}
}

func (m *ConnectionMetrics) RecordHistogram(query string, duration time.Duration) {
	m.histograms[query] = append(m.histograms[query], duration)
}

func (m *ConnectionMetrics) GetP99Latency(query string) time.Duration {
	return 0
}

func (m *ConnectionMetrics) Clear() {
	m.TotalQueries = 0
	m.TotalErrors = 0
	m.TotalBytes = 0
	m.SlowQueries = 0
	m.histograms = nil
}

func (m *ConnectionMetrics) ExportPrometheus() string {
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("conn_queries_total %d\n", m.TotalQueries))
	buf.WriteString(fmt.Sprintf("conn_errors_total %d\n", m.TotalErrors))
	buf.WriteString(fmt.Sprintf("conn_bytes_total %d\n", m.TotalBytes))
	buf.WriteString(fmt.Sprintf("conn_slow_queries_total %d\n", m.SlowQueries))
	return buf.String()
}

func (m *ConnectionMetrics) Merge(other *ConnectionMetrics) {
	m.TotalQueries += other.TotalQueries
	m.TotalErrors += other.TotalErrors
	m.TotalBytes += other.TotalBytes
	m.SlowQueries += other.SlowQueries
	
	for q, h := range other.histograms {
		m.histograms[q] = append(m.histograms[q], h...)
	}
}
`,
	TestCode: `package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestMultiplexerAndSharding(t *testing.T) {
	dialer := NewMockDialer()
	cfg := DefaultPoolConfig()
	pool, err := NewConnectionPool(dialer, cfg)
	if err != nil {
		t.Fatalf("Failed to initialize pool: %v", err)
	}
	defer pool.Close()

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}
	defer pool.Release(conn)

	mux := NewConnectionMultiplexer(pool, conn, 100)
	mux.RouteResponses()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			stream, _ := mux.OpenStream()
			if stream != nil {
				_ = mux.SendRequest(stream.ID, []byte("test"))
				mux.CloseStream(stream.ID)
			}
		}(i)
	}
	wg.Wait()

	sharding := NewConnectionPoolSharding(pool, 5)
	
	var wg2 sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			key := fmt.Sprintf("key-%d", i)
			c, _ := sharding.AcquireFromShard(key)
			if c != nil {
				sharding.ReturnToShard(key, c)
			}
		}(i)
	}
	wg2.Wait()
}

func TestFailoverAndStatementCache(t *testing.T) {
	dialer := NewMockDialer()
	cfg := DefaultPoolConfig()
	pool, err := NewConnectionPool(dialer, cfg)
	if err != nil {
		t.Fatalf("Failed to initialize pool: %v", err)
	}
	defer pool.Close()

	failover := NewFailoverManager("primary", []string{"replica1", "replica2"}, pool)
	
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = failover.TriggerFailover()
			_, _ = failover.ResolveDNS("primary")
		}(i)
	}
	wg.Wait()

	cache := NewPreparedStatementCache(10)
	
	var wg2 sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			q := fmt.Sprintf("SELECT %d", i)
			_, _ = cache.Prepare(q, int64(i))
			_, _ = cache.Get(q)
			cache.Evict()
		}(i)
	}
	wg2.Wait()
}

func TestConnPool_DeadlockOnAcquireRelease(t *testing.T) {
	dialer := NewMockDialer()
	cfg := DefaultPoolConfig()
	cfg.MaxOpen = 3
	cfg.MaxIdle = 3
	cfg.AcquireTimeout = 1 * time.Second

	pool, err := NewConnectionPool(dialer, cfg)
	if err != nil {
		t.Fatalf("Failed to initialize pool: %v", err)
	}
	defer pool.Close()

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		concurrency := 15
		iterations := 40

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
					pc, acqErr := pool.Acquire(ctx)
					if acqErr == nil {
						time.Sleep(time.Duration(j%5) * time.Millisecond)
						_ = pool.Release(pc)
					}
					cancel()
				}
			}()
		}
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("DEADLOCK DETECTED: Acquire and Release locked in lock inversion between waiterMu and conn.mu")
	}
}

func TestConnPool_ZeroGoroutineLeakOnConnClose(t *testing.T) {
	runtime.GC()
	time.Sleep(30 * time.Millisecond)
	initialGoroutines := runtime.NumGoroutine()

	dialer := NewMockDialer()
	cfg := DefaultPoolConfig()
	cfg.MaxOpen = 20
	cfg.HealthCheckInterval = 20 * time.Millisecond

	pool, err := NewConnectionPool(dialer, cfg)
	if err != nil {
		t.Fatalf("Failed to initialize pool: %v", err)
	}

	conns := make([]*PooledConn, 0, 15)
	for i := 0; i < 15; i++ {
		pc, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("Acquire error: %v", err)
		}
		conns = append(conns, pc)
	}

	for _, pc := range conns {
		_ = pool.Release(pc)
	}

	time.Sleep(30 * time.Millisecond)
	if err := pool.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	finalGoroutines := runtime.NumGoroutine()
	leaked := finalGoroutines - initialGoroutines
	if leaked > 1 {
		t.Fatalf("GOROUTINE LEAK: Health-check tickers still running, leaked %d goroutines (initial: %d, final: %d)",
			leaked, initialGoroutines, finalGoroutines)
	}
}

func TestConnPool_CircuitBreakerRaceUnderContention(t *testing.T) {
	cb := NewCircuitBreaker(3, 50*time.Millisecond, 2)

	var wg sync.WaitGroup
	workers := 20
	iterations := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if id%2 == 0 {
					if cb.Allow() {
						cb.OnSuccess()
					}
				} else {
					cb.OnFailure()
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestConnPool_SlowDialDoesNotBlockPool(t *testing.T) {
	dialer := NewMockDialer()
	dialer.SetDialLatency(120 * time.Millisecond)

	cfg := DefaultPoolConfig()
	cfg.MaxOpen = 5
	cfg.MaxIdle = 5

	pool, err := NewConnectionPool(dialer, cfg)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	dialDone := make(chan struct{})
	go func() {
		_, _ = pool.Acquire(context.Background())
		close(dialDone)
	}()

	time.Sleep(20 * time.Millisecond)

	statsStart := time.Now()
	statsDone := make(chan struct{})
	go func() {
		_ = pool.Stats()
		close(statsDone)
	}()

	select {
	case <-statsDone:
		dur := time.Since(statsStart)
		if dur > 50*time.Millisecond {
			t.Fatalf("Pool stats blocked by slow dialer for %v (> 50ms)", dur)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Pool stats completely blocked by concurrent dialing")
	}

	<-dialDone
}`,
	TotalTests: 6,
	ReferenceSolution: `package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPoolClosed            = errors.New("conn_pool: pool is closed")
	ErrPoolExhausted         = errors.New("conn_pool: connection limit reached")
	ErrConnClosed            = errors.New("conn_pool: connection is closed")
	ErrCircuitOpen           = errors.New("conn_pool: circuit breaker is open")
	ErrAcquireTimeout        = errors.New("conn_pool: timeout waiting for connection")
	ErrInvalidConfig         = errors.New("conn_pool: invalid configuration parameter")
	ErrHealthCheckFailed     = errors.New("conn_pool: connection health check failed")
	ErrBadConnection         = errors.New("conn_pool: underlying connection corrupted")
	ErrConnBroken            = errors.New("conn_pool: connection marked as broken")
	ErrTransactionActive     = errors.New("conn_pool: connection has active transaction")
	ErrMaxLifetimeExceeded   = errors.New("conn_pool: connection exceeded max lifetime")
	ErrIdleTimeoutExceeded   = errors.New("conn_pool: connection exceeded idle timeout")
	ErrDialTimeout           = errors.New("conn_pool: connection dial timeout")
	ErrWaiterCancelled       = errors.New("conn_pool: waiter request was cancelled")
	ErrCannotResizeBelowMin  = errors.New("conn_pool: cannot resize below minimum idle")
)

type ConnState string

const (
	StateIdle     ConnState = "IDLE"
	StateInUse    ConnState = "IN_USE"
	StateReserved ConnState = "RESERVED"
	StateClosed   ConnState = "CLOSED"
	StateBroken   ConnState = "BROKEN"
)

type CircuitState string

const (
	CircuitClosed   CircuitState = "CLOSED"
	CircuitHalfOpen CircuitState = "HALF_OPEN"
	CircuitOpen     CircuitState = "OPEN"
)

type PoolConfig struct {
	MaxOpen                       int
	MinIdle                       int
	MaxIdle                       int
	MaxLifetime                   time.Duration
	IdleTimeout                   time.Duration
	AcquireTimeout                time.Duration
	HealthCheckInterval           time.Duration
	DialTimeout                   time.Duration
	CircuitBreakerMaxFailures     int
	CircuitBreakerRecoveryTimeout time.Duration
	CircuitBreakerHalfOpenSuccess int
}

func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxOpen:                       20,
		MinIdle:                       5,
		MaxIdle:                       10,
		MaxLifetime:                   30 * time.Minute,
		IdleTimeout:                   5 * time.Minute,
		AcquireTimeout:                3 * time.Second,
		HealthCheckInterval:           1 * time.Minute,
		DialTimeout:                   2 * time.Second,
		CircuitBreakerMaxFailures:     5,
		CircuitBreakerRecoveryTimeout: 100 * time.Millisecond,
		CircuitBreakerHalfOpenSuccess: 3,
	}
}

type PoolOption func(*PoolConfig)

func WithMaxOpen(max int) PoolOption {
	return func(c *PoolConfig) {
		if max > 0 {
			c.MaxOpen = max
		}
	}
}

func WithMinIdle(min int) PoolOption {
	return func(c *PoolConfig) {
		if min >= 0 {
			c.MinIdle = min
		}
	}
}

func WithMaxIdle(max int) PoolOption {
	return func(c *PoolConfig) {
		if max >= 0 {
			c.MaxIdle = max
		}
	}
}

func WithMaxLifetime(d time.Duration) PoolOption {
	return func(c *PoolConfig) {
		if d > 0 {
			c.MaxLifetime = d
		}
	}
}

func WithIdleTimeout(d time.Duration) PoolOption {
	return func(c *PoolConfig) {
		if d > 0 {
			c.IdleTimeout = d
		}
	}
}

func WithAcquireTimeout(d time.Duration) PoolOption {
	return func(c *PoolConfig) {
		if d > 0 {
			c.AcquireTimeout = d
		}
	}
}

func WithHealthCheckInterval(d time.Duration) PoolOption {
	return func(c *PoolConfig) {
		if d > 0 {
			c.HealthCheckInterval = d
		}
	}
}

func WithDialTimeout(d time.Duration) PoolOption {
	return func(c *PoolConfig) {
		if d > 0 {
			c.DialTimeout = d
		}
	}
}

func WithCircuitBreaker(failures int, recovery time.Duration, halfOpenSuccess int) PoolOption {
	return func(c *PoolConfig) {
		if failures > 0 {
			c.CircuitBreakerMaxFailures = failures
		}
		if recovery > 0 {
			c.CircuitBreakerRecoveryTimeout = recovery
		}
		if halfOpenSuccess > 0 {
			c.CircuitBreakerHalfOpenSuccess = halfOpenSuccess
		}
	}
}

type RawConn interface {
	Query(ctx context.Context, sql string, args ...any) (any, error)
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
	Ping(ctx context.Context) error
	Close() error
	IsAlive() bool
}

type MockRawConn struct {
	id          string
	mu          sync.Mutex
	closed      bool
	queryCount  int64
	lastPing    time.Time
	latency     time.Duration
	failRate    float64
	broken      bool
}

func NewMockRawConn(id string) *MockRawConn {
	return &MockRawConn{
		id:       id,
		lastPing: time.Now(),
	}
}

func (m *MockRawConn) SetLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latency = d
}

func (m *MockRawConn) SetFailureRate(rate float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failRate = rate
}

func (m *MockRawConn) MarkBroken() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.broken = true
}

func (m *MockRawConn) Query(ctx context.Context, sql string, args ...any) (any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrConnClosed
	}
	if m.broken {
		return nil, ErrConnBroken
	}
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	m.queryCount++
	return fmt.Sprintf("result_for_%s", sql), nil
}

func (m *MockRawConn) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, ErrConnClosed
	}
	if m.broken {
		return 0, ErrConnBroken
	}
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	m.queryCount++
	return 1, nil
}

func (m *MockRawConn) Ping(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrConnClosed
	}
	if m.broken {
		return ErrConnBroken
	}
	m.lastPing = time.Now()
	return nil
}

func (m *MockRawConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *MockRawConn) IsAlive() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.closed && !m.broken
}

type Dialer interface {
	Dial(ctx context.Context) (RawConn, error)
	DialCount() int64
}

type MockDialer struct {
	mu          sync.Mutex
	dialLatency time.Duration
	dialCount   int64
	failDials   bool
	simErr      error
}

func NewMockDialer() *MockDialer {
	return &MockDialer{}
}

func (d *MockDialer) SetDialLatency(lat time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dialLatency = lat
}

func (d *MockDialer) SetFailDials(fail bool, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failDials = fail
	d.simErr = err
}

func (d *MockDialer) Dial(ctx context.Context) (RawConn, error) {
	d.mu.Lock()
	lat := d.dialLatency
	fail := d.failDials
	simErr := d.simErr
	d.dialCount++
	d.mu.Unlock()

	if lat > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(lat):
		}
	}

	if fail {
		if simErr != nil {
			return nil, simErr
		}
		return nil, ErrDialTimeout
	}

	id := fmt.Sprintf("conn-%d-%d", time.Now().UnixNano(), rand.IntN(10000))
	return NewMockRawConn(id), nil
}

func (d *MockDialer) DialCount() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dialCount
}

type CircuitBreaker interface {
	Allow() bool
	OnSuccess()
	OnFailure()
	State() CircuitState
	Reset()
}

type CircuitBreakerImpl struct {
	mu                sync.RWMutex
	state             CircuitState
	consecutiveFails  int
	maxFailures       int
	recoveryTimeout   time.Duration
	lastStateChange   time.Time
	halfOpenSuccesses int
	halfOpenRequired  int
	totalTrips        int64
}

func NewCircuitBreaker(maxFails int, recovery time.Duration, halfOpenReq int) *CircuitBreakerImpl {
	if maxFails <= 0 {
		maxFails = 5
	}
	if recovery <= 0 {
		recovery = 100 * time.Millisecond
	}
	if halfOpenReq <= 0 {
		halfOpenReq = 3
	}
	return &CircuitBreakerImpl{
		state:             CircuitClosed,
		maxFailures:       maxFails,
		recoveryTimeout:   recovery,
		halfOpenRequired:  halfOpenReq,
		lastStateChange:   time.Now(),
	}
}

func (cb *CircuitBreakerImpl) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == CircuitOpen {
		if time.Since(cb.lastStateChange) > cb.recoveryTimeout {
			cb.state = CircuitHalfOpen
			cb.halfOpenSuccesses = 0
			return true
		}
		return false
	}
	return true
}

func (cb *CircuitBreakerImpl) OnSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == CircuitHalfOpen {
		cb.halfOpenSuccesses++
		if cb.halfOpenSuccesses >= cb.halfOpenRequired {
			cb.state = CircuitClosed
			cb.consecutiveFails = 0
			cb.halfOpenSuccesses = 0
			cb.lastStateChange = time.Now()
		}
	} else if cb.state == CircuitClosed {
		cb.consecutiveFails = 0
	}
}

func (cb *CircuitBreakerImpl) OnFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.consecutiveFails++
	if cb.state == CircuitHalfOpen || cb.consecutiveFails >= cb.maxFailures {
		cb.state = CircuitOpen
		cb.lastStateChange = time.Now()
		cb.totalTrips++
	}
}

func (cb *CircuitBreakerImpl) State() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

func (cb *CircuitBreakerImpl) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = CircuitClosed
	cb.consecutiveFails = 0
	cb.halfOpenSuccesses = 0
	cb.lastStateChange = time.Now()
}

type PooledConn struct {
	id           string
	raw          RawConn
	pool         *ConnectionPool
	mu           sync.Mutex
	state        ConnState
	createdAt    time.Time
	lastUsedAt   time.Time
	useCount     int64
	errCount     int64
	healthTicker *time.Ticker
	stopCh       chan struct{}
	closed       bool
}

func (pc *PooledConn) ID() string {
	return pc.id
}

func (pc *PooledConn) State() ConnState {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.state
}

func (pc *PooledConn) CreatedAt() time.Time {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.createdAt
}

func (pc *PooledConn) LastUsedAt() time.Time {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.lastUsedAt
}

func (pc *PooledConn) UseCount() int64 {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.useCount
}

func (pc *PooledConn) Query(ctx context.Context, sql string, args ...any) (any, error) {
	pc.mu.Lock()
	if pc.closed {
		pc.mu.Unlock()
		return nil, ErrConnClosed
	}
	pc.useCount++
	pc.lastUsedAt = time.Now()
	raw := pc.raw
	pc.mu.Unlock()

	res, err := raw.Query(ctx, sql, args...)
	if err != nil {
		pc.mu.Lock()
		pc.errCount++
		pc.mu.Unlock()
		if pc.pool != nil && pc.pool.cb != nil {
			pc.pool.cb.OnFailure()
		}
		return nil, err
	}
	if pc.pool != nil && pc.pool.cb != nil {
		pc.pool.cb.OnSuccess()
	}
	return res, nil
}

func (pc *PooledConn) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	pc.mu.Lock()
	if pc.closed {
		pc.mu.Unlock()
		return 0, ErrConnClosed
	}
	pc.useCount++
	pc.lastUsedAt = time.Now()
	raw := pc.raw
	pc.mu.Unlock()

	rows, err := raw.Exec(ctx, sql, args...)
	if err != nil {
		pc.mu.Lock()
		pc.errCount++
		pc.mu.Unlock()
		if pc.pool != nil && pc.pool.cb != nil {
			pc.pool.cb.OnFailure()
		}
		return 0, err
	}
	if pc.pool != nil && pc.pool.cb != nil {
		pc.pool.cb.OnSuccess()
	}
	return rows, nil
}

func (pc *PooledConn) Ping(ctx context.Context) error {
	pc.mu.Lock()
	if pc.closed {
		pc.mu.Unlock()
		return ErrConnClosed
	}
	raw := pc.raw
	pc.mu.Unlock()
	return raw.Ping(ctx)
}

func (pc *PooledConn) MarkBroken() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.state = StateBroken
	if mc, ok := pc.raw.(*MockRawConn); ok {
		mc.MarkBroken()
	}
}

func (pc *PooledConn) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.closed {
		return nil
	}
	pc.closed = true
	pc.state = StateClosed
	if pc.healthTicker != nil {
		pc.healthTicker.Stop()
	}
	if pc.stopCh != nil {
		select {
		case <-pc.stopCh:
		default:
			close(pc.stopCh)
		}
	}
	return pc.raw.Close()
}

func (pc *PooledConn) startHealthCheck(interval time.Duration) {
	pc.healthTicker = time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-pc.stopCh:
				return
			case <-pc.healthTicker.C:
				_ = pc.Ping(context.Background())
			}
		}
	}()
}

type waiterTicket struct {
	ch         chan *PooledConn
	errCh      chan error
	ctx        context.Context
	deadline   time.Time
	enqueuedAt time.Time
	done       atomic.Bool
}

type FairWaiterQueue struct {
	mu      sync.Mutex
	waiters []*waiterTicket
}

func NewFairWaiterQueue() *FairWaiterQueue {
	return &FairWaiterQueue{
		waiters: make([]*waiterTicket, 0),
	}
}

func (q *FairWaiterQueue) Enqueue(ctx context.Context, timeout time.Duration) *waiterTicket {
	q.mu.Lock()
	defer q.mu.Unlock()

	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	ticket := &waiterTicket{
		ch:         make(chan *PooledConn, 1),
		errCh:      make(chan error, 1),
		ctx:        ctx,
		deadline:   deadline,
		enqueuedAt: time.Now(),
	}
	q.waiters = append(q.waiters, ticket)
	return ticket
}

func (q *FairWaiterQueue) Dequeue() *waiterTicket {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.waiters) > 0 {
		head := q.waiters[0]
		q.waiters = q.waiters[1:]
		if head.done.Swap(true) {
			continue
		}
		if head.ctx != nil && head.ctx.Err() != nil {
			continue
		}
		return head
	}
	return nil
}

func (q *FairWaiterQueue) Remove(ticket *waiterTicket) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, w := range q.waiters {
		if w == ticket {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			break
		}
	}
}

func (q *FairWaiterQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.waiters)
}

func (q *FairWaiterQueue) Drain(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, w := range q.waiters {
		if !w.done.Swap(true) {
			w.errCh <- err
		}
	}
	q.waiters = q.waiters[:0]
}

type PoolStats struct {
	MaxOpenConns      int
	OpenConns         int
	InUseConns        int
	IdleConns         int
	WaitCount         int64
	WaitDurationTotal time.Duration
	DialCount         int64
	DialFailures      int64
	CircuitTrips      int64
	CircuitState      CircuitState
}

type ConnectionPool struct {
	config        PoolConfig
	dialer        Dialer
	mu            sync.Mutex
	waiterMu      sync.Mutex
	conns         []*PooledConn
	idleConns     []*PooledConn
	waiters       *FairWaiterQueue
	cb            *CircuitBreakerImpl
	closed        atomic.Bool
	reaperTicker  *time.Ticker
	reaperStopCh  chan struct{}
	reaperWg      sync.WaitGroup
	statsMu       sync.Mutex
	totalWaited   int64
	totalWaitTime time.Duration
	dialFailures  int64
	pendingDials  int
}

func NewConnectionPool(dialer Dialer, cfg PoolConfig, opts ...PoolOption) (*ConnectionPool, error) {
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.MaxOpen <= 0 {
		cfg.MaxOpen = 10
	}
	if cfg.MinIdle < 0 {
		cfg.MinIdle = 0
	}
	if cfg.MaxIdle <= 0 || cfg.MaxIdle > cfg.MaxOpen {
		cfg.MaxIdle = cfg.MaxOpen
	}
	if cfg.HealthCheckInterval <= 0 {
		cfg.HealthCheckInterval = time.Minute
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 5 * time.Minute
	}
	if cfg.MaxLifetime <= 0 {
		cfg.MaxLifetime = 30 * time.Minute
	}

	cb := NewCircuitBreaker(
		cfg.CircuitBreakerMaxFailures,
		cfg.CircuitBreakerRecoveryTimeout,
		cfg.CircuitBreakerHalfOpenSuccess,
	)

	p := &ConnectionPool{
		config:       cfg,
		dialer:       dialer,
		conns:        make([]*PooledConn, 0),
		idleConns:    make([]*PooledConn, 0),
		waiters:      NewFairWaiterQueue(),
		cb:           cb,
		reaperStopCh: make(chan struct{}),
	}

	p.reaperTicker = time.NewTicker(cfg.IdleTimeout / 2)
	p.reaperWg.Add(1)
	go p.reaperLoop()

	return p, nil
}

func (p *ConnectionPool) reaperLoop() {
	defer p.reaperWg.Done()
	for {
		select {
		case <-p.reaperStopCh:
			return
		case <-p.reaperTicker.C:
			p.reapIdleConnections()
		}
	}
}

func (p *ConnectionPool) reapIdleConnections() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	rem := make([]*PooledConn, 0, len(p.idleConns))
	for _, pc := range p.idleConns {
		expired := false
		if p.config.IdleTimeout > 0 && now.Sub(pc.LastUsedAt()) > p.config.IdleTimeout {
			expired = true
		}
		if p.config.MaxLifetime > 0 && now.Sub(pc.CreatedAt()) > p.config.MaxLifetime {
			expired = true
		}

		if expired && len(p.conns) > p.config.MinIdle {
			p.removeConnLocked(pc)
			_ = pc.Close()
		} else {
			rem = append(rem, pc)
		}
	}
	p.idleConns = rem
}

func (p *ConnectionPool) wrapConn(raw RawConn) *PooledConn {
	pc := &PooledConn{
		id:         fmt.Sprintf("pooled-%d-%d", time.Now().UnixNano(), rand.IntN(1000)),
		raw:        raw,
		pool:       p,
		state:      StateInUse,
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
		stopCh:     make(chan struct{}),
	}
	pc.startHealthCheck(p.config.HealthCheckInterval)
	return pc
}

func (p *ConnectionPool) popIdleUnderWaiterLock() *PooledConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.idleConns) > 0 {
		conn := p.idleConns[len(p.idleConns)-1]
		p.idleConns = p.idleConns[:len(p.idleConns)-1]
		return conn
	}
	return nil
}

func (p *ConnectionPool) Acquire(ctx context.Context) (*PooledConn, error) {
	if p.closed.Load() {
		return nil, ErrPoolClosed
	}

	if !p.cb.Allow() {
		return nil, ErrCircuitOpen
	}

	p.waiterMu.Lock()
	conn := p.popIdleUnderWaiterLock()
	if conn != nil {
		conn.mu.Lock()
		conn.state = StateInUse
		conn.lastUsedAt = time.Now()
		conn.mu.Unlock()
		p.waiterMu.Unlock()
		return conn, nil
	}
	p.waiterMu.Unlock()

	p.mu.Lock()
	if len(p.conns)+p.pendingDials < p.config.MaxOpen {
		p.pendingDials++
		p.mu.Unlock()

		rawConn, err := p.dialer.Dial(ctx)
		p.mu.Lock()
		p.pendingDials--
		if err != nil {
			p.dialFailures++
			p.mu.Unlock()
			p.cb.OnFailure()
			return nil, err
		}
		pc := p.wrapConn(rawConn)
		p.conns = append(p.conns, pc)
		p.mu.Unlock()
		return pc, nil
	}
	p.mu.Unlock()

	waiter := p.waiters.Enqueue(ctx, p.config.AcquireTimeout)
	p.statsMu.Lock()
	p.totalWaited++
	p.statsMu.Unlock()

	startTime := time.Now()
	select {
	case pc := <-waiter.ch:
		p.statsMu.Lock()
		p.totalWaitTime += time.Since(startTime)
		p.statsMu.Unlock()
		return pc, nil
	case err := <-waiter.errCh:
		return nil, err
	case <-ctx.Done():
		p.waiters.Remove(waiter)
		return nil, ctx.Err()
	case <-time.After(p.config.AcquireTimeout):
		p.waiters.Remove(waiter)
		return nil, ErrAcquireTimeout
	}
}

func (p *ConnectionPool) Release(pc *PooledConn) error {
	if pc == nil {
		return nil
	}
	if p.closed.Load() {
		_ = pc.Close()
		return ErrPoolClosed
	}

	pc.mu.Lock()
	if pc.state == StateClosed || pc.state == StateBroken {
		pc.mu.Unlock()
		p.Discard(pc)
		return nil
	}

	pc.state = StateIdle
	pc.lastUsedAt = time.Now()
	pc.mu.Unlock()

	p.handoverToWaiter(pc)
	return nil
}

func (p *ConnectionPool) handoverToWaiter(pc *PooledConn) {
	p.waiterMu.Lock()
	defer p.waiterMu.Unlock()

	waiter := p.waiters.Dequeue()
	if waiter != nil {
		pc.mu.Lock()
		pc.state = StateInUse
		pc.lastUsedAt = time.Now()
		pc.mu.Unlock()
		waiter.ch <- pc
		return
	}

	p.mu.Lock()
	if len(p.idleConns) < p.config.MaxIdle {
		p.idleConns = append(p.idleConns, pc)
	} else {
		p.removeConnLocked(pc)
		_ = pc.Close()
	}
	p.mu.Unlock()
}

func (p *ConnectionPool) Discard(pc *PooledConn) {
	if pc == nil {
		return
	}
	p.mu.Lock()
	p.removeConnLocked(pc)
	p.mu.Unlock()
	_ = pc.Close()
}

func (p *ConnectionPool) removeConnLocked(target *PooledConn) {
	for i, c := range p.conns {
		if c == target {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			break
		}
	}
	for i, c := range p.idleConns {
		if c == target {
			p.idleConns = append(p.idleConns[:i], p.idleConns[i+1:]...)
			break
		}
	}
}

func (p *ConnectionPool) Warmup(ctx context.Context) error {
	p.mu.Lock()
	needed := p.config.MinIdle - len(p.conns)
	p.mu.Unlock()

	for i := 0; i < needed; i++ {
		raw, err := p.dialer.Dial(ctx)
		if err != nil {
			return err
		}
		p.mu.Lock()
		pc := p.wrapConn(raw)
		pc.state = StateIdle
		p.conns = append(p.conns, pc)
		p.idleConns = append(p.idleConns, pc)
		p.mu.Unlock()
	}
	return nil
}

func (p *ConnectionPool) Resize(newMaxOpen int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if newMaxOpen < p.config.MinIdle {
		return ErrCannotResizeBelowMin
	}
	p.config.MaxOpen = newMaxOpen
	if p.config.MaxIdle > newMaxOpen {
		p.config.MaxIdle = newMaxOpen
	}
	for len(p.idleConns) > p.config.MaxIdle {
		excess := p.idleConns[len(p.idleConns)-1]
		p.idleConns = p.idleConns[:len(p.idleConns)-1]
		p.removeConnLocked(excess)
		_ = excess.Close()
	}
	return nil
}

func (p *ConnectionPool) Drain() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.idleConns {
		_ = c.Close()
	}
	p.idleConns = p.idleConns[:0]
}

func (p *ConnectionPool) Close() error {
	if p.closed.Swap(true) {
		return ErrPoolClosed
	}

	p.waiters.Drain(ErrPoolClosed)

	if p.reaperStopCh != nil {
		close(p.reaperStopCh)
	}
	if p.reaperTicker != nil {
		p.reaperTicker.Stop()
	}
	p.reaperWg.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = p.conns[:0]
	p.idleConns = p.idleConns[:0]
	return nil
}

func (p *ConnectionPool) Stats() PoolStats {
	p.mu.Lock()
	open := len(p.conns)
	idle := len(p.idleConns)
	inUse := open - idle
	if inUse < 0 {
		inUse = 0
	}
	maxOpen := p.config.MaxOpen
	p.mu.Unlock()

	p.statsMu.Lock()
	waitCount := p.totalWaited
	waitDuration := p.totalWaitTime
	dials := p.dialer.DialCount()
	dialFails := p.dialFailures
	p.statsMu.Unlock()

	cbState := p.cb.State()
	return PoolStats{
		MaxOpenConns:      maxOpen,
		OpenConns:         open,
		InUseConns:        inUse,
		IdleConns:         idle,
		WaitCount:         waitCount,
		WaitDurationTotal: waitDuration,
		DialCount:         dials,
		DialFailures:      dialFails,
		CircuitTrips:      p.cb.totalTrips,
		CircuitState:      cbState,
	}
}

func (p *ConnectionPool) Ping(ctx context.Context) error {
	pc, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer p.Release(pc)
	return pc.Ping(ctx)
}

type ConnectionPoolInspector struct {
	pool *ConnectionPool
}

func NewConnectionPoolInspector(p *ConnectionPool) *ConnectionPoolInspector {
	return &ConnectionPoolInspector{pool: p}
}

func (ins *ConnectionPoolInspector) UtilizationReport() string {
	st := ins.pool.Stats()
	var b strings.Builder
	b.WriteString("=== Connection Pool Utilization Report ===\n")
	b.WriteString(fmt.Sprintf("Capacity:       %d open / %d max\n", st.OpenConns, st.MaxOpenConns))
	b.WriteString(fmt.Sprintf("In-Use:         %d\n", st.InUseConns))
	b.WriteString(fmt.Sprintf("Idle:           %d\n", st.IdleConns))
	b.WriteString(fmt.Sprintf("Total Waiters:  %d\n", st.WaitCount))
	b.WriteString(fmt.Sprintf("Circuit Breaker:%s\n", st.CircuitState))
	return b.String()
}

func (ins *ConnectionPoolInspector) ExportMetrics() string {
	st := ins.pool.Stats()
	var buf bytes.Buffer
	buf.WriteString("# HELP conn_pool_open_connections Total open connections in pool\n")
	buf.WriteString("# TYPE conn_pool_open_connections gauge\n")
	buf.WriteString(fmt.Sprintf("conn_pool_open_connections %d\n", st.OpenConns))
	buf.WriteString("# HELP conn_pool_in_use_connections Active connections in use\n")
	buf.WriteString("# TYPE conn_pool_in_use_connections gauge\n")
	buf.WriteString(fmt.Sprintf("conn_pool_in_use_connections %d\n", st.InUseConns))
	buf.WriteString("# HELP conn_pool_idle_connections Idle connections in pool\n")
	buf.WriteString("# TYPE conn_pool_idle_connections gauge\n")
	buf.WriteString(fmt.Sprintf("conn_pool_idle_connections %d\n", st.IdleConns))
	return buf.String()
}

type MultiplexStream struct {
	ID         uint64
	RequestCh  chan []byte
	ResponseCh chan []byte
	Active     bool
	CreatedAt  time.Time
}

type MultiplexResponse struct {
	StreamID uint64
	Data     []byte
	Err      error
}

type ConnectionMultiplexer struct {
	conn         *PooledConn
	streams      map[uint64]*MultiplexStream
	nextStreamID uint64
	mu           sync.RWMutex
	maxStreams   int
	responseCh   chan *MultiplexResponse
	stopCh       chan struct{}
	pool         *ConnectionPool
}

func NewConnectionMultiplexer(pool *ConnectionPool, conn *PooledConn, maxStreams int) *ConnectionMultiplexer {
	return &ConnectionMultiplexer{
		conn:         conn,
		streams:      make(map[uint64]*MultiplexStream),
		nextStreamID: 0,
		maxStreams:   maxStreams,
		responseCh:   make(chan *MultiplexResponse, 100),
		stopCh:       make(chan struct{}),
		pool:         pool,
	}
}

func (m *ConnectionMultiplexer) OpenStream() (*MultiplexStream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.streams) >= m.maxStreams {
		return nil, errors.New("max streams reached")
	}
	m.nextStreamID++
	id := m.nextStreamID

	stream := &MultiplexStream{
		ID:         id,
		RequestCh:  make(chan []byte, 10),
		ResponseCh: make(chan []byte, 10),
		Active:     true,
		CreatedAt:  time.Now(),
	}

	m.streams[id] = stream
	return stream, nil
}

func (m *ConnectionMultiplexer) CloseStream(streamID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if stream, ok := m.streams[streamID]; ok {
		stream.Active = false
		close(stream.RequestCh)
		close(stream.ResponseCh)
		delete(m.streams, streamID)
	}
}

func (m *ConnectionMultiplexer) SendRequest(streamID uint64, data []byte) error {
	m.mu.RLock()
	stream, ok := m.streams[streamID]
	m.mu.RUnlock()
	if !ok || !stream.Active {
		return errors.New("stream not found or inactive")
	}
	
	select {
	case stream.RequestCh <- data:
		return nil
	case <-time.After(100 * time.Millisecond):
		return errors.New("timeout sending request")
	}
}

func (m *ConnectionMultiplexer) RouteResponses() {
	go func() {
		for {
			select {
			case <-m.stopCh:
				return
			case resp := <-m.responseCh:
				m.mu.RLock()
				stream, ok := m.streams[resp.StreamID]
				m.mu.RUnlock()
				if ok && stream.Active {
					select {
					case stream.ResponseCh <- resp.Data:
					default:
					}
				}
			}
		}
	}()
}

func (m *ConnectionMultiplexer) CloseAll() {
	close(m.stopCh)
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, stream := range m.streams {
		stream.Active = false
		close(stream.RequestCh)
		close(stream.ResponseCh)
		delete(m.streams, id)
	}
}

func (m *ConnectionMultiplexer) GetActiveStreamCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.streams)
}

func (m *ConnectionMultiplexer) RebalanceStreams(newConn *PooledConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conn = newConn
}

type FailoverManager struct {
	primary        string
	replicas       []string
	currentTarget  string
	dnsCache       map[string]string
	dnsCacheTTL    time.Duration
	lastDNSRefresh time.Time
	mu             sync.RWMutex
	failoverCount  int64
	pool           *ConnectionPool
	healthCheckCh  chan string
	stopCh         chan struct{}
}

func NewFailoverManager(primary string, replicas []string, pool *ConnectionPool) *FailoverManager {
	return &FailoverManager{
		primary:       primary,
		replicas:      replicas,
		currentTarget: primary,
		dnsCache:      make(map[string]string),
		dnsCacheTTL:   5 * time.Minute,
		pool:          pool,
		healthCheckCh: make(chan string, 100),
		stopCh:        make(chan struct{}),
	}
}

func (f *FailoverManager) ResolveDNS(host string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if time.Since(f.lastDNSRefresh) < f.dnsCacheTTL {
		if cached, ok := f.dnsCache[host]; ok {
			return cached, nil
		}
	}
	f.dnsCache[host] = host
	f.lastDNSRefresh = time.Now()
	return host, nil
}

func (f *FailoverManager) TriggerFailover() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.replicas) == 0 {
		return errors.New("no replicas available")
	}
	
	nextIdx := int(f.failoverCount % int64(len(f.replicas)))
	f.currentTarget = f.replicas[nextIdx]
	f.failoverCount++
	
	select {
	case f.healthCheckCh <- f.primary:
	default:
	}
	
	return nil
}

func (f *FailoverManager) StartHealthMonitor() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-f.stopCh:
				return
			case <-ticker.C:
				f.mu.Lock()
				target := f.currentTarget
				f.mu.Unlock()
				if target != f.primary {
					f.mu.Lock()
					f.currentTarget = f.primary
					f.mu.Unlock()
				}
			}
		}
	}()
}

func (f *FailoverManager) StopHealthMonitor() {
	close(f.stopCh)
}

func (f *FailoverManager) GetCurrentTarget() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.currentTarget
}

func (f *FailoverManager) RecordFailoverEvent(from, to string, reason error) {
}

func (f *FailoverManager) GetFailoverHistory() []string {
	return []string{}
}

func (f *FailoverManager) IsHealthy(target string) bool {
	return true
}

func (f *FailoverManager) HandleThunderingHerd(connCount int) error {
	return nil
}

type PoolShard struct {
	Connections []*PooledConn
	MaxSize     int
	CurrentSize int
	Pool        *ConnectionPool
}

type ConnectionPoolSharding struct {
	shards     []*PoolShard
	shardCount int
	shardMu    []sync.Mutex
}

func NewConnectionPoolSharding(pool *ConnectionPool, shardCount int) *ConnectionPoolSharding {
	if shardCount <= 0 {
		shardCount = 1
	}
	shards := make([]*PoolShard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = &PoolShard{
			Connections: make([]*PooledConn, 0),
			MaxSize:     pool.config.MaxOpen / shardCount,
			Pool:        pool,
		}
	}
	return &ConnectionPoolSharding{
		shards:     shards,
		shardCount: shardCount,
		shardMu:    make([]sync.Mutex, shardCount),
	}
}

func (s *ConnectionPoolSharding) GetShard(key string) int {
	var hash uint32
	for _, c := range key {
		hash = (hash * 31) + uint32(c)
	}
	return int(hash % uint32(s.shardCount))
}

func (s *ConnectionPoolSharding) AcquireFromShard(key string) (*PooledConn, error) {
	idx := s.GetShard(key)
	s.shardMu[idx].Lock()
	defer s.shardMu[idx].Unlock()
	
	if len(s.shards[idx].Connections) == 0 {
		return nil, errors.New("shard is empty")
	}
	
	conn := s.shards[idx].Connections[0]
	s.shards[idx].Connections = s.shards[idx].Connections[1:]
	s.shards[idx].CurrentSize--
	return conn, nil
}

func (s *ConnectionPoolSharding) ReturnToShard(key string, conn *PooledConn) {
	idx := s.GetShard(key)
	s.shardMu[idx].Lock()
	defer s.shardMu[idx].Unlock()
	
	if s.shards[idx].CurrentSize >= s.shards[idx].MaxSize {
		_ = conn.Close()
		return
	}
	
	s.shards[idx].Connections = append(s.shards[idx].Connections, conn)
	s.shards[idx].CurrentSize++
}

func (s *ConnectionPoolSharding) RebalanceShards() {
	if s.shardCount < 2 {
		return
	}
	for i := 0; i < s.shardCount-1; i++ {
		func() {
			s.shardMu[i].Lock()
			defer s.shardMu[i].Unlock()
			s.shardMu[i+1].Lock()
			defer s.shardMu[i+1].Unlock()
			
			if len(s.shards[i].Connections) > len(s.shards[i+1].Connections)+1 {
				conn := s.shards[i].Connections[0]
				s.shards[i].Connections = s.shards[i].Connections[1:]
				s.shards[i].CurrentSize--
				s.shards[i+1].Connections = append(s.shards[i+1].Connections, conn)
				s.shards[i+1].CurrentSize++
			}
		}()
	}
}

func (s *ConnectionPoolSharding) GetShardStats() []int {
	stats := make([]int, s.shardCount)
	for i := range s.shards {
		s.shardMu[i].Lock()
		stats[i] = s.shards[i].CurrentSize
		s.shardMu[i].Unlock()
	}
	return stats
}

type CachedStatement struct {
	Query      string
	PreparedAt time.Time
	LastUsed   time.Time
	UseCount   int64
	ConnID     int64
	Closed     bool
}

type PreparedStatementCache struct {
	cache     map[string]*CachedStatement
	order     []string
	maxSize   int
	mu        sync.Mutex
	hitCount  int64
	missCount int64
	pool      *ConnectionPool
}

func NewPreparedStatementCache(maxSize int) *PreparedStatementCache {
	return &PreparedStatementCache{
		cache:   make(map[string]*CachedStatement),
		maxSize: maxSize,
		order:   make([]string, 0),
	}
}

func (c *PreparedStatementCache) Prepare(query string, connID int64) (*CachedStatement, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if stmt, ok := c.cache[query]; ok {
		c.hitCount++
		stmt.LastUsed = time.Now()
		stmt.UseCount++
		
		for i, q := range c.order {
			if q == query {
				c.order = append(c.order[:i], c.order[i+1:]...)
				break
			}
		}
		c.order = append(c.order, query)
		
		return stmt, nil
	}
	
	if len(c.cache) >= c.maxSize {
		if len(c.order) > 0 {
			oldestQuery := c.order[0]
			c.order = c.order[1:]
			if stmt, ok := c.cache[oldestQuery]; ok {
				stmt.Closed = true
				delete(c.cache, oldestQuery)
			}
		}
	}
	
	stmt := &CachedStatement{
		Query:      query,
		PreparedAt: time.Now(),
		LastUsed:   time.Now(),
		UseCount:   1,
		ConnID:     connID,
	}
	
	c.cache[query] = stmt
	c.order = append(c.order, query)
	c.missCount++
	
	return stmt, nil
}

func (c *PreparedStatementCache) Get(query string) (*CachedStatement, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	stmt, ok := c.cache[query]
	if ok {
		stmt.LastUsed = time.Now()
		stmt.UseCount++
		c.hitCount++
		
		for i, q := range c.order {
			if q == query {
				c.order = append(c.order[:i], c.order[i+1:]...)
				break
			}
		}
		c.order = append(c.order, query)
	}
	return stmt, ok
}

func (c *PreparedStatementCache) Evict() *CachedStatement {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if len(c.order) == 0 {
		return nil
	}
	
	oldestQuery := c.order[0]
	c.order = c.order[1:]
	
	stmt := c.cache[oldestQuery]
	if stmt != nil {
		stmt.Closed = true
		delete(c.cache, oldestQuery)
	}
	
	return stmt
}

func (c *PreparedStatementCache) EvictByAge(maxAge time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	now := time.Now()
	count := 0
	
	var newOrder []string
	for _, query := range c.order {
		if stmt, ok := c.cache[query]; ok {
			if now.Sub(stmt.PreparedAt) >= maxAge {
				stmt.Closed = true
				delete(c.cache, query)
				count++
			} else {
				newOrder = append(newOrder, query)
			}
		}
	}
	c.order = newOrder
	return count
}

func (c *PreparedStatementCache) Invalidate(query string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if stmt, ok := c.cache[query]; ok {
		stmt.Closed = true
		delete(c.cache, query)
		for i, q := range c.order {
			if q == query {
				c.order = append(c.order[:i], c.order[i+1:]...)
				break
			}
		}
	}
}

func (c *PreparedStatementCache) InvalidateByConn(connID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	var newOrder []string
	for _, query := range c.order {
		if stmt, ok := c.cache[query]; ok {
			if stmt.ConnID == connID {
				stmt.Closed = true
				delete(c.cache, query)
			} else {
				newOrder = append(newOrder, query)
			}
		}
	}
	c.order = newOrder
}

func (c *PreparedStatementCache) GetHitRate() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	total := c.hitCount + c.missCount
	if total == 0 {
		return 0
	}
	return float64(c.hitCount) / float64(total)
}

func (c *PreparedStatementCache) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	for _, stmt := range c.cache {
		stmt.Closed = true
	}
	c.cache = make(map[string]*CachedStatement)
	c.order = make([]string, 0)
}

type ConnectionMetrics struct {
	TotalQueries int64
	TotalErrors  int64
	TotalBytes   int64
	SlowQueries  int64
	mu           sync.RWMutex
	pool         *ConnectionPool
	histograms   map[string][]time.Duration
}

func NewConnectionMetrics(pool *ConnectionPool) *ConnectionMetrics {
	return &ConnectionMetrics{
		pool:       pool,
		histograms: make(map[string][]time.Duration),
	}
}

func (m *ConnectionMetrics) RecordQuery(duration time.Duration, bytes int64, err error) {
	atomic.AddInt64(&m.TotalQueries, 1)
	atomic.AddInt64(&m.TotalBytes, bytes)
	if err != nil {
		atomic.AddInt64(&m.TotalErrors, 1)
	}
	if duration > 100*time.Millisecond {
		atomic.AddInt64(&m.SlowQueries, 1)
	}
}

func (m *ConnectionMetrics) RecordHistogram(query string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.histograms == nil {
		m.histograms = make(map[string][]time.Duration)
	}
	m.histograms[query] = append(m.histograms[query], duration)
}

func (m *ConnectionMetrics) GetP99Latency(query string) time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	h, ok := m.histograms[query]
	if !ok || len(h) == 0 {
		return 0
	}
	
	return h[len(h)*99/100]
}

func (m *ConnectionMetrics) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	atomic.StoreInt64(&m.TotalQueries, 0)
	atomic.StoreInt64(&m.TotalErrors, 0)
	atomic.StoreInt64(&m.TotalBytes, 0)
	atomic.StoreInt64(&m.SlowQueries, 0)
	m.histograms = make(map[string][]time.Duration)
}

func (m *ConnectionMetrics) ExportPrometheus() string {
	tq := atomic.LoadInt64(&m.TotalQueries)
	te := atomic.LoadInt64(&m.TotalErrors)
	tb := atomic.LoadInt64(&m.TotalBytes)
	sq := atomic.LoadInt64(&m.SlowQueries)
	
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("conn_queries_total %d\n", tq))
	buf.WriteString(fmt.Sprintf("conn_errors_total %d\n", te))
	buf.WriteString(fmt.Sprintf("conn_bytes_total %d\n", tb))
	buf.WriteString(fmt.Sprintf("conn_slow_queries_total %d\n", sq))
	return buf.String()
}

func (m *ConnectionMetrics) Merge(other *ConnectionMetrics) {
	atomic.AddInt64(&m.TotalQueries, atomic.LoadInt64(&other.TotalQueries))
	atomic.AddInt64(&m.TotalErrors, atomic.LoadInt64(&other.TotalErrors))
	atomic.AddInt64(&m.TotalBytes, atomic.LoadInt64(&other.TotalBytes))
	atomic.AddInt64(&m.SlowQueries, atomic.LoadInt64(&other.SlowQueries))
	
	other.mu.RLock()
	defer other.mu.RUnlock()
	
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.histograms == nil {
		m.histograms = make(map[string][]time.Duration)
	}
	for q, h := range other.histograms {
		m.histograms[q] = append(m.histograms[q], h...)
	}
}
`,
}
