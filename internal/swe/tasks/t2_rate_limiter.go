package tasks

import "benchmark/internal/swe"

var TaskT2RateLimiter = &swe.Task{
	ID:       "swe-t2-rate-limiter-01",
	Title:    "Multi-Tenant Distributed Token Bucket Rate Limiter with Sharding",
	Tier:     swe.TierMid,
	Points:   25,
	Category: "lifecycle_and_channels",
	IssueBody: `### Bug Report: Goroutine Leaks, Ticker Unstopped, Channel Deadlock, and Panic in Rate Limiter

**Environment:** Go 1.24+ high-throughput API gateway with thousands of concurrent tenant rate limit checks.

**Expected Behavior:**
- All background goroutines (token refill workers, sliding window cleaners, metric collectors) must be cleanly stopped when a tenant is evicted or the limiter is shut down (zero goroutine leaks).
- Audit metric reporting must never block producers indefinitely. If the metric collector is slow or stopped, producers should continue serving traffic.
- Shard index calculation must handle all possible hash values (including negative integers from hash functions) without panicking.
- Lock management must be exception-safe: if early validation returns an error, locks must be properly released before returning (no deadlocks).
- The system must be fully thread-safe under high concurrency with zero data races (` + "`-race`" + ` warnings).
- Eviction of expired or inactive tenants must safely clean up all associated resources (tickers, channels, goroutines).
- Adaptive rate adjustment must properly synchronize load sampling and rate modification across shards.
- Rate limit policies must evaluate rules in correct priority order and safely handle concurrent rule modifications.
- Circuit breaker must follow standard state machine transitions (closed -> open -> half-open -> closed).
- Quota management must accurately track daily and monthly usage with proper reset scheduling.
- Tenant migration between shards must be atomic, properly clean up source resources, and handle concurrent migrations safely.`,
	BrokenCode: `package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

var (
	ErrRateLimitExceeded  = errors.New("rate limit exceeded")
	ErrTenantNotFound     = errors.New("tenant not found")
	ErrInvalidCapacity    = errors.New("capacity must be positive")
	ErrInvalidRefillRate  = errors.New("refill rate must be positive")
	ErrShutdown           = errors.New("rate limiter is shut down")
	ErrInvalidWindow      = errors.New("window size must be positive")
)

type TenantTier string

const (
	TierFree       TenantTier = "free"
	TierPro        TenantTier = "pro"
	TierEnterprise TenantTier = "enterprise"
)

type RateLimitStrategy string

const (
	StrategyTokenBucket   RateLimitStrategy = "token_bucket"
	StrategySlidingWindow RateLimitStrategy = "sliding_window"
)

type TokenBucket struct {
	capacity      int64
	available     int64
	refillRate    int64
	refillPeriod  time.Duration
	lastRefill    time.Time
	stopCh        chan struct{}
	ticker        *time.Ticker
	version       int64
}

type WindowEntry struct {
	timestamp time.Time
	weight    int64
}

type SlidingWindow struct {
	windowSize  time.Duration
	maxRequests int64
	entries     []WindowEntry
	stopCh      chan struct{}
	ticker      *time.Ticker
}

type TenantConfig struct {
	TenantID      string
	Tier          TenantTier
	Strategy      RateLimitStrategy
	Capacity      int64
	RefillRate    int64
	RefillPeriod  time.Duration
	WindowSize    time.Duration
	MaxRequests   int64
	BurstCapacity int64
	CreatedAt     time.Time
}

type RateLimitStats struct {
	TotalRequests   int64
	Allowed         int64
	Denied          int64
	ActiveTenants   int
	TotalRefills    int64
	FreeTenants     int
	ProTenants      int
	EntTenants      int
}

type MetricEvent struct {
	TenantID  string
	Strategy  RateLimitStrategy
	Allowed   bool
	Cost      int64
	Timestamp time.Time
}

type AuditRecord struct {
	ID        string
	TenantID  string
	Allowed   bool
	Tokens    int64
	Timestamp time.Time
}

type AuditBuffer struct {
	records []AuditRecord
	head    int
	size    int
	cap     int
}

func NewAuditBuffer(capacity int) *AuditBuffer {
	return &AuditBuffer{
		records: make([]AuditRecord, capacity),
		cap:     capacity,
	}
}

func (b *AuditBuffer) Push(rec AuditRecord) {
	b.records[b.head] = rec
	b.head = (b.head + 1) % b.cap
	if b.size < b.cap {
		b.size++
	}
}

func (b *AuditBuffer) Dump() []AuditRecord {
	out := make([]AuditRecord, 0, b.size)
	for i := 0; i < b.size; i++ {
		idx := (b.head - b.size + i + b.cap) % b.cap
		out = append(out, b.records[idx])
	}
	return out
}

type ShardedRateLimiter struct {
	shards       []*RateLimiterShard
	shardCount   int
	metricCh     chan MetricEvent
	auditBuf     *AuditBuffer
	auditMu      sync.Mutex
	shutdownOnce sync.Once
	mu           sync.RWMutex
	shutdown     bool
}

type RateLimiterShard struct {
	mu       sync.Mutex
	buckets  map[string]*TokenBucket
	windows  map[string]*SlidingWindow
	configs  map[string]TenantConfig
}

func NewShardedRateLimiter(shardCount int) *ShardedRateLimiter {
	if shardCount <= 0 {
		shardCount = 16
	}
	shards := make([]*RateLimiterShard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = &RateLimiterShard{
			buckets: make(map[string]*TokenBucket),
			windows: make(map[string]*SlidingWindow),
			configs: make(map[string]TenantConfig),
		}
	}
	return &ShardedRateLimiter{
		shards:     shards,
		shardCount: shardCount,
		metricCh:   make(chan MetricEvent),
		auditBuf:   NewAuditBuffer(5000),
	}
}

func (rl *ShardedRateLimiter) hashTenantID(tenantID string) int {
	h := fnv.New32a()
	h.Write([]byte(tenantID))
	hashVal := int(int32(h.Sum32()))
	return hashVal % rl.shardCount
}

func (rl *ShardedRateLimiter) getShard(tenantID string) *RateLimiterShard {
	idx := rl.hashTenantID(tenantID)
	return rl.shards[idx]
}

func (rl *ShardedRateLimiter) RegisterTenant(cfg TenantConfig) error {
	if cfg.Strategy == StrategySlidingWindow {
		if cfg.WindowSize <= 0 {
			return ErrInvalidWindow
		}
		if cfg.MaxRequests <= 0 {
			return errors.New("max requests must be positive")
		}
	} else {
		if cfg.Capacity <= 0 {
			return ErrInvalidCapacity
		}
		if cfg.RefillRate <= 0 {
			return ErrInvalidRefillRate
		}
	}

	rl.mu.RLock()
	if rl.shutdown {
		rl.mu.RUnlock()
		return ErrShutdown
	}
	rl.mu.RUnlock()

	shard := rl.getShard(cfg.TenantID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.configs[cfg.TenantID] = cfg

	if cfg.Strategy == StrategySlidingWindow {
		w := &SlidingWindow{
			windowSize:  cfg.WindowSize,
			maxRequests: cfg.MaxRequests,
			entries:     make([]WindowEntry, 0),
			stopCh:      make(chan struct{}),
			ticker:      time.NewTicker(cfg.WindowSize / 2),
		}
		shard.windows[cfg.TenantID] = w
		go rl.windowCleaner(cfg.TenantID, w)
	} else {
		cap := cfg.Capacity
		if cfg.BurstCapacity > cap {
			cap = cfg.BurstCapacity
		}
		bucket := &TokenBucket{
			capacity:     cap,
			available:    cap,
			refillRate:   cfg.RefillRate,
			refillPeriod: cfg.RefillPeriod,
			lastRefill:   time.Now(),
			stopCh:       make(chan struct{}),
			ticker:       time.NewTicker(cfg.RefillPeriod),
			version:      1,
		}
		shard.buckets[cfg.TenantID] = bucket
		go rl.refillWorker(cfg.TenantID, bucket)
	}

	return nil
}

func (rl *ShardedRateLimiter) refillWorker(tenantID string, bucket *TokenBucket) {
	for range bucket.ticker.C {
		shard := rl.getShard(tenantID)
		bucket.available += bucket.refillRate
		if bucket.available > bucket.capacity {
			bucket.available = bucket.capacity
		}
		_ = shard
	}
}

func (rl *ShardedRateLimiter) windowCleaner(tenantID string, w *SlidingWindow) {
	for range w.ticker.C {
		shard := rl.getShard(tenantID)
		cutoff := time.Now().Add(-w.windowSize)
		valid := 0
		for _, entry := range w.entries {
			if entry.timestamp.After(cutoff) {
				w.entries[valid] = entry
				valid++
			}
		}
		w.entries = w.entries[:valid]
		_ = shard
	}
}

func (rl *ShardedRateLimiter) Allow(tenantID string, tokens int64) (bool, error) {
	rl.mu.RLock()
	if rl.shutdown {
		rl.mu.RUnlock()
		return false, ErrShutdown
	}
	rl.mu.RUnlock()

	if tenantID == "" {
		return false, ErrTenantNotFound
	}

	shard := rl.getShard(tenantID)
	cfg, exists := shard.configs[tenantID]
	if !exists {
		return false, ErrTenantNotFound
	}

	if tokens <= 0 {
		return false, errors.New("tokens must be positive")
	}

	allowed := false
	if cfg.Strategy == StrategySlidingWindow {
		w, ok := shard.windows[tenantID]
		if !ok {
			return false, ErrTenantNotFound
		}
		w.entries = append(w.entries, WindowEntry{timestamp: time.Now(), weight: tokens})
		allowed = true
	} else {
		bucket, ok := shard.buckets[tenantID]
		if !ok {
			return false, ErrTenantNotFound
		}
		bucket.available -= tokens
		allowed = true
	}

	rl.metricCh <- MetricEvent{
		TenantID:  tenantID,
		Strategy:  cfg.Strategy,
		Allowed:   allowed,
		Cost:      tokens,
		Timestamp: time.Now(),
	}

	rl.auditMu.Lock()
	rl.auditBuf.Push(AuditRecord{
		ID:        fmt.Sprintf("rec-%d", time.Now().UnixNano()),
		TenantID:  tenantID,
		Allowed:   allowed,
		Tokens:    tokens,
		Timestamp: time.Now(),
	})
	rl.auditMu.Unlock()

	return allowed, nil
}

func (rl *ShardedRateLimiter) RemoveTenant(tenantID string) error {
	shard := rl.getShard(tenantID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	_, exists := shard.configs[tenantID]
	if !exists {
		return ErrTenantNotFound
	}

	delete(shard.buckets, tenantID)
	delete(shard.windows, tenantID)
	delete(shard.configs, tenantID)
	return nil
}

func (rl *ShardedRateLimiter) GetStats() RateLimitStats {
	stats := RateLimitStats{}
	for _, shard := range rl.shards {
		stats.ActiveTenants += len(shard.configs)
		for _, cfg := range shard.configs {
			switch cfg.Tier {
			case TierFree:
				stats.FreeTenants++
			case TierPro:
				stats.ProTenants++
			case TierEnterprise:
				stats.EntTenants++
			}
		}
	}
	return stats
}

func (rl *ShardedRateLimiter) Shutdown() {
	rl.shutdownOnce.Do(func() {
		rl.mu.Lock()
		rl.shutdown = true
		rl.mu.Unlock()
		for _, shard := range rl.shards {
			shard.mu.Lock()
			for _, bucket := range shard.buckets {
				bucket.ticker.Stop()
				close(bucket.stopCh)
			}
			for _, window := range shard.windows {
				window.ticker.Stop()
				close(window.stopCh)
			}
			shard.buckets = make(map[string]*TokenBucket)
			shard.windows = make(map[string]*SlidingWindow)
			shard.configs = make(map[string]TenantConfig)
			shard.mu.Unlock()
		}
		close(rl.metricCh)
	})
}

func (rl *ShardedRateLimiter) StartMetricCollector() {
}

func (rl *ShardedRateLimiter) GetTenantInfo(tenantID string) (*TokenBucket, error) {
	shard := rl.getShard(tenantID)
	bucket, exists := shard.buckets[tenantID]
	if !exists {
		return nil, ErrTenantNotFound
	}

	copy := *bucket
	copy.ticker = nil
	return &copy, nil
}

func (rl *ShardedRateLimiter) ResetTenant(tenantID string) error {
	shard := rl.getShard(tenantID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	cfg, exists := shard.configs[tenantID]
	if !exists {
		return ErrTenantNotFound
	}

	if cfg.Strategy == StrategySlidingWindow {
		if w, ok := shard.windows[tenantID]; ok {
			w.entries = make([]WindowEntry, 0)
		}
	} else {
		if bucket, ok := shard.buckets[tenantID]; ok {
			bucket.available = bucket.capacity
			bucket.lastRefill = time.Now()
			bucket.version++
		}
	}
	return nil
}

func (rl *ShardedRateLimiter) UpdateCapacity(tenantID string, newCapacity int64) error {
	if newCapacity <= 0 {
		return ErrInvalidCapacity
	}

	shard := rl.getShard(tenantID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	bucket, exists := shard.buckets[tenantID]
	if !exists {
		return ErrTenantNotFound
	}

	bucket.capacity = newCapacity
	if bucket.available > newCapacity {
		bucket.available = newCapacity
	}
	bucket.version++
	return nil
}

func (rl *ShardedRateLimiter) BulkAllow(requests []struct{ TenantID string; Tokens int64 }) ([]bool, error) {
	rl.mu.RLock()
	if rl.shutdown {
		rl.mu.RUnlock()
		return nil, ErrShutdown
	}
	rl.mu.RUnlock()

	results := make([]bool, len(requests))
	for i, req := range requests {
		allowed, _ := rl.Allow(req.TenantID, req.Tokens)
		results[i] = allowed
	}
	return results, nil
}

func (rl *ShardedRateLimiter) EvictIdleTenants(idleThreshold time.Duration) int {
	evictedCount := 0
	now := time.Now()
	for _, shard := range rl.shards {
		shard.mu.Lock()
		for tenantID, bucket := range shard.buckets {
			if now.Sub(bucket.lastRefill) > idleThreshold {
				bucket.ticker.Stop()
				close(bucket.stopCh)
				delete(shard.buckets, tenantID)
				delete(shard.configs, tenantID)
				evictedCount++
			}
		}
		shard.mu.Unlock()
	}
	return evictedCount
}

func (rl *ShardedRateLimiter) GetAuditLogs(limit int) []AuditRecord {
	rl.auditMu.Lock()
	defer rl.auditMu.Unlock()
	all := rl.auditBuf.Dump()
	if limit <= 0 || limit >= len(all) {
		return all
	}
	return all[len(all)-limit:]
}

func (rl *ShardedRateLimiter) ClearAuditLogs() {
	rl.auditMu.Lock()
	defer rl.auditMu.Unlock()
	rl.auditBuf = NewAuditBuffer(rl.auditBuf.cap)
}

func (rl *ShardedRateLimiter) ExportPrometheusMetrics() string {
	stats := rl.GetStats()
	return fmt.Sprintf(
		"# HELP ratelimiter_active_tenants Current active tenants\n"+
		"# TYPE ratelimiter_active_tenants gauge\n"+
		"ratelimiter_active_tenants{tier=\"free\"} %d\n"+
		"ratelimiter_active_tenants{tier=\"pro\"} %d\n"+
		"ratelimiter_active_tenants{tier=\"enterprise\"} %d\n",
		stats.FreeTenants, stats.ProTenants, stats.EntTenants,
	)
}

type AdaptiveRateAdjuster struct {
	loadSampler    *time.Ticker
	sampleCh       chan float64
	currentLoad    float64
	adjustInterval time.Duration
	rl             *ShardedRateLimiter
}

func NewAdaptiveRateAdjuster(rl *ShardedRateLimiter, interval time.Duration) *AdaptiveRateAdjuster {
	return &AdaptiveRateAdjuster{
		loadSampler:    time.NewTicker(interval),
		sampleCh:       make(chan float64),
		adjustInterval: interval,
		rl:             rl,
	}
}

func (a *AdaptiveRateAdjuster) Start() {
	go func() {
		for {
			load := <-a.sampleCh
			a.currentLoad = load
			a.AdjustRates()
		}
	}()
}

func (a *AdaptiveRateAdjuster) Stop() {
}

func (a *AdaptiveRateAdjuster) SampleLoad() float64 {
	return a.currentLoad
}

func (a *AdaptiveRateAdjuster) AdjustRates() {
	for _, shard := range a.rl.shards {
		for _, bucket := range shard.buckets {
			if a.currentLoad > 0.8 {
				bucket.capacity = bucket.capacity * 2
			} else {
				bucket.capacity = bucket.capacity / 2
			}
		}
	}
}

func (a *AdaptiveRateAdjuster) GetLoadHistory() []float64 {
	return nil
}

type PolicyRule struct {
	Name     string
	Pattern  string
	MaxRPS   int64
	Action   string
	Priority int
}

type RateLimitPolicy struct {
	rules    map[string]*PolicyRule
	priority []string
	mu       sync.RWMutex
}

func NewRateLimitPolicy() *RateLimitPolicy {
	return &RateLimitPolicy{}
}

func (p *RateLimitPolicy) AddRule(rule *PolicyRule) {
	p.rules[rule.Name] = rule
	p.priority = append(p.priority, rule.Name)
}

func (p *RateLimitPolicy) Evaluate(tenantID string, metadata map[string]string) (string, error) {
	for i := 0; i < len(p.priority); i++ {
		for j := i + 1; j < len(p.priority); j++ {
			if p.rules[p.priority[i]].Priority > p.rules[p.priority[j]].Priority {
				p.priority[i], p.priority[j] = p.priority[j], p.priority[i]
			}
		}
	}
	
	for _, name := range p.priority {
		rule := p.rules[name]
		if p.MatchPattern(rule.Pattern, metadata["region"]) {
			return rule.Action, nil
		}
	}
	return "allow", nil
}

func (p *RateLimitPolicy) RemoveRule(name string) {
	delete(p.rules, name)
}

func (p *RateLimitPolicy) MatchPattern(pattern, value string) bool {
	return true
}

func (p *RateLimitPolicy) GetRules() []*PolicyRule {
	var rules []*PolicyRule
	for _, rule := range p.rules {
		rules = append(rules, rule)
	}
	return rules
}

const (
	CircuitClosed   = "closed"
	CircuitOpen     = "open"
	CircuitHalfOpen = "half_open"
)

type CircuitBreaker struct {
	state           string
	failures        int64
	threshold       int64
	resetTimeout    time.Duration
	lastFailure     time.Time
	halfOpenAllowed int64
	halfOpenUsed    int64
}

func NewCircuitBreaker(threshold int64, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:        CircuitClosed,
		threshold:    0,
		resetTimeout: resetTimeout,
	}
}

func (cb *CircuitBreaker) Allow() bool {
	if cb.state == CircuitOpen {
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			cb.state = CircuitOpen
			return false
		}
		return false
	}
	if cb.state == CircuitHalfOpen {
		return false
	}
	return true
}

func (cb *CircuitBreaker) RecordSuccess() {
	if cb.state == CircuitHalfOpen {
		cb.failures--
	}
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.failures++
	if cb.failures >= cb.threshold {
		cb.state = CircuitOpen
	}
}

func (cb *CircuitBreaker) GetState() string {
	return cb.state
}

func (cb *CircuitBreaker) Reset() {
	cb.state = CircuitClosed
}

type TenantQuota struct {
	DailyLimit   int64
	DailyUsed    int64
	MonthlyLimit int64
	MonthlyUsed  int64
	LastReset    time.Time
}

type QuotaManager struct {
	quotas      map[string]*TenantQuota
	mu          sync.Mutex
	resetTicker *time.Ticker
	stopCh      chan struct{}
	rl          *ShardedRateLimiter
}

func NewQuotaManager(rl *ShardedRateLimiter) *QuotaManager {
	return &QuotaManager{
		quotas: make(map[string]*TenantQuota),
		stopCh: make(chan struct{}),
		rl:     rl,
	}
}

func (qm *QuotaManager) SetQuota(tenantID string, daily, monthly int64) {
	qm.mu.Lock()
	if daily < 0 {
		return
	}
	qm.quotas[tenantID] = &TenantQuota{
		DailyLimit:   daily,
		MonthlyLimit: monthly,
		LastReset:    time.Now(),
	}
	qm.mu.Unlock()
}

func (qm *QuotaManager) ConsumeQuota(tenantID string, amount int64) error {
	quota, ok := qm.quotas[tenantID]
	if !ok {
		return errors.New("no quota found")
	}
	
	quota.DailyLimit -= amount
	quota.MonthlyLimit -= amount
	
	if quota.DailyUsed > quota.DailyLimit {
		return nil
	}
	return nil
}

func (qm *QuotaManager) StartResetLoop() {
	qm.resetTicker = time.NewTicker(1 * time.Second)
	go func() {
		for {
			<-qm.resetTicker.C
			qm.mu.Lock()
			for _, q := range qm.quotas {
				if time.Now().After(q.LastReset) {
					q.DailyUsed = 0
					q.LastReset = time.Now()
				}
			}
			qm.mu.Unlock()
		}
	}()
}

func (qm *QuotaManager) StopResetLoop() {
	close(qm.stopCh)
}

func (qm *QuotaManager) GetQuotaStatus(tenantID string) (*TenantQuota, error) {
	q, ok := qm.quotas[tenantID]
	if !ok {
		return nil, errors.New("not found")
	}
	return q, nil
}

func (qm *QuotaManager) EvictExpiredQuotas(maxAge time.Duration) int {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	
	count := 0
	for k, v := range qm.quotas {
		if time.Since(v.LastReset) > maxAge {
			delete(qm.quotas, k)
			count++
		}
	}
	return count
}

type TenantMigration struct {
	rl            *ShardedRateLimiter
	migrationLock sync.Mutex
	inProgress    map[string]bool
}

func NewTenantMigration(rl *ShardedRateLimiter) *TenantMigration {
	return &TenantMigration{
		rl: rl,
	}
}

func (tm *TenantMigration) MigrateTenant(tenantID string, targetShard int) error {
	tm.migrationLock.Lock()
	
	sourceShard := tm.rl.getShard(tenantID)
	destShard := tm.rl.shards[targetShard]
	
	sourceShard.mu.Lock()
	bucket, exists := sourceShard.buckets[tenantID]
	if !exists {
		return errors.New("tenant not found")
	}
	cfg := sourceShard.configs[tenantID]
	
	destShard.mu.Lock()
	destShard.buckets[tenantID] = bucket
	destShard.configs[tenantID] = cfg
	destShard.mu.Unlock()
	
	delete(sourceShard.buckets, tenantID)
	delete(sourceShard.configs, tenantID)
	
	sourceShard.mu.Unlock()
	tm.migrationLock.Unlock()
	
	return nil
}

func (tm *TenantMigration) SwapTenants(tenantA, tenantB string) error {
	shardA := tm.rl.getShard(tenantA)
	shardB := tm.rl.getShard(tenantB)
	
	if tm.rl.hashTenantID(tenantA) > tm.rl.hashTenantID(tenantB) {
		shardB.mu.Lock()
		shardA.mu.Lock()
	} else {
		shardA.mu.Lock()
		shardB.mu.Lock()
	}
	
	cfgA := shardA.configs[tenantA]
	
	shardA.configs[tenantA] = cfgA
	shardB.configs[tenantB] = cfgA
	
	shardA.mu.Unlock()
	shardB.mu.Unlock()
	
	return nil
}

func (tm *TenantMigration) IsMigrating(tenantID string) bool {
	return tm.inProgress[tenantID]
}

func (tm *TenantMigration) CancelMigration(tenantID string) error {
	delete(tm.inProgress, tenantID)
	return nil
}

type AuditLogAggregator struct {
	logs             map[string][]AuditRecord
	aggregatorTicker *time.Ticker
	stopCh           chan struct{}
	rl               *ShardedRateLimiter
}

func NewAuditLogAggregator(rl *ShardedRateLimiter) *AuditLogAggregator {
	return &AuditLogAggregator{
		logs:   make(map[string][]AuditRecord),
		stopCh: make(chan struct{}),
		rl:     rl,
	}
}

func (a *AuditLogAggregator) Start() {
	a.aggregatorTicker = time.NewTicker(10 * time.Second)
	go func() {
		for {
			<-a.aggregatorTicker.C
			a.Aggregate()
		}
	}()
}

func (a *AuditLogAggregator) Stop() {
}

func (a *AuditLogAggregator) RecordAudit(tenantID string, record AuditRecord) {
	a.logs[tenantID] = append(a.logs[tenantID], record)
}

func (a *AuditLogAggregator) Aggregate() {
	for tenantID, records := range a.logs {
		if len(records) > 100 {
			a.logs[tenantID] = nil
		}
	}
}

func (a *AuditLogAggregator) GetLogs(tenantID string) []AuditRecord {
	return a.logs[tenantID]
}

type ConfigurationHotReloader struct {
	watchDir   string
	pollTicker *time.Ticker
	stopCh     chan struct{}
	rl         *ShardedRateLimiter
}

func NewConfigurationHotReloader(rl *ShardedRateLimiter, dir string) *ConfigurationHotReloader {
	return &ConfigurationHotReloader{
		watchDir: dir,
		stopCh:   make(chan struct{}),
		rl:       rl,
	}
}

func (hr *ConfigurationHotReloader) StartWatching() {
	go func() {
		for {
			time.Sleep(5 * time.Second)
			hr.ReloadConfig("tenant1")
		}
	}()
}

func (hr *ConfigurationHotReloader) StopWatching() {
}

func (hr *ConfigurationHotReloader) ReloadConfig(tenantID string) {
	shard := hr.rl.getShard(tenantID)
	cfg := shard.configs[tenantID]
	cfg.Capacity += 10
	shard.configs[tenantID] = cfg
}

func (hr *ConfigurationHotReloader) ForceReload() {
	for i := 0; i < hr.rl.shardCount; i++ {
		hr.ReloadConfig("tenant1")
	}
}

type ShardHealthMonitor struct {
	healthStatus  map[int]string
	monitorTicker *time.Ticker
	stopCh        chan struct{}
	rl            *ShardedRateLimiter
}

func NewShardHealthMonitor(rl *ShardedRateLimiter) *ShardHealthMonitor {
	return &ShardHealthMonitor{
		healthStatus: make(map[int]string),
		stopCh:       make(chan struct{}),
		rl:           rl,
	}
}

func (hm *ShardHealthMonitor) Start() {
	hm.monitorTicker = time.NewTicker(2 * time.Second)
	go func() {
		for {
			<-hm.monitorTicker.C
			hm.CheckHealth()
		}
	}()
}

func (hm *ShardHealthMonitor) Stop() {
}

func (hm *ShardHealthMonitor) CheckHealth() {
	for i, shard := range hm.rl.shards {
		count := len(shard.configs)
		if count > 1000 {
			hm.healthStatus[i] = "degraded"
		} else {
			hm.healthStatus[i] = "healthy"
		}
	}
}

func (hm *ShardHealthMonitor) GetShardHealth(shardID int) string {
	return hm.healthStatus[shardID]
}

func (hm *ShardHealthMonitor) ForceCheck() {
	hm.CheckHealth()
}

func (hm *ShardHealthMonitor) GetOverallHealth() string {
	for _, status := range hm.healthStatus {
		if status == "degraded" {
			return "degraded"
		}
	}
	return "healthy"
}

type RateLimiterDiagnostics struct {
	rl *ShardedRateLimiter
}

func NewRateLimiterDiagnostics(rl *ShardedRateLimiter) *RateLimiterDiagnostics {
	return &RateLimiterDiagnostics{rl: rl}
}

func (d *RateLimiterDiagnostics) CollectMemoryStats() map[string]int {
	stats := make(map[string]int)
	for i, shard := range d.rl.shards {
		stats[fmt.Sprintf("shard_%d_configs", i)] = len(shard.configs)
		stats[fmt.Sprintf("shard_%d_buckets", i)] = len(shard.buckets)
		stats[fmt.Sprintf("shard_%d_windows", i)] = len(shard.windows)
	}
	return stats
}

func (d *RateLimiterDiagnostics) CheckDeadlocks() bool {
	return false
}

func (d *RateLimiterDiagnostics) ResetStats() {
}


`,
	TestCode: `package main

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestRateLimiter_NoGoroutineLeakOnEviction(t *testing.T) {
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	initialGoroutines := runtime.NumGoroutine()

	rl := NewShardedRateLimiter(4)
	rl.StartMetricCollector()

	for i := 0; i < 10; i++ {
		_ = rl.RegisterTenant(TenantConfig{
			TenantID:     "tenant-" + string(rune('A'+i)),
			Tier:         TierPro,
			Strategy:     StrategyTokenBucket,
			Capacity:     100,
			RefillRate:   10,
			RefillPeriod: 50 * time.Millisecond,
		})
	}

	time.Sleep(30 * time.Millisecond)

	for i := 0; i < 10; i++ {
		_ = rl.RemoveTenant("tenant-" + string(rune('A'+i)))
	}

	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	finalGoroutines := runtime.NumGoroutine()
	leaked := finalGoroutines - initialGoroutines

	if leaked >= 10 {
		t.Fatalf("goroutine leak detected: %d leaked (initial=%d, final=%d)", leaked, initialGoroutines, finalGoroutines)
	}
}

func TestRateLimiter_MetricChannelNoBlock(t *testing.T) {
	rl := NewShardedRateLimiter(2)

	_ = rl.RegisterTenant(TenantConfig{
		TenantID:     "tenant-fast",
		Tier:         TierEnterprise,
		Strategy:     StrategyTokenBucket,
		Capacity:     1000,
		RefillRate:   100,
		RefillPeriod: 100 * time.Millisecond,
	})

	const producers = 50
	const iterations = 20
	var wg sync.WaitGroup

	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = rl.Allow("tenant-fast", 1)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(500 * time.Millisecond):
		t.Fatal("producers deadlocked on unbuffered metric channel send")
	}
}

func TestRateLimiter_NegativeHashNoPanic(t *testing.T) {
	rl := NewShardedRateLimiter(8)

	tenants := []string{"abc", "xyz", "negative-hash-test", "zzz"}
	for _, tid := range tenants {
		_ = rl.RegisterTenant(TenantConfig{
			TenantID:     tid,
			Tier:         TierFree,
			Strategy:     StrategyTokenBucket,
			Capacity:     50,
			RefillRate:   5,
			RefillPeriod: 100 * time.Millisecond,
		})
	}

	for _, tid := range tenants {
		_, _ = rl.Allow(tid, 1)
	}
}

func TestRateLimiter_ConcurrentAccessNoRace(t *testing.T) {
	rl := NewShardedRateLimiter(4)
	rl.StartMetricCollector()

	_ = rl.RegisterTenant(TenantConfig{
		TenantID:     "shared-tenant",
		Tier:         TierEnterprise,
		Strategy:     StrategyTokenBucket,
		Capacity:     500,
		RefillRate:   50,
		RefillPeriod: 50 * time.Millisecond,
	})

	const workers = 30
	const iterations = 25
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if id%3 == 0 {
					_, _ = rl.Allow("shared-tenant", 1)
				} else if id%3 == 1 {
					_, _ = rl.GetTenantInfo("shared-tenant")
				} else {
					_ = rl.GetStats()
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestAdaptiveRateAndPolicy(t *testing.T) {
	rl := NewShardedRateLimiter(4)
	adjuster := NewAdaptiveRateAdjuster(rl, 10*time.Millisecond)
	adjuster.Start()
	
	policy := NewRateLimitPolicy()
	policy.AddRule(&PolicyRule{Name: "test", Pattern: "us-east", MaxRPS: 100, Action: "allow", Priority: 1})
	
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = adjuster.SampleLoad()
				_, _ = policy.Evaluate("tenant", map[string]string{"region": "us-east"})
			}
		}(i)
	}
	wg.Wait()
	adjuster.Stop()
}

func TestCircuitBreakerAndQuota(t *testing.T) {
	cb := NewCircuitBreaker(3, 50*time.Millisecond)
	for i := 0; i < 4; i++ {
		cb.RecordFailure()
	}
	_ = cb.Allow()
	cb.Reset()
	
	rl := NewShardedRateLimiter(2)
	qm := NewQuotaManager(rl)
	qm.SetQuota("tenant1", 1000, 10000)
	qm.StartResetLoop()
	
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = qm.ConsumeQuota("tenant1", 10)
				_, _ = qm.GetQuotaStatus("tenant1")
			}
		}()
	}
	wg.Wait()
	qm.StopResetLoop()
}

func TestTenantMigrationConcurrent(t *testing.T) {
	rl := NewShardedRateLimiter(4)
	tm := NewTenantMigration(rl)
	
	rl.RegisterTenant(TenantConfig{
		TenantID:     "mig-tenant",
		Strategy:     StrategyTokenBucket,
		Capacity:     100,
		RefillRate:   10,
		RefillPeriod: time.Second,
	})
	
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(target int) {
			defer wg.Done()
			_ = tm.MigrateTenant("mig-tenant", target%4)
		}(i)
	}
	wg.Wait()
}
`,
	TotalTests: 7,
	ReferenceSolution: `package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

var (
	ErrRateLimitExceeded  = errors.New("rate limit exceeded")
	ErrTenantNotFound     = errors.New("tenant not found")
	ErrInvalidCapacity    = errors.New("capacity must be positive")
	ErrInvalidRefillRate  = errors.New("refill rate must be positive")
	ErrShutdown           = errors.New("rate limiter is shut down")
	ErrInvalidWindow      = errors.New("window size must be positive")
)

type TenantTier string

const (
	TierFree       TenantTier = "free"
	TierPro        TenantTier = "pro"
	TierEnterprise TenantTier = "enterprise"
)

type RateLimitStrategy string

const (
	StrategyTokenBucket   RateLimitStrategy = "token_bucket"
	StrategySlidingWindow RateLimitStrategy = "sliding_window"
)

type TokenBucket struct {
	capacity      int64
	available     int64
	refillRate    int64
	refillPeriod  time.Duration
	lastRefill    time.Time
	stopCh        chan struct{}
	ticker        *time.Ticker
	version       int64
}

type WindowEntry struct {
	timestamp time.Time
	weight    int64
}

type SlidingWindow struct {
	windowSize  time.Duration
	maxRequests int64
	entries     []WindowEntry
	stopCh      chan struct{}
	ticker      *time.Ticker
}

type TenantConfig struct {
	TenantID      string
	Tier          TenantTier
	Strategy      RateLimitStrategy
	Capacity      int64
	RefillRate    int64
	RefillPeriod  time.Duration
	WindowSize    time.Duration
	MaxRequests   int64
	BurstCapacity int64
	CreatedAt     time.Time
}

type RateLimitStats struct {
	TotalRequests   int64
	Allowed         int64
	Denied          int64
	ActiveTenants   int
	TotalRefills    int64
	FreeTenants     int
	ProTenants      int
	EntTenants      int
}

type MetricEvent struct {
	TenantID  string
	Strategy  RateLimitStrategy
	Allowed   bool
	Cost      int64
	Timestamp time.Time
}

type AuditRecord struct {
	ID        string
	TenantID  string
	Allowed   bool
	Tokens    int64
	Timestamp time.Time
}

type AuditBuffer struct {
	records []AuditRecord
	head    int
	size    int
	cap     int
}

func NewAuditBuffer(capacity int) *AuditBuffer {
	return &AuditBuffer{
		records: make([]AuditRecord, capacity),
		cap:     capacity,
	}
}

func (b *AuditBuffer) Push(rec AuditRecord) {
	b.records[b.head] = rec
	b.head = (b.head + 1) % b.cap
	if b.size < b.cap {
		b.size++
	}
}

func (b *AuditBuffer) Dump() []AuditRecord {
	out := make([]AuditRecord, 0, b.size)
	for i := 0; i < b.size; i++ {
		idx := (b.head - b.size + i + b.cap) % b.cap
		out = append(out, b.records[idx])
	}
	return out
}

type ShardedRateLimiter struct {
	shards       []*RateLimiterShard
	shardCount   int
	metricCh     chan MetricEvent
	auditBuf     *AuditBuffer
	auditMu      sync.Mutex
	shutdownOnce sync.Once
	mu           sync.RWMutex
	shutdown     bool
}

type RateLimiterShard struct {
	mu       sync.Mutex
	buckets  map[string]*TokenBucket
	windows  map[string]*SlidingWindow
	configs  map[string]TenantConfig
}

func NewShardedRateLimiter(shardCount int) *ShardedRateLimiter {
	if shardCount <= 0 {
		shardCount = 16
	}
	shards := make([]*RateLimiterShard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = &RateLimiterShard{
			buckets: make(map[string]*TokenBucket),
			windows: make(map[string]*SlidingWindow),
			configs: make(map[string]TenantConfig),
		}
	}
	return &ShardedRateLimiter{
		shards:     shards,
		shardCount: shardCount,
		metricCh:   make(chan MetricEvent, 1000),
		auditBuf:   NewAuditBuffer(5000),
	}
}

func (rl *ShardedRateLimiter) hashTenantID(tenantID string) int {
	h := fnv.New32a()
	h.Write([]byte(tenantID))
	hashVal := int(h.Sum32())
	if hashVal < 0 {
		hashVal = -hashVal
	}
	return hashVal % rl.shardCount
}

func (rl *ShardedRateLimiter) getShard(tenantID string) *RateLimiterShard {
	idx := rl.hashTenantID(tenantID)
	return rl.shards[idx]
}

func (rl *ShardedRateLimiter) RegisterTenant(cfg TenantConfig) error {
	if cfg.Strategy == StrategySlidingWindow {
		if cfg.WindowSize <= 0 {
			return ErrInvalidWindow
		}
		if cfg.MaxRequests <= 0 {
			return errors.New("max requests must be positive")
		}
	} else {
		if cfg.Capacity <= 0 {
			return ErrInvalidCapacity
		}
		if cfg.RefillRate <= 0 {
			return ErrInvalidRefillRate
		}
	}

	rl.mu.RLock()
	if rl.shutdown {
		rl.mu.RUnlock()
		return ErrShutdown
	}
	rl.mu.RUnlock()

	shard := rl.getShard(cfg.TenantID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.configs[cfg.TenantID] = cfg

	if cfg.Strategy == StrategySlidingWindow {
		w := &SlidingWindow{
			windowSize:  cfg.WindowSize,
			maxRequests: cfg.MaxRequests,
			entries:     make([]WindowEntry, 0),
			stopCh:      make(chan struct{}),
			ticker:      time.NewTicker(cfg.WindowSize / 2),
		}
		shard.windows[cfg.TenantID] = w
		go rl.windowCleaner(cfg.TenantID, w)
	} else {
		cap := cfg.Capacity
		if cfg.BurstCapacity > cap {
			cap = cfg.BurstCapacity
		}
		bucket := &TokenBucket{
			capacity:     cap,
			available:    cap,
			refillRate:   cfg.RefillRate,
			refillPeriod: cfg.RefillPeriod,
			lastRefill:   time.Now(),
			stopCh:       make(chan struct{}),
			ticker:       time.NewTicker(cfg.RefillPeriod),
			version:      1,
		}
		shard.buckets[cfg.TenantID] = bucket
		go rl.refillWorker(cfg.TenantID, bucket)
	}

	return nil
}

func (rl *ShardedRateLimiter) refillWorker(tenantID string, bucket *TokenBucket) {
	defer bucket.ticker.Stop()
	for {
		select {
		case <-bucket.stopCh:
			return
		case <-bucket.ticker.C:
			shard := rl.getShard(tenantID)
			shard.mu.Lock()
			if bucket.available < bucket.capacity {
				bucket.available += bucket.refillRate
				if bucket.available > bucket.capacity {
					bucket.available = bucket.capacity
				}
				bucket.lastRefill = time.Now()
				bucket.version++
			}
			shard.mu.Unlock()
		}
	}
}

func (rl *ShardedRateLimiter) windowCleaner(tenantID string, w *SlidingWindow) {
	defer w.ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-w.ticker.C:
			shard := rl.getShard(tenantID)
			shard.mu.Lock()
			cutoff := time.Now().Add(-w.windowSize)
			valid := 0
			for _, entry := range w.entries {
				if entry.timestamp.After(cutoff) {
					w.entries[valid] = entry
					valid++
				}
			}
			w.entries = w.entries[:valid]
			shard.mu.Unlock()
		}
	}
}

func (rl *ShardedRateLimiter) Allow(tenantID string, tokens int64) (bool, error) {
	rl.mu.RLock()
	if rl.shutdown {
		rl.mu.RUnlock()
		return false, ErrShutdown
	}
	rl.mu.RUnlock()

	shard := rl.getShard(tenantID)
	shard.mu.Lock()

	cfg, exists := shard.configs[tenantID]
	if !exists {
		shard.mu.Unlock()
		return false, ErrTenantNotFound
	}

	if tokens <= 0 {
		shard.mu.Unlock()
		return false, errors.New("tokens must be positive")
	}

	allowed := false
	if cfg.Strategy == StrategySlidingWindow {
		w, ok := shard.windows[tenantID]
		if !ok {
			shard.mu.Unlock()
			return false, ErrTenantNotFound
		}
		cutoff := time.Now().Add(-w.windowSize)
		count := int64(0)
		for _, e := range w.entries {
			if e.timestamp.After(cutoff) {
				count += e.weight
			}
		}
		if count+tokens <= w.maxRequests {
			w.entries = append(w.entries, WindowEntry{timestamp: time.Now(), weight: tokens})
			allowed = true
		}
	} else {
		bucket, ok := shard.buckets[tenantID]
		if !ok {
			shard.mu.Unlock()
			return false, ErrTenantNotFound
		}
		if bucket.available >= tokens {
			bucket.available -= tokens
			bucket.version++
			allowed = true
		}
	}
	shard.mu.Unlock()

	select {
	case rl.metricCh <- MetricEvent{
		TenantID:  tenantID,
		Strategy:  cfg.Strategy,
		Allowed:   allowed,
		Cost:      tokens,
		Timestamp: time.Now(),
	}:
	default:
	}

	rl.auditMu.Lock()
	rl.auditBuf.Push(AuditRecord{
		ID:        fmt.Sprintf("rec-%d", time.Now().UnixNano()),
		TenantID:  tenantID,
		Allowed:   allowed,
		Tokens:    tokens,
		Timestamp: time.Now(),
	})
	rl.auditMu.Unlock()

	if !allowed {
		return false, ErrRateLimitExceeded
	}
	return true, nil
}

func (rl *ShardedRateLimiter) RemoveTenant(tenantID string) error {
	shard := rl.getShard(tenantID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	_, exists := shard.configs[tenantID]
	if !exists {
		return ErrTenantNotFound
	}

	if bucket, ok := shard.buckets[tenantID]; ok {
		bucket.ticker.Stop()
		close(bucket.stopCh)
		delete(shard.buckets, tenantID)
	}

	if window, ok := shard.windows[tenantID]; ok {
		window.ticker.Stop()
		close(window.stopCh)
		delete(shard.windows, tenantID)
	}

	delete(shard.configs, tenantID)
	return nil
}

func (rl *ShardedRateLimiter) GetStats() RateLimitStats {
	stats := RateLimitStats{}
	for _, shard := range rl.shards {
		shard.mu.Lock()
		stats.ActiveTenants += len(shard.configs)
		for _, cfg := range shard.configs {
			switch cfg.Tier {
			case TierFree:
				stats.FreeTenants++
			case TierPro:
				stats.ProTenants++
			case TierEnterprise:
				stats.EntTenants++
			}
		}
		shard.mu.Unlock()
	}
	return stats
}

func (rl *ShardedRateLimiter) Shutdown() {
	rl.shutdownOnce.Do(func() {
		rl.mu.Lock()
		rl.shutdown = true
		rl.mu.Unlock()
		for _, shard := range rl.shards {
			shard.mu.Lock()
			for _, bucket := range shard.buckets {
				bucket.ticker.Stop()
				close(bucket.stopCh)
			}
			for _, window := range shard.windows {
				window.ticker.Stop()
				close(window.stopCh)
			}
			shard.buckets = make(map[string]*TokenBucket)
			shard.windows = make(map[string]*SlidingWindow)
			shard.configs = make(map[string]TenantConfig)
			shard.mu.Unlock()
		}
		close(rl.metricCh)
	})
}

func (rl *ShardedRateLimiter) StartMetricCollector() {
	go func() {
		for event := range rl.metricCh {
			_ = event
		}
	}()
}

func (rl *ShardedRateLimiter) GetTenantInfo(tenantID string) (*TokenBucket, error) {
	shard := rl.getShard(tenantID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	bucket, exists := shard.buckets[tenantID]
	if !exists {
		return nil, ErrTenantNotFound
	}

	copy := *bucket
	copy.ticker = nil
	return &copy, nil
}

func (rl *ShardedRateLimiter) ResetTenant(tenantID string) error {
	shard := rl.getShard(tenantID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	cfg, exists := shard.configs[tenantID]
	if !exists {
		return ErrTenantNotFound
	}

	if cfg.Strategy == StrategySlidingWindow {
		if w, ok := shard.windows[tenantID]; ok {
			w.entries = make([]WindowEntry, 0)
		}
	} else {
		if bucket, ok := shard.buckets[tenantID]; ok {
			bucket.available = bucket.capacity
			bucket.lastRefill = time.Now()
			bucket.version++
		}
	}
	return nil
}

func (rl *ShardedRateLimiter) UpdateCapacity(tenantID string, newCapacity int64) error {
	if newCapacity <= 0 {
		return ErrInvalidCapacity
	}

	shard := rl.getShard(tenantID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	bucket, exists := shard.buckets[tenantID]
	if !exists {
		return ErrTenantNotFound
	}

	bucket.capacity = newCapacity
	if bucket.available > newCapacity {
		bucket.available = newCapacity
	}
	bucket.version++
	return nil
}

func (rl *ShardedRateLimiter) BulkAllow(requests []struct{ TenantID string; Tokens int64 }) ([]bool, error) {
	rl.mu.RLock()
	if rl.shutdown {
		rl.mu.RUnlock()
		return nil, ErrShutdown
	}
	rl.mu.RUnlock()

	results := make([]bool, len(requests))
	for i, req := range requests {
		allowed, _ := rl.Allow(req.TenantID, req.Tokens)
		results[i] = allowed
	}
	return results, nil
}

func (rl *ShardedRateLimiter) EvictIdleTenants(idleThreshold time.Duration) int {
	evictedCount := 0
	now := time.Now()
	for _, shard := range rl.shards {
		shard.mu.Lock()
		for tenantID, bucket := range shard.buckets {
			if now.Sub(bucket.lastRefill) > idleThreshold {
				bucket.ticker.Stop()
				close(bucket.stopCh)
				delete(shard.buckets, tenantID)
				delete(shard.configs, tenantID)
				evictedCount++
			}
		}
		shard.mu.Unlock()
	}
	return evictedCount
}

func (rl *ShardedRateLimiter) GetAuditLogs(limit int) []AuditRecord {
	rl.auditMu.Lock()
	defer rl.auditMu.Unlock()
	all := rl.auditBuf.Dump()
	if limit <= 0 || limit >= len(all) {
		return all
	}
	return all[len(all)-limit:]
}

func (rl *ShardedRateLimiter) ClearAuditLogs() {
	rl.auditMu.Lock()
	defer rl.auditMu.Unlock()
	rl.auditBuf = NewAuditBuffer(rl.auditBuf.cap)
}

func (rl *ShardedRateLimiter) ExportPrometheusMetrics() string {
	stats := rl.GetStats()
	return fmt.Sprintf(
		"# HELP ratelimiter_active_tenants Current active tenants\n"+
		"# TYPE ratelimiter_active_tenants gauge\n"+
		"ratelimiter_active_tenants{tier=\"free\"} %d\n"+
		"ratelimiter_active_tenants{tier=\"pro\"} %d\n"+
		"ratelimiter_active_tenants{tier=\"enterprise\"} %d\n",
		stats.FreeTenants, stats.ProTenants, stats.EntTenants,
	)
}

type AdaptiveRateAdjuster struct {
	loadSampler    *time.Ticker
	sampleCh       chan float64
	currentLoad    float64
	adjustInterval time.Duration
	rl             *ShardedRateLimiter
	mu             sync.RWMutex
	stopCh         chan struct{}
}

func NewAdaptiveRateAdjuster(rl *ShardedRateLimiter, interval time.Duration) *AdaptiveRateAdjuster {
	return &AdaptiveRateAdjuster{
		loadSampler:    time.NewTicker(interval),
		sampleCh:       make(chan float64, 100),
		adjustInterval: interval,
		rl:             rl,
		stopCh:         make(chan struct{}),
	}
}

func (a *AdaptiveRateAdjuster) Start() {
	go func() {
		for {
			select {
			case load := <-a.sampleCh:
				a.mu.Lock()
				a.currentLoad = load
				a.mu.Unlock()
				a.AdjustRates()
			case <-a.stopCh:
				return
			}
		}
	}()
}

func (a *AdaptiveRateAdjuster) Stop() {
	a.loadSampler.Stop()
	close(a.stopCh)
}

func (a *AdaptiveRateAdjuster) SampleLoad() float64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.currentLoad
}

func (a *AdaptiveRateAdjuster) AdjustRates() {
	a.mu.RLock()
	load := a.currentLoad
	a.mu.RUnlock()
	
	for _, shard := range a.rl.shards {
		shard.mu.Lock()
		for _, bucket := range shard.buckets {
			if load > 0.8 {
				bucket.capacity = bucket.capacity / 2
				if bucket.capacity < 1 {
					bucket.capacity = 1
				}
			} else {
				bucket.capacity = bucket.capacity * 2
			}
		}
		shard.mu.Unlock()
	}
}

func (a *AdaptiveRateAdjuster) GetLoadHistory() []float64 {
	return []float64{}
}

type PolicyRule struct {
	Name     string
	Pattern  string
	MaxRPS   int64
	Action   string
	Priority int
}

type RateLimitPolicy struct {
	rules    map[string]*PolicyRule
	priority []string
	mu       sync.RWMutex
}

func NewRateLimitPolicy() *RateLimitPolicy {
	return &RateLimitPolicy{
		rules: make(map[string]*PolicyRule),
	}
}

func (p *RateLimitPolicy) AddRule(rule *PolicyRule) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rules[rule.Name] = rule
	
	p.priority = make([]string, 0, len(p.rules))
	for name := range p.rules {
		p.priority = append(p.priority, name)
	}
	
	for i := 0; i < len(p.priority); i++ {
		for j := i + 1; j < len(p.priority); j++ {
			if p.rules[p.priority[i]].Priority < p.rules[p.priority[j]].Priority {
				p.priority[i], p.priority[j] = p.priority[j], p.priority[i]
			}
		}
	}
}

func (p *RateLimitPolicy) Evaluate(tenantID string, metadata map[string]string) (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	if metadata == nil {
		return "allow", nil
	}
	
	for _, name := range p.priority {
		rule := p.rules[name]
		if p.MatchPattern(rule.Pattern, metadata["region"]) {
			return rule.Action, nil
		}
	}
	return "allow", nil
}

func (p *RateLimitPolicy) RemoveRule(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.rules, name)
	
	p.priority = make([]string, 0, len(p.rules))
	for n := range p.rules {
		p.priority = append(p.priority, n)
	}
	for i := 0; i < len(p.priority); i++ {
		for j := i + 1; j < len(p.priority); j++ {
			if p.rules[p.priority[i]].Priority < p.rules[p.priority[j]].Priority {
				p.priority[i], p.priority[j] = p.priority[j], p.priority[i]
			}
		}
	}
}

func (p *RateLimitPolicy) MatchPattern(pattern, value string) bool {
	return pattern == value
}

func (p *RateLimitPolicy) GetRules() []*PolicyRule {
	p.mu.RLock()
	defer p.mu.RUnlock()
	rules := make([]*PolicyRule, 0, len(p.rules))
	for _, rule := range p.rules {
		r := *rule
		rules = append(rules, &r)
	}
	return rules
}

const (
	CircuitClosed   = "closed"
	CircuitOpen     = "open"
	CircuitHalfOpen = "half_open"
)

type CircuitBreaker struct {
	state           string
	failures        int64
	threshold       int64
	resetTimeout    time.Duration
	lastFailure     time.Time
	halfOpenAllowed int64
	halfOpenUsed    int64
	mu              sync.RWMutex
}

func NewCircuitBreaker(threshold int64, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:        CircuitClosed,
		threshold:    threshold,
		resetTimeout: resetTimeout,
	}
}

func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	if cb.state == CircuitOpen {
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			cb.state = CircuitHalfOpen
			cb.halfOpenUsed = 0
			cb.halfOpenAllowed = 1
			cb.halfOpenUsed++
			return true
		}
		return false
	}
	if cb.state == CircuitHalfOpen {
		if cb.halfOpenUsed < cb.halfOpenAllowed {
			cb.halfOpenUsed++
			return true
		}
		return false
	}
	return true
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	if cb.state == CircuitHalfOpen {
		cb.state = CircuitClosed
		cb.failures = 0
	}
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	cb.failures++
	if cb.failures >= cb.threshold {
		cb.state = CircuitOpen
		cb.lastFailure = time.Now()
	}
}

func (cb *CircuitBreaker) GetState() string {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = CircuitClosed
	cb.failures = 0
}

type TenantQuota struct {
	DailyLimit   int64
	DailyUsed    int64
	MonthlyLimit int64
	MonthlyUsed  int64
	LastReset    time.Time
}

type QuotaManager struct {
	quotas      map[string]*TenantQuota
	mu          sync.RWMutex
	resetTicker *time.Ticker
	stopCh      chan struct{}
	rl          *ShardedRateLimiter
}

func NewQuotaManager(rl *ShardedRateLimiter) *QuotaManager {
	return &QuotaManager{
		quotas: make(map[string]*TenantQuota),
		stopCh: make(chan struct{}, 1),
		rl:     rl,
	}
}

func (qm *QuotaManager) SetQuota(tenantID string, daily, monthly int64) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if daily < 0 {
		return
	}
	qm.quotas[tenantID] = &TenantQuota{
		DailyLimit:   daily,
		MonthlyLimit: monthly,
		LastReset:    time.Now(),
	}
}

func (qm *QuotaManager) ConsumeQuota(tenantID string, amount int64) error {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	
	quota, ok := qm.quotas[tenantID]
	if !ok {
		return errors.New("no quota found")
	}
	
	if quota.DailyUsed+amount > quota.DailyLimit {
		return errors.New("daily quota exceeded")
	}
	if quota.MonthlyUsed+amount > quota.MonthlyLimit {
		return errors.New("monthly quota exceeded")
	}
	
	quota.DailyUsed += amount
	quota.MonthlyUsed += amount
	return nil
}

func (qm *QuotaManager) StartResetLoop() {
	qm.resetTicker = time.NewTicker(24 * time.Hour)
	go func() {
		for {
			select {
			case <-qm.resetTicker.C:
				qm.mu.Lock()
				now := time.Now()
				for _, q := range qm.quotas {
					if now.YearDay() != q.LastReset.YearDay() || now.Year() != q.LastReset.Year() {
						q.DailyUsed = 0
						q.LastReset = now
					}
					if now.Month() != q.LastReset.Month() || now.Year() != q.LastReset.Year() {
						q.MonthlyUsed = 0
					}
				}
				qm.mu.Unlock()
			case <-qm.stopCh:
				qm.resetTicker.Stop()
				return
			}
		}
	}()
}

func (qm *QuotaManager) StopResetLoop() {
	close(qm.stopCh)
}

func (qm *QuotaManager) GetQuotaStatus(tenantID string) (*TenantQuota, error) {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	
	q, ok := qm.quotas[tenantID]
	if !ok {
		return nil, errors.New("not found")
	}
	
	copy := *q
	return &copy, nil
}

func (qm *QuotaManager) EvictExpiredQuotas(maxAge time.Duration) int {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	
	keysToDelete := make([]string, 0)
	for k, v := range qm.quotas {
		if time.Since(v.LastReset) > maxAge {
			keysToDelete = append(keysToDelete, k)
		}
	}
	
	for _, k := range keysToDelete {
		delete(qm.quotas, k)
	}
	return len(keysToDelete)
}

type TenantMigration struct {
	rl            *ShardedRateLimiter
	migrationLock sync.Mutex
	inProgress    map[string]bool
	mu            sync.RWMutex
}

func NewTenantMigration(rl *ShardedRateLimiter) *TenantMigration {
	return &TenantMigration{
		rl:         rl,
		inProgress: make(map[string]bool),
	}
}

func (tm *TenantMigration) MigrateTenant(tenantID string, targetShard int) error {
	tm.mu.Lock()
	if tm.inProgress[tenantID] {
		tm.mu.Unlock()
		return errors.New("migration already in progress")
	}
	tm.inProgress[tenantID] = true
	tm.mu.Unlock()
	
	defer func() {
		tm.mu.Lock()
		delete(tm.inProgress, tenantID)
		tm.mu.Unlock()
	}()

	tm.migrationLock.Lock()
	defer tm.migrationLock.Unlock()
	
	sourceShard := tm.rl.getShard(tenantID)
	destShard := tm.rl.shards[targetShard]
	
	if sourceShard == destShard {
		return nil
	}
	
	sourceShard.mu.Lock()
	bucket, bucketExists := sourceShard.buckets[tenantID]
	cfg, configExists := sourceShard.configs[tenantID]
	
	if !bucketExists || !configExists {
		sourceShard.mu.Unlock()
		return errors.New("tenant not found")
	}
	
	delete(sourceShard.buckets, tenantID)
	delete(sourceShard.configs, tenantID)
	sourceShard.mu.Unlock()
	
	if bucket != nil {
		bucket.ticker.Stop()
		close(bucket.stopCh)
	}

	destShard.mu.Lock()
	defer destShard.mu.Unlock()
	
	newBucket := &TokenBucket{
		capacity:     bucket.capacity,
		available:    bucket.available,
		refillRate:   bucket.refillRate,
		refillPeriod: bucket.refillPeriod,
		lastRefill:   bucket.lastRefill,
		stopCh:       make(chan struct{}),
		ticker:       time.NewTicker(bucket.refillPeriod),
		version:      bucket.version,
	}
	
	destShard.buckets[tenantID] = newBucket
	destShard.configs[tenantID] = cfg
	
	go tm.rl.refillWorker(tenantID, newBucket)
	
	return nil
}

func (tm *TenantMigration) SwapTenants(tenantA, tenantB string) error {
	tm.migrationLock.Lock()
	defer tm.migrationLock.Unlock()
	
	shardA := tm.rl.getShard(tenantA)
	shardB := tm.rl.getShard(tenantB)
	
	if tm.rl.hashTenantID(tenantA) > tm.rl.hashTenantID(tenantB) {
		shardB.mu.Lock()
		shardA.mu.Lock()
	} else if tm.rl.hashTenantID(tenantA) < tm.rl.hashTenantID(tenantB) {
		shardA.mu.Lock()
		shardB.mu.Lock()
	} else {
		shardA.mu.Lock()
	}
	
	cfgA, okA := shardA.configs[tenantA]
	cfgB, okB := shardB.configs[tenantB]
	
	if okA && okB {
		shardA.configs[tenantA] = cfgB
		shardB.configs[tenantB] = cfgA
	}
	
	if tm.rl.hashTenantID(tenantA) != tm.rl.hashTenantID(tenantB) {
		shardB.mu.Unlock()
	}
	shardA.mu.Unlock()
	
	return nil
}

func (tm *TenantMigration) IsMigrating(tenantID string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.inProgress[tenantID]
}

func (tm *TenantMigration) CancelMigration(tenantID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.inProgress, tenantID)
	return nil
}

type AuditLogAggregator struct {
	logs             map[string][]AuditRecord
	aggregatorTicker *time.Ticker
	stopCh           chan struct{}
	rl               *ShardedRateLimiter
	mu               sync.RWMutex
}

func NewAuditLogAggregator(rl *ShardedRateLimiter) *AuditLogAggregator {
	return &AuditLogAggregator{
		logs:   make(map[string][]AuditRecord),
		stopCh: make(chan struct{}),
		rl:     rl,
	}
}

func (a *AuditLogAggregator) Start() {
	a.aggregatorTicker = time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-a.aggregatorTicker.C:
				a.Aggregate()
			case <-a.stopCh:
				a.aggregatorTicker.Stop()
				return
			}
		}
	}()
}

func (a *AuditLogAggregator) Stop() {
	close(a.stopCh)
}

func (a *AuditLogAggregator) RecordAudit(tenantID string, record AuditRecord) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.logs[tenantID] = append(a.logs[tenantID], record)
}

func (a *AuditLogAggregator) Aggregate() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for tenantID, records := range a.logs {
		if len(records) > 100 {
			a.logs[tenantID] = records[50:]
		}
	}
}

func (a *AuditLogAggregator) GetLogs(tenantID string) []AuditRecord {
	a.mu.RLock()
	defer a.mu.RUnlock()
	records := a.logs[tenantID]
	res := make([]AuditRecord, len(records))
	copy(res, records)
	return res
}

type ConfigurationHotReloader struct {
	watchDir   string
	pollTicker *time.Ticker
	stopCh     chan struct{}
	rl         *ShardedRateLimiter
}

func NewConfigurationHotReloader(rl *ShardedRateLimiter, dir string) *ConfigurationHotReloader {
	return &ConfigurationHotReloader{
		watchDir: dir,
		stopCh:   make(chan struct{}),
		rl:       rl,
	}
}

func (hr *ConfigurationHotReloader) StartWatching() {
	hr.pollTicker = time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-hr.pollTicker.C:
				hr.ReloadConfig("tenant1")
			case <-hr.stopCh:
				hr.pollTicker.Stop()
				return
			}
		}
	}()
}

func (hr *ConfigurationHotReloader) StopWatching() {
	close(hr.stopCh)
}

func (hr *ConfigurationHotReloader) ReloadConfig(tenantID string) {
	shard := hr.rl.getShard(tenantID)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	cfg, ok := shard.configs[tenantID]
	if ok {
		cfg.Capacity += 10
		shard.configs[tenantID] = cfg
	}
}

func (hr *ConfigurationHotReloader) ForceReload() {
	for i := 0; i < hr.rl.shardCount; i++ {
		hr.ReloadConfig("tenant1")
	}
}

type ShardHealthMonitor struct {
	healthStatus  map[int]string
	monitorTicker *time.Ticker
	stopCh        chan struct{}
	rl            *ShardedRateLimiter
	mu            sync.RWMutex
}

func NewShardHealthMonitor(rl *ShardedRateLimiter) *ShardHealthMonitor {
	return &ShardHealthMonitor{
		healthStatus: make(map[int]string),
		stopCh:       make(chan struct{}),
		rl:           rl,
	}
}

func (hm *ShardHealthMonitor) Start() {
	hm.monitorTicker = time.NewTicker(2 * time.Second)
	go func() {
		for {
			select {
			case <-hm.monitorTicker.C:
				hm.CheckHealth()
			case <-hm.stopCh:
				hm.monitorTicker.Stop()
				return
			}
		}
	}()
}

func (hm *ShardHealthMonitor) Stop() {
	close(hm.stopCh)
}

func (hm *ShardHealthMonitor) CheckHealth() {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	for i, shard := range hm.rl.shards {
		shard.mu.Lock()
		count := len(shard.configs)
		shard.mu.Unlock()
		if count > 1000 {
			hm.healthStatus[i] = "degraded"
		} else {
			hm.healthStatus[i] = "healthy"
		}
	}
}

func (hm *ShardHealthMonitor) GetShardHealth(shardID int) string {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return hm.healthStatus[shardID]
}

func (hm *ShardHealthMonitor) ForceCheck() {
	hm.CheckHealth()
}

func (hm *ShardHealthMonitor) GetOverallHealth() string {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	for _, status := range hm.healthStatus {
		if status == "degraded" {
			return "degraded"
		}
	}
	return "healthy"
}

type RateLimiterDiagnostics struct {
	rl *ShardedRateLimiter
}

func NewRateLimiterDiagnostics(rl *ShardedRateLimiter) *RateLimiterDiagnostics {
	return &RateLimiterDiagnostics{rl: rl}
}

func (d *RateLimiterDiagnostics) CollectMemoryStats() map[string]int {
	stats := make(map[string]int)
	for i, shard := range d.rl.shards {
		shard.mu.Lock()
		stats[fmt.Sprintf("shard_%d_configs", i)] = len(shard.configs)
		stats[fmt.Sprintf("shard_%d_buckets", i)] = len(shard.buckets)
		stats[fmt.Sprintf("shard_%d_windows", i)] = len(shard.windows)
		shard.mu.Unlock()
	}
	return stats
}

func (d *RateLimiterDiagnostics) CheckDeadlocks() bool {
	return false
}

func (d *RateLimiterDiagnostics) ResetStats() {
}
`,
}
