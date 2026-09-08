package tasks

import "benchmark/internal/swe"

var TaskT3DistributedCache = &swe.Task{
	ID:       "swe-t3-distributed-cache-01",
	Title:    "Multi-Tier Sharded LRU Cache with Asynchronous Write-Behind and Singleflight Coalescing",
	Tier:     swe.TierSenior,
	Points:   30,
	Category: "distributed_cache_concurrency",
	IssueBody: `### Bug Report: Deadlock under Contention, LRU Data Race, Singleflight Map Race, and Goroutine Leaks in Distributed Cache

**Environment:** High-throughput Go 1.24+ multi-tier caching service with sharded in-memory LRU stores, asynchronous write-behind persistence, and singleflight request deduplication.

**Expected Behavior:**
- Write-behind buffer queuing must never block while holding shard locks. Asynchronous persistence notifications must not cause deadlocks under high write contention.
- Updating LRU access order during read operations must be strictly thread-safe. Concurrent reads on the same cache shard must never cause data races or corrupt doubly linked list pointers (` + "`-race`" + ` clean).
- Singleflight request coalescing must safely synchronize call registrations and completions without concurrent map read/write races or panics.
- Cache disposal via ` + "`Close()`" + ` must cleanly stop all background workers, TTL cleaner tickers, and write-behind flushers, leaving zero leaked goroutines.
- The cache must satisfy all consistency, eviction, and TTL expiration guarantees under heavy multi-goroutine workloads.
- Cache warming must prefetch keys in priority order without blocking the caller and handle worker lifecycle safely.
- Eviction policy selection (LRU/LFU/ARC) must correctly identify the least valuable cache entry and handle concurrent access.`,
	BrokenCode: `package main

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrKeyNotFound          = errors.New("cache: key not found")
	ErrKeyExpired           = errors.New("cache: key expired")
	ErrCacheClosed          = errors.New("cache: instance closed")
	ErrCapacityExceeded     = errors.New("cache: shard capacity exceeded")
	ErrWriteBehindQueueFull = errors.New("cache: write-behind buffer queue full")
	ErrInvalidConfig        = errors.New("cache: invalid configuration")
	ErrShardNotFound        = errors.New("cache: shard not found")
	ErrSingleflightFailed   = errors.New("cache: singleflight compute failed")
	ErrInvalidTTL           = errors.New("cache: ttl must be non-negative")
	ErrEmptyKey             = errors.New("cache: key cannot be empty")
	ErrStorageUnavailable   = errors.New("cache: storage backend unavailable")
	ErrSnapshotCorrupted    = errors.New("cache: snapshot corrupted")
	ErrTransactionConflict  = errors.New("cache: transaction conflict")
	ErrBatchSizeExceeded    = errors.New("cache: batch size exceeded limit")
	ErrBackendTimeout       = errors.New("cache: backend write timeout")
)

type EvictionPolicy string

const (
	PolicyLRU  EvictionPolicy = "lru"
	PolicyLFU  EvictionPolicy = "lfu"
	PolicyFIFO EvictionPolicy = "fifo"
	PolicyNone EvictionPolicy = "none"
)

type WriteOpType string

const (
	OpSet    WriteOpType = "SET"
	OpDelete WriteOpType = "DELETE"
	OpTouch  WriteOpType = "TOUCH"
	OpExpire WriteOpType = "EXPIRE"
)

type CacheTier string

const (
	TierL1Hot  CacheTier = "l1_hot"
	TierL2Warm CacheTier = "l2_warm"
	TierL3Cold CacheTier = "l3_cold"
)

type SyncMode string

const (
	SyncModeAsync        SyncMode = "async"
	SyncModeWriteThrough SyncMode = "write_through"
)

type CacheConfig struct {
	NumShards                int
	MaxEntriesPerShard       int
	MaxMemoryBytesPerShard   int64
	DefaultTTL               time.Duration
	EvictionPolicy           EvictionPolicy
	WriteBehindQueueSize     int
	WriteBehindBatchSize     int
	WriteBehindFlushInterval time.Duration
	CleanupInterval          time.Duration
	SyncMode                 SyncMode
	HighWatermarkRatio       float64
	LowWatermarkRatio        float64
}

func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		NumShards:                8,
		MaxEntriesPerShard:       1024,
		MaxMemoryBytesPerShard:   64 * 1024 * 1024,
		DefaultTTL:               30 * time.Minute,
		EvictionPolicy:           PolicyLRU,
		WriteBehindQueueSize:     512,
		WriteBehindBatchSize:     64,
		WriteBehindFlushInterval: 100 * time.Millisecond,
		CleanupInterval:          5 * time.Minute,
		SyncMode:                 SyncModeAsync,
		HighWatermarkRatio:       0.90,
		LowWatermarkRatio:        0.75,
	}
}

type CacheOption func(*CacheConfig)

func WithNumShards(shards int) CacheOption {
	return func(c *CacheConfig) {
		if shards > 0 {
			c.NumShards = shards
		}
	}
}

func WithMaxEntriesPerShard(maxEntries int) CacheOption {
	return func(c *CacheConfig) {
		if maxEntries > 0 {
			c.MaxEntriesPerShard = maxEntries
		}
	}
}

func WithMaxMemoryBytesPerShard(bytes int64) CacheOption {
	return func(c *CacheConfig) {
		if bytes > 0 {
			c.MaxMemoryBytesPerShard = bytes
		}
	}
}

func WithDefaultTTL(ttl time.Duration) CacheOption {
	return func(c *CacheConfig) {
		if ttl >= 0 {
			c.DefaultTTL = ttl
		}
	}
}

func WithEvictionPolicy(policy EvictionPolicy) CacheOption {
	return func(c *CacheConfig) {
		c.EvictionPolicy = policy
	}
}

func WithWriteBehindQueueSize(sz int) CacheOption {
	return func(c *CacheConfig) {
		if sz > 0 {
			c.WriteBehindQueueSize = sz
		}
	}
}

func WithWriteBehindBatchSize(sz int) CacheOption {
	return func(c *CacheConfig) {
		if sz > 0 {
			c.WriteBehindBatchSize = sz
		}
	}
}

func WithWriteBehindFlushInterval(interval time.Duration) CacheOption {
	return func(c *CacheConfig) {
		if interval > 0 {
			c.WriteBehindFlushInterval = interval
		}
	}
}

func WithCleanupInterval(interval time.Duration) CacheOption {
	return func(c *CacheConfig) {
		if interval > 0 {
			c.CleanupInterval = interval
		}
	}
}

func WithSyncMode(mode SyncMode) CacheOption {
	return func(c *CacheConfig) {
		c.SyncMode = mode
	}
}

type LRUNode struct {
	key          string
	value        any
	cost         int64
	expiresAt    time.Time
	accessCount  int64
	lastAccessed time.Time
	firstSeen    time.Time
	isDirty      bool
	version      uint64
	tier         CacheTier
	hitStreak    int
	prev         *LRUNode
	next         *LRUNode
}

type DoublyLinkedList struct {
	head      *LRUNode
	tail      *LRUNode
	size      int
	totalCost int64
}

func NewDoublyLinkedList() *DoublyLinkedList {
	return &DoublyLinkedList{
		head:      nil,
		tail:      nil,
		size:      0,
		totalCost: 0,
	}
}

func (l *DoublyLinkedList) Len() int {
	return l.size
}

func (l *DoublyLinkedList) TotalCost() int64 {
	return l.totalCost
}

func (l *DoublyLinkedList) PushFront(node *LRUNode) {
	if node == nil {
		return
	}
	node.prev = nil
	node.next = l.head
	if l.head != nil {
		l.head.prev = node
	} else {
		l.tail = node
	}
	l.head = node
	l.size++
	l.totalCost += node.cost
}

func (l *DoublyLinkedList) PushBack(node *LRUNode) {
	if node == nil {
		return
	}
	node.next = nil
	node.prev = l.tail
	if l.tail != nil {
		l.tail.next = node
	} else {
		l.head = node
	}
	l.tail = node
	l.size++
	l.totalCost += node.cost
}

func (l *DoublyLinkedList) MoveToFront(node *LRUNode) {
	if node == nil || l.head == node {
		return
	}
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		l.tail = node.prev
	}
	node.prev = nil
	node.next = l.head
	if l.head != nil {
		l.head.prev = node
	}
	l.head = node
}

func (l *DoublyLinkedList) MoveToBack(node *LRUNode) {
	if node == nil || l.tail == node {
		return
	}
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		l.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	}
	node.next = nil
	node.prev = l.tail
	if l.tail != nil {
		l.tail.next = node
	}
	l.tail = node
}

func (l *DoublyLinkedList) Remove(node *LRUNode) {
	if node == nil {
		return
	}
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		l.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		l.tail = node.prev
	}
	node.prev = nil
	node.next = nil
	l.size--
	l.totalCost -= node.cost
	if l.size <= 0 {
		l.head = nil
		l.tail = nil
		l.size = 0
		l.totalCost = 0
	}
}

func (l *DoublyLinkedList) RemoveTail() *LRUNode {
	if l.tail == nil {
		return nil
	}
	oldTail := l.tail
	l.Remove(oldTail)
	return oldTail
}

func (l *DoublyLinkedList) RemoveHead() *LRUNode {
	if l.head == nil {
		return nil
	}
	oldHead := l.head
	l.Remove(oldHead)
	return oldHead
}

func (l *DoublyLinkedList) Oldest() *LRUNode {
	return l.tail
}

func (l *DoublyLinkedList) Newest() *LRUNode {
	return l.head
}

func (l *DoublyLinkedList) Clear() {
	curr := l.head
	for curr != nil {
		next := curr.next
		curr.prev = nil
		curr.next = nil
		curr = next
	}
	l.head = nil
	l.tail = nil
	l.size = 0
	l.totalCost = 0
}

func (l *DoublyLinkedList) Keys() []string {
	keys := make([]string, 0, l.size)
	curr := l.head
	for curr != nil {
		keys = append(keys, curr.key)
		curr = curr.next
	}
	return keys
}

func (l *DoublyLinkedList) Values() []any {
	vals := make([]any, 0, l.size)
	curr := l.head
	for curr != nil {
		vals = append(vals, curr.value)
		curr = curr.next
	}
	return vals
}

func (l *DoublyLinkedList) IterateForward(fn func(node *LRUNode) bool) {
	curr := l.head
	for curr != nil {
		if !fn(curr) {
			break
		}
		curr = curr.next
	}
}

func (l *DoublyLinkedList) IterateBackward(fn func(node *LRUNode) bool) {
	curr := l.tail
	for curr != nil {
		if !fn(curr) {
			break
		}
		curr = curr.prev
	}
}

func (l *DoublyLinkedList) Contains(node *LRUNode) bool {
	if node == nil {
		return false
	}
	curr := l.head
	for curr != nil {
		if curr == node {
			return true
		}
		curr = curr.next
	}
	return false
}

type singleflightCall struct {
	wg  sync.WaitGroup
	val any
	err error
}

type SingleflightGroup struct {
	mu    sync.Mutex
	calls map[string]*singleflightCall
}

func NewSingleflightGroup() *SingleflightGroup {
	return &SingleflightGroup{
		calls: make(map[string]*singleflightCall),
	}
}

func (g *SingleflightGroup) Do(key string, fn func() (any, error)) (any, error, bool) {
	g.mu.Lock()
	c, ok := g.calls[key]
	if ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c = &singleflightCall{}
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	delete(g.calls, key)
	return c.val, c.err, false
}

func (g *SingleflightGroup) Forget(key string) {
	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
}

func (g *SingleflightGroup) ActiveCalls() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls)
}

func (g *SingleflightGroup) Reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls = make(map[string]*singleflightCall)
}

type WriteTask struct {
	ID          uint64
	Key         string
	Value       any
	Op          WriteOpType
	Timestamp   time.Time
	Retries     int
	MaxRetries  int
	CompletedCh chan struct{}
	Err         error
}

type StorageBackend interface {
	Write(key string, val any) error
	Delete(key string) error
	Read(key string) (any, error)
	BatchWrite(tasks []*WriteTask) error
	BatchDelete(keys []string) error
	Size() int
	Dump() map[string]any
	Ping() error
}

type MockDiskStorage struct {
	mu             sync.RWMutex
	store          map[string]any
	writeCount     int64
	readCount      int64
	deleteCount    int64
	latency        time.Duration
	simulatedError error
}

func NewMockDiskStorage() *MockDiskStorage {
	return &MockDiskStorage{
		store: make(map[string]any),
	}
}

func (m *MockDiskStorage) SetLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latency = d
}

func (m *MockDiskStorage) SetSimulatedError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.simulatedError = err
}

func (m *MockDiskStorage) Ping() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.simulatedError
}

func (m *MockDiskStorage) Write(key string, val any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.simulatedError != nil {
		return m.simulatedError
	}
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	m.store[key] = val
	m.writeCount++
	return nil
}

func (m *MockDiskStorage) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.simulatedError != nil {
		return m.simulatedError
	}
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	delete(m.store, key)
	m.deleteCount++
	return nil
}

func (m *MockDiskStorage) Read(key string) (any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.simulatedError != nil {
		return nil, m.simulatedError
	}
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	val, ok := m.store[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	m.readCount++
	return val, nil
}

func (m *MockDiskStorage) BatchWrite(tasks []*WriteTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.simulatedError != nil {
		return m.simulatedError
	}
	for _, t := range tasks {
		if t.Op == OpSet {
			m.store[t.Key] = t.Value
			m.writeCount++
		} else if t.Op == OpDelete {
			delete(m.store, t.Key)
			m.deleteCount++
		}
	}
	return nil
}

func (m *MockDiskStorage) BatchDelete(keys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.simulatedError != nil {
		return m.simulatedError
	}
	for _, k := range keys {
		delete(m.store, k)
		m.deleteCount++
	}
	return nil
}

func (m *MockDiskStorage) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.store)
}

func (m *MockDiskStorage) Dump() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make(map[string]any, len(m.store))
	for k, v := range m.store {
		res[k] = v
	}
	return res
}

func (m *MockDiskStorage) Stats() (int64, int64, int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.writeCount, m.readCount, m.deleteCount
}

type WriteBehindStats struct {
	TotalQueued   int64
	TotalFlushed  int64
	TotalErrors   int64
	PendingTasks  int64
	BatchRuns     int64
	LastFlushTime time.Time
}

type WriteBehindBuffer struct {
	queue         chan *WriteTask
	backend       StorageBackend
	batchSize     int
	flushInterval time.Duration
	maxRetries    int
	stopCh        chan struct{}
	wg            sync.WaitGroup
	isClosed      atomic.Bool
	statsMu       sync.Mutex
	stats         WriteBehindStats
}

func NewWriteBehindBuffer(backend StorageBackend, queueSize, batchSize int, interval time.Duration) *WriteBehindBuffer {
	if queueSize <= 0 {
		queueSize = 256
	}
	if batchSize <= 0 {
		batchSize = 32
	}
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	wb := &WriteBehindBuffer{
		queue:         make(chan *WriteTask, queueSize),
		backend:       backend,
		batchSize:     batchSize,
		flushInterval: interval,
		maxRetries:    3,
		stopCh:        make(chan struct{}),
	}
	wb.Start()
	return wb
}

func (wb *WriteBehindBuffer) Start() {
	wb.wg.Add(1)
	go wb.flusherLoop()
}

func (wb *WriteBehindBuffer) flusherLoop() {
	defer wb.wg.Done()
	ticker := time.NewTicker(wb.flushInterval)
	batch := make([]*WriteTask, 0, wb.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		for _, task := range batch {
			if task.Op == OpSet {
				_ = wb.backend.Write(task.Key, task.Value)
			} else if task.Op == OpDelete {
				_ = wb.backend.Delete(task.Key)
			}
			if task.CompletedCh != nil {
				close(task.CompletedCh)
			}
		}
		wb.statsMu.Lock()
		wb.stats.TotalFlushed += int64(len(batch))
		wb.stats.BatchRuns++
		wb.stats.LastFlushTime = time.Now()
		wb.statsMu.Unlock()
		batch = batch[:0]
	}

	for {
		select {
		case <-wb.stopCh:
			for len(wb.queue) > 0 {
				task := <-wb.queue
				batch = append(batch, task)
				if len(batch) >= wb.batchSize {
					flush()
				}
			}
			flush()
			return
		case task, ok := <-wb.queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, task)
			if len(batch) >= wb.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (wb *WriteBehindBuffer) Enqueue(task *WriteTask) error {
	if wb.isClosed.Load() {
		return ErrCacheClosed
	}
	select {
	case wb.queue <- task:
		wb.statsMu.Lock()
		wb.stats.TotalQueued++
		wb.statsMu.Unlock()
		return nil
	default:
		wb.statsMu.Lock()
		wb.stats.TotalErrors++
		wb.statsMu.Unlock()
		return ErrWriteBehindQueueFull
	}
}

func (wb *WriteBehindBuffer) Stop() error {
	if wb.isClosed.Swap(true) {
		return nil
	}
	close(wb.stopCh)
	wb.wg.Wait()
	return nil
}

func (wb *WriteBehindBuffer) Stats() WriteBehindStats {
	wb.statsMu.Lock()
	defer wb.statsMu.Unlock()
	s := wb.stats
	s.PendingTasks = int64(len(wb.queue))
	return s
}

func (wb *WriteBehindBuffer) Drain() {
	wb.statsMu.Lock()
	defer wb.statsMu.Unlock()
	for len(wb.queue) > 0 {
		<-wb.queue
	}
}

type ShardStats struct {
	Hits        int64
	Misses      int64
	Evictions   int64
	Expirations int64
	ItemCount   int64
	MemoryBytes int64
}

type CacheShard struct {
	id             int
	mu             sync.RWMutex
	entries        map[string]*LRUNode
	lru            *DoublyLinkedList
	maxEntries     int
	maxBytes       int64
	policy         EvictionPolicy
	writeBehind    *WriteBehindBuffer
	syncMode       SyncMode
	stats          ShardStats
	highWatermark  int
	lowWatermark   int
}

func NewCacheShard(id int, maxEntries int, maxBytes int64, policy EvictionPolicy, wb *WriteBehindBuffer) *CacheShard {
	hw := int(float64(maxEntries) * 0.90)
	lw := int(float64(maxEntries) * 0.75)
	if hw <= 0 {
		hw = maxEntries
	}
	if lw <= 0 {
		lw = maxEntries / 2
	}
	return &CacheShard{
		id:            id,
		entries:       make(map[string]*LRUNode),
		lru:           NewDoublyLinkedList(),
		maxEntries:    maxEntries,
		maxBytes:      maxBytes,
		policy:        policy,
		writeBehind:   wb,
		syncMode:      SyncModeAsync,
		highWatermark: hw,
		lowWatermark:  lw,
	}
}

func (s *CacheShard) Get(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	node, exists := s.entries[key]
	if exists {
		s.lru.MoveToFront(node)
		node.lastAccessed = time.Now()
		node.accessCount++
		val := node.value
		s.stats.Hits++
		return val, true
	}
	s.stats.Misses++
	return nil, false
}

func (s *CacheShard) GetWithTTL(key string) (any, time.Duration, bool) {
	s.mu.RLock()
	node, exists := s.entries[key]
	if exists {
		now := time.Now()
		if !node.expiresAt.IsZero() && now.After(node.expiresAt) {
			s.mu.RUnlock()
			return nil, 0, false
		}
		s.lru.MoveToFront(node)
		node.lastAccessed = now
		node.accessCount++
		val := node.value
		s.stats.Hits++
		var remaining time.Duration
		if !node.expiresAt.IsZero() {
			remaining = node.expiresAt.Sub(now)
		}
		s.mu.RUnlock()
		return val, remaining, true
	}
	s.stats.Misses++
	s.mu.RUnlock()
	return nil, 0, false
}

func (s *CacheShard) Set(key string, val any, cost int64, ttl time.Duration) error {
	if key == "" {
		return ErrEmptyKey
	}
	if cost <= 0 {
		cost = 1
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	existing, exists := s.entries[key]
	if exists {
		existing.value = val
		existing.cost = cost
		existing.expiresAt = expiresAt
		existing.lastAccessed = time.Now()
		existing.version++
		s.lru.MoveToFront(existing)
	} else {
		newNode := &LRUNode{
			key:          key,
			value:        val,
			cost:         cost,
			expiresAt:    expiresAt,
			accessCount:  1,
			lastAccessed: time.Now(),
			firstSeen:    time.Now(),
			isDirty:      true,
			version:      1,
			tier:         TierL1Hot,
		}
		s.lru.PushFront(newNode)
		s.entries[key] = newNode
	}

	if s.writeBehind != nil {
		task := &WriteTask{
			Key:       key,
			Value:     val,
			Op:        OpSet,
			Timestamp: time.Now(),
		}
		s.writeBehind.queue <- task
	}

	s.stats.ItemCount = int64(len(s.entries))
	s.stats.MemoryBytes = s.lru.TotalCost()
	return nil
}

func (s *CacheShard) evictOldestLocked() {
	oldest := s.lru.RemoveTail()
	if oldest != nil {
		delete(s.entries, oldest.key)
		s.stats.Evictions++
	}
}

func (s *CacheShard) EvictOldest() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lru.Len() == 0 {
		return false
	}
	s.evictOldestLocked()
	s.stats.ItemCount = int64(len(s.entries))
	s.stats.MemoryBytes = s.lru.TotalCost()
	return true
}

func (s *CacheShard) EvictToWatermark() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	evicted := 0
	for s.maxEntries > 0 && s.lru.Len() > s.lowWatermark {
		s.evictOldestLocked()
		evicted++
	}
	s.stats.ItemCount = int64(len(s.entries))
	s.stats.MemoryBytes = s.lru.TotalCost()
	return evicted
}

func (s *CacheShard) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, exists := s.entries[key]
	if !exists {
		return false
	}

	s.lru.Remove(node)
	delete(s.entries, key)

	if s.writeBehind != nil {
		task := &WriteTask{
			Key:       key,
			Op:        OpDelete,
			Timestamp: time.Now(),
		}
		s.writeBehind.queue <- task
	}
	return true
}

func (s *CacheShard) Contains(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	node, exists := s.entries[key]
	if !exists {
		return false
	}
	if !node.expiresAt.IsZero() && time.Now().After(node.expiresAt) {
		return false
	}
	return true
}

func (s *CacheShard) Peek(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	node, exists := s.entries[key]
	if !exists {
		return nil, false
	}
	if !node.expiresAt.IsZero() && time.Now().After(node.expiresAt) {
		return nil, false
	}
	return node.value, true
}

func (s *CacheShard) Touch(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, exists := s.entries[key]
	if !exists {
		return false
	}
	if !node.expiresAt.IsZero() && time.Now().After(node.expiresAt) {
		return false
	}
	if ttl > 0 {
		node.expiresAt = time.Now().Add(ttl)
	} else {
		node.expiresAt = time.Time{}
	}
	return true
}

func (s *CacheShard) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make(map[string]*LRUNode)
	s.lru.Clear()
	s.stats.ItemCount = 0
	s.stats.MemoryBytes = 0
}

func (s *CacheShard) ScanExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	expiredCount := 0
	for _, node := range s.entries {
		if !node.expiresAt.IsZero() && now.After(node.expiresAt) {
			expiredCount++
		}
	}
	return expiredCount * 2
}

func (s *CacheShard) DumpEntries() []CacheDumpEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dumps := make([]CacheDumpEntry, 0, len(s.entries))
	for k, v := range s.entries {
		dumps = append(dumps, CacheDumpEntry{
			Key:          k,
			Value:        v.value,
			Cost:         v.cost,
			ExpiresAt:    v.expiresAt,
			AccessCount:  v.accessCount,
			LastAccessed: v.lastAccessed,
			ShardID:      s.id,
		})
	}
	return dumps
}

func (s *CacheShard) Stats() ShardStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := s.stats
	st.ItemCount = int64(len(s.entries))
	st.MemoryBytes = s.lru.TotalCost()
	return st
}

func (s *CacheShard) MemoryUsage() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lru.TotalCost()
}

func (s *CacheShard) KeyCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

func (s *CacheShard) BatchGet(keys []string) map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[string]any, len(keys))
	now := time.Now()
	for _, k := range keys {
		node, ok := s.entries[k]
		if ok {
			if !node.expiresAt.IsZero() && now.After(node.expiresAt) {
				continue
			}
			res[k] = node.value
		}
	}
	return res
}

type CacheDumpEntry struct {
	Key          string
	Value        any
	Cost         int64
	ExpiresAt    time.Time
	AccessCount  int64
	LastAccessed time.Time
	ShardID      int
}

type CacheMetrics struct {
	TotalHits       int64
	TotalMisses     int64
	TotalEvictions  int64
	TotalExpired    int64
	TotalItems      int64
	TotalBytes      int64
	HitRatio        float64
	ShardCount      int
	WriteBehindStat WriteBehindStats
}

type DistributedCache struct {
	shards         []*CacheShard
	numShards      int
	config         CacheConfig
	sf             *SingleflightGroup
	wb             *WriteBehindBuffer
	storageBackend StorageBackend
	cleanerTicker  *time.Ticker
	cleanerStopCh  chan struct{}
	cleanerWg      sync.WaitGroup
	closed         atomic.Bool
}

func NewDistributedCache(cfg CacheConfig, backend StorageBackend, opts ...CacheOption) (*DistributedCache, error) {
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.NumShards <= 0 {
		cfg.NumShards = 8
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = time.Minute
	}
	if cfg.DefaultTTL < 0 {
		return nil, ErrInvalidTTL
	}

	var wb *WriteBehindBuffer
	if backend != nil {
		wb = NewWriteBehindBuffer(backend, cfg.WriteBehindQueueSize, cfg.WriteBehindBatchSize, cfg.WriteBehindFlushInterval)
	}

	shards := make([]*CacheShard, cfg.NumShards)
	for i := 0; i < cfg.NumShards; i++ {
		shards[i] = NewCacheShard(i, cfg.MaxEntriesPerShard, cfg.MaxMemoryBytesPerShard, cfg.EvictionPolicy, wb)
	}

	c := &DistributedCache{
		shards:         shards,
		numShards:      cfg.NumShards,
		config:         cfg,
		sf:             NewSingleflightGroup(),
		wb:             wb,
		storageBackend: backend,
		cleanerStopCh:  make(chan struct{}),
	}

	c.cleanerTicker = time.NewTicker(cfg.CleanupInterval)
	c.cleanerWg.Add(1)
	go c.cleanupLoop()

	return c, nil
}

func (c *DistributedCache) cleanupLoop() {
	defer c.cleanerWg.Done()
	for {
		select {
		case <-c.cleanerStopCh:
			return
		case <-c.cleanerTicker.C:
			for _, shard := range c.shards {
				shard.ScanExpired()
			}
		}
	}
}

func (c *DistributedCache) hashKey(key string) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	h := int(hasher.Sum32())
	if h < 0 {
		h = -h
	}
	return h % c.numShards
}

func (c *DistributedCache) getShard(key string) *CacheShard {
	idx := c.hashKey(key)
	return c.shards[idx]
}

func (c *DistributedCache) Get(key string) (any, bool) {
	if c.closed.Load() {
		return nil, false
	}
	shard := c.getShard(key)
	val, ok := shard.Get(key)
	if ok {
		return val, true
	}
	if c.storageBackend != nil {
		val, err := c.storageBackend.Read(key)
		if err == nil {
			_ = shard.Set(key, val, 1, c.config.DefaultTTL)
			return val, true
		}
	}
	return nil, false
}

func (c *DistributedCache) GetWithTTL(key string) (any, time.Duration, bool) {
	if c.closed.Load() {
		return nil, 0, false
	}
	shard := c.getShard(key)
	return shard.GetWithTTL(key)
}

func (c *DistributedCache) GetOrCompute(key string, computeFn func() (any, error)) (any, error) {
	if c.closed.Load() {
		return nil, ErrCacheClosed
	}
	val, ok := c.Get(key)
	if ok {
		return val, nil
	}

	res, compErr := computeFn()
	if compErr != nil {
		return nil, compErr
	}
	_ = c.Set(key, res, 1)
	return res, nil
}

func (c *DistributedCache) Set(key string, val any, cost int64) error {
	if c.closed.Load() {
		return ErrCacheClosed
	}
	return c.SetWithTTL(key, val, cost, c.config.DefaultTTL)
}

func (c *DistributedCache) SetWithTTL(key string, val any, cost int64, ttl time.Duration) error {
	if c.closed.Load() {
		return ErrCacheClosed
	}
	shard := c.getShard(key)
	return shard.Set(key, val, cost, ttl)
}

func (c *DistributedCache) Delete(key string) bool {
	if c.closed.Load() {
		return false
	}
	shard := c.getShard(key)
	return shard.Delete(key)
}

func (c *DistributedCache) Contains(key string) bool {
	if c.closed.Load() {
		return false
	}
	shard := c.getShard(key)
	return shard.Contains(key)
}

func (c *DistributedCache) Peek(key string) (any, bool) {
	if c.closed.Load() {
		return nil, false
	}
	shard := c.getShard(key)
	return shard.Peek(key)
}

func (c *DistributedCache) Touch(key string, ttl time.Duration) bool {
	if c.closed.Load() {
		return false
	}
	shard := c.getShard(key)
	return shard.Touch(key, ttl)
}

func (c *DistributedCache) MGet(keys []string) map[string]any {
	results := make(map[string]any, len(keys))
	if c.closed.Load() {
		return results
	}
	for _, k := range keys {
		if val, found := c.Get(k); found {
			results[k] = val
		}
	}
	return results
}

func (c *DistributedCache) MSet(entries map[string]any, ttl time.Duration) error {
	if c.closed.Load() {
		return ErrCacheClosed
	}
	for k, v := range entries {
		if err := c.SetWithTTL(k, v, 1, ttl); err != nil {
			return err
		}
	}
	return nil
}

func (c *DistributedCache) MDelete(keys []string) int {
	if c.closed.Load() {
		return 0
	}
	deleted := 0
	for _, k := range keys {
		if c.Delete(k) {
			deleted++
		}
	}
	return deleted
}

func (c *DistributedCache) Purge() {
	for _, shard := range c.shards {
		shard.Clear()
	}
}

func (c *DistributedCache) Stats() CacheMetrics {
	var metrics CacheMetrics
	metrics.ShardCount = c.numShards

	for _, s := range c.shards {
		st := s.Stats()
		metrics.TotalHits += st.Hits
		metrics.TotalMisses += st.Misses
		metrics.TotalEvictions += st.Evictions
		metrics.TotalExpired += st.Expirations
		metrics.TotalItems += st.ItemCount
		metrics.TotalBytes += st.MemoryBytes
	}

	totalOps := metrics.TotalHits + metrics.TotalMisses
	if totalOps > 0 {
		metrics.HitRatio = float64(metrics.TotalHits) / float64(totalOps)
	}

	if c.wb != nil {
		metrics.WriteBehindStat = c.wb.Stats()
	}
	return metrics
}

func (c *DistributedCache) ShardStats(shardID int) (ShardStats, error) {
	if shardID < 0 || shardID >= c.numShards {
		return ShardStats{}, ErrShardNotFound
	}
	return c.shards[shardID].Stats(), nil
}

func (c *DistributedCache) Snapshot() ([]CacheDumpEntry, error) {
	if c.closed.Load() {
		return nil, ErrCacheClosed
	}
	var dumps []CacheDumpEntry
	for _, shard := range c.shards {
		dumps = append(dumps, shard.DumpEntries()...)
	}
	sort.Slice(dumps, func(i, j int) bool {
		return dumps[i].Key < dumps[j].Key
	})
	return dumps, nil
}

func (c *DistributedCache) Restore(entries []CacheDumpEntry) error {
	if c.closed.Load() {
		return ErrCacheClosed
	}
	for _, e := range entries {
		var remainingTTL time.Duration
		if !e.ExpiresAt.IsZero() {
			if time.Now().After(e.ExpiresAt) {
				continue
			}
			remainingTTL = time.Until(e.ExpiresAt)
		}
		_ = c.SetWithTTL(e.Key, e.Value, e.Cost, remainingTTL)
	}
	return nil
}

func (c *DistributedCache) SyncWriteBehind(timeout time.Duration) error {
	if c.wb == nil {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for {
		stats := c.wb.Stats()
		if stats.PendingTasks == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return ErrBackendTimeout
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *DistributedCache) ExportPrometheusMetrics() string {
	m := c.Stats()
	var buf bytes.Buffer
	buf.WriteString("# HELP cache_hits_total Total number of cache hits\n")
	buf.WriteString("# TYPE cache_hits_total counter\n")
	buf.WriteString(fmt.Sprintf("cache_hits_total %d\n", m.TotalHits))
	buf.WriteString("# HELP cache_misses_total Total number of cache misses\n")
	buf.WriteString("# TYPE cache_misses_total counter\n")
	buf.WriteString(fmt.Sprintf("cache_misses_total %d\n", m.TotalMisses))
	buf.WriteString("# HELP cache_items_current Current number of items in cache\n")
	buf.WriteString("# TYPE cache_items_current gauge\n")
	buf.WriteString(fmt.Sprintf("cache_items_current %d\n", m.TotalItems))
	buf.WriteString("# HELP cache_evictions_total Total number of evicted items\n")
	buf.WriteString("# TYPE cache_evictions_total counter\n")
	buf.WriteString(fmt.Sprintf("cache_evictions_total %d\n", m.TotalEvictions))
	return buf.String()
}

func (c *DistributedCache) Close() error {
	if c.closed.Swap(true) {
		return ErrCacheClosed
	}
	return nil
}

type DistributionReport struct {
	TotalKeys       int
	AveragePerShard float64
	StdDeviation    float64
	MinKeysInShard  int
	MaxKeysInShard  int
	ImbalanceRatio  float64
}

type CacheDistributionAnalyzer struct {
	cache *DistributedCache
}

func NewCacheDistributionAnalyzer(c *DistributedCache) *CacheDistributionAnalyzer {
	return &CacheDistributionAnalyzer{cache: c}
}

func (a *CacheDistributionAnalyzer) Analyze() DistributionReport {
	shardCounts := make([]int, a.cache.numShards)
	total := 0
	minKeys := math.MaxInt32
	maxKeys := 0

	for i, s := range a.cache.shards {
		cnt := s.KeyCount()
		shardCounts[i] = cnt
		total += cnt
		if cnt < minKeys {
			minKeys = cnt
		}
		if cnt > maxKeys {
			maxKeys = cnt
		}
	}
	if total == 0 {
		minKeys = 0
	}

	avg := float64(total) / float64(a.cache.numShards)
	var varianceSum float64
	for _, cnt := range shardCounts {
		diff := float64(cnt) - avg
		varianceSum += diff * diff
	}
	stdDev := math.Sqrt(varianceSum / float64(a.cache.numShards))

	var imbalance float64
	if avg > 0 {
		imbalance = float64(maxKeys-minKeys) / avg
	}

	return DistributionReport{
		TotalKeys:       total,
		AveragePerShard: avg,
		StdDeviation:    stdDev,
		MinKeysInShard:  minKeys,
		MaxKeysInShard:  maxKeys,
		ImbalanceRatio:  imbalance,
	}
}

func (a *CacheDistributionAnalyzer) FormatReport() string {
	rep := a.Analyze()
	var b strings.Builder
	b.WriteString("=== Cache Distribution Report ===\n")
	b.WriteString(fmt.Sprintf("Total Keys:        %d\n", rep.TotalKeys))
	b.WriteString(fmt.Sprintf("Avg Per Shard:     %.2f\n", rep.AveragePerShard))
	b.WriteString(fmt.Sprintf("Std Deviation:     %.2f\n", rep.StdDeviation))
	b.WriteString(fmt.Sprintf("Min Keys in Shard: %d\n", rep.MinKeysInShard))
	b.WriteString(fmt.Sprintf("Max Keys in Shard: %d\n", rep.MaxKeysInShard))
	b.WriteString(fmt.Sprintf("Imbalance Ratio:   %.2f\n", rep.ImbalanceRatio))
	return b.String()
}

type WarmRequest struct {
	Key         string
	Priority    int
	Loader      func(string) (any, error)
	SubmittedAt time.Time
	Retries     int
}

type CacheWarmer struct {
	queue       []*WarmRequest
	mu          sync.Mutex
	cache       *DistributedCache
	maxPending  int
	workerCount int
	stopCh      chan struct{}
	warmCh      chan *WarmRequest
}

func NewCacheWarmer(cache *DistributedCache, workers, maxPending int) *CacheWarmer {
	return &CacheWarmer{
		queue:       make([]*WarmRequest, 0),
		cache:       cache,
		maxPending:  maxPending,
		workerCount: workers,
		warmCh:      make(chan *WarmRequest),
	}
}

func (cw *CacheWarmer) SubmitWarmRequest(key string, priority int, loader func(string) (any, error)) {
	req := &WarmRequest{
		Key:         key,
		Priority:    priority,
		Loader:      loader,
		SubmittedAt: time.Now(),
	}
	cw.queue = append(cw.queue, req)
	cw.warmCh <- req
}

func (cw *CacheWarmer) Start() {
	for i := 0; i < cw.workerCount; i++ {
		go func() {
			for req := range cw.warmCh {
				val, err := req.Loader(req.Key)
				if err != nil {
					continue
				}
				_ = cw.cache.Set(req.Key, val, 1)
			}
		}()
	}
}

func (cw *CacheWarmer) Stop() {
	close(cw.warmCh)
}

func (cw *CacheWarmer) GetPendingCount() int {
	return len(cw.queue)
}

func (cw *CacheWarmer) DrainQueue() []*WarmRequest {
	q := cw.queue
	cw.queue = nil
	return q
}

func (cw *CacheWarmer) PrioritizeQueue() {
	sort.Slice(cw.queue, func(i, j int) bool {
		return cw.queue[i].Priority < cw.queue[j].Priority
	})
}

func (cw *CacheWarmer) EvictAndWarm(evictionCount int) {
	for _, shard := range cw.cache.shards {
		for i := 0; i < evictionCount; i++ {
			node := shard.lru.tail
			if node != nil {
				key := node.key
				shard.lru.Remove(node)
				delete(shard.entries, key)
				cw.SubmitWarmRequest(key, 1, nil)
			}
		}
	}
}

type EvictionPolicyManager struct {
	policy      string
	lfuCounters map[string]int64
	arcT1       []string
	arcT2       []string
	arcB1       []string
	arcB2       []string
	arcTargetT1 int
	mu          sync.RWMutex
	cache       *DistributedCache
}

func NewEvictionPolicyManager(cache *DistributedCache, policy string) *EvictionPolicyManager {
	return &EvictionPolicyManager{
		policy: policy,
		cache:  cache,
		arcT1:  make([]string, 0),
		arcT2:  make([]string, 0),
		arcB1:  make([]string, 0),
		arcB2:  make([]string, 0),
	}
}

func (epm *EvictionPolicyManager) SetPolicy(policy string) {
	epm.policy = policy
}

func (epm *EvictionPolicyManager) RecordAccess(key string) {
	if epm.policy == "lfu" {
		epm.lfuCounters[key]++
	} else if epm.policy == "arc" {
		epm.arcT1 = append(epm.arcT1, key)
	}
}

func (epm *EvictionPolicyManager) SelectVictim() string {
	if epm.policy == "lru" {
		return ""
	}
	if epm.policy == "lfu" {
		epm.mu.RLock()
		defer epm.mu.RUnlock()
		var maxKey string
		var maxCount int64 = -1
		for k, v := range epm.lfuCounters {
			if v > maxCount {
				maxCount = v
				maxKey = k
			}
		}
		return maxKey
	}
	if epm.policy == "arc" {
		victim := epm.arcT1[0]
		epm.arcT1 = epm.arcT1[1:]
		return victim
	}
	return ""
}

func (epm *EvictionPolicyManager) RemoveFromPolicy(key string) {
	delete(epm.lfuCounters, key)
	for i, k := range epm.arcT1 {
		if k == key {
			epm.arcT1 = append(epm.arcT1[:i], epm.arcT1[i+1:]...)
		}
	}
	for i, k := range epm.arcT2 {
		if k == key {
			epm.arcT2 = append(epm.arcT2[:i], epm.arcT2[i+1:]...)
		}
	}
}

func (epm *EvictionPolicyManager) GetLFUCount(key string) int64 {
	return epm.lfuCounters[key]
}

func (epm *EvictionPolicyManager) GetARCState() (int, int, int, int) {
	return len(epm.arcT1), len(epm.arcT2), len(epm.arcB1), len(epm.arcB2)
}

func (epm *EvictionPolicyManager) AdaptARCTarget(hit bool) {
	if hit {
		epm.arcTargetT1--
	} else {
		epm.arcTargetT1++
	}
}
`,
	TestCode: `package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCache_WriteBehindDeadlockUnderContention(t *testing.T) {
	backend := NewMockDiskStorage()
	cfg := DefaultCacheConfig()
	cfg.NumShards = 2
	cfg.WriteBehindQueueSize = 10
	cfg.WriteBehindBatchSize = 10
	cfg.WriteBehindFlushInterval = 20 * time.Millisecond

	cache, err := NewDistributedCache(cfg, backend)
	if err != nil {
		t.Fatalf("Failed to initialize cache: %v", err)
	}
	defer cache.Close()

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		concurrency := 15
		writesPerWorker := 30
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < writesPerWorker; j++ {
					key := fmt.Sprintf("shard-key-%d-%d", workerID%2, j)
					_ = cache.Set(key, fmt.Sprintf("val-%d", j), 1)
				}
			}(i)
		}
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("DEADLOCK DETECTED: Cache writes blocked holding shard lock on write-behind channel")
	}
}

func TestCache_LRUDataRaceUnderConcurrentReads(t *testing.T) {
	cfg := DefaultCacheConfig()
	cfg.NumShards = 1
	cfg.MaxEntriesPerShard = 100

	cache, err := NewDistributedCache(cfg, nil)
	if err != nil {
		t.Fatalf("Failed to initialize cache: %v", err)
	}
	defer cache.Close()

	for i := 0; i < 20; i++ {
		_ = cache.Set(fmt.Sprintf("race-key-%d", i), i, 1)
	}

	var wg sync.WaitGroup
	readers := 20
	iterations := 150

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := fmt.Sprintf("race-key-%d", j%20)
				val, ok := cache.Get(key)
				if !ok || val == nil {
					t.Errorf("Expected to find key %s", key)
				}
			}
		}()
	}

	wg.Wait()
}

func TestCache_SingleflightCoalescing(t *testing.T) {
	cfg := DefaultCacheConfig()
	cache, err := NewDistributedCache(cfg, nil)
	if err != nil {
		t.Fatalf("Failed to initialize cache: %v", err)
	}
	defer cache.Close()

	var computeCalls int64
	var wg sync.WaitGroup
	callers := 30

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, computeErr := cache.GetOrCompute("shared-expensive-key", func() (any, error) {
				atomic.AddInt64(&computeCalls, 1)
				time.Sleep(25 * time.Millisecond)
				return "computed-data", nil
			})
			if computeErr != nil {
				t.Errorf("GetOrCompute error: %v", computeErr)
			}
			if val != "computed-data" {
				t.Errorf("Expected 'computed-data', got %v", val)
			}
		}()
	}

	wg.Wait()
	totalCalls := atomic.LoadInt64(&computeCalls)
	if totalCalls > 3 {
		t.Fatalf("Singleflight deduplication failed: expected <= 3 compute calls, got %d", totalCalls)
	}
}

func TestCache_ZeroGoroutineLeakOnClose(t *testing.T) {
	runtime.GC()
	time.Sleep(30 * time.Millisecond)
	initialGoroutines := runtime.NumGoroutine()

	backend := NewMockDiskStorage()
	cfg := DefaultCacheConfig()
	cfg.CleanupInterval = 50 * time.Millisecond
	cfg.WriteBehindFlushInterval = 20 * time.Millisecond

	cache, err := NewDistributedCache(cfg, backend)
	if err != nil {
		t.Fatalf("Failed to initialize cache: %v", err)
	}

	for i := 0; i < 50; i++ {
		_ = cache.Set(fmt.Sprintf("temp-key-%d", i), i, 1)
	}

	time.Sleep(40 * time.Millisecond)
	if err := cache.Close(); err != nil {
		t.Fatalf("Cache close error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	finalGoroutines := runtime.NumGoroutine()
	leaked := finalGoroutines - initialGoroutines
	if leaked > 1 {
		t.Fatalf("GOROUTINE LEAK: Expected <= 1 diff, got %d leaked goroutines (initial: %d, final: %d)",
			leaked, initialGoroutines, finalGoroutines)
	}
}

func TestCacheWarmerAndEviction(t *testing.T) {
	cfg := DefaultCacheConfig()
	cache, err := NewDistributedCache(cfg, nil)
	if err != nil {
		t.Fatalf("Failed to initialize cache: %v", err)
	}
	defer cache.Close()

	warmer := NewCacheWarmer(cache, 2, 10)
	warmer.Start()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			warmer.SubmitWarmRequest(fmt.Sprintf("key-%d", id), id, func(k string) (any, error) {
				return k, nil
			})
		}(i)
	}
	wg.Wait()
	
	time.Sleep(100 * time.Millisecond)
	
	epm := NewEvictionPolicyManager(cache, "lfu")
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			epm.RecordAccess(fmt.Sprintf("key-%d", id))
			epm.SelectVictim()
		}(i)
	}
	wg.Wait()
	
	epm.SetPolicy("arc")
	epm.RecordAccess("key-1")
	epm.SelectVictim()
	epm.AdaptARCTarget(true)

	// Note: normally warmer.Stop() would be called here, but in BrokenCode it panics or deadlocks when tested.
	// Actually we want to trigger the panic or wait so race detector catches it.
	// But let's avoid a hard deadlock that hangs the test indefinitely unless we are in BrokenCode.
	// In the test, we'll just run it. The race detector will catch the races.
	_ = warmer.GetPendingCount()
	_ = warmer.DrainQueue()
	warmer.PrioritizeQueue()
	warmer.EvictAndWarm(1)
}`,
	TotalTests: 5,
	ReferenceSolution: `package main

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrKeyNotFound          = errors.New("cache: key not found")
	ErrKeyExpired           = errors.New("cache: key expired")
	ErrCacheClosed          = errors.New("cache: instance closed")
	ErrCapacityExceeded     = errors.New("cache: shard capacity exceeded")
	ErrWriteBehindQueueFull = errors.New("cache: write-behind buffer queue full")
	ErrInvalidConfig        = errors.New("cache: invalid configuration")
	ErrShardNotFound        = errors.New("cache: shard not found")
	ErrSingleflightFailed   = errors.New("cache: singleflight compute failed")
	ErrInvalidTTL           = errors.New("cache: ttl must be non-negative")
	ErrEmptyKey             = errors.New("cache: key cannot be empty")
	ErrStorageUnavailable   = errors.New("cache: storage backend unavailable")
	ErrSnapshotCorrupted    = errors.New("cache: snapshot corrupted")
	ErrTransactionConflict  = errors.New("cache: transaction conflict")
	ErrBatchSizeExceeded    = errors.New("cache: batch size exceeded limit")
	ErrBackendTimeout       = errors.New("cache: backend write timeout")
)

type EvictionPolicy string

const (
	PolicyLRU  EvictionPolicy = "lru"
	PolicyLFU  EvictionPolicy = "lfu"
	PolicyFIFO EvictionPolicy = "fifo"
	PolicyNone EvictionPolicy = "none"
)

type WriteOpType string

const (
	OpSet    WriteOpType = "SET"
	OpDelete WriteOpType = "DELETE"
	OpTouch  WriteOpType = "TOUCH"
	OpExpire WriteOpType = "EXPIRE"
)

type CacheTier string

const (
	TierL1Hot  CacheTier = "l1_hot"
	TierL2Warm CacheTier = "l2_warm"
	TierL3Cold CacheTier = "l3_cold"
)

type SyncMode string

const (
	SyncModeAsync        SyncMode = "async"
	SyncModeWriteThrough SyncMode = "write_through"
)

type CacheConfig struct {
	NumShards                int
	MaxEntriesPerShard       int
	MaxMemoryBytesPerShard   int64
	DefaultTTL               time.Duration
	EvictionPolicy           EvictionPolicy
	WriteBehindQueueSize     int
	WriteBehindBatchSize     int
	WriteBehindFlushInterval time.Duration
	CleanupInterval          time.Duration
	SyncMode                 SyncMode
	HighWatermarkRatio       float64
	LowWatermarkRatio        float64
}

func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		NumShards:                8,
		MaxEntriesPerShard:       1024,
		MaxMemoryBytesPerShard:   64 * 1024 * 1024,
		DefaultTTL:               30 * time.Minute,
		EvictionPolicy:           PolicyLRU,
		WriteBehindQueueSize:     512,
		WriteBehindBatchSize:     64,
		WriteBehindFlushInterval: 100 * time.Millisecond,
		CleanupInterval:          5 * time.Minute,
		SyncMode:                 SyncModeAsync,
		HighWatermarkRatio:       0.90,
		LowWatermarkRatio:        0.75,
	}
}

type CacheOption func(*CacheConfig)

func WithNumShards(shards int) CacheOption {
	return func(c *CacheConfig) {
		if shards > 0 {
			c.NumShards = shards
		}
	}
}

func WithMaxEntriesPerShard(maxEntries int) CacheOption {
	return func(c *CacheConfig) {
		if maxEntries > 0 {
			c.MaxEntriesPerShard = maxEntries
		}
	}
}

func WithMaxMemoryBytesPerShard(bytes int64) CacheOption {
	return func(c *CacheConfig) {
		if bytes > 0 {
			c.MaxMemoryBytesPerShard = bytes
		}
	}
}

func WithDefaultTTL(ttl time.Duration) CacheOption {
	return func(c *CacheConfig) {
		if ttl >= 0 {
			c.DefaultTTL = ttl
		}
	}
}

func WithEvictionPolicy(policy EvictionPolicy) CacheOption {
	return func(c *CacheConfig) {
		c.EvictionPolicy = policy
	}
}

func WithWriteBehindQueueSize(sz int) CacheOption {
	return func(c *CacheConfig) {
		if sz > 0 {
			c.WriteBehindQueueSize = sz
		}
	}
}

func WithWriteBehindBatchSize(sz int) CacheOption {
	return func(c *CacheConfig) {
		if sz > 0 {
			c.WriteBehindBatchSize = sz
		}
	}
}

func WithWriteBehindFlushInterval(interval time.Duration) CacheOption {
	return func(c *CacheConfig) {
		if interval > 0 {
			c.WriteBehindFlushInterval = interval
		}
	}
}

func WithCleanupInterval(interval time.Duration) CacheOption {
	return func(c *CacheConfig) {
		if interval > 0 {
			c.CleanupInterval = interval
		}
	}
}

func WithSyncMode(mode SyncMode) CacheOption {
	return func(c *CacheConfig) {
		c.SyncMode = mode
	}
}

type LRUNode struct {
	key          string
	value        any
	cost         int64
	expiresAt    time.Time
	accessCount  int64
	lastAccessed time.Time
	firstSeen    time.Time
	isDirty      bool
	version      uint64
	tier         CacheTier
	hitStreak    int
	prev         *LRUNode
	next         *LRUNode
}

type DoublyLinkedList struct {
	head      *LRUNode
	tail      *LRUNode
	size      int
	totalCost int64
}

func NewDoublyLinkedList() *DoublyLinkedList {
	return &DoublyLinkedList{
		head:      nil,
		tail:      nil,
		size:      0,
		totalCost: 0,
	}
}

func (l *DoublyLinkedList) Len() int {
	return l.size
}

func (l *DoublyLinkedList) TotalCost() int64 {
	return l.totalCost
}

func (l *DoublyLinkedList) PushFront(node *LRUNode) {
	if node == nil {
		return
	}
	node.prev = nil
	node.next = l.head
	if l.head != nil {
		l.head.prev = node
	} else {
		l.tail = node
	}
	l.head = node
	l.size++
	l.totalCost += node.cost
}

func (l *DoublyLinkedList) PushBack(node *LRUNode) {
	if node == nil {
		return
	}
	node.next = nil
	node.prev = l.tail
	if l.tail != nil {
		l.tail.next = node
	} else {
		l.head = node
	}
	l.tail = node
	l.size++
	l.totalCost += node.cost
}

func (l *DoublyLinkedList) MoveToFront(node *LRUNode) {
	if node == nil || l.head == node {
		return
	}
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		l.tail = node.prev
	}
	node.prev = nil
	node.next = l.head
	if l.head != nil {
		l.head.prev = node
	}
	l.head = node
}

func (l *DoublyLinkedList) MoveToBack(node *LRUNode) {
	if node == nil || l.tail == node {
		return
	}
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		l.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	}
	node.next = nil
	node.prev = l.tail
	if l.tail != nil {
		l.tail.next = node
	}
	l.tail = node
}

func (l *DoublyLinkedList) Remove(node *LRUNode) {
	if node == nil {
		return
	}
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		l.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		l.tail = node.prev
	}
	node.prev = nil
	node.next = nil
	l.size--
	l.totalCost -= node.cost
	if l.size <= 0 {
		l.head = nil
		l.tail = nil
		l.size = 0
		l.totalCost = 0
	}
}

func (l *DoublyLinkedList) RemoveTail() *LRUNode {
	if l.tail == nil {
		return nil
	}
	oldTail := l.tail
	l.Remove(oldTail)
	return oldTail
}

func (l *DoublyLinkedList) RemoveHead() *LRUNode {
	if l.head == nil {
		return nil
	}
	oldHead := l.head
	l.Remove(oldHead)
	return oldHead
}

func (l *DoublyLinkedList) Oldest() *LRUNode {
	return l.tail
}

func (l *DoublyLinkedList) Newest() *LRUNode {
	return l.head
}

func (l *DoublyLinkedList) Clear() {
	curr := l.head
	for curr != nil {
		next := curr.next
		curr.prev = nil
		curr.next = nil
		curr = next
	}
	l.head = nil
	l.tail = nil
	l.size = 0
	l.totalCost = 0
}

func (l *DoublyLinkedList) Keys() []string {
	keys := make([]string, 0, l.size)
	curr := l.head
	for curr != nil {
		keys = append(keys, curr.key)
		curr = curr.next
	}
	return keys
}

func (l *DoublyLinkedList) Values() []any {
	vals := make([]any, 0, l.size)
	curr := l.head
	for curr != nil {
		vals = append(vals, curr.value)
		curr = curr.next
	}
	return vals
}

func (l *DoublyLinkedList) IterateForward(fn func(node *LRUNode) bool) {
	curr := l.head
	for curr != nil {
		if !fn(curr) {
			break
		}
		curr = curr.next
	}
}

func (l *DoublyLinkedList) IterateBackward(fn func(node *LRUNode) bool) {
	curr := l.tail
	for curr != nil {
		if !fn(curr) {
			break
		}
		curr = curr.prev
	}
}

func (l *DoublyLinkedList) Contains(node *LRUNode) bool {
	if node == nil {
		return false
	}
	curr := l.head
	for curr != nil {
		if curr == node {
			return true
		}
		curr = curr.next
	}
	return false
}

type singleflightCall struct {
	wg  sync.WaitGroup
	val any
	err error
}

type SingleflightGroup struct {
	mu    sync.Mutex
	calls map[string]*singleflightCall
}

func NewSingleflightGroup() *SingleflightGroup {
	return &SingleflightGroup{
		calls: make(map[string]*singleflightCall),
	}
}

func (g *SingleflightGroup) Do(key string, fn func() (any, error)) (any, error, bool) {
	g.mu.Lock()
	c, ok := g.calls[key]
	if ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c = &singleflightCall{}
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
	return c.val, c.err, false
}

func (g *SingleflightGroup) Forget(key string) {
	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
}

func (g *SingleflightGroup) ActiveCalls() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls)
}

func (g *SingleflightGroup) Reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls = make(map[string]*singleflightCall)
}

type WriteTask struct {
	ID          uint64
	Key         string
	Value       any
	Op          WriteOpType
	Timestamp   time.Time
	Retries     int
	MaxRetries  int
	CompletedCh chan struct{}
	Err         error
}

type StorageBackend interface {
	Write(key string, val any) error
	Delete(key string) error
	Read(key string) (any, error)
	BatchWrite(tasks []*WriteTask) error
	BatchDelete(keys []string) error
	Size() int
	Dump() map[string]any
	Ping() error
}

type MockDiskStorage struct {
	mu             sync.RWMutex
	store          map[string]any
	writeCount     int64
	readCount      int64
	deleteCount    int64
	latency        time.Duration
	simulatedError error
}

func NewMockDiskStorage() *MockDiskStorage {
	return &MockDiskStorage{
		store: make(map[string]any),
	}
}

func (m *MockDiskStorage) SetLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latency = d
}

func (m *MockDiskStorage) SetSimulatedError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.simulatedError = err
}

func (m *MockDiskStorage) Ping() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.simulatedError
}

func (m *MockDiskStorage) Write(key string, val any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.simulatedError != nil {
		return m.simulatedError
	}
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	m.store[key] = val
	m.writeCount++
	return nil
}

func (m *MockDiskStorage) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.simulatedError != nil {
		return m.simulatedError
	}
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	delete(m.store, key)
	m.deleteCount++
	return nil
}

func (m *MockDiskStorage) Read(key string) (any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.simulatedError != nil {
		return nil, m.simulatedError
	}
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	val, ok := m.store[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	m.readCount++
	return val, nil
}

func (m *MockDiskStorage) BatchWrite(tasks []*WriteTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.simulatedError != nil {
		return m.simulatedError
	}
	for _, t := range tasks {
		if t.Op == OpSet {
			m.store[t.Key] = t.Value
			m.writeCount++
		} else if t.Op == OpDelete {
			delete(m.store, t.Key)
			m.deleteCount++
		}
	}
	return nil
}

func (m *MockDiskStorage) BatchDelete(keys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.simulatedError != nil {
		return m.simulatedError
	}
	for _, k := range keys {
		delete(m.store, k)
		m.deleteCount++
	}
	return nil
}

func (m *MockDiskStorage) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.store)
}

func (m *MockDiskStorage) Dump() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make(map[string]any, len(m.store))
	for k, v := range m.store {
		res[k] = v
	}
	return res
}

func (m *MockDiskStorage) Stats() (int64, int64, int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.writeCount, m.readCount, m.deleteCount
}

type WriteBehindStats struct {
	TotalQueued   int64
	TotalFlushed  int64
	TotalErrors   int64
	PendingTasks  int64
	BatchRuns     int64
	LastFlushTime time.Time
}

type WriteBehindBuffer struct {
	queue         chan *WriteTask
	backend       StorageBackend
	batchSize     int
	flushInterval time.Duration
	maxRetries    int
	stopCh        chan struct{}
	wg            sync.WaitGroup
	isClosed      atomic.Bool
	statsMu       sync.Mutex
	stats         WriteBehindStats
}

func NewWriteBehindBuffer(backend StorageBackend, queueSize, batchSize int, interval time.Duration) *WriteBehindBuffer {
	if queueSize <= 0 {
		queueSize = 256
	}
	if batchSize <= 0 {
		batchSize = 32
	}
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	wb := &WriteBehindBuffer{
		queue:         make(chan *WriteTask, queueSize),
		backend:       backend,
		batchSize:     batchSize,
		flushInterval: interval,
		maxRetries:    3,
		stopCh:        make(chan struct{}),
	}
	wb.Start()
	return wb
}

func (wb *WriteBehindBuffer) Start() {
	wb.wg.Add(1)
	go wb.flusherLoop()
}

func (wb *WriteBehindBuffer) flusherLoop() {
	defer wb.wg.Done()
	ticker := time.NewTicker(wb.flushInterval)
	batch := make([]*WriteTask, 0, wb.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		for _, task := range batch {
			if task.Op == OpSet {
				_ = wb.backend.Write(task.Key, task.Value)
			} else if task.Op == OpDelete {
				_ = wb.backend.Delete(task.Key)
			}
			if task.CompletedCh != nil {
				close(task.CompletedCh)
			}
		}
		wb.statsMu.Lock()
		wb.stats.TotalFlushed += int64(len(batch))
		wb.stats.BatchRuns++
		wb.stats.LastFlushTime = time.Now()
		wb.statsMu.Unlock()
		batch = batch[:0]
	}

	for {
		select {
		case <-wb.stopCh:
			for len(wb.queue) > 0 {
				task := <-wb.queue
				batch = append(batch, task)
				if len(batch) >= wb.batchSize {
					flush()
				}
			}
			flush()
			return
		case task, ok := <-wb.queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, task)
			if len(batch) >= wb.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (wb *WriteBehindBuffer) Enqueue(task *WriteTask) error {
	if wb.isClosed.Load() {
		return ErrCacheClosed
	}
	select {
	case wb.queue <- task:
		wb.statsMu.Lock()
		wb.stats.TotalQueued++
		wb.statsMu.Unlock()
		return nil
	default:
		wb.statsMu.Lock()
		wb.stats.TotalErrors++
		wb.statsMu.Unlock()
		return ErrWriteBehindQueueFull
	}
}

func (wb *WriteBehindBuffer) Stop() error {
	if wb.isClosed.Swap(true) {
		return nil
	}
	close(wb.stopCh)
	wb.wg.Wait()
	return nil
}

func (wb *WriteBehindBuffer) Stats() WriteBehindStats {
	wb.statsMu.Lock()
	defer wb.statsMu.Unlock()
	s := wb.stats
	s.PendingTasks = int64(len(wb.queue))
	return s
}

func (wb *WriteBehindBuffer) Drain() {
	wb.statsMu.Lock()
	defer wb.statsMu.Unlock()
	for len(wb.queue) > 0 {
		<-wb.queue
	}
}

type ShardStats struct {
	Hits        int64
	Misses      int64
	Evictions   int64
	Expirations int64
	ItemCount   int64
	MemoryBytes int64
}

type CacheShard struct {
	id             int
	mu             sync.RWMutex
	entries        map[string]*LRUNode
	lru            *DoublyLinkedList
	maxEntries     int
	maxBytes       int64
	policy         EvictionPolicy
	writeBehind    *WriteBehindBuffer
	syncMode       SyncMode
	stats          ShardStats
	highWatermark  int
	lowWatermark   int
}

func NewCacheShard(id int, maxEntries int, maxBytes int64, policy EvictionPolicy, wb *WriteBehindBuffer) *CacheShard {
	hw := int(float64(maxEntries) * 0.90)
	lw := int(float64(maxEntries) * 0.75)
	if hw <= 0 {
		hw = maxEntries
	}
	if lw <= 0 {
		lw = maxEntries / 2
	}
	return &CacheShard{
		id:            id,
		entries:       make(map[string]*LRUNode),
		lru:           NewDoublyLinkedList(),
		maxEntries:    maxEntries,
		maxBytes:      maxBytes,
		policy:        policy,
		writeBehind:   wb,
		syncMode:      SyncModeAsync,
		highWatermark: hw,
		lowWatermark:  lw,
	}
}

func (s *CacheShard) Get(key string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, exists := s.entries[key]
	if exists {
		if !node.expiresAt.IsZero() && time.Now().After(node.expiresAt) {
			return nil, false
		}
		s.lru.MoveToFront(node)
		node.lastAccessed = time.Now()
		node.accessCount++
		val := node.value
		s.stats.Hits++
		return val, true
	}
	s.stats.Misses++
	return nil, false
}

func (s *CacheShard) GetWithTTL(key string) (any, time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, exists := s.entries[key]
	if exists {
		now := time.Now()
		if !node.expiresAt.IsZero() && now.After(node.expiresAt) {
			return nil, 0, false
		}
		s.lru.MoveToFront(node)
		node.lastAccessed = now
		node.accessCount++
		val := node.value
		s.stats.Hits++
		var remaining time.Duration
		if !node.expiresAt.IsZero() {
			remaining = node.expiresAt.Sub(now)
		}
		return val, remaining, true
	}
	s.stats.Misses++
	return nil, 0, false
}

func (s *CacheShard) Set(key string, val any, cost int64, ttl time.Duration) error {
	if key == "" {
		return ErrEmptyKey
	}
	if cost <= 0 {
		cost = 1
	}

	s.mu.Lock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	existing, exists := s.entries[key]
	if exists {
		existing.value = val
		existing.cost = cost
		existing.expiresAt = expiresAt
		existing.lastAccessed = time.Now()
		existing.version++
		s.lru.MoveToFront(existing)
	} else {
		for s.maxEntries > 0 && s.lru.Len() >= s.maxEntries {
			s.evictOldestLocked()
		}
		for s.maxBytes > 0 && s.lru.TotalCost()+cost > s.maxBytes && s.lru.Len() > 0 {
			s.evictOldestLocked()
		}

		newNode := &LRUNode{
			key:          key,
			value:        val,
			cost:         cost,
			expiresAt:    expiresAt,
			accessCount:  1,
			lastAccessed: time.Now(),
			firstSeen:    time.Now(),
			isDirty:      true,
			version:      1,
			tier:         TierL1Hot,
		}
		s.lru.PushFront(newNode)
		s.entries[key] = newNode
	}

	var writeTask *WriteTask
	if s.writeBehind != nil {
		writeTask = &WriteTask{
			Key:       key,
			Value:     val,
			Op:        OpSet,
			Timestamp: time.Now(),
		}
	}

	s.stats.ItemCount = int64(len(s.entries))
	s.stats.MemoryBytes = s.lru.TotalCost()
	s.mu.Unlock()

	if writeTask != nil && s.writeBehind != nil {
		_ = s.writeBehind.Enqueue(writeTask)
	}
	return nil
}

func (s *CacheShard) evictOldestLocked() {
	oldest := s.lru.RemoveTail()
	if oldest != nil {
		delete(s.entries, oldest.key)
		s.stats.Evictions++
	}
}

func (s *CacheShard) EvictOldest() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lru.Len() == 0 {
		return false
	}
	s.evictOldestLocked()
	s.stats.ItemCount = int64(len(s.entries))
	s.stats.MemoryBytes = s.lru.TotalCost()
	return true
}

func (s *CacheShard) EvictToWatermark() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	evicted := 0
	for s.maxEntries > 0 && s.lru.Len() > s.lowWatermark {
		s.evictOldestLocked()
		evicted++
	}
	s.stats.ItemCount = int64(len(s.entries))
	s.stats.MemoryBytes = s.lru.TotalCost()
	return evicted
}

func (s *CacheShard) Delete(key string) bool {
	var writeTask *WriteTask
	s.mu.Lock()

	node, exists := s.entries[key]
	if !exists {
		s.mu.Unlock()
		return false
	}

	s.lru.Remove(node)
	delete(s.entries, key)
	s.stats.ItemCount = int64(len(s.entries))
	s.stats.MemoryBytes = s.lru.TotalCost()

	if s.writeBehind != nil {
		writeTask = &WriteTask{
			Key:       key,
			Op:        OpDelete,
			Timestamp: time.Now(),
		}
	}
	s.mu.Unlock()

	if writeTask != nil && s.writeBehind != nil {
		_ = s.writeBehind.Enqueue(writeTask)
	}
	return true
}

func (s *CacheShard) Contains(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	node, exists := s.entries[key]
	if !exists {
		return false
	}
	if !node.expiresAt.IsZero() && time.Now().After(node.expiresAt) {
		return false
	}
	return true
}

func (s *CacheShard) Peek(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	node, exists := s.entries[key]
	if !exists {
		return nil, false
	}
	if !node.expiresAt.IsZero() && time.Now().After(node.expiresAt) {
		return nil, false
	}
	return node.value, true
}

func (s *CacheShard) Touch(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, exists := s.entries[key]
	if !exists {
		return false
	}
	if !node.expiresAt.IsZero() && time.Now().After(node.expiresAt) {
		return false
	}
	if ttl > 0 {
		node.expiresAt = time.Now().Add(ttl)
	} else {
		node.expiresAt = time.Time{}
	}
	return true
}

func (s *CacheShard) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make(map[string]*LRUNode)
	s.lru.Clear()
	s.stats.ItemCount = 0
	s.stats.MemoryBytes = 0
}

func (s *CacheShard) ScanExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	expiredCount := 0
	for key, node := range s.entries {
		if !node.expiresAt.IsZero() && now.After(node.expiresAt) {
			s.lru.Remove(node)
			delete(s.entries, key)
			expiredCount++
			s.stats.Expirations++
		}
	}
	s.stats.ItemCount = int64(len(s.entries))
	s.stats.MemoryBytes = s.lru.TotalCost()
	return expiredCount
}

func (s *CacheShard) DumpEntries() []CacheDumpEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dumps := make([]CacheDumpEntry, 0, len(s.entries))
	for k, v := range s.entries {
		dumps = append(dumps, CacheDumpEntry{
			Key:          k,
			Value:        v.value,
			Cost:         v.cost,
			ExpiresAt:    v.expiresAt,
			AccessCount:  v.accessCount,
			LastAccessed: v.lastAccessed,
			ShardID:      s.id,
		})
	}
	return dumps
}

func (s *CacheShard) Stats() ShardStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := s.stats
	st.ItemCount = int64(len(s.entries))
	st.MemoryBytes = s.lru.TotalCost()
	return st
}

func (s *CacheShard) MemoryUsage() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lru.TotalCost()
}

func (s *CacheShard) KeyCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

func (s *CacheShard) BatchGet(keys []string) map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[string]any, len(keys))
	now := time.Now()
	for _, k := range keys {
		node, ok := s.entries[k]
		if ok {
			if !node.expiresAt.IsZero() && now.After(node.expiresAt) {
				continue
			}
			res[k] = node.value
		}
	}
	return res
}

type CacheDumpEntry struct {
	Key          string
	Value        any
	Cost         int64
	ExpiresAt    time.Time
	AccessCount  int64
	LastAccessed time.Time
	ShardID      int
}

type CacheMetrics struct {
	TotalHits       int64
	TotalMisses     int64
	TotalEvictions  int64
	TotalExpired    int64
	TotalItems      int64
	TotalBytes      int64
	HitRatio        float64
	ShardCount      int
	WriteBehindStat WriteBehindStats
}

type DistributedCache struct {
	shards         []*CacheShard
	numShards      int
	config         CacheConfig
	sf             *SingleflightGroup
	wb             *WriteBehindBuffer
	storageBackend StorageBackend
	cleanerTicker  *time.Ticker
	cleanerStopCh  chan struct{}
	cleanerWg      sync.WaitGroup
	closed         atomic.Bool
}

func NewDistributedCache(cfg CacheConfig, backend StorageBackend, opts ...CacheOption) (*DistributedCache, error) {
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.NumShards <= 0 {
		cfg.NumShards = 8
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = time.Minute
	}
	if cfg.DefaultTTL < 0 {
		return nil, ErrInvalidTTL
	}

	var wb *WriteBehindBuffer
	if backend != nil {
		wb = NewWriteBehindBuffer(backend, cfg.WriteBehindQueueSize, cfg.WriteBehindBatchSize, cfg.WriteBehindFlushInterval)
	}

	shards := make([]*CacheShard, cfg.NumShards)
	for i := 0; i < cfg.NumShards; i++ {
		shards[i] = NewCacheShard(i, cfg.MaxEntriesPerShard, cfg.MaxMemoryBytesPerShard, cfg.EvictionPolicy, wb)
	}

	c := &DistributedCache{
		shards:         shards,
		numShards:      cfg.NumShards,
		config:         cfg,
		sf:             NewSingleflightGroup(),
		wb:             wb,
		storageBackend: backend,
		cleanerStopCh:  make(chan struct{}),
	}

	c.cleanerTicker = time.NewTicker(cfg.CleanupInterval)
	c.cleanerWg.Add(1)
	go c.cleanupLoop()

	return c, nil
}

func (c *DistributedCache) cleanupLoop() {
	defer c.cleanerWg.Done()
	for {
		select {
		case <-c.cleanerStopCh:
			return
		case <-c.cleanerTicker.C:
			for _, shard := range c.shards {
				shard.ScanExpired()
			}
		}
	}
}

func (c *DistributedCache) hashKey(key string) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	h := int(hasher.Sum32())
	if h < 0 {
		h = -h
	}
	return h % c.numShards
}

func (c *DistributedCache) getShard(key string) *CacheShard {
	idx := c.hashKey(key)
	return c.shards[idx]
}

func (c *DistributedCache) Get(key string) (any, bool) {
	if c.closed.Load() {
		return nil, false
	}
	shard := c.getShard(key)
	val, ok := shard.Get(key)
	if ok {
		return val, true
	}
	if c.storageBackend != nil {
		val, err := c.storageBackend.Read(key)
		if err == nil {
			_ = shard.Set(key, val, 1, c.config.DefaultTTL)
			return val, true
		}
	}
	return nil, false
}

func (c *DistributedCache) GetWithTTL(key string) (any, time.Duration, bool) {
	if c.closed.Load() {
		return nil, 0, false
	}
	shard := c.getShard(key)
	return shard.GetWithTTL(key)
}

func (c *DistributedCache) GetOrCompute(key string, computeFn func() (any, error)) (any, error) {
	if c.closed.Load() {
		return nil, ErrCacheClosed
	}
	val, ok := c.Get(key)
	if ok {
		return val, nil
	}

	computedVal, err, _ := c.sf.Do(key, func() (any, error) {
		v, found := c.Get(key)
		if found {
			return v, nil
		}
		res, compErr := computeFn()
		if compErr != nil {
			return nil, compErr
		}
		_ = c.Set(key, res, 1)
		return res, nil
	})

	if err != nil {
		return nil, err
	}
	return computedVal, nil
}

func (c *DistributedCache) Set(key string, val any, cost int64) error {
	if c.closed.Load() {
		return ErrCacheClosed
	}
	return c.SetWithTTL(key, val, cost, c.config.DefaultTTL)
}

func (c *DistributedCache) SetWithTTL(key string, val any, cost int64, ttl time.Duration) error {
	if c.closed.Load() {
		return ErrCacheClosed
	}
	shard := c.getShard(key)
	return shard.Set(key, val, cost, ttl)
}

func (c *DistributedCache) Delete(key string) bool {
	if c.closed.Load() {
		return false
	}
	shard := c.getShard(key)
	return shard.Delete(key)
}

func (c *DistributedCache) Contains(key string) bool {
	if c.closed.Load() {
		return false
	}
	shard := c.getShard(key)
	return shard.Contains(key)
}

func (c *DistributedCache) Peek(key string) (any, bool) {
	if c.closed.Load() {
		return nil, false
	}
	shard := c.getShard(key)
	return shard.Peek(key)
}

func (c *DistributedCache) Touch(key string, ttl time.Duration) bool {
	if c.closed.Load() {
		return false
	}
	shard := c.getShard(key)
	return shard.Touch(key, ttl)
}

func (c *DistributedCache) MGet(keys []string) map[string]any {
	results := make(map[string]any, len(keys))
	if c.closed.Load() {
		return results
	}
	for _, k := range keys {
		if val, found := c.Get(k); found {
			results[k] = val
		}
	}
	return results
}

func (c *DistributedCache) MSet(entries map[string]any, ttl time.Duration) error {
	if c.closed.Load() {
		return ErrCacheClosed
	}
	for k, v := range entries {
		if err := c.SetWithTTL(k, v, 1, ttl); err != nil {
			return err
		}
	}
	return nil
}

func (c *DistributedCache) MDelete(keys []string) int {
	if c.closed.Load() {
		return 0
	}
	deleted := 0
	for _, k := range keys {
		if c.Delete(k) {
			deleted++
		}
	}
	return deleted
}

func (c *DistributedCache) Purge() {
	for _, shard := range c.shards {
		shard.Clear()
	}
}

func (c *DistributedCache) Stats() CacheMetrics {
	var metrics CacheMetrics
	metrics.ShardCount = c.numShards

	for _, s := range c.shards {
		st := s.Stats()
		metrics.TotalHits += st.Hits
		metrics.TotalMisses += st.Misses
		metrics.TotalEvictions += st.Evictions
		metrics.TotalExpired += st.Expirations
		metrics.TotalItems += st.ItemCount
		metrics.TotalBytes += st.MemoryBytes
	}

	totalOps := metrics.TotalHits + metrics.TotalMisses
	if totalOps > 0 {
		metrics.HitRatio = float64(metrics.TotalHits) / float64(totalOps)
	}

	if c.wb != nil {
		metrics.WriteBehindStat = c.wb.Stats()
	}
	return metrics
}

func (c *DistributedCache) ShardStats(shardID int) (ShardStats, error) {
	if shardID < 0 || shardID >= c.numShards {
		return ShardStats{}, ErrShardNotFound
	}
	return c.shards[shardID].Stats(), nil
}

func (c *DistributedCache) Snapshot() ([]CacheDumpEntry, error) {
	if c.closed.Load() {
		return nil, ErrCacheClosed
	}
	var dumps []CacheDumpEntry
	for _, shard := range c.shards {
		dumps = append(dumps, shard.DumpEntries()...)
	}
	sort.Slice(dumps, func(i, j int) bool {
		return dumps[i].Key < dumps[j].Key
	})
	return dumps, nil
}

func (c *DistributedCache) Restore(entries []CacheDumpEntry) error {
	if c.closed.Load() {
		return ErrCacheClosed
	}
	for _, e := range entries {
		var remainingTTL time.Duration
		if !e.ExpiresAt.IsZero() {
			if time.Now().After(e.ExpiresAt) {
				continue
			}
			remainingTTL = time.Until(e.ExpiresAt)
		}
		_ = c.SetWithTTL(e.Key, e.Value, e.Cost, remainingTTL)
	}
	return nil
}

func (c *DistributedCache) SyncWriteBehind(timeout time.Duration) error {
	if c.wb == nil {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for {
		stats := c.wb.Stats()
		if stats.PendingTasks == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return ErrBackendTimeout
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *DistributedCache) ExportPrometheusMetrics() string {
	m := c.Stats()
	var buf bytes.Buffer
	buf.WriteString("# HELP cache_hits_total Total number of cache hits\n")
	buf.WriteString("# TYPE cache_hits_total counter\n")
	buf.WriteString(fmt.Sprintf("cache_hits_total %d\n", m.TotalHits))
	buf.WriteString("# HELP cache_misses_total Total number of cache misses\n")
	buf.WriteString("# TYPE cache_misses_total counter\n")
	buf.WriteString(fmt.Sprintf("cache_misses_total %d\n", m.TotalMisses))
	buf.WriteString("# HELP cache_items_current Current number of items in cache\n")
	buf.WriteString("# TYPE cache_items_current gauge\n")
	buf.WriteString(fmt.Sprintf("cache_items_current %d\n", m.TotalItems))
	buf.WriteString("# HELP cache_evictions_total Total number of evicted items\n")
	buf.WriteString("# TYPE cache_evictions_total counter\n")
	buf.WriteString(fmt.Sprintf("cache_evictions_total %d\n", m.TotalEvictions))
	return buf.String()
}

func (c *DistributedCache) Close() error {
	if c.closed.Swap(true) {
		return ErrCacheClosed
	}
	if c.cleanerStopCh != nil {
		close(c.cleanerStopCh)
	}
	if c.cleanerTicker != nil {
		c.cleanerTicker.Stop()
	}
	c.cleanerWg.Wait()
	if c.wb != nil {
		_ = c.wb.Stop()
	}
	return nil
}

type DistributionReport struct {
	TotalKeys       int
	AveragePerShard float64
	StdDeviation    float64
	MinKeysInShard  int
	MaxKeysInShard  int
	ImbalanceRatio  float64
}

type CacheDistributionAnalyzer struct {
	cache *DistributedCache
}

func NewCacheDistributionAnalyzer(c *DistributedCache) *CacheDistributionAnalyzer {
	return &CacheDistributionAnalyzer{cache: c}
}

func (a *CacheDistributionAnalyzer) Analyze() DistributionReport {
	shardCounts := make([]int, a.cache.numShards)
	total := 0
	minKeys := math.MaxInt32
	maxKeys := 0

	for i, s := range a.cache.shards {
		cnt := s.KeyCount()
		shardCounts[i] = cnt
		total += cnt
		if cnt < minKeys {
			minKeys = cnt
		}
		if cnt > maxKeys {
			maxKeys = cnt
		}
	}
	if total == 0 {
		minKeys = 0
	}

	avg := float64(total) / float64(a.cache.numShards)
	var varianceSum float64
	for _, cnt := range shardCounts {
		diff := float64(cnt) - avg
		varianceSum += diff * diff
	}
	stdDev := math.Sqrt(varianceSum / float64(a.cache.numShards))

	var imbalance float64
	if avg > 0 {
		imbalance = float64(maxKeys-minKeys) / avg
	}

	return DistributionReport{
		TotalKeys:       total,
		AveragePerShard: avg,
		StdDeviation:    stdDev,
		MinKeysInShard:  minKeys,
		MaxKeysInShard:  maxKeys,
		ImbalanceRatio:  imbalance,
	}
}

func (a *CacheDistributionAnalyzer) FormatReport() string {
	rep := a.Analyze()
	var b strings.Builder
	b.WriteString("=== Cache Distribution Report ===\n")
	b.WriteString(fmt.Sprintf("Total Keys:        %d\n", rep.TotalKeys))
	b.WriteString(fmt.Sprintf("Avg Per Shard:     %.2f\n", rep.AveragePerShard))
	b.WriteString(fmt.Sprintf("Std Deviation:     %.2f\n", rep.StdDeviation))
	b.WriteString(fmt.Sprintf("Min Keys in Shard: %d\n", rep.MinKeysInShard))
	b.WriteString(fmt.Sprintf("Max Keys in Shard: %d\n", rep.MaxKeysInShard))
	b.WriteString(fmt.Sprintf("Imbalance Ratio:   %.2f\n", rep.ImbalanceRatio))
	return b.String()
}

type WarmRequest struct {
	Key         string
	Priority    int
	Loader      func(string) (any, error)
	SubmittedAt time.Time
	Retries     int
}

type CacheWarmer struct {
	queue       []*WarmRequest
	mu          sync.Mutex
	cache       *DistributedCache
	maxPending  int
	workerCount int
	stopCh      chan struct{}
	warmCh      chan *WarmRequest
	wg          sync.WaitGroup
	inQueue     map[string]bool
}

func NewCacheWarmer(cache *DistributedCache, workers, maxPending int) *CacheWarmer {
	if maxPending <= 0 {
		maxPending = 1000
	}
	if workers <= 0 {
		workers = 1
	}
	return &CacheWarmer{
		queue:       make([]*WarmRequest, 0),
		cache:       cache,
		maxPending:  maxPending,
		workerCount: workers,
		stopCh:      make(chan struct{}),
		warmCh:      make(chan *WarmRequest, maxPending),
		inQueue:     make(map[string]bool),
	}
}

func (cw *CacheWarmer) SubmitWarmRequest(key string, priority int, loader func(string) (any, error)) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	if len(cw.queue) >= cw.maxPending {
		return
	}
	if cw.inQueue[key] {
		return
	}
	req := &WarmRequest{
		Key:         key,
		Priority:    priority,
		Loader:      loader,
		SubmittedAt: time.Now(),
	}
	cw.queue = append(cw.queue, req)
	cw.inQueue[key] = true
	select {
	case cw.warmCh <- req:
	default:
	}
}

func (cw *CacheWarmer) Start() {
	for i := 0; i < cw.workerCount; i++ {
		cw.wg.Add(1)
		go func() {
			defer cw.wg.Done()
			for {
				select {
				case <-cw.stopCh:
					return
				case req, ok := <-cw.warmCh:
					if !ok {
						return
					}
					if req.Loader != nil {
						val, err := req.Loader(req.Key)
						if err == nil {
							_ = cw.cache.Set(req.Key, val, 1)
						}
					}
					cw.mu.Lock()
					cw.inQueue[req.Key] = false
					cw.mu.Unlock()
				}
			}
		}()
	}
}

func (cw *CacheWarmer) Stop() {
	select {
	case <-cw.stopCh:
	default:
		close(cw.stopCh)
	}
	cw.wg.Wait()
}

func (cw *CacheWarmer) GetPendingCount() int {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return len(cw.queue)
}

func (cw *CacheWarmer) DrainQueue() []*WarmRequest {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	q := cw.queue
	cw.queue = make([]*WarmRequest, 0)
	for _, req := range q {
		cw.inQueue[req.Key] = false
	}
	return q
}

func (cw *CacheWarmer) PrioritizeQueue() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	sort.Slice(cw.queue, func(i, j int) bool {
		return cw.queue[i].Priority > cw.queue[j].Priority
	})
}

func (cw *CacheWarmer) EvictAndWarm(evictionCount int) {
	for _, shard := range cw.cache.shards {
		shard.mu.Lock()
		for i := 0; i < evictionCount; i++ {
			node := shard.lru.RemoveTail()
			if node != nil {
				delete(shard.entries, node.key)
				cw.SubmitWarmRequest(node.key, 1, func(k string) (any, error) {
					return "rewarmed", nil
				})
			}
		}
		shard.mu.Unlock()
	}
}

type EvictionPolicyManager struct {
	policy      string
	lfuCounters map[string]int64
	arcT1       []string
	arcT2       []string
	arcB1       []string
	arcB2       []string
	arcTargetT1 int
	mu          sync.RWMutex
	cache       *DistributedCache
}

func NewEvictionPolicyManager(cache *DistributedCache, policy string) *EvictionPolicyManager {
	return &EvictionPolicyManager{
		policy:      policy,
		cache:       cache,
		lfuCounters: make(map[string]int64),
		arcT1:       make([]string, 0),
		arcT2:       make([]string, 0),
		arcB1:       make([]string, 0),
		arcB2:       make([]string, 0),
	}
}

func (epm *EvictionPolicyManager) SetPolicy(policy string) {
	epm.mu.Lock()
	defer epm.mu.Unlock()
	epm.policy = policy
	epm.lfuCounters = make(map[string]int64)
	epm.arcT1 = make([]string, 0)
	epm.arcT2 = make([]string, 0)
	epm.arcB1 = make([]string, 0)
	epm.arcB2 = make([]string, 0)
	epm.arcTargetT1 = 0
}

func (epm *EvictionPolicyManager) RecordAccess(key string) {
	epm.mu.Lock()
	defer epm.mu.Unlock()
	if epm.policy == "lfu" {
		epm.lfuCounters[key]++
	} else if epm.policy == "arc" {
		found := false
		for i, k := range epm.arcT1 {
			if k == key {
				epm.arcT1 = append(epm.arcT1[:i], epm.arcT1[i+1:]...)
				epm.arcT2 = append(epm.arcT2, key)
				found = true
				break
			}
		}
		if !found {
			for i, k := range epm.arcT2 {
				if k == key {
					epm.arcT2 = append(epm.arcT2[:i], epm.arcT2[i+1:]...)
					epm.arcT2 = append(epm.arcT2, key)
					found = true
					break
				}
			}
		}
		if !found {
			epm.arcT1 = append(epm.arcT1, key)
		}
	}
}

func (epm *EvictionPolicyManager) SelectVictim() string {
	epm.mu.RLock()
	defer epm.mu.RUnlock()
	if epm.policy == "lru" {
		return ""
	}
	if epm.policy == "lfu" {
		var minKey string
		var minCount int64 = math.MaxInt64
		for k, v := range epm.lfuCounters {
			if v < minCount {
				minCount = v
				minKey = k
			}
		}
		return minKey
	}
	if epm.policy == "arc" {
		if len(epm.arcT1) > 0 {
			victim := epm.arcT1[0]
			epm.arcT1 = epm.arcT1[1:]
			epm.arcB1 = append(epm.arcB1, victim)
			return victim
		} else if len(epm.arcT2) > 0 {
			victim := epm.arcT2[0]
			epm.arcT2 = epm.arcT2[1:]
			epm.arcB2 = append(epm.arcB2, victim)
			return victim
		}
	}
	return ""
}

func (epm *EvictionPolicyManager) RemoveFromPolicy(key string) {
	epm.mu.Lock()
	defer epm.mu.Unlock()
	delete(epm.lfuCounters, key)
	for i, k := range epm.arcT1 {
		if k == key {
			epm.arcT1 = append(epm.arcT1[:i], epm.arcT1[i+1:]...)
			break
		}
	}
	for i, k := range epm.arcT2 {
		if k == key {
			epm.arcT2 = append(epm.arcT2[:i], epm.arcT2[i+1:]...)
			break
		}
	}
}

func (epm *EvictionPolicyManager) GetLFUCount(key string) int64 {
	epm.mu.RLock()
	defer epm.mu.RUnlock()
	return epm.lfuCounters[key]
}

func (epm *EvictionPolicyManager) GetARCState() (int, int, int, int) {
	epm.mu.RLock()
	defer epm.mu.RUnlock()
	return len(epm.arcT1), len(epm.arcT2), len(epm.arcB1), len(epm.arcB2)
}

func (epm *EvictionPolicyManager) AdaptARCTarget(hit bool) {
	epm.mu.Lock()
	defer epm.mu.Unlock()
	if hit {
		epm.arcTargetT1++
	} else {
		epm.arcTargetT1--
		if epm.arcTargetT1 < 0 {
			epm.arcTargetT1 = 0
		}
	}
}
`,
}
