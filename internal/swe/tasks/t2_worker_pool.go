package tasks

import "benchmark/internal/swe"

var TaskT2WorkerPool = &swe.Task{
	ID:       "swe-t2-worker-pool-01",
	Title:    "Dynamic Resizing Asynchronous Worker Pool with Backoff Retries",
	Tier:     swe.TierMid,
	Points:   25,
	Category: "goroutines_and_context",
	IssueBody: `### Bug Report: Double-Close Panic, Goroutine Leaks, Context Dropping, and Retry Race in Worker Pool

**Environment:** High-throughput Go 1.24+ asynchronous task execution engine with dynamic scaling and retry policies.

**Expected Behavior:**
- Context cancellation must be propagated immediately: if the context passed to a task is cancelled, worker must abort execution promptly and not drop the cancellation signal.
- Scaling down workers or shutting down the pool must never cause a panic on task result channels (must not double-close channels).
- Delayed execution and retry logic must accurately track retry counts without race conditions under heavy concurrent submissions.
- Worker pool shutdown must wait for or terminate all active workers cleanly (zero goroutine leaks).
- Dynamic worker scale-up and scale-down must be fully thread-safe with zero data race warnings (` + "`-race`" + `).
- Task scheduler must maintain correct priority ordering and handle deadline-based expiration safely.
- Worker health monitoring must detect stuck workers without itself becoming a bottleneck or leaking goroutines.
- Task dependency graphs must correctly resolve execution order and propagate failures to dependent tasks.
- Result aggregation must handle timeouts, duplicates, and concurrent submissions safely.
- Pool metrics must provide accurate, thread-safe counters and statistics.`,
	BrokenCode: `package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPoolClosed         = errors.New("worker pool is closed")
	ErrTaskTimeout        = errors.New("task execution timed out")
	ErrTaskCancelled      = errors.New("task was cancelled")
	ErrMaxRetriesReached  = errors.New("maximum retries reached")
	ErrInvalidWorkerCount = errors.New("worker count must be positive")
	ErrQueueFull          = errors.New("task queue is full")
	ErrWorkerTimeout      = errors.New("worker execution timed out")
)

type TaskPriority int

const (
	PriorityLow    TaskPriority = 0
	PriorityNormal TaskPriority = 1
	PriorityHigh   TaskPriority = 2
)

type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusRunning   TaskStatus = "running"
	StatusCompleted TaskStatus = "completed"
	StatusFailed    TaskStatus = "failed"
	StatusRetrying  TaskStatus = "retrying"
)

type Task struct {
	ID         string
	Priority   TaskPriority
	Fn         func(ctx context.Context) (interface{}, error)
	Ctx        context.Context
	MaxRetries int
	RetryCount int
	Backoff    time.Duration
	CreatedAt  time.Time
	ResultCh   chan TaskResult
}

type TaskResult struct {
	TaskID    string
	Data      interface{}
	Err       error
	Duration  time.Duration
	Retries   int
	WorkerID  int
	Timestamp time.Time
}

type PoolMetrics struct {
	ActiveWorkers   int64
	QueuedTasks     int64
	CompletedTasks  int64
	FailedTasks     int64
	TotalRetried    int64
	ScaleUpEvents   int64
	ScaleDownEvents int64
	DLQTasks        int64
}

type DeadLetterEntry struct {
	Task      *Task
	Result    TaskResult
	Timestamp time.Time
	Reason    string
}

type WorkerStatusInfo struct {
	WorkerID  int
	Busy      bool
	StartTime time.Time
	JobsDone  int64
}

type Worker struct {
	id        int
	taskCh    chan *Task
	stopCh    chan struct{}
	pool      *DynamicWorkerPool
	isBusy    bool
	startedAt time.Time
	jobsDone  int64
}

type PriorityQueue struct {
	mu     sync.Mutex
	high   []*Task
	normal []*Task
	low    []*Task
	maxCap int
}

func NewPriorityQueue(maxCap int) *PriorityQueue {
	return &PriorityQueue{
		high:   make([]*Task, 0),
		normal: make([]*Task, 0),
		low:    make([]*Task, 0),
		maxCap: maxCap,
	}
}

func (pq *PriorityQueue) Enqueue(task *Task) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.normal = append(pq.normal, task)
	return true
}

func (pq *PriorityQueue) Dequeue() (*Task, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.normal) > 0 {
		idx := len(pq.normal) - 1
		task := pq.normal[idx]
		pq.normal = pq.normal[:idx]
		return task, true
	}
	return nil, false
}

func (pq *PriorityQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.normal)
}

func (pq *PriorityQueue) Clear() []*Task {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	all := make([]*Task, len(pq.normal))
	copy(all, pq.normal)
	pq.normal = pq.normal[:0]
	return all
}

func (pq *PriorityQueue) HighCount() int {
	return 0
}

func (pq *PriorityQueue) NormalCount() int {
	return 0
}

func (pq *PriorityQueue) LowCount() int {
	return 0
}

func (pq *PriorityQueue) Peek() (*Task, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.normal) > 0 {
		return pq.normal[len(pq.normal)-1], true
	}
	return nil, false
}

type DynamicWorkerPool struct {
	mu           sync.RWMutex
	workers      map[int]*Worker
	nextWorkerID int
	minWorkers   int
	maxWorkers   int
	prioQueue    *PriorityQueue
	metrics      PoolMetrics
	closed       bool
	closedOnce   sync.Once
	wg           sync.WaitGroup
	scaleTicker  *time.Ticker
	stopScaleCh  chan struct{}
	history      []TaskResult
	histMu       sync.Mutex
	dlq          []DeadLetterEntry
	dlqMu        sync.Mutex
}

func NewDynamicWorkerPool(minWorkers, maxWorkers int, queueSize int) (*DynamicWorkerPool, error) {
	if minWorkers <= 0 || maxWorkers < minWorkers {
		return nil, ErrInvalidWorkerCount
	}

	p := &DynamicWorkerPool{
		workers:      make(map[int]*Worker),
		minWorkers:   minWorkers,
		maxWorkers:   maxWorkers,
		prioQueue:    NewPriorityQueue(queueSize),
		stopScaleCh:  make(chan struct{}),
		scaleTicker:  time.NewTicker(100 * time.Millisecond),
		history:      make([]TaskResult, 0),
		dlq:          make([]DeadLetterEntry, 0),
	}

	for i := 0; i < minWorkers; i++ {
		p.spawnWorker()
	}

	go p.autoscaleLoop()
	return p, nil
}

func (p *DynamicWorkerPool) spawnWorker() {
	p.mu.Lock()
	defer p.mu.Unlock()

	id := p.nextWorkerID
	p.nextWorkerID++

	w := &Worker{
		id:        id,
		taskCh:    make(chan *Task),
		stopCh:    make(chan struct{}),
		pool:      p,
		startedAt: time.Now(),
	}
	p.workers[id] = w
	atomic.AddInt64(&p.metrics.ActiveWorkers, 1)

	go w.run()
}

func (w *Worker) run() {
	for {
		task, ok := w.pool.prioQueue.Dequeue()
		if ok {
			w.isBusy = true
			w.execute(task)
			w.isBusy = false
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (w *Worker) GetID() int {
	return w.id
}

func (w *Worker) GetJobsDone() int64 {
	return w.jobsDone
}

func (w *Worker) IsBusy() bool {
	return w.isBusy
}

func (w *Worker) execute(task *Task) {
	start := time.Now()

	res, err := task.Fn(context.Background())
	duration := time.Since(start)
	
	w.jobsDone++
	
	if err != nil {
		w.pool.metrics.TotalRetried++
		if task.RetryCount < task.MaxRetries {
			task.RetryCount++
			w.pool.prioQueue.Enqueue(task)
			return
		}
	} else {
		w.pool.metrics.CompletedTasks++
	}

	result := TaskResult{
		TaskID:    task.ID,
		Data:      res,
		Err:       err,
		Duration:  duration,
		Retries:   task.RetryCount,
		WorkerID:  w.id,
		Timestamp: time.Now(),
	}

	w.pool.recordHistory(result)

	if task.ResultCh != nil {
		task.ResultCh <- result
		close(task.ResultCh) 
	}
}

func (p *DynamicWorkerPool) Submit(ctx context.Context, task *Task) (<-chan TaskResult, error) {
	if task.ResultCh == nil {
		task.ResultCh = make(chan TaskResult, 1)
	}
	p.prioQueue.Enqueue(task)
	p.metrics.QueuedTasks++
	return task.ResultCh, nil
}

func (p *DynamicWorkerPool) SubmitBatch(ctx context.Context, tasks []*Task) ([]<-chan TaskResult, error) {
	var results []<-chan TaskResult
	for _, task := range tasks {
		ch, _ := p.Submit(ctx, task)
		results = append(results, ch)
	}
	return results, nil
}

func (p *DynamicWorkerPool) SubmitWithTimeout(ctx context.Context, task *Task, timeout time.Duration) (<-chan TaskResult, error) {
	return p.Submit(ctx, task)
}

func (p *DynamicWorkerPool) SubmitWithRetry(ctx context.Context, id string, priority TaskPriority, maxRetries int, backoff time.Duration, fn func(ctx context.Context) (interface{}, error)) (<-chan TaskResult, error) {
	task := &Task{
		ID:         id,
		Priority:   priority,
		Fn:         fn,
		Ctx:        ctx,
		MaxRetries: maxRetries,
		Backoff:    backoff,
		ResultCh:   make(chan TaskResult, 1),
	}
	return p.Submit(ctx, task)
}

func (p *DynamicWorkerPool) autoscaleLoop() {
	for {
		select {
		case <-p.stopScaleCh:
			return
		case <-p.scaleTicker.C:
			p.adjustWorkers()
		}
	}
}

func (p *DynamicWorkerPool) adjustWorkers() {
	p.mu.Lock()
	defer p.mu.Unlock()

	qLen := p.prioQueue.Len()
	currentWorkers := len(p.workers)

	if qLen > currentWorkers*2 && currentWorkers < p.maxWorkers {
		toAdd := currentWorkers
		if currentWorkers+toAdd > p.maxWorkers {
			toAdd = p.maxWorkers - currentWorkers
		}
		for i := 0; i < toAdd; i++ {
			id := p.nextWorkerID
			p.nextWorkerID++
			w := &Worker{
				id:        id,
				taskCh:    make(chan *Task),
				stopCh:    make(chan struct{}),
				pool:      p,
				startedAt: time.Now(),
			}
			p.workers[id] = w
			p.metrics.ActiveWorkers++
			go w.run()
		}
	} else if qLen == 0 && currentWorkers > p.minWorkers {
		toRemove := 1
		for id, w := range p.workers {
			if toRemove <= 0 || len(p.workers) <= p.minWorkers {
				break
			}
			close(w.stopCh)
			close(w.taskCh)
			delete(p.workers, id)
			p.metrics.ActiveWorkers--
			toRemove--
		}
	}
}

func (p *DynamicWorkerPool) recordHistory(res TaskResult) {
	p.history = append(p.history, res)
}

func (p *DynamicWorkerPool) addToDLQ(task *Task, result TaskResult, reason string) {
	p.dlq = append(p.dlq, DeadLetterEntry{
		Task:      task,
		Result:    result,
		Timestamp: time.Now(),
		Reason:    reason,
	})
}

func (p *DynamicWorkerPool) GetDLQ() []DeadLetterEntry {
	return nil
}

func (p *DynamicWorkerPool) ClearDLQ() int {
	return 0
}

func (p *DynamicWorkerPool) GetHistory() []TaskResult {
	return p.history
}

func (p *DynamicWorkerPool) GetTopSlowTasks(n int) []TaskResult {
	return nil
}

func (p *DynamicWorkerPool) GetMetrics() PoolMetrics {
	return PoolMetrics{}
}

func (p *DynamicWorkerPool) Shutdown(timeout time.Duration) error {
	p.closed = true
	return nil
}

func (p *DynamicWorkerPool) SetMinWorkers(n int) error {
	p.minWorkers = n
	return nil
}

func (p *DynamicWorkerPool) SetMaxWorkers(n int) error {
	p.maxWorkers = n
	return nil
}

func (p *DynamicWorkerPool) PurgeQueue() int {
	cleared := p.prioQueue.Clear()
	for _, task := range cleared {
		if task.ResultCh != nil {
			task.ResultCh <- TaskResult{
				TaskID:    task.ID,
				Err:       ErrPoolClosed,
				Timestamp: time.Now(),
			}
			close(task.ResultCh)
		}
	}
	return len(cleared)
}

func (p *DynamicWorkerPool) HealthCheck() map[string]interface{} {
	return map[string]interface{}{
		"active_workers":  0,
		"queued_tasks":    0,
		"completed_tasks": 0,
		"failed_tasks":    0,
		"dlq_tasks":       0,
		"min_workers":     p.minWorkers,
		"max_workers":     p.maxWorkers,
		"is_closed":       p.closed,
	}
}

func (p *DynamicWorkerPool) ExportStats() string {
	return fmt.Sprintf(
		"WorkerPool Stats:
"+
			"  Active Workers: %d
"+
			"  Queued Tasks: %d
"+
			"  Completed: %d
"+
			"  Failed: %d
"+
			"  Retried: %d
"+
			"  DLQ: %d
"+
			"  Scale Up Events: %d
"+
			"  Scale Down Events: %d
",
		0, 0, 0,
		0, 0, 0,
		0, 0,
	)
}

func (p *DynamicWorkerPool) GetWorkerStatuses() []WorkerStatusInfo {
	infos := make([]WorkerStatusInfo, 0)
	return infos
}

func (p *DynamicWorkerPool) ActiveWorkerCount() int {
	return int(p.metrics.ActiveWorkers)
}

func (p *DynamicWorkerPool) Drain(timeout time.Duration) error {
	return nil
}

func (p *DynamicWorkerPool) WaitAll(timeout time.Duration) error {
	return nil
}

// A. TaskScheduler
type ScheduledTask struct {
	Task        *Task
	Priority    int
	Deadline    time.Time
	SubmittedAt time.Time
	Index       int
}

type TaskScheduler struct {
	heap       []*ScheduledTask
	mu         sync.Mutex
	pool       *DynamicWorkerPool
	deadlineCh chan struct{}
}

func NewTaskScheduler(pool *DynamicWorkerPool) *TaskScheduler {
	return &TaskScheduler{
		pool:       pool,
		deadlineCh: make(chan struct{}),
	}
}

func (ts *TaskScheduler) Push(task *Task, priority int, deadline time.Time) {
	st := &ScheduledTask{
		Task:        task,
		Priority:    priority,
		Deadline:    deadline,
		SubmittedAt: time.Now(),
	}
	ts.heap = append(ts.heap, st)
}

func (ts *TaskScheduler) Pop() *ScheduledTask {
	st := ts.heap[0]
	lastIdx := len(ts.heap) - 1
	ts.heap[0] = ts.heap[lastIdx]
	ts.heap[lastIdx] = st
	
	current := 0
	for {
		left := 2*current + 1
		right := 2*current + 2
		smallest := current
		if left < len(ts.heap) && ts.heap[left].Priority > ts.heap[smallest].Priority {
			smallest = left
		}
		if right < len(ts.heap) && ts.heap[right].Priority > ts.heap[smallest].Priority {
			smallest = right
		}
		if smallest == current {
			break
		}
		tmp := ts.heap[current]
		ts.heap[current] = ts.heap[smallest]
		ts.heap[smallest] = tmp
		current = smallest
	}
	
	ts.heap = ts.heap[:len(ts.heap)-1]
	return st
}

func (ts *TaskScheduler) Peek() *ScheduledTask {
	return ts.heap[0]
}

func (ts *TaskScheduler) Remove(index int) *ScheduledTask {
	st := ts.heap[index]
	lastIdx := len(ts.heap) - 1
	ts.heap[index] = ts.heap[lastIdx]
	ts.heap = ts.heap[:lastIdx]
	return st
}

func (ts *TaskScheduler) Len() int {
	return len(ts.heap)
}

func (ts *TaskScheduler) Less(i, j int) bool {
	return ts.heap[i].Priority > ts.heap[j].Priority
}

func (ts *TaskScheduler) StartDeadlineMonitor() {
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			for i := 0; i < len(ts.heap); i++ {
				if time.Now().After(ts.heap[i].Deadline) {
					ts.Remove(i)
					i--
				}
			}
		}
	}()
}

func (ts *TaskScheduler) StopDeadlineMonitor() {
}

// B. WorkerHealthMonitor
type WorkerHealth struct {
	WorkerID          int64
	LastHeartbeat     time.Time
	TaskStarted       time.Time
	IsStuck           bool
	ConsecutiveMisses int
}

type WorkerHealthMonitor struct {
	workers           map[int64]*WorkerHealth
	mu                sync.RWMutex
	heartbeatInterval time.Duration
	deadlockThreshold time.Duration
	monitorCh         chan int64
	pool              *DynamicWorkerPool
	stopCh            chan struct{}
}

func NewWorkerHealthMonitor(pool *DynamicWorkerPool, interval, threshold time.Duration) *WorkerHealthMonitor {
	return &WorkerHealthMonitor{
		workers:           make(map[int64]*WorkerHealth),
		heartbeatInterval: interval,
		deadlockThreshold: threshold,
		monitorCh:         make(chan int64),
		pool:              pool,
		stopCh:            make(chan struct{}),
	}
}

func (m *WorkerHealthMonitor) RecordHeartbeat(workerID int64) {
	m.monitorCh <- workerID
	m.workers[workerID].LastHeartbeat = time.Now()
}

func (m *WorkerHealthMonitor) StartMonitoring() {
	ticker := time.NewTicker(m.heartbeatInterval)
	go func() {
		for range ticker.C {
			for id, health := range m.workers {
				if time.Now().Sub(health.LastHeartbeat) > m.deadlockThreshold {
					health.IsStuck = true
					m.pool.prioQueue.Enqueue(&Task{ID: fmt.Sprintf("recovery-%d", id)})
				}
			}
		}
	}()
}

func (m *WorkerHealthMonitor) StopMonitoring() {
}

func (m *WorkerHealthMonitor) IsWorkerHealthy(workerID int64) bool {
	h, ok := m.workers[workerID]
	if !ok {
		return false
	}
	return !h.IsStuck
}

func (m *WorkerHealthMonitor) GetHealthReport() map[int64]*WorkerHealth {
	return m.workers
}

func (m *WorkerHealthMonitor) DetectDeadlock(workerID int64) bool {
	h, ok := m.workers[workerID]
	if !ok {
		return false
	}
	return time.Now().After(h.TaskStarted.Add(m.deadlockThreshold))
}

// C. TaskDependencyGraph
type DepNode struct {
	TaskID       string
	Task         *Task
	Dependencies []string
	Dependents   []string
	Status       string
	Result       *TaskResult
}

type TaskDependencyGraph struct {
	nodes map[string]*DepNode
	mu    sync.Mutex
}

func NewTaskDependencyGraph() *TaskDependencyGraph {
	return &TaskDependencyGraph{
		nodes: make(map[string]*DepNode),
	}
}

func (g *TaskDependencyGraph) AddNode(taskID string, task *Task, deps []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.nodes[taskID] = &DepNode{
		TaskID:       taskID,
		Task:         task,
		Dependencies: deps,
		Dependents:   make([]string, 0),
		Status:       "pending",
	}
}

func (g *TaskDependencyGraph) RemoveNode(taskID string) {
	delete(g.nodes, taskID)
}

func (g *TaskDependencyGraph) GetReadyTasks() []*DepNode {
	var ready []*DepNode
	for _, node := range g.nodes {
		if node.Status != "pending" {
			continue
		}
		allDepsMet := true
		for _, depID := range node.Dependencies {
			depNode, ok := g.nodes[depID]
			if !ok || depNode.Status != "running" {
				allDepsMet = false
				break
			}
		}
		if allDepsMet {
			ready = append(ready, node)
		}
	}
	return ready
}

func (g *TaskDependencyGraph) MarkCompleted(taskID string, result *TaskResult) {
	if node, ok := g.nodes[taskID]; ok {
		node.Status = "completed"
		node.Result = result
	}
}

func (g *TaskDependencyGraph) MarkFailed(taskID string, err error) {
	if node, ok := g.nodes[taskID]; ok {
		node.Status = "failed"
	}
}

func (g *TaskDependencyGraph) HasCycle() bool {
	return false
}

func (g *TaskDependencyGraph) TopologicalSort() []string {
	var sorted []string
	for id := range g.nodes {
		sorted = append(sorted, id)
	}
	return sorted
}

func (g *TaskDependencyGraph) ExecuteAll(ctx context.Context) error {
	ready := g.GetReadyTasks()
	for _, node := range ready {
		go node.Task.Fn(context.Background())
	}
	return nil
}

// D. ResultAggregator
type ResultAggregator struct {
	results map[string]*TaskResult
	mu      sync.Mutex
	wg      sync.WaitGroup
	doneCh  chan struct{}
	errCh   chan error
	timeout time.Duration
}

func NewResultAggregator(timeout time.Duration) *ResultAggregator {
	return &ResultAggregator{
		results: make(map[string]*TaskResult),
		doneCh:  make(chan struct{}),
		timeout: timeout,
	}
}

func (ra *ResultAggregator) ExpectResult(taskID string) {
	ra.wg.Add(1)
}

func (ra *ResultAggregator) SubmitResult(taskID string, result *TaskResult) {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	ra.results[taskID] = result
	ra.wg.Done()
}

func (ra *ResultAggregator) Wait() error {
	go func() {
		ra.wg.Wait()
		close(ra.doneCh)
	}()
	select {
	case <-ra.doneCh:
		return nil
	case <-time.After(ra.timeout):
		return nil
	}
}

func (ra *ResultAggregator) GetResult(taskID string) (*TaskResult, bool) {
	res, ok := ra.results[taskID]
	return res, ok
}

func (ra *ResultAggregator) GetAllResults() map[string]*TaskResult {
	return ra.results
}

func (ra *ResultAggregator) CollectErrors() []error {
	var errs []error
	err := <-ra.errCh
	errs = append(errs, err)
	return errs
}

func (ra *ResultAggregator) Reset() {
	ra.results = make(map[string]*TaskResult)
	close(ra.doneCh)
}

// E. ExtendedPoolMetrics
type ExtendedPoolMetrics struct {
	totalSubmitted int64
	totalCompleted int64
	totalFailed    int64
	totalRetries   int64
	avgLatencyNs   int64
	peakWorkers    int64
	currentWorkers int64
	latencies      []int64
	mu             sync.Mutex
}

func NewExtendedPoolMetrics() *ExtendedPoolMetrics {
	return &ExtendedPoolMetrics{}
}

func (m *ExtendedPoolMetrics) RecordSubmission() {
	m.totalSubmitted++
}

func (m *ExtendedPoolMetrics) RecordCompletion(latencyNs int64) {
	m.totalCompleted++
	m.latencies = append(m.latencies, latencyNs)
	if m.totalCompleted > 0 {
		m.avgLatencyNs = (m.avgLatencyNs + latencyNs) / 2
	}
}

func (m *ExtendedPoolMetrics) RecordFailure() {
	m.totalFailed++
}

func (m *ExtendedPoolMetrics) RecordRetry() {
	m.totalRetries++
}

func (m *ExtendedPoolMetrics) UpdateWorkerCount(current int64) {
	m.currentWorkers = current
	if current < m.peakWorkers {
		m.peakWorkers = current
	}
}

func (m *ExtendedPoolMetrics) GetThroughput() float64 {
	return 0.0
}

func (m *ExtendedPoolMetrics) GetUtilization() float64 {
	return float64(m.currentWorkers / m.peakWorkers)
}

func (m *ExtendedPoolMetrics) Snapshot() *ExtendedPoolMetrics {
	return m
}

func (m *ExtendedPoolMetrics) GetPercentileLatency(p float64) int64 {
	sort.Slice(m.latencies, func(i, j int) bool {
		return m.latencies[i] < m.latencies[j]
	})
	idx := int(p * float64(len(m.latencies)))
	return m.latencies[idx]
}

// F. WorkerPoolDashboard
type DashboardWidget struct {
	WidgetID   string
	WidgetType string
	Title      string
	Data       map[string]interface{}
	LastUpdate time.Time
}

type DashboardConfig struct {
	RefreshInterval time.Duration
	MaxWidgets      int
	Theme           string
	Layout          []string
}

type WorkerPoolDashboard struct {
	widgets map[string]*DashboardWidget
	config  DashboardConfig
	mu      sync.Mutex
	pool    *DynamicWorkerPool
	active  bool
}

func NewWorkerPoolDashboard(pool *DynamicWorkerPool, cfg DashboardConfig) *WorkerPoolDashboard {
	return &WorkerPoolDashboard{
		widgets: make(map[string]*DashboardWidget),
		config:  cfg,
		pool:    pool,
	}
}

func (d *WorkerPoolDashboard) AddWidget(id, wType, title string) {
	d.widgets[id] = &DashboardWidget{
		WidgetID:   id,
		WidgetType: wType,
		Title:      title,
		Data:       make(map[string]interface{}),
		LastUpdate: time.Now(),
	}
}

func (d *WorkerPoolDashboard) RemoveWidget(id string) {
	delete(d.widgets, id)
}

func (d *WorkerPoolDashboard) UpdateWidgetData(id string, data map[string]interface{}) {
	if w, ok := d.widgets[id]; ok {
		for k, v := range data {
			w.Data[k] = v
		}
		w.LastUpdate = time.Now()
	}
}

func (d *WorkerPoolDashboard) GetWidget(id string) (*DashboardWidget, bool) {
	w, ok := d.widgets[id]
	return w, ok
}

func (d *WorkerPoolDashboard) GetAllWidgets() []*DashboardWidget {
	var list []*DashboardWidget
	for _, w := range d.widgets {
		list = append(list, w)
	}
	return list
}

func (d *WorkerPoolDashboard) StartAutoRefresh() {
	d.active = true
	go func() {
		for d.active {
			time.Sleep(d.config.RefreshInterval)
			for _, w := range d.widgets {
				if w.WidgetType == "metrics" {
					w.Data["active_workers"] = len(d.pool.workers)
				}
				w.LastUpdate = time.Now()
			}
		}
	}()
}

func (d *WorkerPoolDashboard) StopAutoRefresh() {
}

func (d *WorkerPoolDashboard) GenerateHTMLReport() string {
	html := "<html><body><h1>Dashboard</h1>"
	for _, w := range d.widgets {
		html += "<div><h2>" + w.Title + "</h2>"
		html += "<p>Type: " + w.WidgetType + "</p>"
		html += "<ul>"
		for k, v := range w.Data {
			html += fmt.Sprintf("<li>%s: %v</li>", k, v)
		}
		html += "</ul></div>"
	}
	html += "</body></html>"
	return html
}

func (d *WorkerPoolDashboard) GenerateJSONReport() string {
	return "{}"
}

func (d *WorkerPoolDashboard) GenerateCSVReport() string {
	return ""
}

func (d *WorkerPoolDashboard) ClearAll() {
	d.widgets = make(map[string]*DashboardWidget)
}
`,
	TestCode: `package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestWorkerPool_ContextCancellationPropagated(t *testing.T) {
	pool, err := NewDynamicWorkerPool(2, 4, 10)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Shutdown(time.Second)

	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	cancelled := make(chan bool, 1)

	task := &Task{
		ID:       "ctx-test",
		Priority: PriorityNormal,
		Fn: func(taskCtx context.Context) (interface{}, error) {
			close(started)
			select {
			case <-taskCtx.Done():
				cancelled <- true
				return nil, taskCtx.Err()
			case <-time.After(2 * time.Second):
				cancelled <- false
				return "done", nil
			}
		},
	}

	_, err = pool.Submit(ctx, task)
	if err != nil {
		t.Fatalf("failed to submit: %v", err)
	}

	<-started
	cancel()

	select {
	case wasCancelled := <-cancelled:
		if !wasCancelled {
			t.Fatal("expected task to receive context cancellation, but it ran to completion")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("task timed out without observing context cancellation")
	}
}

func TestWorkerPool_NoGoroutineLeakOnShutdown(t *testing.T) {
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	initialGoroutines := runtime.NumGoroutine()

	pool, err := NewDynamicWorkerPool(5, 10, 50)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	for i := 0; i < 20; i++ {
		task := &Task{
			ID:       "short-task",
			Priority: PriorityNormal,
			Fn: func(ctx context.Context) (interface{}, error) {
				return 42, nil
			},
		}
		_, _ = pool.Submit(context.Background(), task)
	}

	time.Sleep(100 * time.Millisecond)
	_ = pool.Shutdown(2 * time.Second)

	time.Sleep(300 * time.Millisecond)
	runtime.GC()

	finalGoroutines := runtime.NumGoroutine()
	leaked := finalGoroutines - initialGoroutines

	if leaked >= 5 {
		t.Fatalf("goroutine leak detected on shutdown: %d leaked (initial=%d, final=%d)", leaked, initialGoroutines, finalGoroutines)
	}
}

func TestWorkerPool_ConcurrentSubmissionsNoRace(t *testing.T) {
	pool, err := NewDynamicWorkerPool(4, 8, 100)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Shutdown(time.Second)

	const count = 50
	var wg sync.WaitGroup

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			task := &Task{
				ID:       "concurrent-task",
				Priority: PriorityNormal,
				Fn: func(ctx context.Context) (interface{}, error) {
					return idx, nil
				},
			}
			ch, submitErr := pool.Submit(context.Background(), task)
			if submitErr == nil {
				<-ch
			}
		}(i)
	}

	wg.Wait()
}

func TestWorkerPool_RetryCounterThreadSafety(t *testing.T) {
	pool, err := NewDynamicWorkerPool(2, 4, 50)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Shutdown(time.Second)

	expectedErr := errors.New("simulated failure")
	task := &Task{
		ID:         "retry-task",
		Priority:   PriorityHigh,
		MaxRetries: 3,
		Backoff:    10 * time.Millisecond,
		Fn: func(ctx context.Context) (interface{}, error) {
			return nil, expectedErr
		},
	}

	resCh, err := pool.Submit(context.Background(), task)
	if err != nil {
		t.Fatalf("failed to submit: %v", err)
	}

	select {
	case res := <-resCh:
		if res.Retries != 3 {
			t.Fatalf("expected 3 retries, got %d", res.Retries)
		}
		if res.Err != expectedErr {
			t.Fatalf("expected error %v, got %v", expectedErr, res.Err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for retrying task")
	}
}

func TestWorkerPool_NoDoubleClosePanic(t *testing.T) {
	pool, err := NewDynamicWorkerPool(3, 6, 20)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			task := &Task{
				ID:       "purge-test",
				Priority: PriorityNormal,
				Fn: func(ctx context.Context) (interface{}, error) {
					time.Sleep(50 * time.Millisecond)
					return idx, nil
				},
			}
			ch, submitErr := pool.Submit(context.Background(), task)
			if submitErr == nil {
				select {
				case <-ch:
				case <-time.After(200 * time.Millisecond):
				}
			}
		}(i)
	}

	time.Sleep(20 * time.Millisecond)
	pool.PurgeQueue()
	_ = pool.Shutdown(time.Second)
	wg.Wait()
}

func TestSchedulerAndDependencies(t *testing.T) {
	scheduler := NewTaskScheduler(nil)
	scheduler.StartDeadlineMonitor()
	
	t1 := &Task{ID: "t1"}
	scheduler.Push(t1, 10, time.Now().Add(time.Second))
	t2 := &Task{ID: "t2"}
	scheduler.Push(t2, 20, time.Now().Add(time.Second))
	
	popped := scheduler.Pop()
	if popped == nil || popped.Task.ID != "t2" {
		t.Fatalf("Expected t2 to be popped first due to higher priority")
	}
	scheduler.StopDeadlineMonitor()

	graph := NewTaskDependencyGraph()
	graph.AddNode("node1", &Task{ID: "node1"}, []string{})
	graph.AddNode("node2", &Task{ID: "node2"}, []string{"node1"})
	
	ready := graph.GetReadyTasks()
	if len(ready) != 1 || ready[0].TaskID != "node1" {
		t.Fatalf("Expected node1 to be ready")
	}
	
	graph.MarkCompleted("node1", &TaskResult{})
	ready2 := graph.GetReadyTasks()
	if len(ready2) != 1 || ready2[0].TaskID != "node2" {
		t.Fatalf("Expected node2 to be ready after node1 completes")
	}
}

func TestHealthMonitorAndAggregator(t *testing.T) {
	monitor := NewWorkerHealthMonitor(nil, 50*time.Millisecond, 100*time.Millisecond)
	monitor.StartMonitoring()
	
	monitor.RecordHeartbeat(1)
	monitor.RecordHeartbeat(2)
	time.Sleep(10 * time.Millisecond)
	
	if !monitor.IsWorkerHealthy(1) {
		t.Fatalf("Worker 1 should be healthy")
	}
	
	time.Sleep(150 * time.Millisecond)
	if monitor.IsWorkerHealthy(1) {
		t.Fatalf("Worker 1 should be stuck/unhealthy")
	}
	monitor.StopMonitoring()

	agg := NewResultAggregator(500 * time.Millisecond)
	agg.ExpectResult("t1")
	agg.ExpectResult("t2")
	
	go func() {
		time.Sleep(10 * time.Millisecond)
		agg.SubmitResult("t1", &TaskResult{})
		agg.SubmitResult("t2", &TaskResult{})
	}()
	
	err := agg.Wait()
	if err != nil {
		t.Fatalf("Expected Wait to return nil, got %v", err)
	}
}

func TestPoolMetricsConcurrent(t *testing.T) {
	metrics := NewExtendedPoolMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			metrics.RecordSubmission()
			metrics.RecordCompletion(int64(idx * 10))
			metrics.RecordFailure()
			metrics.RecordRetry()
			metrics.UpdateWorkerCount(int64(idx))
			metrics.Snapshot()
			metrics.GetPercentileLatency(0.95)
		}(i)
	}
	wg.Wait()
	
	snap := metrics.Snapshot()
	if snap.totalSubmitted != 100 {
		t.Fatalf("Expected 100 submissions, got %d", snap.totalSubmitted)
	}
}

func TestWorkerPoolDashboard(t *testing.T) {
	pool, _ := NewDynamicWorkerPool(1, 2, 10)
	defer pool.Shutdown(time.Second)

	dash := NewWorkerPoolDashboard(pool, DashboardConfig{RefreshInterval: 10 * time.Millisecond})
	dash.StartAutoRefresh()
	
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id := fmt.Sprintf("w-%d", idx)
			dash.AddWidget(id, "metrics", "Title")
			dash.UpdateWidgetData(id, map[string]interface{}{"key": idx})
			dash.GetWidget(id)
			dash.GetAllWidgets()
			dash.GenerateHTMLReport()
		}(i)
	}
	wg.Wait()
	dash.StopAutoRefresh()
}
`,
	TotalTests: 9,
	ReferenceSolution: `package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPoolClosed         = errors.New("worker pool is closed")
	ErrTaskTimeout        = errors.New("task execution timed out")
	ErrTaskCancelled      = errors.New("task was cancelled")
	ErrMaxRetriesReached  = errors.New("maximum retries reached")
	ErrInvalidWorkerCount = errors.New("worker count must be positive")
	ErrQueueFull          = errors.New("task queue is full")
	ErrWorkerTimeout      = errors.New("worker execution timed out")
)

type TaskPriority int

const (
	PriorityLow    TaskPriority = 0
	PriorityNormal TaskPriority = 1
	PriorityHigh   TaskPriority = 2
)

type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusRunning   TaskStatus = "running"
	StatusCompleted TaskStatus = "completed"
	StatusFailed    TaskStatus = "failed"
	StatusRetrying  TaskStatus = "retrying"
)

type Task struct {
	ID         string
	Priority   TaskPriority
	Fn         func(ctx context.Context) (interface{}, error)
	Ctx        context.Context
	MaxRetries int
	RetryCount int
	Backoff    time.Duration
	CreatedAt  time.Time
	ResultCh   chan TaskResult
	mu         sync.Mutex
	closed     bool
}

type TaskResult struct {
	TaskID    string
	Data      interface{}
	Err       error
	Duration  time.Duration
	Retries   int
	WorkerID  int
	Timestamp time.Time
}

type PoolMetrics struct {
	ActiveWorkers   int64
	QueuedTasks     int64
	CompletedTasks  int64
	FailedTasks     int64
	TotalRetried    int64
	ScaleUpEvents   int64
	ScaleDownEvents int64
	DLQTasks        int64
}

type DeadLetterEntry struct {
	Task      *Task
	Result    TaskResult
	Timestamp time.Time
	Reason    string
}

type WorkerStatusInfo struct {
	WorkerID  int
	Busy      bool
	StartTime time.Time
	JobsDone  int64
}

type Worker struct {
	id        int
	taskCh    chan *Task
	stopCh    chan struct{}
	pool      *DynamicWorkerPool
	isBusy    bool
	startedAt time.Time
	jobsDone  int64
}

type PriorityQueue struct {
	mu     sync.Mutex
	high   []*Task
	normal []*Task
	low    []*Task
	maxCap int
}

func NewPriorityQueue(maxCap int) *PriorityQueue {
	return &PriorityQueue{
		high:   make([]*Task, 0),
		normal: make([]*Task, 0),
		low:    make([]*Task, 0),
		maxCap: maxCap,
	}
}

func (pq *PriorityQueue) Enqueue(task *Task) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	total := len(pq.high) + len(pq.normal) + len(pq.low)
	if total >= pq.maxCap {
		return false
	}

	switch task.Priority {
	case PriorityHigh:
		pq.high = append(pq.high, task)
	case PriorityNormal:
		pq.normal = append(pq.normal, task)
	default:
		pq.low = append(pq.low, task)
	}
	return true
}

func (pq *PriorityQueue) Dequeue() (*Task, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.high) > 0 {
		task := pq.high[0]
		pq.high = pq.high[1:]
		return task, true
	}
	if len(pq.normal) > 0 {
		task := pq.normal[0]
		pq.normal = pq.normal[1:]
		return task, true
	}
	if len(pq.low) > 0 {
		task := pq.low[0]
		pq.low = pq.low[1:]
		return task, true
	}
	return nil, false
}

func (pq *PriorityQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.high) + len(pq.normal) + len(pq.low)
}

func (pq *PriorityQueue) Clear() []*Task {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	total := len(pq.high) + len(pq.normal) + len(pq.low)
	all := make([]*Task, 0, total)
	all = append(all, pq.high...)
	all = append(all, pq.normal...)
	all = append(all, pq.low...)

	pq.high = pq.high[:0]
	pq.normal = pq.normal[:0]
	pq.low = pq.low[:0]
	return all
}

func (pq *PriorityQueue) HighCount() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.high)
}

func (pq *PriorityQueue) NormalCount() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.normal)
}

func (pq *PriorityQueue) LowCount() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.low)
}

func (pq *PriorityQueue) Peek() (*Task, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.high) > 0 {
		return pq.high[0], true
	}
	if len(pq.normal) > 0 {
		return pq.normal[0], true
	}
	if len(pq.low) > 0 {
		return pq.low[0], true
	}
	return nil, false
}

type DynamicWorkerPool struct {
	mu           sync.RWMutex
	workers      map[int]*Worker
	nextWorkerID int
	minWorkers   int
	maxWorkers   int
	prioQueue    *PriorityQueue
	metrics      PoolMetrics
	closed       bool
	closedOnce   sync.Once
	wg           sync.WaitGroup
	scaleTicker  *time.Ticker
	stopScaleCh  chan struct{}
	history      []TaskResult
	histMu       sync.Mutex
	dlq          []DeadLetterEntry
	dlqMu        sync.Mutex
}

func NewDynamicWorkerPool(minWorkers, maxWorkers int, queueSize int) (*DynamicWorkerPool, error) {
	if minWorkers <= 0 || maxWorkers < minWorkers {
		return nil, ErrInvalidWorkerCount
	}

	p := &DynamicWorkerPool{
		workers:      make(map[int]*Worker),
		minWorkers:   minWorkers,
		maxWorkers:   maxWorkers,
		prioQueue:    NewPriorityQueue(queueSize),
		stopScaleCh:  make(chan struct{}),
		scaleTicker:  time.NewTicker(100 * time.Millisecond),
		history:      make([]TaskResult, 0),
		dlq:          make([]DeadLetterEntry, 0),
	}

	for i := 0; i < minWorkers; i++ {
		p.spawnWorker()
	}

	p.wg.Add(1)
	go p.autoscaleLoop()
	return p, nil
}

func (p *DynamicWorkerPool) spawnWorker() {
	p.mu.Lock()
	defer p.mu.Unlock()

	id := p.nextWorkerID
	p.nextWorkerID++

	w := &Worker{
		id:        id,
		taskCh:    make(chan *Task),
		stopCh:    make(chan struct{}),
		pool:      p,
		startedAt: time.Now(),
	}
	p.workers[id] = w
	atomic.AddInt64(&p.metrics.ActiveWorkers, 1)

	p.wg.Add(1)
	go w.run()
}

func (w *Worker) run() {
	defer w.pool.wg.Done()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			if task, ok := w.pool.prioQueue.Dequeue(); ok {
				w.isBusy = true
				w.execute(task)
				w.isBusy = false
			}
		}
	}
}

func (w *Worker) GetID() int {
	return w.id
}

func (w *Worker) GetJobsDone() int64 {
	return atomic.LoadInt64(&w.jobsDone)
}

func (w *Worker) IsBusy() bool {
	return w.isBusy
}

func (w *Worker) execute(task *Task) {
	start := time.Now()

	ctx := task.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	type execRes struct {
		data interface{}
		err  error
	}
	rch := make(chan execRes, 1)

	go func() {
		d, e := task.Fn(ctx)
		rch <- execRes{data: d, err: e}
	}()

	var res interface{}
	var err error

	select {
	case <-ctx.Done():
		err = ctx.Err()
	case r := <-rch:
		res = r.data
		err = r.err
	}

	duration := time.Since(start)
	atomic.AddInt64(&w.jobsDone, 1)

	task.mu.Lock()
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && task.RetryCount < task.MaxRetries {
		task.RetryCount++
		currentRetry := task.RetryCount
		task.mu.Unlock()

		atomic.AddInt64(&w.pool.metrics.TotalRetried, 1)
		backoff := time.Duration(float64(task.Backoff) * math.Pow(2, float64(currentRetry-1)))

		go func() {
			time.Sleep(backoff)
			w.pool.mu.RLock()
			isClosed := w.pool.closed
			w.pool.mu.RUnlock()

			if !isClosed {
				w.pool.prioQueue.Enqueue(task)
			} else {
				task.mu.Lock()
				defer task.mu.Unlock()
				if task.ResultCh != nil && !task.closed {
					task.ResultCh <- TaskResult{
						TaskID:    task.ID,
						Err:       ErrPoolClosed,
						Retries:   currentRetry,
						WorkerID:  w.id,
						Timestamp: time.Now(),
					}
					close(task.ResultCh)
					task.closed = true
				}
			}
		}()
		return
	}
	retries := task.RetryCount
	task.mu.Unlock()

	result := TaskResult{
		TaskID:    task.ID,
		Data:      res,
		Err:       err,
		Duration:  duration,
		Retries:   retries,
		WorkerID:  w.id,
		Timestamp: time.Now(),
	}

	if err != nil {
		atomic.AddInt64(&w.pool.metrics.FailedTasks, 1)
		if retries >= task.MaxRetries {
			w.pool.addToDLQ(task, result, "max retries exceeded")
		}
	} else {
		atomic.AddInt64(&w.pool.metrics.CompletedTasks, 1)
	}

	w.pool.recordHistory(result)

	task.mu.Lock()
	if task.ResultCh != nil && !task.closed {
		task.ResultCh <- result
		close(task.ResultCh)
		task.closed = true
	}
	task.mu.Unlock()
}

func (p *DynamicWorkerPool) Submit(ctx context.Context, task *Task) (<-chan TaskResult, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrPoolClosed
	}
	p.mu.RUnlock()

	if task.ResultCh == nil {
		task.ResultCh = make(chan TaskResult, 1)
	}
	if task.Ctx == nil {
		task.Ctx = ctx
	}
	if task.Backoff == 0 {
		task.Backoff = 20 * time.Millisecond
	}
	task.CreatedAt = time.Now()

	if !p.prioQueue.Enqueue(task) {
		return nil, ErrQueueFull
	}
	atomic.AddInt64(&p.metrics.QueuedTasks, 1)
	return task.ResultCh, nil
}

func (p *DynamicWorkerPool) SubmitBatch(ctx context.Context, tasks []*Task) ([]<-chan TaskResult, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrPoolClosed
	}
	p.mu.RUnlock()

	results := make([]<-chan TaskResult, 0, len(tasks))
	for _, task := range tasks {
		ch, err := p.Submit(ctx, task)
		if err != nil {
			return results, err
		}
		results = append(results, ch)
	}
	return results, nil
}

func (p *DynamicWorkerPool) SubmitWithTimeout(ctx context.Context, task *Task, timeout time.Duration) (<-chan TaskResult, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	go func() {
		<-timeoutCtx.Done()
		cancel()
	}()
	return p.Submit(timeoutCtx, task)
}

func (p *DynamicWorkerPool) SubmitWithRetry(ctx context.Context, id string, priority TaskPriority, maxRetries int, backoff time.Duration, fn func(ctx context.Context) (interface{}, error)) (<-chan TaskResult, error) {
	task := &Task{
		ID:         id,
		Priority:   priority,
		Fn:         fn,
		Ctx:        ctx,
		MaxRetries: maxRetries,
		Backoff:    backoff,
		ResultCh:   make(chan TaskResult, 1),
	}
	return p.Submit(ctx, task)
}

func (p *DynamicWorkerPool) autoscaleLoop() {
	defer p.wg.Done()
	for {
		select {
		case <-p.stopScaleCh:
			return
		case <-p.scaleTicker.C:
			p.adjustWorkers()
		}
	}
}

func (p *DynamicWorkerPool) adjustWorkers() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	qLen := p.prioQueue.Len()
	currentWorkers := len(p.workers)

	if qLen > currentWorkers*2 && currentWorkers < p.maxWorkers {
		toAdd := currentWorkers
		if currentWorkers+toAdd > p.maxWorkers {
			toAdd = p.maxWorkers - currentWorkers
		}
		for i := 0; i < toAdd; i++ {
			id := p.nextWorkerID
			p.nextWorkerID++
			w := &Worker{
				id:        id,
				taskCh:    make(chan *Task),
				stopCh:    make(chan struct{}),
				pool:      p,
				startedAt: time.Now(),
			}
			p.workers[id] = w
			atomic.AddInt64(&p.metrics.ActiveWorkers, 1)
			atomic.AddInt64(&p.metrics.ScaleUpEvents, 1)
			p.wg.Add(1)
			go w.run()
		}
	} else if qLen == 0 && currentWorkers > p.minWorkers {
		toRemove := 1
		for id, w := range p.workers {
			if toRemove <= 0 || len(p.workers) <= p.minWorkers {
				break
			}
			close(w.stopCh)
			delete(p.workers, id)
			atomic.AddInt64(&p.metrics.ActiveWorkers, -1)
			atomic.AddInt64(&p.metrics.ScaleDownEvents, 1)
			toRemove--
		}
	}
}

func (p *DynamicWorkerPool) recordHistory(res TaskResult) {
	p.histMu.Lock()
	defer p.histMu.Unlock()
	p.history = append(p.history, res)
}

func (p *DynamicWorkerPool) addToDLQ(task *Task, result TaskResult, reason string) {
	p.dlqMu.Lock()
	defer p.dlqMu.Unlock()
	p.dlq = append(p.dlq, DeadLetterEntry{
		Task:      task,
		Result:    result,
		Timestamp: time.Now(),
		Reason:    reason,
	})
	atomic.AddInt64(&p.metrics.DLQTasks, 1)
}

func (p *DynamicWorkerPool) GetDLQ() []DeadLetterEntry {
	p.dlqMu.Lock()
	defer p.dlqMu.Unlock()
	cpy := make([]DeadLetterEntry, len(p.dlq))
	copy(cpy, p.dlq)
	return cpy
}

func (p *DynamicWorkerPool) ClearDLQ() int {
	p.dlqMu.Lock()
	defer p.dlqMu.Unlock()
	count := len(p.dlq)
	p.dlq = make([]DeadLetterEntry, 0)
	atomic.StoreInt64(&p.metrics.DLQTasks, 0)
	return count
}

func (p *DynamicWorkerPool) GetHistory() []TaskResult {
	p.histMu.Lock()
	defer p.histMu.Unlock()
	cpy := make([]TaskResult, len(p.history))
	copy(cpy, p.history)
	return cpy
}

func (p *DynamicWorkerPool) GetTopSlowTasks(n int) []TaskResult {
	p.histMu.Lock()
	defer p.histMu.Unlock()

	sorted := make([]TaskResult, len(p.history))
	copy(sorted, p.history)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Duration > sorted[j].Duration
	})

	if n > len(sorted) {
		n = len(sorted)
	}
	return sorted[:n]
}

func (p *DynamicWorkerPool) GetMetrics() PoolMetrics {
	return PoolMetrics{
		ActiveWorkers:   atomic.LoadInt64(&p.metrics.ActiveWorkers),
		QueuedTasks:     int64(p.prioQueue.Len()),
		CompletedTasks:  atomic.LoadInt64(&p.metrics.CompletedTasks),
		FailedTasks:     atomic.LoadInt64(&p.metrics.FailedTasks),
		TotalRetried:    atomic.LoadInt64(&p.metrics.TotalRetried),
		ScaleUpEvents:   atomic.LoadInt64(&p.metrics.ScaleUpEvents),
		ScaleDownEvents: atomic.LoadInt64(&p.metrics.ScaleDownEvents),
		DLQTasks:        atomic.LoadInt64(&p.metrics.DLQTasks),
	}
}

func (p *DynamicWorkerPool) Shutdown(timeout time.Duration) error {
	p.closedOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		for _, w := range p.workers {
			close(w.stopCh)
		}
		p.workers = make(map[int]*Worker)
		p.mu.Unlock()

		close(p.stopScaleCh)
		p.scaleTicker.Stop()
	})

	c := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(c)
	}()

	select {
	case <-c:
		return nil
	case <-time.After(timeout):
		return errors.New("shutdown timed out")
	}
}

func (p *DynamicWorkerPool) SetMinWorkers(n int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n <= 0 || n > p.maxWorkers {
		return ErrInvalidWorkerCount
	}
	p.minWorkers = n
	return nil
}

func (p *DynamicWorkerPool) SetMaxWorkers(n int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n < p.minWorkers {
		return ErrInvalidWorkerCount
	}
	p.maxWorkers = n
	return nil
}

func (p *DynamicWorkerPool) PurgeQueue() int {
	cleared := p.prioQueue.Clear()
	for _, task := range cleared {
		task.mu.Lock()
		if task.ResultCh != nil && !task.closed {
			task.ResultCh <- TaskResult{
				TaskID:    task.ID,
				Err:       ErrPoolClosed,
				Timestamp: time.Now(),
			}
			close(task.ResultCh)
			task.closed = true
		}
		task.mu.Unlock()
	}
	return len(cleared)
}

func (p *DynamicWorkerPool) HealthCheck() map[string]interface{} {
	m := p.GetMetrics()
	p.mu.RLock()
	defer p.mu.RUnlock()
	return map[string]interface{}{
		"active_workers":  m.ActiveWorkers,
		"queued_tasks":    m.QueuedTasks,
		"completed_tasks": m.CompletedTasks,
		"failed_tasks":    m.FailedTasks,
		"dlq_tasks":       m.DLQTasks,
		"min_workers":     p.minWorkers,
		"max_workers":     p.maxWorkers,
		"is_closed":       p.closed,
	}
}

func (p *DynamicWorkerPool) ExportStats() string {
	m := p.GetMetrics()
	return fmt.Sprintf(
		"WorkerPool Stats:\n"+
			"  Active Workers: %d\n"+
			"  Queued Tasks: %d\n"+
			"  Completed: %d\n"+
			"  Failed: %d\n"+
			"  Retried: %d\n"+
			"  DLQ: %d\n"+
			"  Scale Up Events: %d\n"+
			"  Scale Down Events: %d\n",
		m.ActiveWorkers, m.QueuedTasks, m.CompletedTasks,
		m.FailedTasks, m.TotalRetried, m.DLQTasks,
		m.ScaleUpEvents, m.ScaleDownEvents,
	)
}

func (p *DynamicWorkerPool) GetWorkerStatuses() []WorkerStatusInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()

	infos := make([]WorkerStatusInfo, 0, len(p.workers))
	for _, w := range p.workers {
		infos = append(infos, WorkerStatusInfo{
			WorkerID:  w.id,
			Busy:      w.isBusy,
			StartTime: w.startedAt,
			JobsDone:  w.GetJobsDone(),
		})
	}
	return infos
}

func (p *DynamicWorkerPool) ActiveWorkerCount() int {
	return int(atomic.LoadInt64(&p.metrics.ActiveWorkers))
}

func (p *DynamicWorkerPool) Drain(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for p.prioQueue.Len() > 0 {
		if time.Now().After(deadline) {
			return ErrTaskTimeout
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func (p *DynamicWorkerPool) WaitAll(timeout time.Duration) error {
	return p.Drain(timeout)
}

// A. TaskScheduler
type ScheduledTask struct {
	Task        *Task
	Priority    int
	Deadline    time.Time
	SubmittedAt time.Time
	Index       int
}

type TaskScheduler struct {
	heap       []*ScheduledTask
	mu         sync.Mutex
	pool       *DynamicWorkerPool
	deadlineCh chan struct{}
}

func NewTaskScheduler(pool *DynamicWorkerPool) *TaskScheduler {
	return &TaskScheduler{
		heap:       make([]*ScheduledTask, 0),
		pool:       pool,
		deadlineCh: make(chan struct{}),
	}
}

func (ts *TaskScheduler) Push(task *Task, priority int, deadline time.Time) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	st := &ScheduledTask{
		Task:        task,
		Priority:    priority,
		Deadline:    deadline,
		SubmittedAt: time.Now(),
		Index:       len(ts.heap),
	}
	ts.heap = append(ts.heap, st)
	ts.siftUp(len(ts.heap) - 1)
}

func (ts *TaskScheduler) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if ts.heap[parent].Priority >= ts.heap[i].Priority {
			break
		}
		ts.heap[parent], ts.heap[i] = ts.heap[i], ts.heap[parent]
		ts.heap[parent].Index = parent
		ts.heap[i].Index = i
		i = parent
	}
}

func (ts *TaskScheduler) Pop() *ScheduledTask {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if len(ts.heap) == 0 {
		return nil
	}
	st := ts.heap[0]
	lastIdx := len(ts.heap) - 1
	ts.heap[0] = ts.heap[lastIdx]
	ts.heap[0].Index = 0
	ts.heap[lastIdx] = nil
	ts.heap = ts.heap[:lastIdx]
	if len(ts.heap) > 0 {
		ts.siftDown(0)
	}
	return st
}

func (ts *TaskScheduler) siftDown(i int) {
	n := len(ts.heap)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		largest := left
		right := left + 1
		if right < n && ts.heap[right].Priority > ts.heap[left].Priority {
			largest = right
		}
		if ts.heap[i].Priority >= ts.heap[largest].Priority {
			break
		}
		ts.heap[i], ts.heap[largest] = ts.heap[largest], ts.heap[i]
		ts.heap[i].Index = i
		ts.heap[largest].Index = largest
		i = largest
	}
}

func (ts *TaskScheduler) Peek() *ScheduledTask {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if len(ts.heap) == 0 {
		return nil
	}
	return ts.heap[0]
}

func (ts *TaskScheduler) Remove(index int) *ScheduledTask {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if index < 0 || index >= len(ts.heap) {
		return nil
	}
	st := ts.heap[index]
	lastIdx := len(ts.heap) - 1
	if index != lastIdx {
		ts.heap[index] = ts.heap[lastIdx]
		ts.heap[index].Index = index
		ts.heap[lastIdx] = nil
		ts.heap = ts.heap[:lastIdx]
		parent := (index - 1) / 2
		if index > 0 && ts.heap[index].Priority > ts.heap[parent].Priority {
			ts.siftUp(index)
		} else {
			ts.siftDown(index)
		}
	} else {
		ts.heap[lastIdx] = nil
		ts.heap = ts.heap[:lastIdx]
	}
	return st
}

func (ts *TaskScheduler) Len() int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return len(ts.heap)
}

func (ts *TaskScheduler) Less(i, j int) bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.heap[i].Priority > ts.heap[j].Priority
}

func (ts *TaskScheduler) StartDeadlineMonitor() {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ts.deadlineCh:
				return
			case <-ticker.C:
				ts.mu.Lock()
				for i := 0; i < len(ts.heap); {
					if time.Now().After(ts.heap[i].Deadline) {
						ts.mu.Unlock()
						ts.Remove(i)
						ts.mu.Lock()
					} else {
						i++
					}
				}
				ts.mu.Unlock()
			}
		}
	}()
}

func (ts *TaskScheduler) StopDeadlineMonitor() {
	close(ts.deadlineCh)
}

// B. WorkerHealthMonitor
type WorkerHealth struct {
	WorkerID          int64
	LastHeartbeat     time.Time
	TaskStarted       time.Time
	IsStuck           bool
	ConsecutiveMisses int
}

type WorkerHealthMonitor struct {
	workers           map[int64]*WorkerHealth
	mu                sync.RWMutex
	heartbeatInterval time.Duration
	deadlockThreshold time.Duration
	monitorCh         chan int64
	pool              *DynamicWorkerPool
	stopCh            chan struct{}
}

func NewWorkerHealthMonitor(pool *DynamicWorkerPool, interval, threshold time.Duration) *WorkerHealthMonitor {
	return &WorkerHealthMonitor{
		workers:           make(map[int64]*WorkerHealth),
		heartbeatInterval: interval,
		deadlockThreshold: threshold,
		monitorCh:         make(chan int64, 100),
		pool:              pool,
		stopCh:            make(chan struct{}),
	}
}

func (m *WorkerHealthMonitor) RecordHeartbeat(workerID int64) {
	select {
	case m.monitorCh <- workerID:
	default:
	}
}

func (m *WorkerHealthMonitor) StartMonitoring() {
	ticker := time.NewTicker(m.heartbeatInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-m.stopCh:
				return
			case wid := <-m.monitorCh:
				m.mu.Lock()
				if h, ok := m.workers[wid]; ok {
					h.LastHeartbeat = time.Now()
					h.ConsecutiveMisses = 0
				} else {
					m.workers[wid] = &WorkerHealth{
						WorkerID:      wid,
						LastHeartbeat: time.Now(),
					}
				}
				m.mu.Unlock()
			case <-ticker.C:
				m.mu.Lock()
				now := time.Now()
				for _, h := range m.workers {
					if now.Sub(h.LastHeartbeat) > m.deadlockThreshold {
						h.IsStuck = true
						h.ConsecutiveMisses++
					}
				}
				m.mu.Unlock()
			}
		}
	}()
}

func (m *WorkerHealthMonitor) StopMonitoring() {
	close(m.stopCh)
}

func (m *WorkerHealthMonitor) IsWorkerHealthy(workerID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.workers[workerID]
	if !ok {
		return false
	}
	return !h.IsStuck
}

func (m *WorkerHealthMonitor) GetHealthReport() map[int64]*WorkerHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cpy := make(map[int64]*WorkerHealth, len(m.workers))
	for k, v := range m.workers {
		cpy[k] = &WorkerHealth{
			WorkerID:          v.WorkerID,
			LastHeartbeat:     v.LastHeartbeat,
			TaskStarted:       v.TaskStarted,
			IsStuck:           v.IsStuck,
			ConsecutiveMisses: v.ConsecutiveMisses,
		}
	}
	return cpy
}

func (m *WorkerHealthMonitor) DetectDeadlock(workerID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.workers[workerID]
	if !ok {
		return false
	}
	if h.TaskStarted.IsZero() {
		return false
	}
	return time.Now().Sub(h.TaskStarted) > m.deadlockThreshold
}

// C. TaskDependencyGraph
type DepNode struct {
	TaskID       string
	Task         *Task
	Dependencies []string
	Dependents   []string
	Status       string
	Result       *TaskResult
}

type TaskDependencyGraph struct {
	nodes map[string]*DepNode
	mu    sync.Mutex
}

func NewTaskDependencyGraph() *TaskDependencyGraph {
	return &TaskDependencyGraph{
		nodes: make(map[string]*DepNode),
	}
}

func (g *TaskDependencyGraph) AddNode(taskID string, task *Task, deps []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	depsCopy := make([]string, len(deps))
	copy(depsCopy, deps)
	
	g.nodes[taskID] = &DepNode{
		TaskID:       taskID,
		Task:         task,
		Dependencies: depsCopy,
		Dependents:   make([]string, 0),
		Status:       "pending",
	}
	
	for _, d := range depsCopy {
		if parent, ok := g.nodes[d]; ok {
			parent.Dependents = append(parent.Dependents, taskID)
		}
	}
}

func (g *TaskDependencyGraph) RemoveNode(taskID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	node, ok := g.nodes[taskID]
	if !ok {
		return
	}
	
	for _, pID := range node.Dependencies {
		if p, exists := g.nodes[pID]; exists {
			var newDeps []string
			for _, d := range p.Dependents {
				if d != taskID {
					newDeps = append(newDeps, d)
				}
			}
			p.Dependents = newDeps
		}
	}
	for _, childID := range node.Dependents {
		if c, exists := g.nodes[childID]; exists {
			var newDeps []string
			for _, d := range c.Dependencies {
				if d != taskID {
					newDeps = append(newDeps, d)
				}
			}
			c.Dependencies = newDeps
		}
	}
	
	delete(g.nodes, taskID)
}

func (g *TaskDependencyGraph) GetReadyTasks() []*DepNode {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	var ready []*DepNode
	for _, node := range g.nodes {
		if node.Status != "pending" {
			continue
		}
		allDepsMet := true
		for _, depID := range node.Dependencies {
			depNode, ok := g.nodes[depID]
			if !ok || depNode.Status != "completed" {
				allDepsMet = false
				break
			}
		}
		if allDepsMet {
			ready = append(ready, node)
		}
	}
	return ready
}

func (g *TaskDependencyGraph) MarkCompleted(taskID string, result *TaskResult) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if node, ok := g.nodes[taskID]; ok {
		node.Status = "completed"
		node.Result = result
	}
}

func (g *TaskDependencyGraph) MarkFailed(taskID string, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	var propagate func(id string)
	propagate = func(id string) {
		if node, ok := g.nodes[id]; ok && node.Status == "pending" {
			node.Status = "failed"
			for _, child := range node.Dependents {
				propagate(child)
			}
		}
	}
	
	if node, ok := g.nodes[taskID]; ok {
		node.Status = "failed"
		for _, child := range node.Dependents {
			propagate(child)
		}
	}
}

func (g *TaskDependencyGraph) HasCycle() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	visited := make(map[string]bool)
	recStack := make(map[string]bool)
	
	var isCyclic func(nodeID string) bool
	isCyclic = func(nodeID string) bool {
		if recStack[nodeID] {
			return true
		}
		if visited[nodeID] {
			return false
		}
		visited[nodeID] = true
		recStack[nodeID] = true
		
		if node, ok := g.nodes[nodeID]; ok {
			for _, dep := range node.Dependencies {
				if isCyclic(dep) {
					return true
				}
			}
		}
		recStack[nodeID] = false
		return false
	}
	
	for id := range g.nodes {
		if !visited[id] {
			if isCyclic(id) {
				return true
			}
		}
	}
	return false
}

func (g *TaskDependencyGraph) TopologicalSort() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	visited := make(map[string]bool)
	var sorted []string
	
	var visit func(id string)
	visit = func(id string) {
		if visited[id] {
			return
		}
		visited[id] = true
		if node, ok := g.nodes[id]; ok {
			for _, dep := range node.Dependencies {
				visit(dep)
			}
		}
		sorted = append(sorted, id)
	}
	
	for id := range g.nodes {
		visit(id)
	}
	return sorted
}

func (g *TaskDependencyGraph) ExecuteAll(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		ready := g.GetReadyTasks()
		if len(ready) == 0 {
			g.mu.Lock()
			allDone := true
			for _, n := range g.nodes {
				if n.Status == "pending" || n.Status == "running" {
					allDone = false
					break
				}
			}
			g.mu.Unlock()
			if allDone {
				break
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		
		for _, node := range ready {
			g.mu.Lock()
			node.Status = "running"
			g.mu.Unlock()
			
			go func(n *DepNode) {
				res, err := n.Task.Fn(ctx)
				if err != nil {
					g.MarkFailed(n.TaskID, err)
				} else {
					g.MarkCompleted(n.TaskID, &TaskResult{Data: res})
				}
			}(node)
		}
	}
	return nil
}

// D. ResultAggregator
type ResultAggregator struct {
	results  map[string]*TaskResult
	mu       sync.Mutex
	wg       sync.WaitGroup
	doneCh   chan struct{}
	errCh    chan error
	timeout  time.Duration
	expected map[string]bool
}

func NewResultAggregator(timeout time.Duration) *ResultAggregator {
	return &ResultAggregator{
		results:  make(map[string]*TaskResult),
		doneCh:   make(chan struct{}),
		errCh:    make(chan error, 100),
		timeout:  timeout,
		expected: make(map[string]bool),
	}
}

func (ra *ResultAggregator) ExpectResult(taskID string) {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	if !ra.expected[taskID] {
		ra.expected[taskID] = true
		ra.wg.Add(1)
	}
}

func (ra *ResultAggregator) SubmitResult(taskID string, result *TaskResult) {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	if ra.expected[taskID] {
		if _, exists := ra.results[taskID]; !exists {
			ra.results[taskID] = result
			ra.wg.Done()
		}
	}
}

func (ra *ResultAggregator) Wait() error {
	waitDone := make(chan struct{})
	go func() {
		ra.wg.Wait()
		close(waitDone)
	}()
	
	select {
	case <-waitDone:
		close(ra.doneCh)
		return nil
	case <-time.After(ra.timeout):
		return errors.New("timeout waiting for results")
	}
}

func (ra *ResultAggregator) GetResult(taskID string) (*TaskResult, bool) {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	res, ok := ra.results[taskID]
	return res, ok
}

func (ra *ResultAggregator) GetAllResults() map[string]*TaskResult {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	cpy := make(map[string]*TaskResult, len(ra.results))
	for k, v := range ra.results {
		cpy[k] = v
	}
	return cpy
}

func (ra *ResultAggregator) CollectErrors() []error {
	var errs []error
	for {
		select {
		case err := <-ra.errCh:
			errs = append(errs, err)
		default:
			return errs
		}
	}
}

func (ra *ResultAggregator) Reset() {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	ra.results = make(map[string]*TaskResult)
	ra.expected = make(map[string]bool)
	ra.doneCh = make(chan struct{})
	ra.errCh = make(chan error, 100)
}

// E. ExtendedPoolMetrics
type ExtendedPoolMetrics struct {
	totalSubmitted int64
	totalCompleted int64
	totalFailed    int64
	totalRetries   int64
	avgLatencyNs   int64
	peakWorkers    int64
	currentWorkers int64
	latencies      []int64
	mu             sync.Mutex
	startTime      time.Time
}

func NewExtendedPoolMetrics() *ExtendedPoolMetrics {
	return &ExtendedPoolMetrics{
		latencies: make([]int64, 0),
		startTime: time.Now(),
	}
}

func (m *ExtendedPoolMetrics) RecordSubmission() {
	atomic.AddInt64(&m.totalSubmitted, 1)
}

func (m *ExtendedPoolMetrics) RecordCompletion(latencyNs int64) {
	atomic.AddInt64(&m.totalCompleted, 1)
	m.mu.Lock()
	m.latencies = append(m.latencies, latencyNs)
	count := int64(len(m.latencies))
	m.mu.Unlock()
	
	for {
		oldAvg := atomic.LoadInt64(&m.avgLatencyNs)
		var newAvg int64
		if count == 1 {
			newAvg = latencyNs
		} else {
			newAvg = oldAvg + (latencyNs-oldAvg)/count
		}
		if atomic.CompareAndSwapInt64(&m.avgLatencyNs, oldAvg, newAvg) {
			break
		}
	}
}

func (m *ExtendedPoolMetrics) RecordFailure() {
	atomic.AddInt64(&m.totalFailed, 1)
}

func (m *ExtendedPoolMetrics) RecordRetry() {
	atomic.AddInt64(&m.totalRetries, 1)
}

func (m *ExtendedPoolMetrics) UpdateWorkerCount(current int64) {
	atomic.StoreInt64(&m.currentWorkers, current)
	for {
		peak := atomic.LoadInt64(&m.peakWorkers)
		if current <= peak {
			break
		}
		if atomic.CompareAndSwapInt64(&m.peakWorkers, peak, current) {
			break
		}
	}
}

func (m *ExtendedPoolMetrics) GetThroughput() float64 {
	completed := atomic.LoadInt64(&m.totalCompleted)
	elapsed := time.Since(m.startTime).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(completed) / elapsed
}

func (m *ExtendedPoolMetrics) GetUtilization() float64 {
	current := atomic.LoadInt64(&m.currentWorkers)
	peak := atomic.LoadInt64(&m.peakWorkers)
	if peak == 0 {
		return 0
	}
	return float64(current) / float64(peak)
}

func (m *ExtendedPoolMetrics) Snapshot() *ExtendedPoolMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	latCpy := make([]int64, len(m.latencies))
	copy(latCpy, m.latencies)
	
	return &ExtendedPoolMetrics{
		totalSubmitted: atomic.LoadInt64(&m.totalSubmitted),
		totalCompleted: atomic.LoadInt64(&m.totalCompleted),
		totalFailed:    atomic.LoadInt64(&m.totalFailed),
		totalRetries:   atomic.LoadInt64(&m.totalRetries),
		avgLatencyNs:   atomic.LoadInt64(&m.avgLatencyNs),
		peakWorkers:    atomic.LoadInt64(&m.peakWorkers),
		currentWorkers: atomic.LoadInt64(&m.currentWorkers),
		latencies:      latCpy,
		startTime:      m.startTime,
	}
}

func (m *ExtendedPoolMetrics) GetPercentileLatency(p float64) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if len(m.latencies) == 0 {
		return 0
	}
	
	sorted := make([]int64, len(m.latencies))
	copy(sorted, m.latencies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// F. WorkerPoolDashboard
type DashboardWidget struct {
	WidgetID   string
	WidgetType string
	Title      string
	Data       map[string]interface{}
	LastUpdate time.Time
}

type DashboardConfig struct {
	RefreshInterval time.Duration
	MaxWidgets      int
	Theme           string
	Layout          []string
}

type WorkerPoolDashboard struct {
	widgets map[string]*DashboardWidget
	config  DashboardConfig
	mu      sync.Mutex
	pool    *DynamicWorkerPool
	active  bool
	stopCh  chan struct{}
}

func NewWorkerPoolDashboard(pool *DynamicWorkerPool, cfg DashboardConfig) *WorkerPoolDashboard {
	return &WorkerPoolDashboard{
		widgets: make(map[string]*DashboardWidget),
		config:  cfg,
		pool:    pool,
		stopCh:  make(chan struct{}),
	}
}

func (d *WorkerPoolDashboard) AddWidget(id, wType, title string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.widgets[id] = &DashboardWidget{
		WidgetID:   id,
		WidgetType: wType,
		Title:      title,
		Data:       make(map[string]interface{}),
		LastUpdate: time.Now(),
	}
}

func (d *WorkerPoolDashboard) RemoveWidget(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.widgets, id)
}

func (d *WorkerPoolDashboard) UpdateWidgetData(id string, data map[string]interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if w, ok := d.widgets[id]; ok {
		for k, v := range data {
			w.Data[k] = v
		}
		w.LastUpdate = time.Now()
	}
}

func (d *WorkerPoolDashboard) GetWidget(id string) (*DashboardWidget, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if w, ok := d.widgets[id]; ok {
		cpy := &DashboardWidget{
			WidgetID:   w.WidgetID,
			WidgetType: w.WidgetType,
			Title:      w.Title,
			Data:       make(map[string]interface{}),
			LastUpdate: w.LastUpdate,
		}
		for k, v := range w.Data {
			cpy.Data[k] = v
		}
		return cpy, true
	}
	return nil, false
}

func (d *WorkerPoolDashboard) GetAllWidgets() []*DashboardWidget {
	d.mu.Lock()
	defer d.mu.Unlock()
	var list []*DashboardWidget
	for _, w := range d.widgets {
		cpy := &DashboardWidget{
			WidgetID:   w.WidgetID,
			WidgetType: w.WidgetType,
			Title:      w.Title,
			Data:       make(map[string]interface{}),
			LastUpdate: w.LastUpdate,
		}
		for k, v := range w.Data {
			cpy.Data[k] = v
		}
		list = append(list, cpy)
	}
	return list
}

func (d *WorkerPoolDashboard) StartAutoRefresh() {
	d.mu.Lock()
	if d.active {
		d.mu.Unlock()
		return
	}
	d.active = true
	d.stopCh = make(chan struct{})
	d.mu.Unlock()

	go func() {
		ticker := time.NewTicker(d.config.RefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-d.stopCh:
				return
			case <-ticker.C:
				d.pool.mu.RLock()
				count := len(d.pool.workers)
				d.pool.mu.RUnlock()

				d.mu.Lock()
				for _, w := range d.widgets {
					if w.WidgetType == "metrics" {
						w.Data["active_workers"] = count
					}
					w.LastUpdate = time.Now()
				}
				d.mu.Unlock()
			}
		}
	}()
}

func (d *WorkerPoolDashboard) StopAutoRefresh() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.active {
		d.active = false
		close(d.stopCh)
	}
}

func (d *WorkerPoolDashboard) GenerateHTMLReport() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	html := "<html><body><h1>Dashboard</h1>"
	for _, w := range d.widgets {
		html += "<div><h2>" + w.Title + "</h2>"
		html += "<p>Type: " + w.WidgetType + "</p>"
		html += "<ul>"
		for k, v := range w.Data {
			html += fmt.Sprintf("<li>%s: %v</li>", k, v)
		}
		html += "</ul></div>"
	}
	html += "</body></html>"
	return html
}

func (d *WorkerPoolDashboard) GenerateJSONReport() string {
	return "{}"
}

func (d *WorkerPoolDashboard) GenerateCSVReport() string {
	return ""
}

func (d *WorkerPoolDashboard) ClearAll() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.widgets = make(map[string]*DashboardWidget)
}
`,
}
