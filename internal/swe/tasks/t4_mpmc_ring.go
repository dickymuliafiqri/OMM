package tasks

import "benchmark/internal/swe"

var TaskT4MPMCRing = &swe.Task{
	ID:       "swe-t4-mpmc-ring-01",
	Title:    "Multi-Producer Multi-Consumer Lock-Free Bounded Ring Buffer",
	Tier:     swe.TierStaff,
	Points:   25,
	Category: "lock_free_concurrency",
	IssueBody: `### Bug Report: Sequence Wraparound, Stale Cell Read, and Race Conditions in Lock-Free MPMC Ring

**Environment:** Go 1.24+ ultra-high throughput lock-free ring buffer engine for low-latency financial messaging.

**Expected Behavior:**

- The engine must be **STRICTLY LOCK-FREE**. Usage of ` + "`sync.Mutex`" + `, ` + "`sync.RWMutex`" + `, or Go channels in the core implementation (` + "`main.go`" + `) is strictly prohibited and guarded by static verification.

- Sequence counters must never suffer from 32-bit truncation or premature wraparound, correctly supporting continuous streaming under high contention.

- Cell value publishing must ensure full release-acquire memory barriers: consumers must never observe stale or uninitialized payload data.

- Batch reservations (` + "`PopBatch`" + ` and ` + "`PushBatch`" + `) must guarantee that all claimed slots are published and valid without index overlapping or data loss across concurrent consumers.

- State checks (` + "`Len`" + `, ` + "`IsEmpty`" + `, ` + "`IsFull`" + `) must return consistent and linearizable bounds without spurious empty/full conditions during heavy producer-consumer churn.

- All tests must pass with zero race warnings under ` + "`go test -race`" + `.`,

	BrokenCode: `package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

var (
	ErrBufferFull         = errors.New("mpmc ring buffer is full")
	ErrBufferEmpty        = errors.New("mpmc ring buffer is empty")
	ErrBufferClosed       = errors.New("mpmc ring buffer is closed")
	ErrTimeout            = errors.New("operation timed out")
	ErrInvalidCapacity    = errors.New("capacity must be a positive power of two")
	ErrBatchTooLarge      = errors.New("batch size exceeds buffer capacity")
	ErrNilDestination     = errors.New("destination slice cannot be nil or empty")
	ErrPartitionNotFound  = errors.New("specified partition does not exist")
	ErrCorruptedMessage   = errors.New("message integrity check failed")
	ErrChecksumMismatch   = errors.New("payload checksum mismatch detected")
)

func NextPowerOfTwo(v uint64) uint64 {
	if v == 0 {
		return 1
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}

func IsPowerOfTwo(v uint64) bool {
	return v != 0 && (v&(v-1)) == 0
}

type WaitStrategyType uint8

const (
	WaitStrategySpin WaitStrategyType = 1
	WaitStrategyYield WaitStrategyType = 2
	WaitStrategySleep WaitStrategyType = 3
	WaitStrategyAdaptive WaitStrategyType = 4
)

type MessagePriority uint8

const (
	PriorityLow MessagePriority = 1
	PriorityNormal MessagePriority = 2
	PriorityHigh MessagePriority = 3
	PriorityCritical MessagePriority = 4
)

func (p MessagePriority) String() string {
	switch p {
	case PriorityLow:
		return "LOW"
	case PriorityNormal:
		return "NORMAL"
	case PriorityHigh:
		return "HIGH"
	case PriorityCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

type RingMessage struct {
	MsgID     uint64
	Timestamp int64
	Topic     string
	Payload   []byte
	Flags     uint32
	Checksum  uint32
}

func ComputeChecksum(payload []byte) uint32 {
	h := fnv.New32a()
	_, _ = h.Write(payload)
	return h.Sum32()
}

func NewRingMessage(id uint64, topic string, payload []byte, flags uint32) RingMessage {
	cpPayload := make([]byte, len(payload))
	copy(cpPayload, payload)
	return RingMessage{
		MsgID:     id,
		Timestamp: time.Now().UnixNano(),
		Topic:     topic,
		Payload:   cpPayload,
		Flags:     flags,
		Checksum:  ComputeChecksum(cpPayload),
	}
}

func (m RingMessage) Validate() error {
	if m.MsgID == 0 {
		return errors.New("msg id cannot be zero")
	}
	if strings.TrimSpace(m.Topic) == "" {
		return errors.New("topic cannot be empty")
	}
	expected := ComputeChecksum(m.Payload)
	if m.Checksum != expected {
		return ErrChecksumMismatch
	}
	return nil
}

func (m RingMessage) String() string {
	return fmt.Sprintf("RingMessage[id=%d topic=%s len=%d flags=%d]", m.MsgID, m.Topic, len(m.Payload), m.Flags)
}

type TelemetryFrame struct {
	DeviceID  string
	SensorID  uint32
	Reading   float64
	Timestamp int64
	SeqNum    uint64
}

func NewTelemetryFrame(dev string, sensor uint32, val float64, seq uint64) TelemetryFrame {
	return TelemetryFrame{
		DeviceID:  dev,
		SensorID:  sensor,
		Reading:   val,
		Timestamp: time.Now().UnixNano(),
		SeqNum:    seq,
	}
}

func (f TelemetryFrame) Validate() error {
	if strings.TrimSpace(f.DeviceID) == "" {
		return errors.New("device id cannot be empty")
	}
	if math.IsNaN(f.Reading) || math.IsInf(f.Reading, 0) {
		return errors.New("invalid reading float value")
	}
	return nil
}

type OrderBookEvent struct {
	OrderID   uint64
	Symbol    string
	Side      uint8 // 1=Buy, 2=Sell
	Price     int64 // in basis points/cents
	Quantity  int64
	Timestamp int64
}

func NewOrderBookEvent(id uint64, sym string, side uint8, price, qty int64) OrderBookEvent {
	return OrderBookEvent{
		OrderID:   id,
		Symbol:    sym,
		Side:      side,
		Price:     price,
		Quantity:  qty,
		Timestamp: time.Now().UnixNano(),
	}
}

func (o OrderBookEvent) Validate() error {
	if o.OrderID == 0 {
		return errors.New("order id cannot be zero")
	}
	if strings.TrimSpace(o.Symbol) == "" {
		return errors.New("symbol cannot be empty")
	}
	if o.Side != 1 && o.Side != 2 {
		return errors.New("side must be 1 (buy) or 2 (sell)")
	}
	if o.Price <= 0 {
		return errors.New("price must be positive")
	}
	if o.Quantity <= 0 {
		return errors.New("quantity must be positive")
	}
	return nil
}

type BackpressureController struct {
	spinLimit  int
	yieldLimit int
	strategy   WaitStrategyType
}

func NewBackpressureController(strategy WaitStrategyType) *BackpressureController {
	return &BackpressureController{
		spinLimit:  100,
		yieldLimit: 500,
		strategy:   strategy,
	}
}

func (bc *BackpressureController) Wait(attempt int) {
	switch bc.strategy {
	case WaitStrategySpin:
		// CPU pause spin loop
		for i := 0; i < 10; i++ {
			_ = i * 2
		}
	case WaitStrategyYield:
		runtime.Gosched()
	case WaitStrategySleep:
		time.Sleep(10 * time.Microsecond)
	case WaitStrategyAdaptive:
		if attempt < bc.spinLimit {
			for i := 0; i < 5; i++ {
				_ = i * 2
			}
		} else if attempt < bc.yieldLimit {
			runtime.Gosched()
		} else {
			time.Sleep(50 * time.Microsecond)
		}
	}
}

type RingMetricsSnapshot struct {
	TotalPushed       uint64
	TotalPopped       uint64
	DropCount         uint64
	FullContention    uint64
	EmptyContention   uint64
	BatchPushCount    uint64
	BatchPopCount     uint64
	SpinCount         uint64
	CurrentOccupancy  uint64
}

type RingMetricsCollector struct {
	totalPushed     atomic.Uint64
	totalPopped     atomic.Uint64
	dropCount       atomic.Uint64
	fullContention  atomic.Uint64
	emptyContention atomic.Uint64
	batchPushCount  atomic.Uint64
	batchPopCount   atomic.Uint64
	spinCount       atomic.Uint64
}

func NewRingMetricsCollector() *RingMetricsCollector {
	return &RingMetricsCollector{}
}

func (m *RingMetricsCollector) RecordPush() {
	m.totalPushed.Add(1)
}

func (m *RingMetricsCollector) RecordPop() {
	m.totalPopped.Add(1)
}

func (m *RingMetricsCollector) RecordDrop() {
	m.dropCount.Add(1)
}

func (m *RingMetricsCollector) RecordFullContention() {
	m.fullContention.Add(1)
}

func (m *RingMetricsCollector) RecordEmptyContention() {
	m.emptyContention.Add(1)
}

func (m *RingMetricsCollector) RecordBatchPush(count int) {
	m.totalPushed.Add(uint64(count))
	m.batchPushCount.Add(1)
}

func (m *RingMetricsCollector) RecordBatchPop(count int) {
	m.totalPopped.Add(uint64(count))
	m.batchPopCount.Add(1)
}

func (m *RingMetricsCollector) RecordSpin() {
	m.spinCount.Add(1)
}

func (m *RingMetricsCollector) Snapshot() RingMetricsSnapshot {
	pushed := m.totalPushed.Load()
	popped := m.totalPopped.Load()
	occ := uint64(0)
	if pushed > popped {
		occ = pushed - popped
	}
	return RingMetricsSnapshot{
		TotalPushed:      pushed,
		TotalPopped:      popped,
		DropCount:        m.dropCount.Load(),
		FullContention:   m.fullContention.Load(),
		EmptyContention:  m.emptyContention.Load(),
		BatchPushCount:   m.batchPushCount.Load(),
		BatchPopCount:    m.batchPopCount.Load(),
		SpinCount:        m.spinCount.Load(),
		CurrentOccupancy: occ,
	}
}

func (s RingMetricsSnapshot) Format() string {
	return fmt.Sprintf("RingMetrics: pushed=%d popped=%d occ=%d drop=%d full=%d empty=%d bPush=%d bPop=%d",
		s.TotalPushed, s.TotalPopped, s.CurrentOccupancy, s.DropCount, s.FullContention, s.EmptyContention, s.BatchPushCount, s.BatchPopCount)
}

// RingCell with 32-bit truncation bug and no memory barriers
type RingCell[T any] struct {
	sequence uint32
	data     T
}

// MPMCRingBuffer implements a bounded multi-producer multi-consumer ring
type MPMCRingBuffer[T any] struct {
	buffer       []RingCell[T]
	capacity     uint64
	mask         uint64
	head         atomic.Uint32 // Bug 1: 32-bit counter wraparound
	tail         atomic.Uint32 // Bug 1: 32-bit counter wraparound
	metrics      *RingMetricsCollector
	backpressure *BackpressureController
	closed       atomic.Bool
}

func NewMPMCRingBuffer[T any](capacity uint64) (*MPMCRingBuffer[T], error) {
	if !IsPowerOfTwo(capacity) {
		return nil, ErrInvalidCapacity
	}
	buf := make([]RingCell[T], capacity)
	for i := uint64(0); i < capacity; i++ {
		buf[i].sequence = uint32(i)
	}
	ring := &MPMCRingBuffer[T]{
		buffer:       buf,
		capacity:     capacity,
		mask:         capacity - 1,
		metrics:      NewRingMetricsCollector(),
		backpressure: NewBackpressureController(WaitStrategyAdaptive),
	}
	return ring, nil
}

func (r *MPMCRingBuffer[T]) Capacity() uint64 {
	return r.capacity
}

func (r *MPMCRingBuffer[T]) Len() uint64 {
	// Bug 4: Wrong read order creates spurious empty/full states
	t := uint64(r.tail.Load())
	h := uint64(r.head.Load())
	if t >= h {
		return t - h
	}
	return 0
}

func (r *MPMCRingBuffer[T]) IsEmpty() bool {
	t := r.tail.Load()
	h := r.head.Load()
	return t == h
}

func (r *MPMCRingBuffer[T]) IsFull() bool {
	t := r.tail.Load()
	h := r.head.Load()
	return uint64(t-h) >= r.capacity
}

func (r *MPMCRingBuffer[T]) TryPush(val T) (bool, error) {
	if r.closed.Load() {
		return false, ErrBufferClosed
	}
	pos := r.tail.Load()
	cell := &r.buffer[pos&uint32(r.mask)]
	seq := cell.sequence
	dif := int32(seq) - int32(pos)
	if dif == 0 {
		if r.tail.CompareAndSwap(pos, pos+1) {
			// Bug 2: publishing sequence BEFORE data is stored, causing race and stale reads
			cell.sequence = pos + 1
			cell.data = val
			r.metrics.RecordPush()
			return true, nil
		}
	}
	return false, ErrBufferFull
}

func (r *MPMCRingBuffer[T]) Push(val T) error {
	attempts := 0
	for {
		if r.closed.Load() {
			return ErrBufferClosed
		}
		ok, _ := r.TryPush(val)
		if ok {
			return nil
		}
		attempts++
		r.metrics.RecordSpin()
		r.backpressure.Wait(attempts)
	}
}

func (r *MPMCRingBuffer[T]) TryPop() (T, bool, error) {
	var zero T
	if r.closed.Load() && r.IsEmpty() {
		return zero, false, ErrBufferClosed
	}
	pos := r.head.Load()
	cell := &r.buffer[pos&uint32(r.mask)]
	// Bug 2: Read data before validating sequence barrier
	val := cell.data
	seq := cell.sequence
	dif := int32(seq) - int32(pos+1)
	if dif == 0 {
		if r.head.CompareAndSwap(pos, pos+1) {
			cell.sequence = pos + uint32(r.capacity)
			r.metrics.RecordPop()
			return val, true, nil
		}
	}
	return zero, false, ErrBufferEmpty
}

func (r *MPMCRingBuffer[T]) Pop() (T, error) {
	var zero T
	attempts := 0
	for {
		if r.closed.Load() && r.IsEmpty() {
			return zero, ErrBufferClosed
		}
		val, ok, _ := r.TryPop()
		if ok {
			return val, nil
		}
		attempts++
		r.metrics.RecordSpin()
		r.backpressure.Wait(attempts)
	}
}

func (r *MPMCRingBuffer[T]) PushBatch(items []T) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}
	if uint64(len(items)) > r.capacity {
		return 0, ErrBatchTooLarge
	}
	succeeded := 0
	for _, item := range items {
		err := r.Push(item)
		if err != nil {
			break
		}
		succeeded++
	}
	return succeeded, nil
}

func (r *MPMCRingBuffer[T]) PopBatch(dest []T) (int, error) {
	if len(dest) == 0 {
		return 0, ErrNilDestination
	}
	// Bug 3: advancing head eagerly without checking if all cells are available
	k := uint32(len(dest))
	startPos := r.head.Add(k) - k
	for i := uint32(0); i < k; i++ {
		pos := startPos + i
		cell := &r.buffer[pos&uint32(r.mask)]
		dest[i] = cell.data
		cell.sequence = pos + uint32(r.capacity)
	}
	r.metrics.RecordBatchPop(int(k))
	return int(k), nil
}

func (r *MPMCRingBuffer[T]) PushTimeout(val T, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	attempts := 0
	for {
		if r.closed.Load() {
			return ErrBufferClosed
		}
		ok, _ := r.TryPush(val)
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return ErrTimeout
		}
		attempts++
		r.backpressure.Wait(attempts)
	}
}

func (r *MPMCRingBuffer[T]) PopTimeout(timeout time.Duration) (T, error) {
	var zero T
	deadline := time.Now().Add(timeout)
	attempts := 0
	for {
		if r.closed.Load() && r.IsEmpty() {
			return zero, ErrBufferClosed
		}
		val, ok, _ := r.TryPop()
		if ok {
			return val, nil
		}
		if time.Now().After(deadline) {
			return zero, ErrTimeout
		}
		attempts++
		r.backpressure.Wait(attempts)
	}
}

func (r *MPMCRingBuffer[T]) Close() {
	r.closed.Store(true)
}

func (r *MPMCRingBuffer[T]) IsClosed() bool {
	return r.closed.Load()
}

func (r *MPMCRingBuffer[T]) Reset() {
	r.head.Store(0)
	r.tail.Store(0)
	for i := uint64(0); i < r.capacity; i++ {
		r.buffer[i].sequence = uint32(i)
	}
	r.closed.Store(false)
}

type PartitionedMPMCRing[T any] struct {
	partitions []*MPMCRingBuffer[T]
	numParts   uint64
	mask       uint64
	roundRobin atomic.Uint64
}

func NewPartitionedMPMCRing[T any](numPartitions int, partitionCap uint64) (*PartitionedMPMCRing[T], error) {
	if numPartitions <= 0 {
		return nil, errors.New("number of partitions must be positive")
	}
	parts := make([]*MPMCRingBuffer[T], numPartitions)
	for i := 0; i < numPartitions; i++ {
		r, err := NewMPMCRingBuffer[T](partitionCap)
		if err != nil {
			return nil, err
		}
		parts[i] = r
	}
	return &PartitionedMPMCRing[T]{
		partitions: parts,
		numParts:   uint64(numPartitions),
		mask:       uint64(numPartitions) - 1,
	}, nil
}

func (p *PartitionedMPMCRing[T]) PushKey(key string, val T) error {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	idx := uint64(h.Sum32()) % p.numParts
	return p.partitions[idx].Push(val)
}

func (p *PartitionedMPMCRing[T]) Push(val T) error {
	idx := p.roundRobin.Add(1) % p.numParts
	return p.partitions[idx].Push(val)
}

func (p *PartitionedMPMCRing[T]) Pop() (T, error) {
	var zero T
	attempts := 0
	for {
		start := p.roundRobin.Add(1) % p.numParts
		for i := uint64(0); i < p.numParts; i++ {
			idx := (start + i) % p.numParts
			val, ok, err := p.partitions[idx].TryPop()
			if ok {
				return val, nil
			}
			if err != nil && err != ErrBufferEmpty {
				return zero, err
			}
		}
		attempts++
		runtime.Gosched()
	}
}

func (p *PartitionedMPMCRing[T]) TotalCapacity() uint64 {
	total := uint64(0)
	for _, part := range p.partitions {
		total += part.Capacity()
	}
	return total
}

func (p *PartitionedMPMCRing[T]) TotalLen() uint64 {
	total := uint64(0)
	for _, part := range p.partitions {
		total += part.Len()
	}
	return total
}

type RingPipelineProcessor struct {
	ring          *MPMCRingBuffer[RingMessage]
	activeWorkers atomic.Int32
	totalHandled  atomic.Uint64
}

func NewRingPipelineProcessor(cap uint64) (*RingPipelineProcessor, error) {
	r, err := NewMPMCRingBuffer[RingMessage](cap)
	if err != nil {
		return nil, err
	}
	return &RingPipelineProcessor{ring: r}, nil
}

func (proc *RingPipelineProcessor) Submit(msg RingMessage) error {
	if err := msg.Validate(); err != nil {
		return err
	}
	return proc.ring.Push(msg)
}

func (proc *RingPipelineProcessor) ProcessOne() (RingMessage, error) {
	msg, err := proc.ring.Pop()
	if err != nil {
		return RingMessage{}, err
	}
	proc.totalHandled.Add(1)
	return msg, nil
}

func (proc *RingPipelineProcessor) HandledCount() uint64 {
	return proc.totalHandled.Load()
}

type TelemetryStreamAggregator struct {
	ring         *MPMCRingBuffer[TelemetryFrame]
	sensorCounts [16]atomic.Uint64
}

func NewTelemetryStreamAggregator(cap uint64) (*TelemetryStreamAggregator, error) {
	r, err := NewMPMCRingBuffer[TelemetryFrame](cap)
	if err != nil {
		return nil, err
	}
	return &TelemetryStreamAggregator{ring: r}, nil
}

func (agg *TelemetryStreamAggregator) Ingest(frame TelemetryFrame) error {
	if err := frame.Validate(); err != nil {
		return err
	}
	idx := frame.SensorID % 16
	agg.sensorCounts[idx].Add(1)
	return agg.ring.Push(frame)
}

func (agg *TelemetryStreamAggregator) Consume() (TelemetryFrame, error) {
	return agg.ring.Pop()
}

func (agg *TelemetryStreamAggregator) CountForSensor(sensorID uint32) uint64 {
	return agg.sensorCounts[sensorID%16].Load()
}

type OrderMatchingEngineRing struct {
	ring       *MPMCRingBuffer[OrderBookEvent]
	totalBuy   atomic.Uint64
	totalSell  atomic.Uint64
	volume     atomic.Int64
}

func NewOrderMatchingEngineRing(cap uint64) (*OrderMatchingEngineRing, error) {
	r, err := NewMPMCRingBuffer[OrderBookEvent](cap)
	if err != nil {
		return nil, err
	}
	return &OrderMatchingEngineRing{ring: r}, nil
}

func (ome *OrderMatchingEngineRing) SubmitOrder(order OrderBookEvent) error {
	if err := order.Validate(); err != nil {
		return err
	}
	if order.Side == 1 {
		ome.totalBuy.Add(1)
	} else {
		ome.totalSell.Add(1)
	}
	ome.volume.Add(order.Price * order.Quantity)
	return ome.ring.Push(order)
}

func (ome *OrderMatchingEngineRing) PollOrder() (OrderBookEvent, error) {
	return ome.ring.Pop()
}

func (ome *OrderMatchingEngineRing) OrderStats() (uint64, uint64, int64) {
	return ome.totalBuy.Load(), ome.totalSell.Load(), ome.volume.Load()
}

// BatchBufferPool implements a lock-free memory recycling pool for slice batches
type PoolNode[T any] struct {
	slice []T
	next  atomic.Pointer[PoolNode[T]]
}

type BatchBufferPool[T any] struct {
	head        atomic.Pointer[PoolNode[T]]
	sliceCap    int
	allocations atomic.Uint64
	recycles    atomic.Uint64
}

func NewBatchBufferPool[T any](sliceCap int) *BatchBufferPool[T] {
	return &BatchBufferPool[T]{
		sliceCap: sliceCap,
	}
}

func (p *BatchBufferPool[T]) Get() []T {
	for {
		oldHead := p.head.Load()
		if oldHead == nil {
			p.allocations.Add(1)
			return make([]T, 0, p.sliceCap)
		}
		next := oldHead.next.Load()
		if p.head.CompareAndSwap(oldHead, next) {
			return oldHead.slice[:0]
		}
	}
}

func (p *BatchBufferPool[T]) Put(slice []T) {
	if cap(slice) < p.sliceCap {
		return
	}
	p.recycles.Add(1)
	node := &PoolNode[T]{slice: slice}
	for {
		oldHead := p.head.Load()
		node.next.Store(oldHead)
		if p.head.CompareAndSwap(oldHead, node) {
			return
		}
	}
}

func (p *BatchBufferPool[T]) Stats() (uint64, uint64) {
	return p.allocations.Load(), p.recycles.Load()
}
// LockFreeRingSnapshotter safely extracts a consistent view of active elements
type LockFreeRingSnapshotter[T any] struct {
	ring *MPMCRingBuffer[T]
}

func NewLockFreeRingSnapshotter[T any](ring *MPMCRingBuffer[T]) *LockFreeRingSnapshotter[T] {
	return &LockFreeRingSnapshotter[T]{ring: ring}
}

func (s *LockFreeRingSnapshotter[T]) Snapshot() []T {
	h := uint64(s.ring.head.Load())
	t := uint64(s.ring.tail.Load())
	if t <= h {
		return nil
	}
	count := t - h
	if count > s.ring.capacity {
		count = s.ring.capacity
	}
	res := make([]T, 0, count)
	for i := uint64(0); i < count; i++ {
		pos := h + i
		cell := &s.ring.buffer[pos&s.ring.mask]
		res = append(res, cell.data)
	}
	return res
}

// RingCursor provides forward iteration over ring elements
type RingCursor[T any] struct {
	ring       *MPMCRingBuffer[T]
	currentPos uint64
	endPos     uint64
}

func (s *LockFreeRingSnapshotter[T]) NewCursor() *RingCursor[T] {
	h := uint64(s.ring.head.Load())
	t := uint64(s.ring.tail.Load())
	return &RingCursor[T]{
		ring:       s.ring,
		currentPos: h,
		endPos:     t,
	}
}

func (c *RingCursor[T]) HasNext() bool {
	return c.currentPos < c.endPos
}

func (c *RingCursor[T]) Next() (T, bool) {
	var zero T
	if !c.HasNext() {
		return zero, false
	}
	pos := c.currentPos
	c.currentPos++
	cell := &c.ring.buffer[pos&c.ring.mask]
	return cell.data, true
}

// RingStreamMultiplexer fans out events to multiple subscriber rings
type RingStreamMultiplexer[T any] struct {
	subscribers []*MPMCRingBuffer[T]
	numSubs     uint64
	dropOnFull  atomic.Bool
	totalSent   atomic.Uint64
	totalDrops  atomic.Uint64
}

func NewRingStreamMultiplexer[T any](subs []*MPMCRingBuffer[T], dropOnFull bool) (*RingStreamMultiplexer[T], error) {
	if len(subs) == 0 {
		return nil, errors.New("must provide at least one subscriber ring")
	}
	m := &RingStreamMultiplexer[T]{
		subscribers: subs,
		numSubs:     uint64(len(subs)),
	}
	m.dropOnFull.Store(dropOnFull)
	return m, nil
}

func (m *RingStreamMultiplexer[T]) Broadcast(item T) (int, error) {
	sent := 0
	for _, sub := range m.subscribers {
		if m.dropOnFull.Load() {
			ok, _ := sub.TryPush(item)
			if ok {
				sent++
			} else {
				m.totalDrops.Add(1)
			}
		} else {
			err := sub.Push(item)
			if err != nil {
				return sent, err
			}
			sent++
		}
	}
	m.totalSent.Add(uint64(sent))
	return sent, nil
}

func (m *RingStreamMultiplexer[T]) Stats() (uint64, uint64) {
	return m.totalSent.Load(), m.totalDrops.Load()
}

// RingStreamDemultiplexer aggregates events from multiple producer rings
type RingStreamDemultiplexer[T any] struct {
	sources       []*MPMCRingBuffer[T]
	numSources    uint64
	roundRobinIdx atomic.Uint64
	totalReceived atomic.Uint64
}

func NewRingStreamDemultiplexer[T any](sources []*MPMCRingBuffer[T]) (*RingStreamDemultiplexer[T], error) {
	if len(sources) == 0 {
		return nil, errors.New("must provide at least one source ring")
	}
	return &RingStreamDemultiplexer[T]{
		sources:    sources,
		numSources: uint64(len(sources)),
	}, nil
}

func (d *RingStreamDemultiplexer[T]) Poll() (T, bool, error) {
	var zero T
	start := d.roundRobinIdx.Add(1) % d.numSources
	for i := uint64(0); i < d.numSources; i++ {
		idx := (start + i) % d.numSources
		val, ok, err := d.sources[idx].TryPop()
		if ok {
			d.totalReceived.Add(1)
			return val, true, nil
		}
		if err != nil && err != ErrBufferEmpty && err != ErrBufferClosed {
			return zero, false, err
		}
	}
	return zero, false, nil
}

func (d *RingStreamDemultiplexer[T]) TotalReceived() uint64 {
	return d.totalReceived.Load()
}

// RingHealthInspector monitors ring latency, skew, and throughput
type RingHealthReport struct {
	Capacity      uint64
	CurrentLen    uint64
	HeadPosition  uint64
	TailPosition  uint64
	SequenceSkew  uint64
	IsFull        bool
	IsEmpty       bool
	IsClosed      bool
	DropCount     uint64
	ContentionOps uint64
}

type RingHealthInspector[T any] struct {
	ring *MPMCRingBuffer[T]
}

func NewRingHealthInspector[T any](ring *MPMCRingBuffer[T]) *RingHealthInspector[T] {
	return &RingHealthInspector[T]{ring: ring}
}

func (ins *RingHealthInspector[T]) Inspect() RingHealthReport {
	h := uint64(ins.ring.head.Load())
	t := uint64(ins.ring.tail.Load())
	snap := ins.ring.metrics.Snapshot()
	skew := uint64(0)
	if t > h {
		skew = t - h
	}
	return RingHealthReport{
		Capacity:      ins.ring.capacity,
		CurrentLen:    ins.ring.Len(),
		HeadPosition:  uint64(h),
		TailPosition:  uint64(t),
		SequenceSkew:  skew,
		IsFull:        ins.ring.IsFull(),
		IsEmpty:       ins.ring.IsEmpty(),
		IsClosed:      ins.ring.IsClosed(),
		DropCount:     snap.DropCount,
		ContentionOps: snap.FullContention + snap.EmptyContention,
	}
}

func (r RingHealthReport) String() string {
	return fmt.Sprintf("Health: len=%d/%d head=%d tail=%d skew=%d full=%v empty=%v drops=%d contentions=%d",
		r.CurrentLen, r.Capacity, r.HeadPosition, r.TailPosition, r.SequenceSkew, r.IsFull, r.IsEmpty, r.DropCount, r.ContentionOps)
}

// DisruptorPipelineStage coordinates sequential lock-free event pipelines
type DisruptorPipelineStage[T any] struct {
	stageID       string
	inRing        *MPMCRingBuffer[T]
	outRing       *MPMCRingBuffer[T]
	processedRows atomic.Uint64
	errorRows     atomic.Uint64
}

func NewDisruptorPipelineStage[T any](id string, in, out *MPMCRingBuffer[T]) *DisruptorPipelineStage[T] {
	return &DisruptorPipelineStage[T]{
		stageID: id,
		inRing:  in,
		outRing: out,
	}
}

func (s *DisruptorPipelineStage[T]) ProcessStep(transform func(in T) (T, error)) error {
	item, err := s.inRing.Pop()
	if err != nil {
		return err
	}
	outItem, err := transform(item)
	if err != nil {
		s.errorRows.Add(1)
		return err
	}
	if s.outRing != nil {
		err := s.outRing.Push(outItem)
		if err != nil {
			s.errorRows.Add(1)
			return err
		}
	}
	s.processedRows.Add(1)
	return nil
}

func (s *DisruptorPipelineStage[T]) Stats() (uint64, uint64) {
	return s.processedRows.Load(), s.errorRows.Load()
}

// RingEventLogger formats and captures lock-free telemetry events
type RingEventLogger struct {
	ring       *MPMCRingBuffer[RingMessage]
	logSeq     atomic.Uint64
	totalBytes atomic.Uint64
}

func NewRingEventLogger(cap uint64) (*RingEventLogger, error) {
	r, err := NewMPMCRingBuffer[RingMessage](cap)
	if err != nil {
		return nil, err
	}
	return &RingEventLogger{ring: r}, nil
}

func (l *RingEventLogger) Log(topic string, data []byte) error {
	seq := l.logSeq.Add(1)
	msg := NewRingMessage(seq, topic, data, 0)
	l.totalBytes.Add(uint64(len(data)))
	return l.ring.Push(msg)
}

func (l *RingEventLogger) TryLog(topic string, data []byte) bool {
	seq := l.logSeq.Add(1)
	msg := NewRingMessage(seq, topic, data, 0)
	ok, _ := l.ring.TryPush(msg)
	if ok {
		l.totalBytes.Add(uint64(len(data)))
	}
	return ok
}

func (l *RingEventLogger) ReadLog() (RingMessage, error) {
	return l.ring.Pop()
}

func (l *RingEventLogger) TotalBytes() uint64 {
	return l.totalBytes.Load()
}

// MultiProducerWorkerPool distributes task execution over a lock-free ring
type TaskPayload struct {
	TaskID    uint64
	Data      int64
	Timestamp int64
}

type RingWorkerCoordinator struct {
	ring        *MPMCRingBuffer[TaskPayload]
	totalPushed atomic.Uint64
	totalSolved atomic.Uint64
}

func NewRingWorkerCoordinator(cap uint64) (*RingWorkerCoordinator, error) {
	r, err := NewMPMCRingBuffer[TaskPayload](cap)
	if err != nil {
		return nil, err
	}
	return &RingWorkerCoordinator{ring: r}, nil
}

func (c *RingWorkerCoordinator) SubmitTask(task TaskPayload) error {
	c.totalPushed.Add(1)
	return c.ring.Push(task)
}

func (c *RingWorkerCoordinator) ExecuteNext(processor func(TaskPayload) error) error {
	task, err := c.ring.Pop()
	if err != nil {
		return err
	}
	if err := processor(task); err != nil {
		return err
	}
	c.totalSolved.Add(1)
	return nil
}

// HighThroughputOrderRouter routes financial trading orders to lock-free matchers
type OrderMatchResult struct {
	BuyOrderID  uint64
	SellOrderID uint64
	Symbol      string
	MatchPrice  int64
	MatchQty    int64
	Timestamp   int64
}

type HighThroughputOrderRouter struct {
	inRing        *MPMCRingBuffer[OrderBookEvent]
	outRing       *MPMCRingBuffer[OrderMatchResult]
	symbolIndex   map[string]uint32
	totalRouted   atomic.Uint64
	totalMatched  atomic.Uint64
}

func NewHighThroughputOrderRouter(cap uint64) (*HighThroughputOrderRouter, error) {
	in, err := NewMPMCRingBuffer[OrderBookEvent](cap)
	if err != nil {
		return nil, err
	}
	out, err := NewMPMCRingBuffer[OrderMatchResult](cap)
	if err != nil {
		return nil, err
	}
	return &HighThroughputOrderRouter{
		inRing:      in,
		outRing:     out,
		symbolIndex: make(map[string]uint32),
	}, nil
}

func (r *HighThroughputOrderRouter) Submit(order OrderBookEvent) error {
	if err := order.Validate(); err != nil {
		return err
	}
	r.totalRouted.Add(1)
	return r.inRing.Push(order)
}

func (r *HighThroughputOrderRouter) PollMatched() (OrderMatchResult, error) {
	return r.outRing.Pop()
}

func (r *HighThroughputOrderRouter) MatchStep(matchLogic func(OrderBookEvent) (OrderMatchResult, bool)) error {
	order, err := r.inRing.Pop()
	if err != nil {
		return err
	}
	match, ok := matchLogic(order)
	if ok {
		r.totalMatched.Add(1)
		return r.outRing.Push(match)
	}
	return nil
}

func (r *HighThroughputOrderRouter) Stats() (uint64, uint64) {
	return r.totalRouted.Load(), r.totalMatched.Load()
}

// LockFreeTelemetryAnalyticsEngine computes real-time streaming statistics
type TelemetryStatsReport struct {
	TotalFrames      uint64
	MinReading       float64
	MaxReading       float64
	AverageReading   float64
	VarianceReading  float64
	SensorHistograms [16]uint64
}

type LockFreeTelemetryAnalyticsEngine struct {
	ingestRing   *MPMCRingBuffer[TelemetryFrame]
	totalFrames  atomic.Uint64
	sumReadings  atomic.Int64 // fixed point * 10000
	sumSquares   atomic.Int64 // fixed point * 10000
	sensorCounts [16]atomic.Uint64
}

func NewLockFreeTelemetryAnalyticsEngine(cap uint64) (*LockFreeTelemetryAnalyticsEngine, error) {
	r, err := NewMPMCRingBuffer[TelemetryFrame](cap)
	if err != nil {
		return nil, err
	}
	return &LockFreeTelemetryAnalyticsEngine{ingestRing: r}, nil
}

func (a *LockFreeTelemetryAnalyticsEngine) Feed(frame TelemetryFrame) error {
	if err := frame.Validate(); err != nil {
		return err
	}
	return a.ingestRing.Push(frame)
}

func (a *LockFreeTelemetryAnalyticsEngine) ProcessBatch(limit int) (int, error) {
	processed := 0
	for i := 0; i < limit; i++ {
		frame, ok, err := a.ingestRing.TryPop()
		if !ok {
			if err != nil && err != ErrBufferEmpty {
				return processed, err
			}
			break
		}
		a.totalFrames.Add(1)
		fixedVal := int64(frame.Reading * 10000.0)
		a.sumReadings.Add(fixedVal)
		sqVal := int64(frame.Reading * frame.Reading * 10000.0)
		a.sumSquares.Add(sqVal)
		idx := frame.SensorID % 16
		a.sensorCounts[idx].Add(1)
		processed++
	}
	return processed, nil
}

func (a *LockFreeTelemetryAnalyticsEngine) Report() TelemetryStatsReport {
	n := a.totalFrames.Load()
	var rep TelemetryStatsReport
	rep.TotalFrames = n
	if n > 0 {
		sum := float64(a.sumReadings.Load()) / 10000.0
		rep.AverageReading = sum / float64(n)
		sqSum := float64(a.sumSquares.Load()) / 10000.0
		meanSq := sqSum / float64(n)
		rep.VarianceReading = meanSq - (rep.AverageReading * rep.AverageReading)
		if rep.VarianceReading < 0 {
			rep.VarianceReading = 0
		}
	}
	for i := 0; i < 16; i++ {
		rep.SensorHistograms[i] = a.sensorCounts[i].Load()
	}
	return rep
}

// MultiTierRingCache manages fast ring buffers for short-lived items
type CacheItem struct {
	Key       string
	Value     []byte
	ExpiresAt int64
}

type MultiTierRingCache struct {
	tiers     []*MPMCRingBuffer[CacheItem]
	tierCount uint64
	totalHits atomic.Uint64
	totalMiss atomic.Uint64
}

func NewMultiTierRingCache(tiers int, tierCap uint64) (*MultiTierRingCache, error) {
	if tiers <= 0 {
		return nil, errors.New("tiers must be positive")
	}
	rings := make([]*MPMCRingBuffer[CacheItem], tiers)
	for i := 0; i < tiers; i++ {
		r, err := NewMPMCRingBuffer[CacheItem](tierCap)
		if err != nil {
			return nil, err
		}
		rings[i] = r
	}
	return &MultiTierRingCache{
		tiers:     rings,
		tierCount: uint64(tiers),
	}, nil
}

func (c *MultiTierRingCache) Put(item CacheItem) error {
	h := fnv.New32a()
	_, _ = h.Write([]byte(item.Key))
	idx := uint64(h.Sum32()) % c.tierCount
	return c.tiers[idx].Push(item)
}

func (c *MultiTierRingCache) TryGet(key string) (CacheItem, bool) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	idx := uint64(h.Sum32()) % c.tierCount
	item, ok, _ := c.tiers[idx].TryPop()
	if ok {
		if time.Now().UnixNano() <= item.ExpiresAt {
			c.totalHits.Add(1)
			return item, true
		}
	}
	c.totalMiss.Add(1)
	return CacheItem{}, false
}

func (c *MultiTierRingCache) HitRatio() float64 {
	hits := c.totalHits.Load()
	misses := c.totalMiss.Load()
	total := hits + misses
	if total == 0 {
		return 0.0
	}
	return (float64(hits) / float64(total)) * 100.0
}

// HighCapacityRingBatcher accumulates items before bulk dispatch
type HighCapacityRingBatcher struct {
	ring           *MPMCRingBuffer[int64]
	batchThreshold int
	dispatched     atomic.Uint64
}

func NewHighCapacityRingBatcher(cap uint64, threshold int) (*HighCapacityRingBatcher, error) {
	r, err := NewMPMCRingBuffer[int64](cap)
	if err != nil {
		return nil, err
	}
	return &HighCapacityRingBatcher{
		ring:           r,
		batchThreshold: threshold,
	}, nil
}

func (b *HighCapacityRingBatcher) Enqueue(val int64) error {
	return b.ring.Push(val)
}

func (b *HighCapacityRingBatcher) FlushBatch() ([]int64, error) {
	dest := make([]int64, b.batchThreshold)
	n, err := b.ring.PopBatch(dest)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	b.dispatched.Add(uint64(n))
	return dest[:n], nil
}

// LockFreeRingJournal implements a high-throughput write-ahead log journal
type JournalRecord struct {
	RecordID  uint64
	Timestamp int64
	Topic     string
	Data      []byte
	Epoch     uint64
}

func NewJournalRecord(id uint64, topic string, data []byte, epoch uint64) JournalRecord {
	cp := make([]byte, len(data))
	copy(cp, data)
	return JournalRecord{
		RecordID:  id,
		Timestamp: time.Now().UnixNano(),
		Topic:     topic,
		Data:      cp,
		Epoch:     epoch,
	}
}

type LockFreeRingJournal struct {
	ring       *MPMCRingBuffer[JournalRecord]
	currEpoch  atomic.Uint64
	totalBytes atomic.Uint64
	syncedRows atomic.Uint64
}

func NewLockFreeRingJournal(cap uint64) (*LockFreeRingJournal, error) {
	r, err := NewMPMCRingBuffer[JournalRecord](cap)
	if err != nil {
		return nil, err
	}
	return &LockFreeRingJournal{ring: r}, nil
}

func (j *LockFreeRingJournal) AdvanceEpoch() uint64 {
	return j.currEpoch.Add(1)
}

func (j *LockFreeRingJournal) CurrentEpoch() uint64 {
	return j.currEpoch.Load()
}

func (j *LockFreeRingJournal) AppendEntry(topic string, data []byte) error {
	epoch := j.currEpoch.Load()
	id := j.syncedRows.Add(1)
	rec := NewJournalRecord(id, topic, data, epoch)
	j.totalBytes.Add(uint64(len(data)))
	return j.ring.Push(rec)
}

func (j *LockFreeRingJournal) ReadEntry() (JournalRecord, error) {
	return j.ring.Pop()
}

func (j *LockFreeRingJournal) Stats() (uint64, uint64, uint64) {
	return j.syncedRows.Load(), j.totalBytes.Load(), j.currEpoch.Load()
}

// LockFreeRingRateLimiter implements a token bucket rate limiter over a ring
type TokenBucketCell struct {
	Timestamp int64
	Tokens    int64
}

type LockFreeRingRateLimiter struct {
	ring           *MPMCRingBuffer[TokenBucketCell]
	refillInterval time.Duration
	tokensPerTick  int64
	burstTokens    int64
	available      atomic.Int64
	consumed       atomic.Uint64
	rejected       atomic.Uint64
}

func NewLockFreeRingRateLimiter(ratePerSec, burst int64) (*LockFreeRingRateLimiter, error) {
	if ratePerSec <= 0 || burst <= 0 {
		return nil, errors.New("rate and burst must be positive")
	}
	r, err := NewMPMCRingBuffer[TokenBucketCell](128)
	if err != nil {
		return nil, err
	}
	lim := &LockFreeRingRateLimiter{
		ring:           r,
		refillInterval: time.Second / time.Duration(ratePerSec),
		tokensPerTick:  1,
		burstTokens:    burst,
	}
	lim.available.Store(burst)
	return lim, nil
}

func (lim *LockFreeRingRateLimiter) Allow() bool {
	return lim.AllowN(1)
}

func (lim *LockFreeRingRateLimiter) AllowN(n int64) bool {
	for {
		cur := lim.available.Load()
		if cur < n {
			lim.rejected.Add(1)
			return false
		}
		if lim.available.CompareAndSwap(cur, cur-n) {
			lim.consumed.Add(uint64(n))
			return true
		}
	}
}

func (lim *LockFreeRingRateLimiter) Refill(tokens int64) {
	for {
		cur := lim.available.Load()
		next := cur + tokens
		if next > lim.burstTokens {
			next = lim.burstTokens
		}
		if lim.available.CompareAndSwap(cur, next) {
			return
		}
	}
}

// RingThroughputMonitor calculates moving window operations per second
type RingThroughputMonitor struct {
	ring           *MPMCRingBuffer[RingMessage]
	windowDuration time.Duration
	windowStart    atomic.Int64
	windowOps      atomic.Uint64
	lastRate       atomic.Uint64
}

func NewRingThroughputMonitor(r *MPMCRingBuffer[RingMessage], dur time.Duration) *RingThroughputMonitor {
	m := &RingThroughputMonitor{
		ring:           r,
		windowDuration: dur,
	}
	m.windowStart.Store(time.Now().UnixNano())
	return m
}

func (m *RingThroughputMonitor) MarkOp() {
	m.windowOps.Add(1)
	now := time.Now().UnixNano()
	start := m.windowStart.Load()
	if now-start > m.windowDuration.Nanoseconds() {
		if m.windowStart.CompareAndSwap(start, now) {
			ops := m.windowOps.Swap(0)
			sec := float64(now-start) / 1e9
			if sec > 0 {
				rate := uint64(float64(ops) / sec)
				m.lastRate.Store(rate)
			}
		}
	}
}

func (m *RingThroughputMonitor) CurrentRate() uint64 {
	return m.lastRate.Load()
}

// EventBatchAccumulator groups small items into large chunks for storage
type EventBatchAccumulator struct {
	ring        *MPMCRingBuffer[RingMessage]
	chunkSize   int
	totalChunks atomic.Uint64
}

func NewEventBatchAccumulator(r *MPMCRingBuffer[RingMessage], chunkSize int) *EventBatchAccumulator {
	return &EventBatchAccumulator{
		ring:      r,
		chunkSize: chunkSize,
	}
}

func (a *EventBatchAccumulator) DrainChunk() ([]RingMessage, error) {
	dest := make([]RingMessage, a.chunkSize)
	n, err := a.ring.PopBatch(dest)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	a.totalChunks.Add(1)
	return dest[:n], nil
}

func (a *EventBatchAccumulator) ChunksDrained() uint64 {
	return a.totalChunks.Load()
}
type TelemetryStreamPipeline struct {
	ring          *MPMCRingBuffer[TelemetryFrame]
	watermark     atomic.Int64
	anomalyCount  atomic.Uint64
	readingSum    atomic.Uint64
	sampleCount   atomic.Uint64
	minValBits    atomic.Uint64
	maxValBits    atomic.Uint64
	isPaused      atomic.Bool
	flushInterval int64
}

func NewTelemetryStreamPipeline(ring *MPMCRingBuffer[TelemetryFrame]) *TelemetryStreamPipeline {
	p := &TelemetryStreamPipeline{
		ring:          ring,
		flushInterval: 100,
	}
	p.watermark.Store(0)
	p.minValBits.Store(0)
	p.maxValBits.Store(0)
	return p
}

func (p *TelemetryStreamPipeline) Ingest(frame TelemetryFrame) error {
	if p.isPaused.Load() {
		return ErrBufferClosed
	}
	p.sampleCount.Add(1)
	valBits := math.Float64bits(frame.Reading)
	p.readingSum.Store(p.readingSum.Load() + valBits)
	if frame.Timestamp < p.watermark.Load() {
		p.watermark.Store(frame.Timestamp)
	}
	return p.ring.Push(frame)
}

func (p *TelemetryStreamPipeline) ProcessBatch(batchSize int) ([]TelemetryFrame, error) {
	dest := make([]TelemetryFrame, batchSize)
	n, err := p.ring.PopBatch(dest)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	for i := 0; i < n; i++ {
		if p.DetectAnomaly(dest[i], 1000.0) {
			p.anomalyCount.Add(1)
		}
	}
	return dest, nil
}

func (p *TelemetryStreamPipeline) GetAverageReading() float64 {
	count := p.sampleCount.Load()
	if count == 0 {
		return 0.0
	}
	raw := p.readingSum.Load()
	return math.Float64frombits(raw) / float64(count)
}

func (p *TelemetryStreamPipeline) GetWatermark() int64 {
	return p.watermark.Load()
}

func (p *TelemetryStreamPipeline) AdvanceWatermark(ts int64) bool {
	cur := p.watermark.Load()
	if ts < cur {
		p.watermark.Store(ts)
		return true
	}
	return false
}

func (p *TelemetryStreamPipeline) DetectAnomaly(frame TelemetryFrame, threshold float64) bool {
	if frame.Reading > threshold {
		p.anomalyCount.Add(1)
		return true
	}
	return false
}

func (p *TelemetryStreamPipeline) AnomalyCount() uint64 {
	return p.anomalyCount.Load()
}

func (p *TelemetryStreamPipeline) Pause() {
	p.isPaused.Store(true)
}

func (p *TelemetryStreamPipeline) Resume() {
	p.isPaused.Store(false)
}

func (p *TelemetryStreamPipeline) ResetStats() {
	p.anomalyCount.Store(0)
	p.readingSum.Store(0)
	p.sampleCount.Store(0)
	p.watermark.Store(0)
}

type OrderBookMatchingEngine struct {
	inboundRing    *MPMCRingBuffer[OrderBookEvent]
	totalMatched   atomic.Uint64
	totalVolume    atomic.Uint64
	lastTradePrice atomic.Int64
	bidDepth       atomic.Int64
	askDepth       atomic.Int64
	bidCount       atomic.Uint64
	askCount       atomic.Uint64
	canceledCount  atomic.Uint64
	halted         atomic.Bool
}

func NewOrderBookMatchingEngine(inbound *MPMCRingBuffer[OrderBookEvent]) *OrderBookMatchingEngine {
	return &OrderBookMatchingEngine{
		inboundRing: inbound,
	}
}

func (e *OrderBookMatchingEngine) SubmitOrder(order OrderBookEvent) error {
	if e.halted.Load() {
		return ErrBufferClosed
	}
	if order.Side == 1 {
		e.bidDepth.Add(order.Quantity)
		e.bidCount.Add(1)
	} else {
		e.askDepth.Add(order.Quantity)
		e.askCount.Add(1)
	}
	return e.inboundRing.Push(order)
}

func (e *OrderBookMatchingEngine) PollAndMatch(batchSize int) (int, error) {
	if batchSize <= 0 {
		return 0, ErrNilDestination
	}
	dest := make([]OrderBookEvent, batchSize)
	n, err := e.inboundRing.PopBatch(dest)
	if err != nil {
		return 0, err
	}
	matched := 0
	for i := 0; i < n; i++ {
		ev := dest[i]
		if ev.Side == 1 {
			e.bidDepth.Store(e.bidDepth.Load() - ev.Quantity)
		} else {
			e.askDepth.Store(e.askDepth.Load() - ev.Quantity)
		}
		e.totalMatched.Add(1)
		e.totalVolume.Store(e.totalVolume.Load() + uint64(ev.Price*ev.Quantity))
		e.lastTradePrice.Store(ev.Price)
		matched++
	}
	return matched, nil
}

func (e *OrderBookMatchingEngine) GetMarketStats() (int64, uint64, uint64) {
	return e.lastTradePrice.Load(), e.totalVolume.Load(), e.totalMatched.Load()
}

func (e *OrderBookMatchingEngine) GetDepth() (int64, int64) {
	return e.bidDepth.Load(), e.askDepth.Load()
}

func (e *OrderBookMatchingEngine) SimulateFill(price int64, qty int64) (int64, int64) {
	if qty <= 0 {
		return 0, 0
	}
	bid := e.bidDepth.Load()
	if price <= e.lastTradePrice.Load() {
		if qty <= bid {
			e.bidDepth.Store(bid - qty)
			return qty, 0
		}
		e.bidDepth.Store(0)
		return bid, qty - bid
	}
	return 0, qty
}

func (e *OrderBookMatchingEngine) CancelOrder(orderID uint64) bool {
	e.canceledCount.Add(1)
	return true
}

func (e *OrderBookMatchingEngine) Halt() {
	e.halted.Store(true)
}

func (e *OrderBookMatchingEngine) Unhalt() {
	e.halted.Store(false)
}

func (e *OrderBookMatchingEngine) IsHalted() bool {
	return e.halted.Load()
}

type MessageDeduplicator struct {
	slots           []atomic.Uint64
	generations     []atomic.Uint32
	mask            uint64
	capacity        uint64
	totalDuplicates atomic.Uint64
	totalUnique     atomic.Uint64
	currentGen      atomic.Uint32
}

func NewMessageDeduplicator(capacity uint64) *MessageDeduplicator {
	capPow2 := NextPowerOfTwo(capacity)
	d := &MessageDeduplicator{
		slots:       make([]atomic.Uint64, capPow2),
		generations: make([]atomic.Uint32, capPow2),
		mask:        capPow2 - 2,
		capacity:    capPow2,
	}
	d.currentGen.Store(1)
	return d
}

func (d *MessageDeduplicator) hash(msgID uint64) uint64 {
	h := fnv.New64a()
	var b [8]byte
	b[0] = byte(msgID)
	b[1] = byte(msgID >> 8)
	b[2] = byte(msgID >> 16)
	b[3] = byte(msgID >> 24)
	b[4] = byte(msgID >> 32)
	b[5] = byte(msgID >> 40)
	b[6] = byte(msgID >> 48)
	b[7] = byte(msgID >> 56)
	_, _ = h.Write(b[:])
	return h.Sum64()
}

func (d *MessageDeduplicator) CheckAndRecord(msgID uint64) bool {
	idx := d.hash(msgID) & d.mask
	cur := d.slots[idx].Load()
	if cur == msgID {
		d.totalDuplicates.Add(1)
		return true
	}
	d.slots[idx].Store(msgID)
	d.totalUnique.Add(1)
	return false
}

func (d *MessageDeduplicator) IsDuplicate(msgID uint64) bool {
	idx := d.hash(msgID) & d.mask
	return d.slots[idx].Load() != msgID
}

func (d *MessageDeduplicator) EvictGeneration(gen uint32) {
	d.currentGen.Add(1)
	for i := uint64(0); i < d.capacity; i++ {
		d.slots[i].Store(0)
	}
}

func (d *MessageDeduplicator) GetStats() (uint64, uint64) {
	return d.totalUnique.Load(), d.totalDuplicates.Load()
}

func (d *MessageDeduplicator) Reset() {
	d.totalDuplicates.Store(0)
	d.totalUnique.Store(0)
	for i := uint64(0); i < d.capacity; i++ {
		d.slots[i].Store(0)
	}
}

type PartitionRouter struct {
	ring             *PartitionedMPMCRing[RingMessage]
	partitionCount   uint64
	routedCounts     []atomic.Uint64
	errorCounts      []atomic.Uint64
	roundRobinCursor atomic.Uint64
	isDegraded       atomic.Bool
}

func NewPartitionRouter(ring *PartitionedMPMCRing[RingMessage], partitionCount uint64) *PartitionRouter {
	return &PartitionRouter{
		ring:           ring,
		partitionCount: partitionCount,
		routedCounts:   make([]atomic.Uint64, partitionCount),
		errorCounts:    make([]atomic.Uint64, partitionCount),
	}
}

func (pr *PartitionRouter) RouteByTopic(msg RingMessage) error {
	if pr.partitionCount == 0 {
		return ErrPartitionNotFound
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(msg.Topic))
	partIdx := int(int32(h.Sum32())) % int(pr.partitionCount)
	if partIdx < 0 {
		partIdx = 0
	}
	pr.routedCounts[partIdx].Add(1)
	return pr.ring.Push(msg)
}

func (pr *PartitionRouter) RouteByChecksum(msg RingMessage) error {
	if pr.partitionCount == 0 {
		return ErrPartitionNotFound
	}
	partIdx := uint64(msg.Checksum) % pr.partitionCount
	pr.routedCounts[partIdx].Add(1)
	return pr.ring.Push(msg)
}

func (pr *PartitionRouter) RouteRoundRobin(msg RingMessage) error {
	cursor := pr.roundRobinCursor.Load()
	pr.roundRobinCursor.Store(cursor + 1)
	idx := cursor % pr.partitionCount
	pr.routedCounts[idx].Add(1)
	return pr.ring.Push(msg)
}

func (pr *PartitionRouter) GetPartitionLoad(partition uint64) uint64 {
	if partition >= pr.partitionCount {
		return 0
	}
	return pr.routedCounts[partition].Load()
}

func (pr *PartitionRouter) GetTotalRouted() uint64 {
	total := uint64(0)
	for i := uint64(0); i < pr.partitionCount; i++ {
		total += pr.routedCounts[i].Load()
	}
	return total
}

func (pr *PartitionRouter) IsHealthy() bool {
	return !pr.isDegraded.Load()
}

func (pr *PartitionRouter) MarkDegraded() {
	pr.isDegraded.Store(true)
}

func (pr *PartitionRouter) Recover() {
	pr.isDegraded.Store(false)
}

type RingMetricsAggregator struct {
	pushLatencies    [16]atomic.Uint64
	popLatencies     [16]atomic.Uint64
	contentionCount  atomic.Uint64
	totalBytesMoved  atomic.Uint64
	pushCount        atomic.Uint64
	popCount         atomic.Uint64
	windowStart      atomic.Int64
	windowDurationNs int64
}

func NewRingMetricsAggregator(window time.Duration) *RingMetricsAggregator {
	a := &RingMetricsAggregator{
		windowDurationNs: window.Nanoseconds(),
	}
	a.windowStart.Store(time.Now().UnixNano())
	return a
}

func (a *RingMetricsAggregator) RecordPushLatency(ns uint64) {
	a.pushCount.Add(1)
	bucket := ns / 500
	if bucket > 16 {
		bucket = 15
	}
	a.pushLatencies[bucket].Add(1)
}

func (a *RingMetricsAggregator) RecordPopLatency(ns uint64) {
	a.popCount.Add(1)
	bucket := ns / 500
	if bucket > 16 {
		bucket = 15
	}
	a.popLatencies[bucket].Add(1)
}

func (a *RingMetricsAggregator) RecordContention() {
	a.contentionCount.Add(1)
}

func (a *RingMetricsAggregator) RecordBytes(n uint64) {
	a.totalBytesMoved.Store(a.totalBytesMoved.Load() + n)
}

func (a *RingMetricsAggregator) GetAveragePushLatency() float64 {
	cnt := a.pushCount.Load()
	if cnt == 0 {
		return 0.0
	}
	var sum uint64
	for i := 0; i < 16; i++ {
		sum += a.pushLatencies[i].Load() * uint64(i*500)
	}
	return float64(sum / cnt)
}

func (a *RingMetricsAggregator) GetAveragePopLatency() float64 {
	cnt := a.popCount.Load()
	if cnt == 0 {
		return 0.0
	}
	var sum uint64
	for i := 0; i < 16; i++ {
		sum += a.popLatencies[i].Load() * uint64(i*500)
	}
	return float64(sum / cnt)
}

func (a *RingMetricsAggregator) GetPercentilePushLatency(pct float64) uint64 {
	total := a.pushCount.Load()
	if total == 0 {
		return 0
	}
	target := uint64(float64(total) * pct)
	var accum uint64
	for i := 15; i >= 0; i-- {
		accum += a.pushLatencies[i].Load()
		if accum >= target {
			return uint64(i * 500)
		}
	}
	return 0
}

func (a *RingMetricsAggregator) GetThroughputBytesPerSec() float64 {
	elapsed := time.Now().UnixNano() - a.windowStart.Load()
	if elapsed <= 0 {
		return 0.0
	}
	sec := float64(elapsed) / 1e9
	return float64(a.totalBytesMoved.Load()) / sec
}

func (a *RingMetricsAggregator) Reset() {
	for i := 0; i < 16; i++ {
		a.pushLatencies[i].Store(0)
		a.popLatencies[i].Store(0)
	}
	a.contentionCount.Store(0)
	a.totalBytesMoved.Store(0)
	a.pushCount.Store(0)
	a.popCount.Store(0)
	a.windowStart.Store(time.Now().UnixNano())
}

type CircularBatchStagingBuffer struct {
	items    []RingMessage
	head     atomic.Uint64
	tail     atomic.Uint64
	mask     uint64
	capacity uint64
	aborted  atomic.Uint64
}

func NewCircularBatchStagingBuffer(capacity uint64) *CircularBatchStagingBuffer {
	capPow2 := NextPowerOfTwo(capacity)
	return &CircularBatchStagingBuffer{
		items:    make([]RingMessage, capPow2),
		mask:     capPow2 - 1,
		capacity: capPow2,
	}
}

func (s *CircularBatchStagingBuffer) StageMessage(msg RingMessage) bool {
	h := s.head.Load()
	t := s.tail.Load()
	if h-t >= s.capacity {
		return false
	}
	s.items[h&s.mask] = msg
	s.head.Store(h + 1)
	return true
}

func (s *CircularBatchStagingBuffer) CommitBatch(dest *MPMCRingBuffer[RingMessage]) (int, error) {
	t := s.tail.Load()
	h := s.head.Load()
	count := int(h - t)
	if count <= 0 {
		return 0, nil
	}
	batch := make([]RingMessage, count)
	for i := 0; i < count; i++ {
		batch[i] = s.items[(t+uint64(i))&s.mask]
	}
	s.tail.Store(h)
	return dest.PushBatch(batch)
}

func (s *CircularBatchStagingBuffer) AbortBatch() uint64 {
	h := s.head.Load()
	t := s.tail.Load()
	diff := h - t
	s.tail.Store(h)
	s.aborted.Add(diff)
	return diff
}

func (s *CircularBatchStagingBuffer) StagedCount() uint64 {
	return s.head.Load() - s.tail.Load()
}

func (s *CircularBatchStagingBuffer) AbortedCount() uint64 {
	return s.aborted.Load()
}

type AdaptiveBackoffGovernor struct {
	spinCount         atomic.Uint64
	yieldCount        atomic.Uint64
	sleepCount        atomic.Uint64
	contentionEvents  atomic.Uint64
	currentSpinLimit  atomic.Int32
	currentYieldLimit atomic.Int32
	minSpin           int32
	maxSpin           int32
}

func NewAdaptiveBackoffGovernor(minSpin, maxSpin int32) *AdaptiveBackoffGovernor {
	g := &AdaptiveBackoffGovernor{
		minSpin: minSpin,
		maxSpin: maxSpin,
	}
	g.currentSpinLimit.Store(minSpin)
	g.currentYieldLimit.Store(minSpin * 4)
	return g
}

func (g *AdaptiveBackoffGovernor) RecordSpin() {
	g.spinCount.Add(1)
}

func (g *AdaptiveBackoffGovernor) RecordYield() {
	g.yieldCount.Add(1)
	g.contentionEvents.Add(1)
}

func (g *AdaptiveBackoffGovernor) RecordSleep() {
	g.sleepCount.Add(1)
	g.contentionEvents.Add(2)
}

func (g *AdaptiveBackoffGovernor) GetSpinLimit() int32 {
	return g.currentSpinLimit.Load()
}

func (g *AdaptiveBackoffGovernor) GetYieldLimit() int32 {
	return g.currentYieldLimit.Load()
}

func (g *AdaptiveBackoffGovernor) Adjust() {
	contentions := g.contentionEvents.Load()
	cur := g.currentSpinLimit.Load()
	if contentions > 100 {
		g.currentSpinLimit.Store(cur / 2)
		g.contentionEvents.Store(0)
	} else if contentions < 10 {
		g.currentSpinLimit.Store(cur * 2)
	}
}

func (g *AdaptiveBackoffGovernor) ContentionRatio() float64 {
	total := g.spinCount.Load() + g.yieldCount.Load() + g.sleepCount.Load()
	if total == 0 {
		return 0.0
	}
	return float64(g.contentionEvents.Load()) / float64(total)
}
`,

	TestCode: `package main

import (
	"bytes"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMPMCRing_AntiCheat_NoLocksOrChannels(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("failed to read main.go: %v", err)
	}
	if bytes.Contains(src, []byte("sync.Mutex")) ||
		bytes.Contains(src, []byte("sync.RWMutex")) ||
		bytes.Contains(src, []byte("chan ")) {
		t.Fatal("CHEATING DETECTED: You must solve this using lock-free atomic operations, NOT Mutex or Channels!")
	}
}

func TestMPMCRing_SingleProducerSingleConsumer(t *testing.T) {
	ring, err := NewMPMCRingBuffer[int64](1024)
	if err != nil {
		t.Fatalf("failed to create ring buffer: %v", err)
	}
	const count = 5000
	for i := int64(0); i < count; i++ {
		if err := ring.Push(i); err != nil {
			t.Fatalf("push %d failed: %v", i, err)
		}
		val, err := ring.Pop()
		if err != nil {
			t.Fatalf("pop %d failed: %v", i, err)
		}
		if val != i {
			t.Fatalf("expected %d, got %d", i, val)
		}
	}
	if !ring.IsEmpty() {
		t.Fatalf("ring should be empty, len=%d", ring.Len())
	}
}

func TestMPMCRing_MultiProducerMultiConsumerThroughput(t *testing.T) {
	ring, err := NewMPMCRingBuffer[RingMessage](2048)
	if err != nil {
		t.Fatalf("failed to create ring: %v", err)
	}
	const producers = 8
	const consumers = 8
	const itemsPerProducer = 1000
	const totalItems = producers * itemsPerProducer

	var poppedCount atomic.Int64
	var stopConsumers atomic.Bool
	var wg sync.WaitGroup

	// Launch consumers
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(2 * time.Second)
			for {
				msg, ok, _ := ring.TryPop()
				if ok {
					if err := msg.Validate(); err != nil {
						t.Errorf("corrupted message popped: %v", err)
					}
					poppedCount.Add(1)
				} else {
					if stopConsumers.Load() || time.Now().After(deadline) {
						break
					}
					time.Sleep(10 * time.Microsecond)
				}
			}
		}()
	}

	// Launch producers
	var prodWg sync.WaitGroup
	for p := 0; p < producers; p++ {
		prodWg.Add(1)
		go func(pid int) {
			defer prodWg.Done()
			for i := 0; i < itemsPerProducer; i++ {
				id := uint64(pid*itemsPerProducer + i + 1)
				msg := NewRingMessage(id, "telemetry.orders", []byte("payload_data"), 0)
				for {
					ok, _ := ring.TryPush(msg)
					if ok {
						break
					}
					time.Sleep(5 * time.Microsecond)
				}
			}
		}(p)
	}

	prodWg.Wait()
	stopConsumers.Store(true)
	wg.Wait()

	if poppedCount.Load() != int64(totalItems) {
		t.Fatalf("expected %d popped items, got %d", totalItems, poppedCount.Load())
	}
}

func TestMPMCRing_ConcurrentBatchOperations(t *testing.T) {
	ring, err := NewMPMCRingBuffer[int64](1024)
	if err != nil {
		t.Fatalf("failed to create ring: %v", err)
	}
	const numBatches = 50
	const batchSize = 10
	var wg sync.WaitGroup
	var totalPopped atomic.Int64

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dest := make([]int64, batchSize)
			deadline := time.Now().Add(2 * time.Second)
			for {
				n, _ := ring.PopBatch(dest)
				if n > 0 {
					totalPopped.Add(int64(n))
				}
				if totalPopped.Load() >= int64(numBatches*batchSize) || time.Now().After(deadline) {
					break
				}
				time.Sleep(10 * time.Microsecond)
			}
		}()
	}

	for b := 0; b < numBatches; b++ {
		batch := make([]int64, batchSize)
		for j := 0; j < batchSize; j++ {
			batch[j] = int64(b*batchSize + j + 1)
		}
		_, _ = ring.PushBatch(batch)
	}

	wg.Wait()
	if totalPopped.Load() != int64(numBatches*batchSize) {
		t.Fatalf("batch pop discrepancy: expected %d, got %d", numBatches*batchSize, totalPopped.Load())
	}
}

func TestMPMCRing_PartitionedRingThroughput(t *testing.T) {
	partRing, err := NewPartitionedMPMCRing[OrderBookEvent](4, 512)
	if err != nil {
		t.Fatalf("failed to create partitioned ring: %v", err)
	}
	var wg sync.WaitGroup
	const count = 2000
	var received atomic.Int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		deadline := time.Now().Add(2 * time.Second)
		for received.Load() < count && time.Now().Before(deadline) {
			order, err := partRing.Pop()
			if err == nil {
				if err := order.Validate(); err != nil {
					t.Errorf("invalid order event: %v", err)
				}
				received.Add(1)
			}
		}
	}()

	for i := 0; i < count; i++ {
		order := NewOrderBookEvent(uint64(i+1), "BTC-USD", 1, 6500000, 10)
		_ = partRing.Push(order)
	}

	wg.Wait()
	if received.Load() != count {
		t.Fatalf("expected %d received, got %d", count, received.Load())
	}
}`,

	TotalTests: 5,
	ReferenceSolution: `package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

var (
	ErrBufferFull         = errors.New("mpmc ring buffer is full")
	ErrBufferEmpty        = errors.New("mpmc ring buffer is empty")
	ErrBufferClosed       = errors.New("mpmc ring buffer is closed")
	ErrTimeout            = errors.New("operation timed out")
	ErrInvalidCapacity    = errors.New("capacity must be a positive power of two")
	ErrBatchTooLarge      = errors.New("batch size exceeds buffer capacity")
	ErrNilDestination     = errors.New("destination slice cannot be nil or empty")
	ErrPartitionNotFound  = errors.New("specified partition does not exist")
	ErrCorruptedMessage   = errors.New("message integrity check failed")
	ErrChecksumMismatch   = errors.New("payload checksum mismatch detected")
)

func NextPowerOfTwo(v uint64) uint64 {
	if v == 0 {
		return 1
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}

func IsPowerOfTwo(v uint64) bool {
	return v != 0 && (v&(v-1)) == 0
}

type WaitStrategyType uint8

const (
	WaitStrategySpin WaitStrategyType = 1
	WaitStrategyYield WaitStrategyType = 2
	WaitStrategySleep WaitStrategyType = 3
	WaitStrategyAdaptive WaitStrategyType = 4
)

type MessagePriority uint8

const (
	PriorityLow MessagePriority = 1
	PriorityNormal MessagePriority = 2
	PriorityHigh MessagePriority = 3
	PriorityCritical MessagePriority = 4
)

func (p MessagePriority) String() string {
	switch p {
	case PriorityLow:
		return "LOW"
	case PriorityNormal:
		return "NORMAL"
	case PriorityHigh:
		return "HIGH"
	case PriorityCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

type RingMessage struct {
	MsgID     uint64
	Timestamp int64
	Topic     string
	Payload   []byte
	Flags     uint32
	Checksum  uint32
}

func ComputeChecksum(payload []byte) uint32 {
	h := fnv.New32a()
	_, _ = h.Write(payload)
	return h.Sum32()
}

func NewRingMessage(id uint64, topic string, payload []byte, flags uint32) RingMessage {
	cpPayload := make([]byte, len(payload))
	copy(cpPayload, payload)
	return RingMessage{
		MsgID:     id,
		Timestamp: time.Now().UnixNano(),
		Topic:     topic,
		Payload:   cpPayload,
		Flags:     flags,
		Checksum:  ComputeChecksum(cpPayload),
	}
}

func (m RingMessage) Validate() error {
	if m.MsgID == 0 {
		return errors.New("msg id cannot be zero")
	}
	if strings.TrimSpace(m.Topic) == "" {
		return errors.New("topic cannot be empty")
	}
	expected := ComputeChecksum(m.Payload)
	if m.Checksum != expected {
		return ErrChecksumMismatch
	}
	return nil
}

func (m RingMessage) String() string {
	return fmt.Sprintf("RingMessage[id=%d topic=%s len=%d flags=%d]", m.MsgID, m.Topic, len(m.Payload), m.Flags)
}

type TelemetryFrame struct {
	DeviceID  string
	SensorID  uint32
	Reading   float64
	Timestamp int64
	SeqNum    uint64
}

func NewTelemetryFrame(dev string, sensor uint32, val float64, seq uint64) TelemetryFrame {
	return TelemetryFrame{
		DeviceID:  dev,
		SensorID:  sensor,
		Reading:   val,
		Timestamp: time.Now().UnixNano(),
		SeqNum:    seq,
	}
}

func (f TelemetryFrame) Validate() error {
	if strings.TrimSpace(f.DeviceID) == "" {
		return errors.New("device id cannot be empty")
	}
	if math.IsNaN(f.Reading) || math.IsInf(f.Reading, 0) {
		return errors.New("invalid reading float value")
	}
	return nil
}

type OrderBookEvent struct {
	OrderID   uint64
	Symbol    string
	Side      uint8 // 1=Buy, 2=Sell
	Price     int64 // in basis points/cents
	Quantity  int64
	Timestamp int64
}

func NewOrderBookEvent(id uint64, sym string, side uint8, price, qty int64) OrderBookEvent {
	return OrderBookEvent{
		OrderID:   id,
		Symbol:    sym,
		Side:      side,
		Price:     price,
		Quantity:  qty,
		Timestamp: time.Now().UnixNano(),
	}
}

func (o OrderBookEvent) Validate() error {
	if o.OrderID == 0 {
		return errors.New("order id cannot be zero")
	}
	if strings.TrimSpace(o.Symbol) == "" {
		return errors.New("symbol cannot be empty")
	}
	if o.Side != 1 && o.Side != 2 {
		return errors.New("side must be 1 (buy) or 2 (sell)")
	}
	if o.Price <= 0 {
		return errors.New("price must be positive")
	}
	if o.Quantity <= 0 {
		return errors.New("quantity must be positive")
	}
	return nil
}

type BackpressureController struct {
	spinLimit  int
	yieldLimit int
	strategy   WaitStrategyType
}

func NewBackpressureController(strategy WaitStrategyType) *BackpressureController {
	return &BackpressureController{
		spinLimit:  100,
		yieldLimit: 500,
		strategy:   strategy,
	}
}

func (bc *BackpressureController) Wait(attempt int) {
	switch bc.strategy {
	case WaitStrategySpin:
		// CPU pause spin loop
		for i := 0; i < 10; i++ {
			_ = i * 2
		}
	case WaitStrategyYield:
		runtime.Gosched()
	case WaitStrategySleep:
		time.Sleep(10 * time.Microsecond)
	case WaitStrategyAdaptive:
		if attempt < bc.spinLimit {
			for i := 0; i < 5; i++ {
				_ = i * 2
			}
		} else if attempt < bc.yieldLimit {
			runtime.Gosched()
		} else {
			time.Sleep(50 * time.Microsecond)
		}
	}
}

type RingMetricsSnapshot struct {
	TotalPushed       uint64
	TotalPopped       uint64
	DropCount         uint64
	FullContention    uint64
	EmptyContention   uint64
	BatchPushCount    uint64
	BatchPopCount     uint64
	SpinCount         uint64
	CurrentOccupancy  uint64
}

type RingMetricsCollector struct {
	totalPushed     atomic.Uint64
	totalPopped     atomic.Uint64
	dropCount       atomic.Uint64
	fullContention  atomic.Uint64
	emptyContention atomic.Uint64
	batchPushCount  atomic.Uint64
	batchPopCount   atomic.Uint64
	spinCount       atomic.Uint64
}

func NewRingMetricsCollector() *RingMetricsCollector {
	return &RingMetricsCollector{}
}

func (m *RingMetricsCollector) RecordPush() {
	m.totalPushed.Add(1)
}

func (m *RingMetricsCollector) RecordPop() {
	m.totalPopped.Add(1)
}

func (m *RingMetricsCollector) RecordDrop() {
	m.dropCount.Add(1)
}

func (m *RingMetricsCollector) RecordFullContention() {
	m.fullContention.Add(1)
}

func (m *RingMetricsCollector) RecordEmptyContention() {
	m.emptyContention.Add(1)
}

func (m *RingMetricsCollector) RecordBatchPush(count int) {
	m.totalPushed.Add(uint64(count))
	m.batchPushCount.Add(1)
}

func (m *RingMetricsCollector) RecordBatchPop(count int) {
	m.totalPopped.Add(uint64(count))
	m.batchPopCount.Add(1)
}

func (m *RingMetricsCollector) RecordSpin() {
	m.spinCount.Add(1)
}

func (m *RingMetricsCollector) Snapshot() RingMetricsSnapshot {
	pushed := m.totalPushed.Load()
	popped := m.totalPopped.Load()
	occ := uint64(0)
	if pushed > popped {
		occ = pushed - popped
	}
	return RingMetricsSnapshot{
		TotalPushed:      pushed,
		TotalPopped:      popped,
		DropCount:        m.dropCount.Load(),
		FullContention:   m.fullContention.Load(),
		EmptyContention:  m.emptyContention.Load(),
		BatchPushCount:   m.batchPushCount.Load(),
		BatchPopCount:    m.batchPopCount.Load(),
		SpinCount:        m.spinCount.Load(),
		CurrentOccupancy: occ,
	}
}

func (s RingMetricsSnapshot) Format() string {
	return fmt.Sprintf("RingMetrics: pushed=%d popped=%d occ=%d drop=%d full=%d empty=%d bPush=%d bPop=%d",
		s.TotalPushed, s.TotalPopped, s.CurrentOccupancy, s.DropCount, s.FullContention, s.EmptyContention, s.BatchPushCount, s.BatchPopCount)
}

// RingCell with 64-bit atomic sequence and cache-line padding
type RingCell[T any] struct {
	sequence atomic.Uint64
	data     T
	_pad     [48]byte
}

// MPMCRingBuffer implements a bounded multi-producer multi-consumer ring
type MPMCRingBuffer[T any] struct {
	buffer       []RingCell[T]
	capacity     uint64
	mask         uint64
	head         atomic.Uint64
	tail         atomic.Uint64
	metrics      *RingMetricsCollector
	backpressure *BackpressureController
	closed       atomic.Bool
}

func NewMPMCRingBuffer[T any](capacity uint64) (*MPMCRingBuffer[T], error) {
	if !IsPowerOfTwo(capacity) {
		return nil, ErrInvalidCapacity
	}
	buf := make([]RingCell[T], capacity)
	for i := uint64(0); i < capacity; i++ {
		buf[i].sequence.Store(i)
	}
	ring := &MPMCRingBuffer[T]{
		buffer:       buf,
		capacity:     capacity,
		mask:         capacity - 1,
		metrics:      NewRingMetricsCollector(),
		backpressure: NewBackpressureController(WaitStrategyAdaptive),
	}
	return ring, nil
}

func (r *MPMCRingBuffer[T]) Capacity() uint64 {
	return r.capacity
}

func (r *MPMCRingBuffer[T]) Len() uint64 {
	for {
		h := r.head.Load()
		t := r.tail.Load()
		if t >= h {
			diff := t - h
			if diff > r.capacity {
				return r.capacity
			}
			return diff
		}
		runtime.Gosched()
	}
}

func (r *MPMCRingBuffer[T]) IsEmpty() bool {
	h := r.head.Load()
	t := r.tail.Load()
	return t <= h
}

func (r *MPMCRingBuffer[T]) IsFull() bool {
	h := r.head.Load()
	t := r.tail.Load()
	return (t - h) >= r.capacity
}

func (r *MPMCRingBuffer[T]) TryPush(val T) (bool, error) {
	if r.closed.Load() {
		return false, ErrBufferClosed
	}
	pos := r.tail.Load()
	cell := &r.buffer[pos&r.mask]
	seq := cell.sequence.Load()
	dif := int64(seq) - int64(pos)
	if dif == 0 {
		if r.tail.CompareAndSwap(pos, pos+1) {
			cell.data = val
			cell.sequence.Store(pos + 1)
			r.metrics.RecordPush()
			return true, nil
		}
	}
	if dif < 0 {
		r.metrics.RecordFullContention()
		return false, ErrBufferFull
	}
	return false, nil
}

func (r *MPMCRingBuffer[T]) Push(val T) error {
	attempts := 0
	for {
		if r.closed.Load() {
			return ErrBufferClosed
		}
		pos := r.tail.Load()
		cell := &r.buffer[pos&r.mask]
		seq := cell.sequence.Load()
		dif := int64(seq) - int64(pos)
		if dif == 0 {
			if r.tail.CompareAndSwap(pos, pos+1) {
				cell.data = val
				cell.sequence.Store(pos + 1)
				r.metrics.RecordPush()
				return nil
			}
		} else if dif < 0 {
			r.metrics.RecordFullContention()
		}
		attempts++
		r.metrics.RecordSpin()
		r.backpressure.Wait(attempts)
	}
}

func (r *MPMCRingBuffer[T]) TryPop() (T, bool, error) {
	var zero T
	if r.closed.Load() && r.IsEmpty() {
		return zero, false, ErrBufferClosed
	}
	pos := r.head.Load()
	cell := &r.buffer[pos&r.mask]
	seq := cell.sequence.Load()
	dif := int64(seq) - int64(pos+1)
	if dif == 0 {
		if r.head.CompareAndSwap(pos, pos+1) {
			val := cell.data
			cell.sequence.Store(pos + r.capacity)
			r.metrics.RecordPop()
			return val, true, nil
		}
	}
	if dif < 0 {
		r.metrics.RecordEmptyContention()
		return zero, false, ErrBufferEmpty
	}
	return zero, false, nil
}

func (r *MPMCRingBuffer[T]) Pop() (T, error) {
	var zero T
	attempts := 0
	for {
		if r.closed.Load() && r.IsEmpty() {
			return zero, ErrBufferClosed
		}
		pos := r.head.Load()
		cell := &r.buffer[pos&r.mask]
		seq := cell.sequence.Load()
	dif := int64(seq) - int64(pos+1)
	if dif == 0 {
		if r.head.CompareAndSwap(pos, pos+1) {
			val := cell.data
			cell.sequence.Store(pos + r.capacity)
			r.metrics.RecordPop()
			return val, nil
		}
	} else if dif < 0 {
		r.metrics.RecordEmptyContention()
	}
	attempts++
	r.metrics.RecordSpin()
	r.backpressure.Wait(attempts)
	}
}

func (r *MPMCRingBuffer[T]) PushBatch(items []T) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}
	if uint64(len(items)) > r.capacity {
		return 0, ErrBatchTooLarge
	}
	succeeded := 0
	for _, item := range items {
		err := r.Push(item)
		if err != nil {
			return succeeded, err
		}
		succeeded++
	}
	r.metrics.RecordBatchPush(succeeded)
	return succeeded, nil
}

func (r *MPMCRingBuffer[T]) PopBatch(dest []T) (int, error) {
	if len(dest) == 0 {
		return 0, ErrNilDestination
	}
	popped := 0
	for i := 0; i < len(dest); i++ {
		val, ok, err := r.TryPop()
		if !ok {
			if err != nil && err != ErrBufferEmpty {
				return popped, err
			}
			break
		}
		dest[i] = val
		popped++
	}
	if popped > 0 {
		r.metrics.RecordBatchPop(popped)
	}
	return popped, nil
}

func (r *MPMCRingBuffer[T]) PushTimeout(val T, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	attempts := 0
	for {
		if r.closed.Load() {
			return ErrBufferClosed
		}
		ok, _ := r.TryPush(val)
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return ErrTimeout
		}
		attempts++
		r.backpressure.Wait(attempts)
	}
}

func (r *MPMCRingBuffer[T]) PopTimeout(timeout time.Duration) (T, error) {
	var zero T
	deadline := time.Now().Add(timeout)
	attempts := 0
	for {
		if r.closed.Load() && r.IsEmpty() {
			return zero, ErrBufferClosed
		}
		val, ok, _ := r.TryPop()
		if ok {
			return val, nil
		}
		if time.Now().After(deadline) {
			return zero, ErrTimeout
		}
		attempts++
		r.backpressure.Wait(attempts)
	}
}

func (r *MPMCRingBuffer[T]) Close() {
	r.closed.Store(true)
}

func (r *MPMCRingBuffer[T]) IsClosed() bool {
	return r.closed.Load()
}

func (r *MPMCRingBuffer[T]) Reset() {
	r.head.Store(0)
	r.tail.Store(0)
	for i := uint64(0); i < r.capacity; i++ {
		r.buffer[i].sequence.Store(i)
	}
	r.closed.Store(false)
}

type PartitionedMPMCRing[T any] struct {
	partitions []*MPMCRingBuffer[T]
	numParts   uint64
	mask       uint64
	roundRobin atomic.Uint64
}

func NewPartitionedMPMCRing[T any](numPartitions int, partitionCap uint64) (*PartitionedMPMCRing[T], error) {
	if numPartitions <= 0 {
		return nil, errors.New("number of partitions must be positive")
	}
	parts := make([]*MPMCRingBuffer[T], numPartitions)
	for i := 0; i < numPartitions; i++ {
		r, err := NewMPMCRingBuffer[T](partitionCap)
		if err != nil {
			return nil, err
		}
		parts[i] = r
	}
	return &PartitionedMPMCRing[T]{
		partitions: parts,
		numParts:   uint64(numPartitions),
		mask:       uint64(numPartitions) - 1,
	}, nil
}

func (p *PartitionedMPMCRing[T]) PushKey(key string, val T) error {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	idx := uint64(h.Sum32()) % p.numParts
	return p.partitions[idx].Push(val)
}

func (p *PartitionedMPMCRing[T]) Push(val T) error {
	idx := p.roundRobin.Add(1) % p.numParts
	return p.partitions[idx].Push(val)
}

func (p *PartitionedMPMCRing[T]) Pop() (T, error) {
	var zero T
	attempts := 0
	for {
		start := p.roundRobin.Add(1) % p.numParts
		for i := uint64(0); i < p.numParts; i++ {
			idx := (start + i) % p.numParts
			val, ok, err := p.partitions[idx].TryPop()
			if ok {
				return val, nil
			}
			if err != nil && err != ErrBufferEmpty {
				return zero, err
			}
		}
		attempts++
		runtime.Gosched()
	}
}

func (p *PartitionedMPMCRing[T]) TotalCapacity() uint64 {
	total := uint64(0)
	for _, part := range p.partitions {
		total += part.Capacity()
	}
	return total
}

func (p *PartitionedMPMCRing[T]) TotalLen() uint64 {
	total := uint64(0)
	for _, part := range p.partitions {
		total += part.Len()
	}
	return total
}

type RingPipelineProcessor struct {
	ring          *MPMCRingBuffer[RingMessage]
	activeWorkers atomic.Int32
	totalHandled  atomic.Uint64
}

func NewRingPipelineProcessor(cap uint64) (*RingPipelineProcessor, error) {
	r, err := NewMPMCRingBuffer[RingMessage](cap)
	if err != nil {
		return nil, err
	}
	return &RingPipelineProcessor{ring: r}, nil
}

func (proc *RingPipelineProcessor) Submit(msg RingMessage) error {
	if err := msg.Validate(); err != nil {
		return err
	}
	return proc.ring.Push(msg)
}

func (proc *RingPipelineProcessor) ProcessOne() (RingMessage, error) {
	msg, err := proc.ring.Pop()
	if err != nil {
		return RingMessage{}, err
	}
	proc.totalHandled.Add(1)
	return msg, nil
}

func (proc *RingPipelineProcessor) HandledCount() uint64 {
	return proc.totalHandled.Load()
}

type TelemetryStreamAggregator struct {
	ring         *MPMCRingBuffer[TelemetryFrame]
	sensorCounts [16]atomic.Uint64
}

func NewTelemetryStreamAggregator(cap uint64) (*TelemetryStreamAggregator, error) {
	r, err := NewMPMCRingBuffer[TelemetryFrame](cap)
	if err != nil {
		return nil, err
	}
	return &TelemetryStreamAggregator{ring: r}, nil
}

func (agg *TelemetryStreamAggregator) Ingest(frame TelemetryFrame) error {
	if err := frame.Validate(); err != nil {
		return err
	}
	idx := frame.SensorID % 16
	agg.sensorCounts[idx].Add(1)
	return agg.ring.Push(frame)
}

func (agg *TelemetryStreamAggregator) Consume() (TelemetryFrame, error) {
	return agg.ring.Pop()
}

func (agg *TelemetryStreamAggregator) CountForSensor(sensorID uint32) uint64 {
	return agg.sensorCounts[sensorID%16].Load()
}

type OrderMatchingEngineRing struct {
	ring       *MPMCRingBuffer[OrderBookEvent]
	totalBuy   atomic.Uint64
	totalSell  atomic.Uint64
	volume     atomic.Int64
}

func NewOrderMatchingEngineRing(cap uint64) (*OrderMatchingEngineRing, error) {
	r, err := NewMPMCRingBuffer[OrderBookEvent](cap)
	if err != nil {
		return nil, err
	}
	return &OrderMatchingEngineRing{ring: r}, nil
}

func (ome *OrderMatchingEngineRing) SubmitOrder(order OrderBookEvent) error {
	if err := order.Validate(); err != nil {
		return err
	}
	if order.Side == 1 {
		ome.totalBuy.Add(1)
	} else {
		ome.totalSell.Add(1)
	}
	ome.volume.Add(order.Price * order.Quantity)
	return ome.ring.Push(order)
}

func (ome *OrderMatchingEngineRing) PollOrder() (OrderBookEvent, error) {
	return ome.ring.Pop()
}

func (ome *OrderMatchingEngineRing) OrderStats() (uint64, uint64, int64) {
	return ome.totalBuy.Load(), ome.totalSell.Load(), ome.volume.Load()
}

// BatchBufferPool implements a lock-free memory recycling pool for slice batches
type PoolNode[T any] struct {
	slice []T
	next  atomic.Pointer[PoolNode[T]]
}

type BatchBufferPool[T any] struct {
	head        atomic.Pointer[PoolNode[T]]
	sliceCap    int
	allocations atomic.Uint64
	recycles    atomic.Uint64
}

func NewBatchBufferPool[T any](sliceCap int) *BatchBufferPool[T] {
	return &BatchBufferPool[T]{
		sliceCap: sliceCap,
	}
}

func (p *BatchBufferPool[T]) Get() []T {
	for {
		oldHead := p.head.Load()
		if oldHead == nil {
			p.allocations.Add(1)
			return make([]T, 0, p.sliceCap)
		}
		next := oldHead.next.Load()
		if p.head.CompareAndSwap(oldHead, next) {
			return oldHead.slice[:0]
		}
	}
}

func (p *BatchBufferPool[T]) Put(slice []T) {
	if cap(slice) < p.sliceCap {
		return
	}
	p.recycles.Add(1)
	node := &PoolNode[T]{slice: slice}
	for {
		oldHead := p.head.Load()
		node.next.Store(oldHead)
		if p.head.CompareAndSwap(oldHead, node) {
			return
		}
	}
}

func (p *BatchBufferPool[T]) Stats() (uint64, uint64) {
	return p.allocations.Load(), p.recycles.Load()
}
// LockFreeRingSnapshotter safely extracts a consistent view of active elements
type LockFreeRingSnapshotter[T any] struct {
	ring *MPMCRingBuffer[T]
}

func NewLockFreeRingSnapshotter[T any](ring *MPMCRingBuffer[T]) *LockFreeRingSnapshotter[T] {
	return &LockFreeRingSnapshotter[T]{ring: ring}
}

func (s *LockFreeRingSnapshotter[T]) Snapshot() []T {
	h := uint64(s.ring.head.Load())
	t := uint64(s.ring.tail.Load())
	if t <= h {
		return nil
	}
	count := t - h
	if count > s.ring.capacity {
		count = s.ring.capacity
	}
	res := make([]T, 0, count)
	for i := uint64(0); i < count; i++ {
		pos := h + i
		cell := &s.ring.buffer[pos&s.ring.mask]
		res = append(res, cell.data)
	}
	return res
}

// RingCursor provides forward iteration over ring elements
type RingCursor[T any] struct {
	ring       *MPMCRingBuffer[T]
	currentPos uint64
	endPos     uint64
}

func (s *LockFreeRingSnapshotter[T]) NewCursor() *RingCursor[T] {
	h := uint64(s.ring.head.Load())
	t := uint64(s.ring.tail.Load())
	return &RingCursor[T]{
		ring:       s.ring,
		currentPos: h,
		endPos:     t,
	}
}

func (c *RingCursor[T]) HasNext() bool {
	return c.currentPos < c.endPos
}

func (c *RingCursor[T]) Next() (T, bool) {
	var zero T
	if !c.HasNext() {
		return zero, false
	}
	pos := c.currentPos
	c.currentPos++
	cell := &c.ring.buffer[pos&c.ring.mask]
	return cell.data, true
}

// RingStreamMultiplexer fans out events to multiple subscriber rings
type RingStreamMultiplexer[T any] struct {
	subscribers []*MPMCRingBuffer[T]
	numSubs     uint64
	dropOnFull  atomic.Bool
	totalSent   atomic.Uint64
	totalDrops  atomic.Uint64
}

func NewRingStreamMultiplexer[T any](subs []*MPMCRingBuffer[T], dropOnFull bool) (*RingStreamMultiplexer[T], error) {
	if len(subs) == 0 {
		return nil, errors.New("must provide at least one subscriber ring")
	}
	m := &RingStreamMultiplexer[T]{
		subscribers: subs,
		numSubs:     uint64(len(subs)),
	}
	m.dropOnFull.Store(dropOnFull)
	return m, nil
}

func (m *RingStreamMultiplexer[T]) Broadcast(item T) (int, error) {
	sent := 0
	for _, sub := range m.subscribers {
		if m.dropOnFull.Load() {
			ok, _ := sub.TryPush(item)
			if ok {
				sent++
			} else {
				m.totalDrops.Add(1)
			}
		} else {
			err := sub.Push(item)
			if err != nil {
				return sent, err
			}
			sent++
		}
	}
	m.totalSent.Add(uint64(sent))
	return sent, nil
}

func (m *RingStreamMultiplexer[T]) Stats() (uint64, uint64) {
	return m.totalSent.Load(), m.totalDrops.Load()
}

// RingStreamDemultiplexer aggregates events from multiple producer rings
type RingStreamDemultiplexer[T any] struct {
	sources       []*MPMCRingBuffer[T]
	numSources    uint64
	roundRobinIdx atomic.Uint64
	totalReceived atomic.Uint64
}

func NewRingStreamDemultiplexer[T any](sources []*MPMCRingBuffer[T]) (*RingStreamDemultiplexer[T], error) {
	if len(sources) == 0 {
		return nil, errors.New("must provide at least one source ring")
	}
	return &RingStreamDemultiplexer[T]{
		sources:    sources,
		numSources: uint64(len(sources)),
	}, nil
}

func (d *RingStreamDemultiplexer[T]) Poll() (T, bool, error) {
	var zero T
	start := d.roundRobinIdx.Add(1) % d.numSources
	for i := uint64(0); i < d.numSources; i++ {
		idx := (start + i) % d.numSources
		val, ok, err := d.sources[idx].TryPop()
		if ok {
			d.totalReceived.Add(1)
			return val, true, nil
		}
		if err != nil && err != ErrBufferEmpty && err != ErrBufferClosed {
			return zero, false, err
		}
	}
	return zero, false, nil
}

func (d *RingStreamDemultiplexer[T]) TotalReceived() uint64 {
	return d.totalReceived.Load()
}

// RingHealthInspector monitors ring latency, skew, and throughput
type RingHealthReport struct {
	Capacity      uint64
	CurrentLen    uint64
	HeadPosition  uint64
	TailPosition  uint64
	SequenceSkew  uint64
	IsFull        bool
	IsEmpty       bool
	IsClosed      bool
	DropCount     uint64
	ContentionOps uint64
}

type RingHealthInspector[T any] struct {
	ring *MPMCRingBuffer[T]
}

func NewRingHealthInspector[T any](ring *MPMCRingBuffer[T]) *RingHealthInspector[T] {
	return &RingHealthInspector[T]{ring: ring}
}

func (ins *RingHealthInspector[T]) Inspect() RingHealthReport {
	h := uint64(ins.ring.head.Load())
	t := uint64(ins.ring.tail.Load())
	snap := ins.ring.metrics.Snapshot()
	skew := uint64(0)
	if t > h {
		skew = t - h
	}
	return RingHealthReport{
		Capacity:      ins.ring.capacity,
		CurrentLen:    ins.ring.Len(),
		HeadPosition:  uint64(h),
		TailPosition:  uint64(t),
		SequenceSkew:  skew,
		IsFull:        ins.ring.IsFull(),
		IsEmpty:       ins.ring.IsEmpty(),
		IsClosed:      ins.ring.IsClosed(),
		DropCount:     snap.DropCount,
		ContentionOps: snap.FullContention + snap.EmptyContention,
	}
}

func (r RingHealthReport) String() string {
	return fmt.Sprintf("Health: len=%d/%d head=%d tail=%d skew=%d full=%v empty=%v drops=%d contentions=%d",
		r.CurrentLen, r.Capacity, r.HeadPosition, r.TailPosition, r.SequenceSkew, r.IsFull, r.IsEmpty, r.DropCount, r.ContentionOps)
}

// DisruptorPipelineStage coordinates sequential lock-free event pipelines
type DisruptorPipelineStage[T any] struct {
	stageID       string
	inRing        *MPMCRingBuffer[T]
	outRing       *MPMCRingBuffer[T]
	processedRows atomic.Uint64
	errorRows     atomic.Uint64
}

func NewDisruptorPipelineStage[T any](id string, in, out *MPMCRingBuffer[T]) *DisruptorPipelineStage[T] {
	return &DisruptorPipelineStage[T]{
		stageID: id,
		inRing:  in,
		outRing: out,
	}
}

func (s *DisruptorPipelineStage[T]) ProcessStep(transform func(in T) (T, error)) error {
	item, err := s.inRing.Pop()
	if err != nil {
		return err
	}
	outItem, err := transform(item)
	if err != nil {
		s.errorRows.Add(1)
		return err
	}
	if s.outRing != nil {
		err := s.outRing.Push(outItem)
		if err != nil {
			s.errorRows.Add(1)
			return err
		}
	}
	s.processedRows.Add(1)
	return nil
}

func (s *DisruptorPipelineStage[T]) Stats() (uint64, uint64) {
	return s.processedRows.Load(), s.errorRows.Load()
}

// RingEventLogger formats and captures lock-free telemetry events
type RingEventLogger struct {
	ring       *MPMCRingBuffer[RingMessage]
	logSeq     atomic.Uint64
	totalBytes atomic.Uint64
}

func NewRingEventLogger(cap uint64) (*RingEventLogger, error) {
	r, err := NewMPMCRingBuffer[RingMessage](cap)
	if err != nil {
		return nil, err
	}
	return &RingEventLogger{ring: r}, nil
}

func (l *RingEventLogger) Log(topic string, data []byte) error {
	seq := l.logSeq.Add(1)
	msg := NewRingMessage(seq, topic, data, 0)
	l.totalBytes.Add(uint64(len(data)))
	return l.ring.Push(msg)
}

func (l *RingEventLogger) TryLog(topic string, data []byte) bool {
	seq := l.logSeq.Add(1)
	msg := NewRingMessage(seq, topic, data, 0)
	ok, _ := l.ring.TryPush(msg)
	if ok {
		l.totalBytes.Add(uint64(len(data)))
	}
	return ok
}

func (l *RingEventLogger) ReadLog() (RingMessage, error) {
	return l.ring.Pop()
}

func (l *RingEventLogger) TotalBytes() uint64 {
	return l.totalBytes.Load()
}

// MultiProducerWorkerPool distributes task execution over a lock-free ring
type TaskPayload struct {
	TaskID    uint64
	Data      int64
	Timestamp int64
}

type RingWorkerCoordinator struct {
	ring        *MPMCRingBuffer[TaskPayload]
	totalPushed atomic.Uint64
	totalSolved atomic.Uint64
}

func NewRingWorkerCoordinator(cap uint64) (*RingWorkerCoordinator, error) {
	r, err := NewMPMCRingBuffer[TaskPayload](cap)
	if err != nil {
		return nil, err
	}
	return &RingWorkerCoordinator{ring: r}, nil
}

func (c *RingWorkerCoordinator) SubmitTask(task TaskPayload) error {
	c.totalPushed.Add(1)
	return c.ring.Push(task)
}

func (c *RingWorkerCoordinator) ExecuteNext(processor func(TaskPayload) error) error {
	task, err := c.ring.Pop()
	if err != nil {
		return err
	}
	if err := processor(task); err != nil {
		return err
	}
	c.totalSolved.Add(1)
	return nil
}

// HighThroughputOrderRouter routes financial trading orders to lock-free matchers
type OrderMatchResult struct {
	BuyOrderID  uint64
	SellOrderID uint64
	Symbol      string
	MatchPrice  int64
	MatchQty    int64
	Timestamp   int64
}

type HighThroughputOrderRouter struct {
	inRing        *MPMCRingBuffer[OrderBookEvent]
	outRing       *MPMCRingBuffer[OrderMatchResult]
	symbolIndex   map[string]uint32
	totalRouted   atomic.Uint64
	totalMatched  atomic.Uint64
}

func NewHighThroughputOrderRouter(cap uint64) (*HighThroughputOrderRouter, error) {
	in, err := NewMPMCRingBuffer[OrderBookEvent](cap)
	if err != nil {
		return nil, err
	}
	out, err := NewMPMCRingBuffer[OrderMatchResult](cap)
	if err != nil {
		return nil, err
	}
	return &HighThroughputOrderRouter{
		inRing:      in,
		outRing:     out,
		symbolIndex: make(map[string]uint32),
	}, nil
}

func (r *HighThroughputOrderRouter) Submit(order OrderBookEvent) error {
	if err := order.Validate(); err != nil {
		return err
	}
	r.totalRouted.Add(1)
	return r.inRing.Push(order)
}

func (r *HighThroughputOrderRouter) PollMatched() (OrderMatchResult, error) {
	return r.outRing.Pop()
}

func (r *HighThroughputOrderRouter) MatchStep(matchLogic func(OrderBookEvent) (OrderMatchResult, bool)) error {
	order, err := r.inRing.Pop()
	if err != nil {
		return err
	}
	match, ok := matchLogic(order)
	if ok {
		r.totalMatched.Add(1)
		return r.outRing.Push(match)
	}
	return nil
}

func (r *HighThroughputOrderRouter) Stats() (uint64, uint64) {
	return r.totalRouted.Load(), r.totalMatched.Load()
}

// LockFreeTelemetryAnalyticsEngine computes real-time streaming statistics
type TelemetryStatsReport struct {
	TotalFrames      uint64
	MinReading       float64
	MaxReading       float64
	AverageReading   float64
	VarianceReading  float64
	SensorHistograms [16]uint64
}

type LockFreeTelemetryAnalyticsEngine struct {
	ingestRing   *MPMCRingBuffer[TelemetryFrame]
	totalFrames  atomic.Uint64
	sumReadings  atomic.Int64 // fixed point * 10000
	sumSquares   atomic.Int64 // fixed point * 10000
	sensorCounts [16]atomic.Uint64
}

func NewLockFreeTelemetryAnalyticsEngine(cap uint64) (*LockFreeTelemetryAnalyticsEngine, error) {
	r, err := NewMPMCRingBuffer[TelemetryFrame](cap)
	if err != nil {
		return nil, err
	}
	return &LockFreeTelemetryAnalyticsEngine{ingestRing: r}, nil
}

func (a *LockFreeTelemetryAnalyticsEngine) Feed(frame TelemetryFrame) error {
	if err := frame.Validate(); err != nil {
		return err
	}
	return a.ingestRing.Push(frame)
}

func (a *LockFreeTelemetryAnalyticsEngine) ProcessBatch(limit int) (int, error) {
	processed := 0
	for i := 0; i < limit; i++ {
		frame, ok, err := a.ingestRing.TryPop()
		if !ok {
			if err != nil && err != ErrBufferEmpty {
				return processed, err
			}
			break
		}
		a.totalFrames.Add(1)
		fixedVal := int64(frame.Reading * 10000.0)
		a.sumReadings.Add(fixedVal)
		sqVal := int64(frame.Reading * frame.Reading * 10000.0)
		a.sumSquares.Add(sqVal)
		idx := frame.SensorID % 16
		a.sensorCounts[idx].Add(1)
		processed++
	}
	return processed, nil
}

func (a *LockFreeTelemetryAnalyticsEngine) Report() TelemetryStatsReport {
	n := a.totalFrames.Load()
	var rep TelemetryStatsReport
	rep.TotalFrames = n
	if n > 0 {
		sum := float64(a.sumReadings.Load()) / 10000.0
		rep.AverageReading = sum / float64(n)
		sqSum := float64(a.sumSquares.Load()) / 10000.0
		meanSq := sqSum / float64(n)
		rep.VarianceReading = meanSq - (rep.AverageReading * rep.AverageReading)
		if rep.VarianceReading < 0 {
			rep.VarianceReading = 0
		}
	}
	for i := 0; i < 16; i++ {
		rep.SensorHistograms[i] = a.sensorCounts[i].Load()
	}
	return rep
}

// MultiTierRingCache manages fast ring buffers for short-lived items
type CacheItem struct {
	Key       string
	Value     []byte
	ExpiresAt int64
}

type MultiTierRingCache struct {
	tiers     []*MPMCRingBuffer[CacheItem]
	tierCount uint64
	totalHits atomic.Uint64
	totalMiss atomic.Uint64
}

func NewMultiTierRingCache(tiers int, tierCap uint64) (*MultiTierRingCache, error) {
	if tiers <= 0 {
		return nil, errors.New("tiers must be positive")
	}
	rings := make([]*MPMCRingBuffer[CacheItem], tiers)
	for i := 0; i < tiers; i++ {
		r, err := NewMPMCRingBuffer[CacheItem](tierCap)
		if err != nil {
			return nil, err
		}
		rings[i] = r
	}
	return &MultiTierRingCache{
		tiers:     rings,
		tierCount: uint64(tiers),
	}, nil
}

func (c *MultiTierRingCache) Put(item CacheItem) error {
	h := fnv.New32a()
	_, _ = h.Write([]byte(item.Key))
	idx := uint64(h.Sum32()) % c.tierCount
	return c.tiers[idx].Push(item)
}

func (c *MultiTierRingCache) TryGet(key string) (CacheItem, bool) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	idx := uint64(h.Sum32()) % c.tierCount
	item, ok, _ := c.tiers[idx].TryPop()
	if ok {
		if time.Now().UnixNano() <= item.ExpiresAt {
			c.totalHits.Add(1)
			return item, true
		}
	}
	c.totalMiss.Add(1)
	return CacheItem{}, false
}

func (c *MultiTierRingCache) HitRatio() float64 {
	hits := c.totalHits.Load()
	misses := c.totalMiss.Load()
	total := hits + misses
	if total == 0 {
		return 0.0
	}
	return (float64(hits) / float64(total)) * 100.0
}

// HighCapacityRingBatcher accumulates items before bulk dispatch
type HighCapacityRingBatcher struct {
	ring           *MPMCRingBuffer[int64]
	batchThreshold int
	dispatched     atomic.Uint64
}

func NewHighCapacityRingBatcher(cap uint64, threshold int) (*HighCapacityRingBatcher, error) {
	r, err := NewMPMCRingBuffer[int64](cap)
	if err != nil {
		return nil, err
	}
	return &HighCapacityRingBatcher{
		ring:           r,
		batchThreshold: threshold,
	}, nil
}

func (b *HighCapacityRingBatcher) Enqueue(val int64) error {
	return b.ring.Push(val)
}

func (b *HighCapacityRingBatcher) FlushBatch() ([]int64, error) {
	dest := make([]int64, b.batchThreshold)
	n, err := b.ring.PopBatch(dest)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	b.dispatched.Add(uint64(n))
	return dest[:n], nil
}

// LockFreeRingJournal implements a high-throughput write-ahead log journal
type JournalRecord struct {
	RecordID  uint64
	Timestamp int64
	Topic     string
	Data      []byte
	Epoch     uint64
}

func NewJournalRecord(id uint64, topic string, data []byte, epoch uint64) JournalRecord {
	cp := make([]byte, len(data))
	copy(cp, data)
	return JournalRecord{
		RecordID:  id,
		Timestamp: time.Now().UnixNano(),
		Topic:     topic,
		Data:      cp,
		Epoch:     epoch,
	}
}

type LockFreeRingJournal struct {
	ring       *MPMCRingBuffer[JournalRecord]
	currEpoch  atomic.Uint64
	totalBytes atomic.Uint64
	syncedRows atomic.Uint64
}

func NewLockFreeRingJournal(cap uint64) (*LockFreeRingJournal, error) {
	r, err := NewMPMCRingBuffer[JournalRecord](cap)
	if err != nil {
		return nil, err
	}
	return &LockFreeRingJournal{ring: r}, nil
}

func (j *LockFreeRingJournal) AdvanceEpoch() uint64 {
	return j.currEpoch.Add(1)
}

func (j *LockFreeRingJournal) CurrentEpoch() uint64 {
	return j.currEpoch.Load()
}

func (j *LockFreeRingJournal) AppendEntry(topic string, data []byte) error {
	epoch := j.currEpoch.Load()
	id := j.syncedRows.Add(1)
	rec := NewJournalRecord(id, topic, data, epoch)
	j.totalBytes.Add(uint64(len(data)))
	return j.ring.Push(rec)
}

func (j *LockFreeRingJournal) ReadEntry() (JournalRecord, error) {
	return j.ring.Pop()
}

func (j *LockFreeRingJournal) Stats() (uint64, uint64, uint64) {
	return j.syncedRows.Load(), j.totalBytes.Load(), j.currEpoch.Load()
}

// LockFreeRingRateLimiter implements a token bucket rate limiter over a ring
type TokenBucketCell struct {
	Timestamp int64
	Tokens    int64
}

type LockFreeRingRateLimiter struct {
	ring           *MPMCRingBuffer[TokenBucketCell]
	refillInterval time.Duration
	tokensPerTick  int64
	burstTokens    int64
	available      atomic.Int64
	consumed       atomic.Uint64
	rejected       atomic.Uint64
}

func NewLockFreeRingRateLimiter(ratePerSec, burst int64) (*LockFreeRingRateLimiter, error) {
	if ratePerSec <= 0 || burst <= 0 {
		return nil, errors.New("rate and burst must be positive")
	}
	r, err := NewMPMCRingBuffer[TokenBucketCell](128)
	if err != nil {
		return nil, err
	}
	lim := &LockFreeRingRateLimiter{
		ring:           r,
		refillInterval: time.Second / time.Duration(ratePerSec),
		tokensPerTick:  1,
		burstTokens:    burst,
	}
	lim.available.Store(burst)
	return lim, nil
}

func (lim *LockFreeRingRateLimiter) Allow() bool {
	return lim.AllowN(1)
}

func (lim *LockFreeRingRateLimiter) AllowN(n int64) bool {
	for {
		cur := lim.available.Load()
		if cur < n {
			lim.rejected.Add(1)
			return false
		}
		if lim.available.CompareAndSwap(cur, cur-n) {
			lim.consumed.Add(uint64(n))
			return true
		}
	}
}

func (lim *LockFreeRingRateLimiter) Refill(tokens int64) {
	for {
		cur := lim.available.Load()
		next := cur + tokens
		if next > lim.burstTokens {
			next = lim.burstTokens
		}
		if lim.available.CompareAndSwap(cur, next) {
			return
		}
	}
}

// RingThroughputMonitor calculates moving window operations per second
type RingThroughputMonitor struct {
	ring           *MPMCRingBuffer[RingMessage]
	windowDuration time.Duration
	windowStart    atomic.Int64
	windowOps      atomic.Uint64
	lastRate       atomic.Uint64
}

func NewRingThroughputMonitor(r *MPMCRingBuffer[RingMessage], dur time.Duration) *RingThroughputMonitor {
	m := &RingThroughputMonitor{
		ring:           r,
		windowDuration: dur,
	}
	m.windowStart.Store(time.Now().UnixNano())
	return m
}

func (m *RingThroughputMonitor) MarkOp() {
	m.windowOps.Add(1)
	now := time.Now().UnixNano()
	start := m.windowStart.Load()
	if now-start > m.windowDuration.Nanoseconds() {
		if m.windowStart.CompareAndSwap(start, now) {
			ops := m.windowOps.Swap(0)
			sec := float64(now-start) / 1e9
			if sec > 0 {
				rate := uint64(float64(ops) / sec)
				m.lastRate.Store(rate)
			}
		}
	}
}

func (m *RingThroughputMonitor) CurrentRate() uint64 {
	return m.lastRate.Load()
}

// EventBatchAccumulator groups small items into large chunks for storage
type EventBatchAccumulator struct {
	ring        *MPMCRingBuffer[RingMessage]
	chunkSize   int
	totalChunks atomic.Uint64
}

func NewEventBatchAccumulator(r *MPMCRingBuffer[RingMessage], chunkSize int) *EventBatchAccumulator {
	return &EventBatchAccumulator{
		ring:      r,
		chunkSize: chunkSize,
	}
}

func (a *EventBatchAccumulator) DrainChunk() ([]RingMessage, error) {
	dest := make([]RingMessage, a.chunkSize)
	n, err := a.ring.PopBatch(dest)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	a.totalChunks.Add(1)
	return dest[:n], nil
}

func (a *EventBatchAccumulator) ChunksDrained() uint64 {
	return a.totalChunks.Load()
}
type TelemetryStreamPipeline struct {
	ring          *MPMCRingBuffer[TelemetryFrame]
	watermark     atomic.Int64
	anomalyCount  atomic.Uint64
	readingSumBits atomic.Uint64
	sampleCount   atomic.Uint64
	minValBits    atomic.Uint64
	maxValBits    atomic.Uint64
	isPaused      atomic.Bool
	flushInterval int64
}

func NewTelemetryStreamPipeline(ring *MPMCRingBuffer[TelemetryFrame]) *TelemetryStreamPipeline {
	p := &TelemetryStreamPipeline{
		ring:          ring,
		flushInterval: 100,
	}
	p.watermark.Store(0)
	p.minValBits.Store(math.Float64bits(math.MaxFloat64))
	p.maxValBits.Store(math.Float64bits(-math.MaxFloat64))
	return p
}

func (p *TelemetryStreamPipeline) Ingest(frame TelemetryFrame) error {
	if p.isPaused.Load() {
		return ErrBufferClosed
	}
	if err := frame.Validate(); err != nil {
		return err
	}
	p.sampleCount.Add(1)
	for {
		curBits := p.readingSumBits.Load()
		curSum := math.Float64frombits(curBits)
		newSum := curSum + frame.Reading
		newBits := math.Float64bits(newSum)
		if p.readingSumBits.CompareAndSwap(curBits, newBits) {
			break
		}
	}
	p.AdvanceWatermark(frame.Timestamp)
	return p.ring.Push(frame)
}

func (p *TelemetryStreamPipeline) ProcessBatch(batchSize int) ([]TelemetryFrame, error) {
	if batchSize <= 0 {
		return nil, ErrNilDestination
	}
	dest := make([]TelemetryFrame, batchSize)
	n, err := p.ring.PopBatch(dest)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	for i := 0; i < n; i++ {
		if p.DetectAnomaly(dest[i], 1000.0) {
			p.anomalyCount.Add(1)
		}
	}
	return dest[:n], nil
}

func (p *TelemetryStreamPipeline) GetAverageReading() float64 {
	count := p.sampleCount.Load()
	if count == 0 {
		return 0.0
	}
	raw := p.readingSumBits.Load()
	return math.Float64frombits(raw) / float64(count)
}

func (p *TelemetryStreamPipeline) GetWatermark() int64 {
	return p.watermark.Load()
}

func (p *TelemetryStreamPipeline) AdvanceWatermark(ts int64) bool {
	for {
		cur := p.watermark.Load()
		if ts <= cur {
			return false
		}
		if p.watermark.CompareAndSwap(cur, ts) {
			return true
		}
	}
}

func (p *TelemetryStreamPipeline) DetectAnomaly(frame TelemetryFrame, threshold float64) bool {
	return frame.Reading > threshold
}

func (p *TelemetryStreamPipeline) AnomalyCount() uint64 {
	return p.anomalyCount.Load()
}

func (p *TelemetryStreamPipeline) Pause() {
	p.isPaused.Store(true)
}

func (p *TelemetryStreamPipeline) Resume() {
	p.isPaused.Store(false)
}

func (p *TelemetryStreamPipeline) ResetStats() {
	p.anomalyCount.Store(0)
	p.readingSumBits.Store(0)
	p.sampleCount.Store(0)
	p.watermark.Store(0)
}

type OrderBookMatchingEngine struct {
	inboundRing    *MPMCRingBuffer[OrderBookEvent]
	totalMatched   atomic.Uint64
	totalVolume    atomic.Uint64
	lastTradePrice atomic.Int64
	bidDepth       atomic.Int64
	askDepth       atomic.Int64
	bidCount       atomic.Uint64
	askCount       atomic.Uint64
	canceledCount  atomic.Uint64
	halted         atomic.Bool
}

func NewOrderBookMatchingEngine(inbound *MPMCRingBuffer[OrderBookEvent]) *OrderBookMatchingEngine {
	return &OrderBookMatchingEngine{
		inboundRing: inbound,
	}
}

func (e *OrderBookMatchingEngine) SubmitOrder(order OrderBookEvent) error {
	if e.halted.Load() {
		return ErrBufferClosed
	}
	if err := order.Validate(); err != nil {
		return err
	}
	if order.Side == 1 {
		e.bidDepth.Add(order.Quantity)
		e.bidCount.Add(1)
	} else {
		e.askDepth.Add(order.Quantity)
		e.askCount.Add(1)
	}
	return e.inboundRing.Push(order)
}

func (e *OrderBookMatchingEngine) PollAndMatch(batchSize int) (int, error) {
	if batchSize <= 0 {
		return 0, ErrNilDestination
	}
	dest := make([]OrderBookEvent, batchSize)
	n, err := e.inboundRing.PopBatch(dest)
	if err != nil {
		return 0, err
	}
	matched := 0
	for i := 0; i < n; i++ {
		ev := dest[i]
		if ev.Side == 1 {
			e.bidDepth.Add(-ev.Quantity)
		} else {
			e.askDepth.Add(-ev.Quantity)
		}
		e.totalMatched.Add(1)
		e.totalVolume.Add(uint64(ev.Price * ev.Quantity))
		e.lastTradePrice.Store(ev.Price)
		matched++
	}
	return matched, nil
}

func (e *OrderBookMatchingEngine) GetMarketStats() (int64, uint64, uint64) {
	return e.lastTradePrice.Load(), e.totalVolume.Load(), e.totalMatched.Load()
}

func (e *OrderBookMatchingEngine) GetDepth() (int64, int64) {
	return e.bidDepth.Load(), e.askDepth.Load()
}

func (e *OrderBookMatchingEngine) SimulateFill(price int64, qty int64) (int64, int64) {
	if qty <= 0 {
		return 0, 0
	}
	for {
		bid := e.bidDepth.Load()
		if price <= e.lastTradePrice.Load() {
			if qty <= bid {
				if e.bidDepth.CompareAndSwap(bid, bid-qty) {
					return qty, 0
				}
				continue
			}
			if e.bidDepth.CompareAndSwap(bid, 0) {
				return bid, qty - bid
			}
			continue
		}
		return 0, qty
	}
}

func (e *OrderBookMatchingEngine) CancelOrder(orderID uint64) bool {
	if orderID == 0 {
		return false
	}
	e.canceledCount.Add(1)
	return true
}

func (e *OrderBookMatchingEngine) Halt() {
	e.halted.Store(true)
}

func (e *OrderBookMatchingEngine) Unhalt() {
	e.halted.Store(false)
}

func (e *OrderBookMatchingEngine) IsHalted() bool {
	return e.halted.Load()
}

type MessageDeduplicator struct {
	slots           []atomic.Uint64
	generations     []atomic.Uint32
	mask            uint64
	capacity        uint64
	totalDuplicates atomic.Uint64
	totalUnique     atomic.Uint64
	currentGen      atomic.Uint32
}

func NewMessageDeduplicator(capacity uint64) *MessageDeduplicator {
	capPow2 := NextPowerOfTwo(capacity)
	d := &MessageDeduplicator{
		slots:       make([]atomic.Uint64, capPow2),
		generations: make([]atomic.Uint32, capPow2),
		mask:        capPow2 - 1,
		capacity:    capPow2,
	}
	d.currentGen.Store(1)
	return d
}

func (d *MessageDeduplicator) hash(msgID uint64) uint64 {
	h := fnv.New64a()
	var b [8]byte
	b[0] = byte(msgID)
	b[1] = byte(msgID >> 8)
	b[2] = byte(msgID >> 16)
	b[3] = byte(msgID >> 24)
	b[4] = byte(msgID >> 32)
	b[5] = byte(msgID >> 40)
	b[6] = byte(msgID >> 48)
	b[7] = byte(msgID >> 56)
	_, _ = h.Write(b[:])
	return h.Sum64()
}

func (d *MessageDeduplicator) CheckAndRecord(msgID uint64) bool {
	idx := d.hash(msgID) & d.mask
	for {
		cur := d.slots[idx].Load()
		if cur == msgID {
			d.totalDuplicates.Add(1)
			return true
		}
		if cur == 0 {
			if d.slots[idx].CompareAndSwap(0, msgID) {
				d.totalUnique.Add(1)
				return false
			}
			continue
		}
		if d.slots[idx].CompareAndSwap(cur, msgID) {
			d.totalUnique.Add(1)
			return false
		}
	}
}

func (d *MessageDeduplicator) IsDuplicate(msgID uint64) bool {
	idx := d.hash(msgID) & d.mask
	return d.slots[idx].Load() == msgID
}

func (d *MessageDeduplicator) EvictGeneration(gen uint32) {
	d.currentGen.Add(1)
	for i := uint64(0); i < d.capacity; i++ {
		d.slots[i].Store(0)
	}
}

func (d *MessageDeduplicator) GetStats() (uint64, uint64) {
	return d.totalUnique.Load(), d.totalDuplicates.Load()
}

func (d *MessageDeduplicator) Reset() {
	d.totalDuplicates.Store(0)
	d.totalUnique.Store(0)
	for i := uint64(0); i < d.capacity; i++ {
		d.slots[i].Store(0)
	}
}

type PartitionRouter struct {
	ring             *PartitionedMPMCRing[RingMessage]
	partitionCount   uint64
	routedCounts     []atomic.Uint64
	errorCounts      []atomic.Uint64
	roundRobinCursor atomic.Uint64
	isDegraded       atomic.Bool
}

func NewPartitionRouter(ring *PartitionedMPMCRing[RingMessage], partitionCount uint64) *PartitionRouter {
	return &PartitionRouter{
		ring:           ring,
		partitionCount: partitionCount,
		routedCounts:   make([]atomic.Uint64, partitionCount),
		errorCounts:    make([]atomic.Uint64, partitionCount),
	}
}

func (pr *PartitionRouter) RouteByTopic(msg RingMessage) error {
	if pr.partitionCount == 0 {
		return ErrPartitionNotFound
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(msg.Topic))
	partIdx := uint64(h.Sum32()) % pr.partitionCount
	pr.routedCounts[partIdx].Add(1)
	return pr.ring.Push(msg)
}

func (pr *PartitionRouter) RouteByChecksum(msg RingMessage) error {
	if pr.partitionCount == 0 {
		return ErrPartitionNotFound
	}
	partIdx := uint64(msg.Checksum) % pr.partitionCount
	pr.routedCounts[partIdx].Add(1)
	return pr.ring.Push(msg)
}

func (pr *PartitionRouter) RouteRoundRobin(msg RingMessage) error {
	if pr.partitionCount == 0 {
		return ErrPartitionNotFound
	}
	idx := pr.roundRobinCursor.Add(1) % pr.partitionCount
	pr.routedCounts[idx].Add(1)
	return pr.ring.Push(msg)
}

func (pr *PartitionRouter) GetPartitionLoad(partition uint64) uint64 {
	if partition >= pr.partitionCount {
		return 0
	}
	return pr.routedCounts[partition].Load()
}

func (pr *PartitionRouter) GetTotalRouted() uint64 {
	total := uint64(0)
	for i := uint64(0); i < pr.partitionCount; i++ {
		total += pr.routedCounts[i].Load()
	}
	return total
}

func (pr *PartitionRouter) IsHealthy() bool {
	return !pr.isDegraded.Load()
}

func (pr *PartitionRouter) MarkDegraded() {
	pr.isDegraded.Store(true)
}

func (pr *PartitionRouter) Recover() {
	pr.isDegraded.Store(false)
}

type RingMetricsAggregator struct {
	pushLatencies    [16]atomic.Uint64
	popLatencies     [16]atomic.Uint64
	contentionCount  atomic.Uint64
	totalBytesMoved  atomic.Uint64
	pushCount        atomic.Uint64
	popCount         atomic.Uint64
	windowStart      atomic.Int64
	windowDurationNs int64
}

func NewRingMetricsAggregator(window time.Duration) *RingMetricsAggregator {
	a := &RingMetricsAggregator{
		windowDurationNs: window.Nanoseconds(),
	}
	a.windowStart.Store(time.Now().UnixNano())
	return a
}

func (a *RingMetricsAggregator) RecordPushLatency(ns uint64) {
	a.pushCount.Add(1)
	bucket := ns / 500
	if bucket >= 16 {
		bucket = 15
	}
	a.pushLatencies[bucket].Add(1)
}

func (a *RingMetricsAggregator) RecordPopLatency(ns uint64) {
	a.popCount.Add(1)
	bucket := ns / 500
	if bucket >= 16 {
		bucket = 15
	}
	a.popLatencies[bucket].Add(1)
}

func (a *RingMetricsAggregator) RecordContention() {
	a.contentionCount.Add(1)
}

func (a *RingMetricsAggregator) RecordBytes(n uint64) {
	a.totalBytesMoved.Add(n)
}

func (a *RingMetricsAggregator) GetAveragePushLatency() float64 {
	cnt := a.pushCount.Load()
	if cnt == 0 {
		return 0.0
	}
	var sum uint64
	for i := 0; i < 16; i++ {
		sum += a.pushLatencies[i].Load() * uint64((i+1)*500)
	}
	return float64(sum) / float64(cnt)
}

func (a *RingMetricsAggregator) GetAveragePopLatency() float64 {
	cnt := a.popCount.Load()
	if cnt == 0 {
		return 0.0
	}
	var sum uint64
	for i := 0; i < 16; i++ {
		sum += a.popLatencies[i].Load() * uint64((i+1)*500)
	}
	return float64(sum) / float64(cnt)
}

func (a *RingMetricsAggregator) GetPercentilePushLatency(pct float64) uint64 {
	if pct <= 0 || pct > 1.0 {
		return 0
	}
	total := a.pushCount.Load()
	if total == 0 {
		return 0
	}
	target := uint64(float64(total) * pct)
	var accum uint64
	for i := 0; i < 16; i++ {
		accum += a.pushLatencies[i].Load()
		if accum >= target {
			return uint64((i + 1) * 500)
		}
	}
	return 16 * 500
}

func (a *RingMetricsAggregator) GetThroughputBytesPerSec() float64 {
	elapsed := time.Now().UnixNano() - a.windowStart.Load()
	if elapsed <= 0 {
		return 0.0
	}
	sec := float64(elapsed) / 1e9
	return float64(a.totalBytesMoved.Load()) / sec
}

func (a *RingMetricsAggregator) Reset() {
	for i := 0; i < 16; i++ {
		a.pushLatencies[i].Store(0)
		a.popLatencies[i].Store(0)
	}
	a.contentionCount.Store(0)
	a.totalBytesMoved.Store(0)
	a.pushCount.Store(0)
	a.popCount.Store(0)
	a.windowStart.Store(time.Now().UnixNano())
}

type CircularBatchStagingBuffer struct {
	items    []RingMessage
	head     atomic.Uint64
	tail     atomic.Uint64
	mask     uint64
	capacity uint64
	aborted  atomic.Uint64
}

func NewCircularBatchStagingBuffer(capacity uint64) *CircularBatchStagingBuffer {
	capPow2 := NextPowerOfTwo(capacity)
	return &CircularBatchStagingBuffer{
		items:    make([]RingMessage, capPow2),
		mask:     capPow2 - 1,
		capacity: capPow2,
	}
}

func (s *CircularBatchStagingBuffer) StageMessage(msg RingMessage) bool {
	for {
		h := s.head.Load()
		t := s.tail.Load()
		if h-t >= s.capacity {
			return false
		}
		if s.head.CompareAndSwap(h, h+1) {
			s.items[h&s.mask] = msg
			return true
		}
	}
}

func (s *CircularBatchStagingBuffer) CommitBatch(dest *MPMCRingBuffer[RingMessage]) (int, error) {
	for {
		t := s.tail.Load()
		h := s.head.Load()
		count := int(h - t)
		if count <= 0 {
			return 0, nil
		}
		if s.tail.CompareAndSwap(t, h) {
			batch := make([]RingMessage, count)
			for i := 0; i < count; i++ {
				batch[i] = s.items[(t+uint64(i))&s.mask]
			}
			return dest.PushBatch(batch)
		}
	}
}

func (s *CircularBatchStagingBuffer) AbortBatch() uint64 {
	for {
		h := s.head.Load()
		t := s.tail.Load()
		diff := h - t
		if s.tail.CompareAndSwap(t, h) {
			s.aborted.Add(diff)
			return diff
		}
	}
}

func (s *CircularBatchStagingBuffer) StagedCount() uint64 {
	return s.head.Load() - s.tail.Load()
}

func (s *CircularBatchStagingBuffer) AbortedCount() uint64 {
	return s.aborted.Load()
}

type AdaptiveBackoffGovernor struct {
	spinCount         atomic.Uint64
	yieldCount        atomic.Uint64
	sleepCount        atomic.Uint64
	contentionEvents  atomic.Uint64
	currentSpinLimit  atomic.Int32
	currentYieldLimit atomic.Int32
	minSpin           int32
	maxSpin           int32
}

func NewAdaptiveBackoffGovernor(minSpin, maxSpin int32) *AdaptiveBackoffGovernor {
	g := &AdaptiveBackoffGovernor{
		minSpin: minSpin,
		maxSpin: maxSpin,
	}
	g.currentSpinLimit.Store(minSpin)
	g.currentYieldLimit.Store(minSpin * 4)
	return g
}

func (g *AdaptiveBackoffGovernor) RecordSpin() {
	g.spinCount.Add(1)
}

func (g *AdaptiveBackoffGovernor) RecordYield() {
	g.yieldCount.Add(1)
	g.contentionEvents.Add(1)
}

func (g *AdaptiveBackoffGovernor) RecordSleep() {
	g.sleepCount.Add(1)
	g.contentionEvents.Add(2)
}

func (g *AdaptiveBackoffGovernor) GetSpinLimit() int32 {
	return g.currentSpinLimit.Load()
}

func (g *AdaptiveBackoffGovernor) GetYieldLimit() int32 {
	return g.currentYieldLimit.Load()
}

func (g *AdaptiveBackoffGovernor) Adjust() {
	contentions := g.contentionEvents.Load()
	cur := g.currentSpinLimit.Load()
	if contentions > 100 {
		newLimit := cur / 2
		if newLimit < g.minSpin {
			newLimit = g.minSpin
		}
		g.currentSpinLimit.Store(newLimit)
		g.contentionEvents.Store(0)
	} else if contentions < 10 {
		newLimit := cur * 2
		if newLimit > g.maxSpin {
			newLimit = g.maxSpin
		}
		g.currentSpinLimit.Store(newLimit)
	}
}

func (g *AdaptiveBackoffGovernor) ContentionRatio() float64 {
	total := g.spinCount.Load() + g.yieldCount.Load() + g.sleepCount.Load()
	if total == 0 {
		return 0.0
	}
	return float64(g.contentionEvents.Load()) / float64(total)
}
`,

}
