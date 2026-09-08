package tasks

import "benchmark/internal/swe"

var TaskT4SagaOrchestrator = &swe.Task{
	ID:       "swe-t4-saga-orchestrator-01",
	Title:    "High-Throughput Lock-Free Distributed Saga Orchestrator for Financial Transactions",
	Tier:     swe.TierStaff,
	Points:   25,
	Category: "lock_free_concurrency",
	IssueBody: `### Bug Report: State Tearing, ABA Race, Multiple Terminal Winners, and Stale DAG Reads in Lock-Free Saga Engine

**Environment:** Go 1.24+ ultra-high throughput distributed financial transaction orchestration engine with strict lock-free execution guarantees.

**Expected Behavior:**

- The engine must be **STRICTLY LOCK-FREE**. Usage of ` + "`sync.Mutex`" + `, ` + "`sync.RWMutex`" + `, or Go channels in the core implementation (` + "`main.go`" + `) is strictly prohibited and guarded by static verification.

- State transitions must be atomic, linearizable, and immune to ABA races: all snapshots must have monotonically increasing versions, and torn states must never be observed.

- Terminal state transitions must guarantee **EXACTLY ONE WINNER**: a saga can either be ` + "`COMMITTED`" + `, ` + "`COMPENSATED`" + `, or ` + "`FAILED`" + `. Concurrent triggers must never result in dual winners or redundant rollback executions.

- Step dependency DAG resolution must ensure proper release-acquire memory visibility across CPU caches. Downstream steps must never observe stale or partially written outputs from completed upstream dependencies.

- Audit log appending must be lock-free and thread-safe without data races or dropped log entries under concurrent multi-step executions.

- All tests must pass with zero race warnings under ` + "`go test -race`" + `.`,

	BrokenCode: `package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

var (
	ErrSagaNotFound        = errors.New("saga not found")
	ErrSagaAlreadyExists   = errors.New("saga already exists with same id")
	ErrInvalidTransition   = errors.New("invalid saga state transition")
	ErrSagaTerminal        = errors.New("saga is already in a terminal state")
	ErrStepNotFound        = errors.New("step not found in saga definition")
	ErrCyclicDependency   = errors.New("cyclic dependency detected in saga dag")
	ErrMissingDependency   = errors.New("step references non-existent dependency")
	ErrStepExecutionFailed = errors.New("step execution returned an error")
	ErrTimeoutExceeded     = errors.New("saga execution deadline exceeded")
	ErrStoreFull           = errors.New("lock-free saga store capacity exceeded")
	ErrNilSnapshot         = errors.New("nil state snapshot provided")
	ErrInvalidCurrency     = errors.New("unsupported currency code")
	ErrInvalidAmount       = errors.New("invalid monetary amount")
)

type SagaStatus uint32

const (
	StatusUnknown      SagaStatus = 0
	StatusPending      SagaStatus = 1
	StatusExecuting    SagaStatus = 2
	StatusCompensating SagaStatus = 3
	StatusCommitted    SagaStatus = 4
	StatusCompensated  SagaStatus = 5
	StatusFailed       SagaStatus = 6
)

func (s SagaStatus) String() string {
	switch s {
	case StatusPending:
		return "PENDING"
	case StatusExecuting:
		return "EXECUTING"
	case StatusCompensating:
		return "COMPENSATING"
	case StatusCommitted:
		return "COMMITTED"
	case StatusCompensated:
		return "COMPENSATED"
	case StatusFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

func (s SagaStatus) IsTerminal() bool {
	return s == StatusCommitted || s == StatusCompensated || s == StatusFailed
}

func (s SagaStatus) CanTransitionTo(target SagaStatus) bool {
	if s.IsTerminal() {
		return false
	}
	switch s {
	case StatusPending:
		return target == StatusExecuting || target == StatusFailed
	case StatusExecuting:
		return target == StatusCompensating || target == StatusCommitted || target == StatusFailed
	case StatusCompensating:
		return target == StatusCompensated || target == StatusFailed
	default:
		return false
	}
}

type StepStatus uint32

const (
	StepStatusPending      StepStatus = 1
	StepStatusExecuting    StepStatus = 2
	StepStatusCompleted    StepStatus = 3
	StepStatusFailed       StepStatus = 4
	StepStatusCompensating StepStatus = 5
	StepStatusCompensated  StepStatus = 6
	StepStatusSkipped      StepStatus = 7
)

func (ss StepStatus) String() string {
	switch ss {
	case StepStatusPending:
		return "PENDING"
	case StepStatusExecuting:
		return "EXECUTING"
	case StepStatusCompleted:
		return "COMPLETED"
	case StepStatusFailed:
		return "FAILED"
	case StepStatusCompensating:
		return "COMPENSATING"
	case StepStatusCompensated:
		return "COMPENSATED"
	case StepStatusSkipped:
		return "SKIPPED"
	default:
		return "UNKNOWN"
	}
}

func (ss StepStatus) IsFinished() bool {
	return ss == StepStatusCompleted || ss == StepStatusFailed || ss == StepStatusCompensated || ss == StepStatusSkipped
}

type ActionType string

const (
	ActionAuthorizePayment     ActionType = "AUTHORIZE_PAYMENT"
	ActionCapturePayment       ActionType = "CAPTURE_PAYMENT"
	ActionReserveInventory     ActionType = "RESERVE_INVENTORY"
	ActionReleaseInventory     ActionType = "RELEASE_INVENTORY"
	ActionEvaluateFraud        ActionType = "EVALUATE_FRAUD"
	ActionPostLedgerEntry      ActionType = "POST_LEDGER_ENTRY"
	ActionReverseLedgerEntry   ActionType = "REVERSE_LEDGER_ENTRY"
	ActionIssueTaxInvoice      ActionType = "ISSUE_TAX_INVOICE"
	ActionCancelTaxInvoice     ActionType = "CANCEL_TAX_INVOICE"
	ActionDispatchNotification ActionType = "DISPATCH_NOTIFICATION"
	ActionLockExchangeRate     ActionType = "LOCK_EXCHANGE_RATE"
	ActionReleaseExchangeRate  ActionType = "RELEASE_EXCHANGE_RATE"
	ActionCreditBeneficiary    ActionType = "CREDIT_BENEFICIARY"
	ActionDebitBeneficiary     ActionType = "DEBIT_BENEFICIARY"
)

type Currency string

const (
	CurrencyUSD Currency = "USD"
	CurrencyEUR Currency = "EUR"
	CurrencyGBP Currency = "GBP"
	CurrencyJPY Currency = "JPY"
	CurrencyCAD Currency = "CAD"
	CurrencyAUD Currency = "AUD"
	CurrencySGD Currency = "SGD"
)

func IsValidCurrency(c Currency) bool {
	switch c {
	case CurrencyUSD, CurrencyEUR, CurrencyGBP, CurrencyJPY, CurrencyCAD, CurrencyAUD, CurrencySGD:
		return true
	default:
		return false
	}
}

type Money struct {
	AmountCents int64
	Currency    Currency
}

func NewMoney(cents int64, cur Currency) (Money, error) {
	if !IsValidCurrency(cur) {
		return Money{}, ErrInvalidCurrency
	}
	if cents < 0 {
		return Money{}, ErrInvalidAmount
	}
	return Money{AmountCents: cents, Currency: cur}, nil
}

func (m Money) Add(other Money) (Money, error) {
	if m.Currency != other.Currency {
		return Money{}, fmt.Errorf("currency mismatch: %s vs %s", m.Currency, other.Currency)
	}
	if math.MaxInt64-m.AmountCents < other.AmountCents {
		return Money{}, errors.New("integer overflow during money addition")
	}
	return Money{AmountCents: m.AmountCents + other.AmountCents, Currency: m.Currency}, nil
}

func (m Money) Sub(other Money) (Money, error) {
	if m.Currency != other.Currency {
		return Money{}, fmt.Errorf("currency mismatch: %s vs %s", m.Currency, other.Currency)
	}
	if m.AmountCents < other.AmountCents {
		return Money{}, errors.New("insufficient funds for subtraction")
	}
	return Money{AmountCents: m.AmountCents - other.AmountCents, Currency: m.Currency}, nil
}

func (m Money) Multiply(factor float64) Money {
	if factor < 0 {
		factor = 0
	}
	result := float64(m.AmountCents) * factor
	return Money{AmountCents: int64(math.Round(result)), Currency: m.Currency}
}

func (m Money) String() string {
	dollars := m.AmountCents / 100
	cents := m.AmountCents % 100
	return fmt.Sprintf("%d.%02d %s", dollars, cents, m.Currency)
}

func (m Money) IsZero() bool {
	return m.AmountCents == 0
}

type CustomerAccount struct {
	AccountID   string
	TenantID    string
	Balance     Money
	Currency    Currency
	Status      string
	RiskScore   float64
	LastUpdated int64
}

func (ca CustomerAccount) CanDebit(amount Money) bool {
	if ca.Status != "ACTIVE" {
		return false
	}
	if ca.Currency != amount.Currency {
		return false
	}
	return ca.Balance.AmountCents >= amount.AmountCents
}

type StepDefinition struct {
	StepID         string
	Name           string
	ActionType     ActionType
	CompensateType ActionType
	DependsOn      []string
	TimeoutMs      int64
	MaxRetries     uint32
	IsCritical     bool
	Compensable    bool
}

func (sd StepDefinition) Validate() error {
	if strings.TrimSpace(sd.StepID) == "" {
		return errors.New("step id cannot be empty")
	}
	if strings.TrimSpace(sd.Name) == "" {
		return errors.New("step name cannot be empty")
	}
	if sd.TimeoutMs < 0 {
		return errors.New("step timeout cannot be negative")
	}
	return nil
}

type StepResult struct {
	StepID                 string
	Status                 StepStatus
	Payload                []byte
	ErrorMessage           string
	ExecutionDurationNanos int64
	AttemptCount           uint32
}

func (sr StepResult) IsSuccess() bool {
	return sr.Status == StepStatusCompleted
}

type SagaMetadata struct {
	SagaID        string
	TenantID      string
	CorrelationID string
	WorkflowType  string
	CreatedAt     int64
	DeadlineAt    int64
	MaxRetries    uint32
}

func (sm SagaMetadata) IsExpired() bool {
	if sm.DeadlineAt <= 0 {
		return false
	}
	return time.Now().UnixNano() > sm.DeadlineAt
}

type AuditLogEntry struct {
	Sequence   uint64
	Timestamp  int64
	SagaID     string
	StepID     string
	FromStatus SagaStatus
	ToStatus   SagaStatus
	Action     string
	Message    string
	Details    string
}

func (ale AuditLogEntry) Format() string {
	return fmt.Sprintf("[%d][%d] saga=%s step=%s %s->%s act=%s msg=%s",
		ale.Sequence, ale.Timestamp, ale.SagaID, ale.StepID, ale.FromStatus, ale.ToStatus, ale.Action, ale.Message)
}

type SagaStateSnapshot struct {
	Version          uint64
	Status           SagaStatus
	ActiveSteps      uint32
	CompletedSteps   uint32
	FailedSteps      uint32
	CompensatedSteps uint32
	TerminalReason   string
	LastUpdated      int64
}

func (s *SagaStateSnapshot) Clone() *SagaStateSnapshot {
	if s == nil {
		return nil
	}
	return &SagaStateSnapshot{
		Version:          s.Version,
		Status:           s.Status,
		ActiveSteps:      s.ActiveSteps,
		CompletedSteps:   s.CompletedSteps,
		FailedSteps:      s.FailedSteps,
		CompensatedSteps: s.CompensatedSteps,
		TerminalReason:   s.TerminalReason,
		LastUpdated:      s.LastUpdated,
	}
}

func (s *SagaStateSnapshot) String() string {
	if s == nil {
		return "<nil>"
	}
	return fmt.Sprintf("v%d status=%s active=%d completed=%d failed=%d compensated=%d reason=%s",
		s.Version, s.Status, s.ActiveSteps, s.CompletedSteps, s.FailedSteps, s.CompensatedSteps, s.TerminalReason)
}

type StepExecutionRecord struct {
	StepID      string
	status      StepStatus
	output      []byte
	errMsg      string
	startedAt   int64
	completedAt int64
	attempts    uint32
}

func NewStepExecutionRecord(stepID string) *StepExecutionRecord {
	return &StepExecutionRecord{
		StepID: stepID,
		status: StepStatusPending,
	}
}

func (r *StepExecutionRecord) GetStatus() StepStatus {
	return r.status
}

func (r *StepExecutionRecord) SetStatus(st StepStatus) {
	r.status = st
}

func (r *StepExecutionRecord) GetOutput() []byte {
	return r.output
}

func (r *StepExecutionRecord) SetOutput(b []byte) {
	r.output = b
}

func (r *StepExecutionRecord) GetError() string {
	return r.errMsg
}

func (r *StepExecutionRecord) SetError(msg string) {
	r.errMsg = msg
}

func (r *StepExecutionRecord) GetAttempts() uint32 {
	return r.attempts
}

func (r *StepExecutionRecord) IncAttempts() uint32 {
	r.attempts++
	return r.attempts
}

func (r *StepExecutionRecord) MarkStarted() {
	r.startedAt = time.Now().UnixNano()
	r.status = StepStatusExecuting
}

func (r *StepExecutionRecord) MarkCompleted(output []byte) {
	r.completedAt = time.Now().UnixNano()
	r.output = output
	r.status = StepStatusCompleted
}

func (r *StepExecutionRecord) MarkFailed(err string) {
	r.completedAt = time.Now().UnixNano()
	r.errMsg = err
	r.status = StepStatusFailed
}

func (r *StepExecutionRecord) Duration() time.Duration {
	if r.completedAt > r.startedAt {
		return time.Duration(r.completedAt - r.startedAt)
	}
	return 0
}

type LockFreeAuditTrail struct {
	entries []AuditLogEntry
	count   atomic.Uint64
}

func NewLockFreeAuditTrail() *LockFreeAuditTrail {
	return &LockFreeAuditTrail{
		entries: make([]AuditLogEntry, 0, 64),
	}
}

func (t *LockFreeAuditTrail) Append(entry AuditLogEntry) uint64 {
	seq := t.count.Add(1)
	entry.Sequence = seq
	t.entries = append(t.entries, entry)
	return seq
}

func (t *LockFreeAuditTrail) Snapshot() []AuditLogEntry {
	res := make([]AuditLogEntry, len(t.entries))
	copy(res, t.entries)
	return res
}

func (t *LockFreeAuditTrail) FilterByStep(stepID string) []AuditLogEntry {
	var matched []AuditLogEntry
	for _, e := range t.entries {
		if e.StepID == stepID {
			matched = append(matched, e)
		}
	}
	return matched
}

func (t *LockFreeAuditTrail) FilterBySaga(sagaID string) []AuditLogEntry {
	var matched []AuditLogEntry
	for _, e := range t.entries {
		if e.SagaID == sagaID {
			matched = append(matched, e)
		}
	}
	return matched
}

func (t *LockFreeAuditTrail) Count() uint64 {
	return t.count.Load()
}

func (t *LockFreeAuditTrail) Latest() (AuditLogEntry, bool) {
	if len(t.entries) == 0 {
		return AuditLogEntry{}, false
	}
	return t.entries[len(t.entries)-1], true
}

type LockFreeDAGNode struct {
	step              StepDefinition
	dependencies      []*LockFreeDAGNode
	dependents        []*LockFreeDAGNode
	totalDependencies uint32
	satisfiedDeps     atomic.Uint32
}

func NewLockFreeDAGNode(step StepDefinition) *LockFreeDAGNode {
	return &LockFreeDAGNode{
		step:              step,
		totalDependencies: uint32(len(step.DependsOn)),
	}
}

func (n *LockFreeDAGNode) IsReady() bool {
	return n.satisfiedDeps.Load() >= n.totalDependencies
}

func (n *LockFreeDAGNode) SatisfyOne() bool {
	newVal := n.satisfiedDeps.Add(1)
	return newVal == n.totalDependencies
}

func (n *LockFreeDAGNode) Reset() {
	n.satisfiedDeps.Store(0)
}

type LockFreeDAGResolver struct {
	nodes    map[string]*LockFreeDAGNode
	nodeList []*LockFreeDAGNode
}

func NewLockFreeDAGResolver(steps []StepDefinition) (*LockFreeDAGResolver, error) {
	nodes := make(map[string]*LockFreeDAGNode, len(steps))
	nodeList := make([]*LockFreeDAGNode, 0, len(steps))
	for _, s := range steps {
		if err := s.Validate(); err != nil {
			return nil, err
		}
		if _, exists := nodes[s.StepID]; exists {
			return nil, fmt.Errorf("duplicate step id: %s", s.StepID)
		}
		node := NewLockFreeDAGNode(s)
		nodes[s.StepID] = node
		nodeList = append(nodeList, node)
	}
	for _, node := range nodeList {
		for _, depID := range node.step.DependsOn {
			depNode, ok := nodes[depID]
			if !ok {
				return nil, fmt.Errorf("%w: step %s depends on %s", ErrMissingDependency, node.step.StepID, depID)
			}
			node.dependencies = append(node.dependencies, depNode)
			depNode.dependents = append(depNode.dependents, node)
		}
	}
	r := &LockFreeDAGResolver{nodes: nodes, nodeList: nodeList}
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *LockFreeDAGResolver) Validate() error {
	visited := make(map[string]int, len(r.nodes))
	var dfs func(id string) error
	dfs = func(id string) error {
		state := visited[id]
		if state == 1 {
			return fmt.Errorf("%w at step %s", ErrCyclicDependency, id)
		}
		if state == 2 {
			return nil
		}
		visited[id] = 1
		node := r.nodes[id]
		for _, dep := range node.dependents {
			if err := dfs(dep.step.StepID); err != nil {
				return err
			}
		}
		visited[id] = 2
		return nil
	}
	for id := range r.nodes {
		if visited[id] == 0 {
			if err := dfs(id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *LockFreeDAGResolver) GetRootSteps() []*LockFreeDAGNode {
	var roots []*LockFreeDAGNode
	for _, n := range r.nodeList {
		if n.totalDependencies == 0 {
			roots = append(roots, n)
		}
	}
	return roots
}

func (r *LockFreeDAGResolver) GetStep(stepID string) (*LockFreeDAGNode, bool) {
	node, ok := r.nodes[stepID]
	return node, ok
}

func (r *LockFreeDAGResolver) MarkStepCompleted(stepID string) ([]*LockFreeDAGNode, error) {
	node, ok := r.nodes[stepID]
	if !ok {
		return nil, ErrStepNotFound
	}
	var newlyReady []*LockFreeDAGNode
	for _, dependent := range node.dependents {
		if dependent.SatisfyOne() {
			newlyReady = append(newlyReady, dependent)
		}
	}
	return newlyReady, nil
}

func (r *LockFreeDAGResolver) Reset() {
	for _, n := range r.nodeList {
		n.Reset()
	}
}

func (r *LockFreeDAGResolver) TotalSteps() int {
	return len(r.nodeList)
}

func (r *LockFreeDAGResolver) TopologicalSort() ([]string, error) {
	inDegree := make(map[string]int, len(r.nodes))
	for id, node := range r.nodes {
		inDegree[id] = int(node.totalDependencies)
	}
	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}
	var sorted []string
	for len(queue) > 0 {
		currID := queue[0]
		queue = queue[1:]
		sorted = append(sorted, currID)
		currNode := r.nodes[currID]
		for _, dep := range currNode.dependents {
			inDegree[dep.step.StepID]--
			if inDegree[dep.step.StepID] == 0 {
				queue = append(queue, dep.step.StepID)
			}
		}
	}
	if len(sorted) != len(r.nodes) {
		return nil, ErrCyclicDependency
	}
	return sorted, nil
}

type LockFreeSagaInstance struct {
	meta       SagaMetadata
	state      atomic.Pointer[SagaStateSnapshot]
	stepMap    map[string]*StepExecutionRecord
	stepList   []*StepExecutionRecord
	dag        *LockFreeDAGResolver
	audit      *LockFreeAuditTrail
	payloadBag atomic.Pointer[map[string]string]
}

func NewLockFreeSagaInstance(meta SagaMetadata, steps []StepDefinition) (*LockFreeSagaInstance, error) {
	dag, err := NewLockFreeDAGResolver(steps)
	if err != nil {
		return nil, err
	}
	stepMap := make(map[string]*StepExecutionRecord, len(steps))
	stepList := make([]*StepExecutionRecord, 0, len(steps))
	for _, s := range steps {
		rec := NewStepExecutionRecord(s.StepID)
		stepMap[s.StepID] = rec
		stepList = append(stepList, rec)
	}
	initialSnap := &SagaStateSnapshot{
		Version:        1,
		Status:         StatusPending,
		ActiveSteps:    0,
		CompletedSteps: 0,
		FailedSteps:     0,
		TerminalReason:  "",
		LastUpdated:    time.Now().UnixNano(),
	}
	bag := make(map[string]string)
	inst := &LockFreeSagaInstance{
		meta:     meta,
		stepMap:  stepMap,
		stepList: stepList,
		dag:      dag,
		audit:    NewLockFreeAuditTrail(),
	}
	inst.state.Store(initialSnap)
	inst.payloadBag.Store(&bag)
	return inst, nil
}

func (s *LockFreeSagaInstance) GetMetadata() SagaMetadata {
	return s.meta
}

func (s *LockFreeSagaInstance) GetSnapshot() *SagaStateSnapshot {
	return s.state.Load()
}

func (s *LockFreeSagaInstance) TransitionTo(to SagaStatus, reason string) bool {
	cur := s.state.Load()
	if cur == nil || cur.Status.IsTerminal() || !cur.Status.CanTransitionTo(to) {
		return false
	}
	cur.Status = to
	cur.TerminalReason = reason
	cur.LastUpdated = time.Now().UnixNano()
	s.state.Store(cur)
	s.audit.Append(AuditLogEntry{
		Timestamp:  cur.LastUpdated,
		SagaID:     s.meta.SagaID,
		StepID:     "",
		FromStatus: cur.Status,
		ToStatus:   to,
		Action:     "TRANSITION",
		Message:    reason,
	})
	return true
}

func (s *LockFreeSagaInstance) Finalize(targetStatus SagaStatus, reason string) bool {
	cur := s.state.Load()
	if cur == nil || cur.Status.IsTerminal() {
		return false
	}
	snap := cur.Clone()
	snap.Status = targetStatus
	snap.TerminalReason = reason
	snap.LastUpdated = time.Now().UnixNano()
	s.state.Store(snap)
	s.audit.Append(AuditLogEntry{
		Timestamp:  snap.LastUpdated,
		SagaID:     s.meta.SagaID,
		StepID:     "",
		FromStatus: cur.Status,
		ToStatus:   targetStatus,
		Action:     "FINALIZE",
		Message:    reason,
	})
	return true
}

func (s *LockFreeSagaInstance) IsTerminal() bool {
	snap := s.state.Load()
	if snap == nil {
		return false
	}
	return snap.Status.IsTerminal()
}

func (s *LockFreeSagaInstance) GetStepRecord(stepID string) (*StepExecutionRecord, bool) {
	rec, ok := s.stepMap[stepID]
	return rec, ok
}

func (s *LockFreeSagaInstance) RecordStepSuccess(stepID string, output []byte) error {
	rec, ok := s.stepMap[stepID]
	if !ok {
		return ErrStepNotFound
	}
	rec.MarkCompleted(output)
	s.audit.Append(AuditLogEntry{
		Timestamp:  time.Now().UnixNano(),
		SagaID:     s.meta.SagaID,
		StepID:     stepID,
		FromStatus: StatusExecuting,
		ToStatus:   StatusExecuting,
		Action:     "STEP_COMPLETED",
		Message:    fmt.Sprintf("step %s succeeded", stepID),
	})
	return nil
}

func (s *LockFreeSagaInstance) RecordStepFailure(stepID string, err string) error {
	rec, ok := s.stepMap[stepID]
	if !ok {
		return ErrStepNotFound
	}
	rec.MarkFailed(err)
	s.audit.Append(AuditLogEntry{
		Timestamp:  time.Now().UnixNano(),
		SagaID:     s.meta.SagaID,
		StepID:     stepID,
		FromStatus: StatusExecuting,
		ToStatus:   StatusCompensating,
		Action:     "STEP_FAILED",
		Message:    fmt.Sprintf("step %s failed: %s", stepID, err),
	})
	return nil
}

func (s *LockFreeSagaInstance) GetPayload(key string) (string, bool) {
	bagPtr := s.payloadBag.Load()
	if bagPtr == nil {
		return "", false
	}
	v, ok := (*bagPtr)[key]
	return v, ok
}

func (s *LockFreeSagaInstance) SetPayload(key, val string) {
	for {
		oldPtr := s.payloadBag.Load()
		newMap := make(map[string]string)
		if oldPtr != nil {
			for k, v := range *oldPtr {
				newMap[k] = v
			}
		}
		newMap[key] = val
		if s.payloadBag.CompareAndSwap(oldPtr, &newMap) {
			return
		}
	}
}

func (s *LockFreeSagaInstance) Summary() string {
	snap := s.state.Load()
	return fmt.Sprintf("Saga[%s] Type=%s Tenant=%s Status=%s Version=%d",
		s.meta.SagaID, s.meta.WorkflowType, s.meta.TenantID, snap.Status, snap.Version)
}

type RegistryBucketNode struct {
	saga *LockFreeSagaInstance
	next atomic.Pointer[RegistryBucketNode]
}

type LockFreeSagaRegistry struct {
	buckets [128]atomic.Pointer[RegistryBucketNode]
	count   atomic.Uint64
}

func NewLockFreeSagaRegistry() *LockFreeSagaRegistry {
	return &LockFreeSagaRegistry{}
}

func hashFNV(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func (r *LockFreeSagaRegistry) Put(saga *LockFreeSagaInstance) error {
	if saga == nil {
		return errors.New("cannot register nil saga")
	}
	id := saga.meta.SagaID
	bucketIdx := hashFNV(id) % 128
	newNode := &RegistryBucketNode{saga: saga}
	for {
		oldHead := r.buckets[bucketIdx].Load()
		curr := oldHead
		for curr != nil {
			if curr.saga.meta.SagaID == id {
				return ErrSagaAlreadyExists
			}
			curr = curr.next.Load()
		}
		newNode.next.Store(oldHead)
		if r.buckets[bucketIdx].CompareAndSwap(oldHead, newNode) {
			r.count.Add(1)
			return nil
		}
	}
}

func (r *LockFreeSagaRegistry) Get(sagaID string) (*LockFreeSagaInstance, bool) {
	bucketIdx := hashFNV(sagaID) % 128
	curr := r.buckets[bucketIdx].Load()
	for curr != nil {
		if curr.saga.meta.SagaID == sagaID {
			return curr.saga, true
		}
		curr = curr.next.Load()
	}
	return nil, false
}

func (r *LockFreeSagaRegistry) Count() int {
	return int(r.count.Load())
}

func (r *LockFreeSagaRegistry) ListAll() []*LockFreeSagaInstance {
	var list []*LockFreeSagaInstance
	for i := 0; i < 128; i++ {
		curr := r.buckets[i].Load()
		for curr != nil {
			list = append(list, curr.saga)
			curr = curr.next.Load()
		}
	}
	return list
}

func (r *LockFreeSagaRegistry) ListByTenant(tenantID string) []*LockFreeSagaInstance {
	all := r.ListAll()
	var res []*LockFreeSagaInstance
	for _, s := range all {
		if s.meta.TenantID == tenantID {
			res = append(res, s)
		}
	}
	return res
}

func (r *LockFreeSagaRegistry) ListActive() []*LockFreeSagaInstance {
	all := r.ListAll()
	var res []*LockFreeSagaInstance
	for _, s := range all {
		if !s.IsTerminal() {
			res = append(res, s)
		}
	}
	return res
}

type MetricsReport struct {
	TotalSagas         uint64
	TotalCommitted     uint64
	TotalCompensated   uint64
	TotalFailed        uint64
	TotalStepRuns      uint64
	TotalStepRetries   uint64
	TotalStepFailures  uint64
	TotalDurationNanos uint64
	AverageDurationMs  float64
	CommitRatePercent  float64
}

type SagaMetricsCollector struct {
	totalSagas         atomic.Uint64
	totalCommitted     atomic.Uint64
	totalCompensated   atomic.Uint64
	totalFailed        atomic.Uint64
	totalStepRuns      atomic.Uint64
	totalStepRetries   atomic.Uint64
	totalStepFailures  atomic.Uint64
	totalDurationNanos atomic.Uint64
}

func NewSagaMetricsCollector() *SagaMetricsCollector {
	return &SagaMetricsCollector{}
}

func (m *SagaMetricsCollector) RecordSagaStarted() {
	m.totalSagas.Add(1)
}

func (m *SagaMetricsCollector) RecordSagaCompleted(st SagaStatus, duration time.Duration) {
	m.totalDurationNanos.Add(uint64(duration.Nanoseconds()))
	switch st {
	case StatusCommitted:
		m.totalCommitted.Add(1)
	case StatusCompensated:
		m.totalCompensated.Add(1)
	case StatusFailed:
		m.totalFailed.Add(1)
	}
}

func (m *SagaMetricsCollector) RecordStepExecution(success bool, retried bool) {
	m.totalStepRuns.Add(1)
	if retried {
		m.totalStepRetries.Add(1)
	}
	if !success {
		m.totalStepFailures.Add(1)
	}
}

func (m *SagaMetricsCollector) Snapshot() MetricsReport {
	sagas := m.totalSagas.Load()
	commits := m.totalCommitted.Load()
	compensations := m.totalCompensated.Load()
	failures := m.totalFailed.Load()
	stepRuns := m.totalStepRuns.Load()
	stepRetries := m.totalStepRetries.Load()
	stepFails := m.totalStepFailures.Load()
	dur := m.totalDurationNanos.Load()

	avgMs := 0.0
	commitRate := 0.0
	totalFinished := commits + compensations + failures
	if totalFinished > 0 {
		avgMs = float64(dur) / float64(totalFinished) / 1e6
		commitRate = (float64(commits) / float64(totalFinished)) * 100.0
	}

	return MetricsReport{
		TotalSagas:         sagas,
		TotalCommitted:     commits,
		TotalCompensated:   compensations,
		TotalFailed:        failures,
		TotalStepRuns:      stepRuns,
		TotalStepRetries:   stepRetries,
		TotalStepFailures:  stepFails,
		TotalDurationNanos: dur,
		AverageDurationMs:  avgMs,
		CommitRatePercent:  commitRate,
	}
}

type StepActionFunc func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult
type StepCompensateFunc func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) error

type SagaEngine struct {
	registry      *LockFreeSagaRegistry
	metrics       *SagaMetricsCollector
	actions       map[ActionType]StepActionFunc
	compensations map[ActionType]StepCompensateFunc
}

func NewSagaEngine() *SagaEngine {
	e := &SagaEngine{
		registry:      NewLockFreeSagaRegistry(),
		metrics:       NewSagaMetricsCollector(),
		actions:       make(map[ActionType]StepActionFunc),
		compensations: make(map[ActionType]StepCompensateFunc),
	}
	e.installDefaultHandlers()
	return e
}

func (e *SagaEngine) RegisterAction(act ActionType, fn StepActionFunc) {
	e.actions[act] = fn
}

func (e *SagaEngine) RegisterCompensation(act ActionType, fn StepCompensateFunc) {
	e.compensations[act] = fn
}

func (e *SagaEngine) SubmitSaga(saga *LockFreeSagaInstance) error {
	e.metrics.RecordSagaStarted()
	return e.registry.Put(saga)
}

func (e *SagaEngine) ExecuteStep(ctx context.Context, saga *LockFreeSagaInstance, node *LockFreeDAGNode) StepResult {
	fn, ok := e.actions[node.step.ActionType]
	if !ok {
		return StepResult{
			StepID:       node.step.StepID,
			Status:       StepStatusFailed,
			ErrorMessage: fmt.Sprintf("no handler registered for action %s", node.step.ActionType),
		}
	}
	start := time.Now()
	var res StepResult
	var attempts uint32
	rec, _ := saga.GetStepRecord(node.step.StepID)
	if rec != nil {
		rec.MarkStarted()
	}
	for {
		attempts++
		if rec != nil {
			rec.IncAttempts()
		}
		res = fn(ctx, saga, node.step)
		res.StepID = node.step.StepID
		res.AttemptCount = attempts
		if res.IsSuccess() || attempts > node.step.MaxRetries {
			break
		}
		e.metrics.RecordStepExecution(false, true)
		runtime.Gosched()
	}
	res.ExecutionDurationNanos = time.Since(start).Nanoseconds()
	e.metrics.RecordStepExecution(res.IsSuccess(), attempts > 1)
	if res.IsSuccess() {
		saga.RecordStepSuccess(node.step.StepID, res.Payload)
	} else {
		saga.RecordStepFailure(node.step.StepID, res.ErrorMessage)
	}
	return res
}

func (e *SagaEngine) ExecuteCompensationWave(ctx context.Context, saga *LockFreeSagaInstance) error {
	order, err := saga.dag.TopologicalSort()
	if err != nil {
		return err
	}
	for i := len(order) - 1; i >= 0; i-- {
		stepID := order[i]
		node, ok := saga.dag.GetStep(stepID)
		if !ok || !node.step.Compensable {
			continue
		}
		rec, ok := saga.GetStepRecord(stepID)
		if !ok || rec.GetStatus() != StepStatusCompleted {
			continue
		}
		compFn, ok := e.compensations[node.step.CompensateType]
		if ok {
			rec.SetStatus(StepStatusCompensating)
			err := compFn(ctx, saga, node.step)
			if err != nil {
				rec.SetStatus(StepStatusFailed)
			} else {
				rec.SetStatus(StepStatusCompensated)
			}
		}
	}
	return nil
}

func (e *SagaEngine) TriggerCommit(saga *LockFreeSagaInstance, reason string) bool {
	snap := saga.GetSnapshot()
	if snap != nil && snap.Status == StatusCommitted {
		return false
	}
	saga.Finalize(StatusCommitted, reason)
	e.metrics.RecordSagaCompleted(StatusCommitted, 0)
	return true
}

func (e *SagaEngine) TriggerCompensate(saga *LockFreeSagaInstance, reason string) bool {
	snap := saga.GetSnapshot()
	if snap != nil && snap.Status == StatusCompensated {
		return false
	}
	saga.Finalize(StatusCompensated, reason)
	e.metrics.RecordSagaCompleted(StatusCompensated, 0)
	return true
}

func (e *SagaEngine) TriggerFail(saga *LockFreeSagaInstance, reason string) bool {
	snap := saga.GetSnapshot()
	if snap != nil && snap.Status == StatusFailed {
		return false
	}
	saga.Finalize(StatusFailed, reason)
	e.metrics.RecordSagaCompleted(StatusFailed, 0)
	return true
}

func (e *SagaEngine) ExecuteWorkflowSync(ctx context.Context, saga *LockFreeSagaInstance) (SagaStatus, error) {
	if !saga.TransitionTo(StatusExecuting, "workflow execution started") {
		return saga.GetSnapshot().Status, ErrInvalidTransition
	}
	ready := saga.dag.GetRootSteps()
	for len(ready) > 0 {
		var nextRound []*LockFreeDAGNode
		for _, node := range ready {
			res := e.ExecuteStep(ctx, saga, node)
			if !res.IsSuccess() {
				if node.step.IsCritical {
					saga.TransitionTo(StatusCompensating, fmt.Sprintf("critical step %s failed", node.step.StepID))
					e.ExecuteCompensationWave(ctx, saga)
					e.TriggerCompensate(saga, "rolled back due to critical step failure")
					return StatusCompensated, nil
				}
			} else {
				unblocked, err := saga.dag.MarkStepCompleted(node.step.StepID)
				if err == nil {
					nextRound = append(nextRound, unblocked...)
				}
			}
		}
		ready = nextRound
	}
	e.TriggerCommit(saga, "all steps completed successfully")
	return StatusCommitted, nil
}

func (e *SagaEngine) installDefaultHandlers() {
	e.RegisterAction(ActionAuthorizePayment, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		saga.SetPayload("auth_id", "AUTH-99281")
		return StepResult{Status: StepStatusCompleted, Payload: []byte("AUTH_OK")}
	})
	e.RegisterAction(ActionCapturePayment, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		authID, ok := saga.GetPayload("auth_id")
		if !ok {
			return StepResult{Status: StepStatusFailed, ErrorMessage: "missing auth_id"}
		}
		saga.SetPayload("capture_id", "CAP-"+authID)
		return StepResult{Status: StepStatusCompleted, Payload: []byte("CAPTURE_OK")}
	})
	e.RegisterAction(ActionEvaluateFraud, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		saga.SetPayload("fraud_score", "0.12")
		return StepResult{Status: StepStatusCompleted, Payload: []byte("FRAUD_CHECK_PASS")}
	})
	e.RegisterAction(ActionReserveInventory, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		saga.SetPayload("res_id", "RES-55102")
		return StepResult{Status: StepStatusCompleted, Payload: []byte("INVENTORY_HELD")}
	})
	e.RegisterAction(ActionPostLedgerEntry, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		saga.SetPayload("ledger_id", "TX-883719")
		return StepResult{Status: StepStatusCompleted, Payload: []byte("LEDGER_POSTED")}
	})
	e.RegisterAction(ActionIssueTaxInvoice, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		saga.SetPayload("tax_inv", "INV-2026-001")
		return StepResult{Status: StepStatusCompleted, Payload: []byte("INVOICE_ISSUED")}
	})
	e.RegisterAction(ActionDispatchNotification, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		return StepResult{Status: StepStatusCompleted, Payload: []byte("NOTIFIED")}
	})
	e.RegisterAction(ActionLockExchangeRate, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		saga.SetPayload("fx_rate", "1.0850")
		return StepResult{Status: StepStatusCompleted, Payload: []byte("FX_LOCKED")}
	})
	e.RegisterAction(ActionCreditBeneficiary, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		return StepResult{Status: StepStatusCompleted, Payload: []byte("CREDITED")}
	})
	e.RegisterAction(ActionDebitBeneficiary, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		return StepResult{Status: StepStatusCompleted, Payload: []byte("DEBITED")}
	})

	e.RegisterCompensation(ActionReleaseInventory, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) error {
		saga.SetPayload("res_id_cancelled", "true")
		return nil
	})
	e.RegisterCompensation(ActionReverseLedgerEntry, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) error {
		saga.SetPayload("ledger_reversed", "true")
		return nil
	})
	e.RegisterCompensation(ActionCancelTaxInvoice, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) error {
		saga.SetPayload("tax_cancelled", "true")
		return nil
	})
	e.RegisterCompensation(ActionReleaseExchangeRate, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) error {
		saga.SetPayload("fx_released", "true")
		return nil
	})
}

func BuildPaymentTransferWorkflow(sagaID, tenantID, fromAcc, toAcc string, amount Money) (*LockFreeSagaInstance, error) {
	meta := SagaMetadata{
		SagaID:        sagaID,
		TenantID:      tenantID,
		CorrelationID: "CORR-" + sagaID,
		WorkflowType:  "PAYMENT_TRANSFER",
		CreatedAt:     time.Now().UnixNano(),
		MaxRetries:    3,
	}
	steps := []StepDefinition{
		{
			StepID:         "step-fraud-check",
			Name:           "Realtime Fraud Assessment",
			ActionType:     ActionEvaluateFraud,
			Compensable:    false,
			IsCritical:     true,
			MaxRetries:     1,
		},
		{
			StepID:         "step-auth-payment",
			Name:           "Authorize Account Payment",
			ActionType:     ActionAuthorizePayment,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "step-hold-inventory",
			Name:           "Reserve Physical Inventory",
			ActionType:     ActionReserveInventory,
			Compensable:    true,
			CompensateType: ActionReleaseInventory,
			DependsOn:      []string{"step-fraud-check", "step-auth-payment"},
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "step-capture-payment",
			Name:           "Capture Authorized Funds",
			ActionType:     ActionCapturePayment,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"step-hold-inventory"},
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "step-post-ledger",
			Name:           "Post Double-Entry Ledger",
			ActionType:     ActionPostLedgerEntry,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"step-capture-payment"},
			IsCritical:     true,
			MaxRetries:     3,
		},
		{
			StepID:         "step-issue-tax",
			Name:           "Generate Fiscal Tax Invoice",
			ActionType:     ActionIssueTaxInvoice,
			Compensable:    true,
			CompensateType: ActionCancelTaxInvoice,
			DependsOn:      []string{"step-post-ledger"},
			IsCritical:     false,
			MaxRetries:     2,
		},
		{
			StepID:         "step-notify-parties",
			Name:           "Dispatch Notification Receipts",
			ActionType:     ActionDispatchNotification,
			Compensable:    false,
			DependsOn:      []string{"step-issue-tax"},
			IsCritical:     false,
			MaxRetries:     1,
		},
	}
	saga, err := NewLockFreeSagaInstance(meta, steps)
	if err != nil {
		return nil, err
	}
	saga.SetPayload("from_acc", fromAcc)
	saga.SetPayload("to_acc", toAcc)
	saga.SetPayload("amount", amount.String())
	return saga, nil
}

func BuildCrossBorderSettlementWorkflow(sagaID, tenantID string, srcCur, dstCur Currency, amount Money) (*LockFreeSagaInstance, error) {
	meta := SagaMetadata{
		SagaID:        sagaID,
		TenantID:      tenantID,
		CorrelationID: "FX-" + sagaID,
		WorkflowType:  "CROSS_BORDER_FX",
		CreatedAt:     time.Now().UnixNano(),
		MaxRetries:    3,
	}
	steps := []StepDefinition{
		{
			StepID:         "step-fx-lock",
			Name:           "Lock FX Treasury Rate",
			ActionType:     ActionLockExchangeRate,
			Compensable:    true,
			CompensateType: ActionReleaseExchangeRate,
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "step-debit-nostro",
			Name:           "Debit Nostro Liquidity Account",
			ActionType:     ActionDebitBeneficiary,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"step-fx-lock"},
			IsCritical:     true,
			MaxRetries:     3,
		},
		{
			StepID:         "step-credit-vostro",
			Name:           "Credit Vostro Settlement Account",
			ActionType:     ActionCreditBeneficiary,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"step-debit-nostro"},
			IsCritical:     true,
			MaxRetries:     3,
		},
		{
			StepID:         "step-fx-notify",
			Name:           "Send SWIFT Settlement Advice",
			ActionType:     ActionDispatchNotification,
			Compensable:    false,
			DependsOn:      []string{"step-credit-vostro"},
			IsCritical:     false,
			MaxRetries:     1,
		},
	}
	inst, err := NewLockFreeSagaInstance(meta, steps)
	if err != nil {
		return nil, err
	}
	inst.SetPayload("src_cur", string(srcCur))
	inst.SetPayload("dst_cur", string(dstCur))
	inst.SetPayload("amount", amount.String())
	return inst, nil
}

type SagaDetailedReport struct {
	SagaID          string
	TenantID        string
	WorkflowType    string
	FinalStatus     string
	Version         uint64
	TotalSteps      int
	CompletedSteps  int
	FailedSteps     int
	AuditEntries    int
	ExecutionTimeMs int64
}

func GenerateSagaReport(saga *LockFreeSagaInstance) SagaDetailedReport {
	snap := saga.GetSnapshot()
	meta := saga.GetMetadata()
	auditLogs := saga.audit.Snapshot()
	completed := 0
	failed := 0
	for _, rec := range saga.stepList {
		if rec.GetStatus() == StepStatusCompleted {
			completed++
		} else if rec.GetStatus() == StepStatusFailed {
			failed++
		}
	}
	return SagaDetailedReport{
		SagaID:          meta.SagaID,
		TenantID:        meta.TenantID,
		WorkflowType:    meta.WorkflowType,
		FinalStatus:     snap.Status.String(),
		Version:         snap.Version,
		TotalSteps:      len(saga.stepList),
		CompletedSteps:  completed,
		FailedSteps:     failed,
		AuditEntries:    len(auditLogs),
		ExecutionTimeMs: (snap.LastUpdated - meta.CreatedAt) / 1e6,
	}
}

func (r SagaDetailedReport) String() string {
	return fmt.Sprintf("Report: ID=%s Type=%s Status=%s Steps=%d/%d Logs=%d Time=%dms",
		r.SagaID, r.WorkflowType, r.FinalStatus, r.CompletedSteps, r.TotalSteps, r.AuditEntries, r.ExecutionTimeMs)
}

func BuildECommerceOrderWorkflow(sagaID, tenantID string, total Money) (*LockFreeSagaInstance, error) {
	meta := SagaMetadata{
		SagaID:        sagaID,
		TenantID:      tenantID,
		CorrelationID: "ORD-" + sagaID,
		WorkflowType:  "ORDER_FULFILLMENT",
		CreatedAt:     time.Now().UnixNano(),
		MaxRetries:    2,
	}
	steps := []StepDefinition{
		{
			StepID:         "ord-reserve-stock",
			Name:           "Hold Warehouse SKU Stock",
			ActionType:     ActionReserveInventory,
			Compensable:    true,
			CompensateType: ActionReleaseInventory,
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "ord-auth-card",
			Name:           "Authorize Credit Card",
			ActionType:     ActionAuthorizePayment,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"ord-reserve-stock"},
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "ord-capture-funds",
			Name:           "Capture Card Settlement",
			ActionType:     ActionCapturePayment,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"ord-auth-card"},
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "ord-tax-invoice",
			Name:           "Generate Order Invoice",
			ActionType:     ActionIssueTaxInvoice,
			Compensable:    true,
			CompensateType: ActionCancelTaxInvoice,
			DependsOn:      []string{"ord-capture-funds"},
			IsCritical:     false,
			MaxRetries:     2,
		},
		{
			StepID:         "ord-notify-buyer",
			Name:           "Send Shipping Confirmation Email",
			ActionType:     ActionDispatchNotification,
			Compensable:    false,
			DependsOn:      []string{"ord-tax-invoice"},
			IsCritical:     false,
			MaxRetries:     1,
		},
	}
	inst, err := NewLockFreeSagaInstance(meta, steps)
	if err != nil {
		return nil, err
	}
	inst.SetPayload("total_amount", total.String())
	return inst, nil
}

func BuildSubscriptionBillingWorkflow(sagaID, tenantID string, monthlyFee Money) (*LockFreeSagaInstance, error) {
	meta := SagaMetadata{
		SagaID:        sagaID,
		TenantID:      tenantID,
		CorrelationID: "SUB-" + sagaID,
		WorkflowType:  "RECURRING_BILLING",
		CreatedAt:     time.Now().UnixNano(),
		MaxRetries:    3,
	}
	steps := []StepDefinition{
		{
			StepID:         "sub-auth-charge",
			Name:           "Authorize Recurring Charge",
			ActionType:     ActionAuthorizePayment,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "sub-capture-charge",
			Name:           "Capture Subscription Payment",
			ActionType:     ActionCapturePayment,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"sub-auth-charge"},
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "sub-ledger-post",
			Name:           "Post Revenue Recognition Ledger",
			ActionType:     ActionPostLedgerEntry,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"sub-capture-charge"},
			IsCritical:     true,
			MaxRetries:     3,
		},
		{
			StepID:         "sub-send-invoice",
			Name:           "Email Invoice Receipt",
			ActionType:     ActionDispatchNotification,
			Compensable:    false,
			DependsOn:      []string{"sub-ledger-post"},
			IsCritical:     false,
			MaxRetries:     1,
		},
	}
	inst, err := NewLockFreeSagaInstance(meta, steps)
	if err != nil {
		return nil, err
	}
	inst.SetPayload("monthly_fee", monthlyFee.String())
	return inst, nil
}
type LockFreeIdempotencyGate struct {
	slots          []atomic.Uint64
	timestamps     []atomic.Int64
	mask           uint64
	capacity       uint64
	ttlNs          int64
	totalAccepted  atomic.Uint64
	totalRejected  atomic.Uint64
}

func NewLockFreeIdempotencyGate(capacity uint64, ttl time.Duration) *LockFreeIdempotencyGate {
	capPow2 := uint64(1)
	for capPow2 < capacity {
		capPow2 <<= 1
	}
	return &LockFreeIdempotencyGate{
		slots:      make([]atomic.Uint64, capPow2),
		timestamps: make([]atomic.Int64, capPow2),
		mask:       capPow2 - 2,
		capacity:   capPow2,
		ttlNs:      ttl.Nanoseconds(),
	}
}

func (g *LockFreeIdempotencyGate) hashKey(key string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	return h
}

func (g *LockFreeIdempotencyGate) CheckAndAcquire(key string, currentTs int64) bool {
	h := g.hashKey(key)
	idx := h & g.mask
	stored := g.slots[idx].Load()
	ts := g.timestamps[idx].Load()
	if stored == h && currentTs-ts < g.ttlNs {
		g.totalRejected.Add(1)
		return true
	}
	g.slots[idx].Store(h)
	g.timestamps[idx].Store(currentTs)
	g.totalAccepted.Add(1)
	return false
}

func (g *LockFreeIdempotencyGate) Release(key string) {
	h := g.hashKey(key)
	idx := h & g.mask
	g.slots[idx].Store(0)
	g.timestamps[idx].Store(0)
}

func (g *LockFreeIdempotencyGate) EvictExpired(currentTs int64) int {
	evicted := 0
	for i := uint64(0); i < g.capacity; i++ {
		ts := g.timestamps[i].Load()
		if ts > 0 && currentTs-ts >= g.ttlNs {
			g.slots[i].Store(0)
			g.timestamps[i].Store(0)
			evicted++
		}
	}
	return evicted
}

func (g *LockFreeIdempotencyGate) GetStats() (uint64, uint64) {
	return g.totalAccepted.Load(), g.totalRejected.Load()
}

func (g *LockFreeIdempotencyGate) Reset() {
	for i := uint64(0); i < g.capacity; i++ {
		g.slots[i].Store(0)
		g.timestamps[i].Store(0)
	}
	g.totalAccepted.Store(0)
	g.totalRejected.Store(0)
}

type CompensationBackoffScheduler struct {
	baseDelayNs    int64
	maxDelayNs     int64
	factor         float64
	maxRetries     int32
	totalScheduled atomic.Uint64
	totalExhausted atomic.Uint64
}

func NewCompensationBackoffScheduler(base, max time.Duration, factor float64, maxRetries int32) *CompensationBackoffScheduler {
	return &CompensationBackoffScheduler{
		baseDelayNs: base.Nanoseconds(),
		maxDelayNs:  max.Nanoseconds(),
		factor:      factor,
		maxRetries:  maxRetries,
	}
}

func (s *CompensationBackoffScheduler) CalculateDelay(retryCount int32) time.Duration {
	if retryCount <= 0 {
		return time.Duration(s.baseDelayNs)
	}
	mult := float64(int64(1) << retryCount)
	delay := float64(s.baseDelayNs) * mult
	if int64(delay) > s.maxDelayNs {
		delay = float64(s.maxDelayNs)
	}
	return time.Duration(int64(delay))
}

func (s *CompensationBackoffScheduler) RecordScheduled() {
	s.totalScheduled.Add(1)
}

func (s *CompensationBackoffScheduler) RecordExhausted() {
	s.totalExhausted.Add(1)
}

func (s *CompensationBackoffScheduler) ShouldRetry(retryCount int32) bool {
	return retryCount >= s.maxRetries
}

func (s *CompensationBackoffScheduler) GetStats() (uint64, uint64) {
	return s.totalScheduled.Load(), s.totalExhausted.Load()
}

type LockFreeTelemetryAggregator struct {
	totalStarted     atomic.Uint64
	totalCommitted   atomic.Uint64
	totalCompensated atomic.Uint64
	totalFailed      atomic.Uint64
	durationSumNs    atomic.Uint64
	latencyBuckets   [16]atomic.Uint64
	peakConcurrent   atomic.Int64
	activeCount      atomic.Int64
}

func NewLockFreeTelemetryAggregator() *LockFreeTelemetryAggregator {
	return &LockFreeTelemetryAggregator{}
}

func (a *LockFreeTelemetryAggregator) RecordStart() {
	a.totalStarted.Add(1)
	act := a.activeCount.Add(1)
	peak := a.peakConcurrent.Load()
	if act > peak {
		a.peakConcurrent.Store(act)
	}
}

func (a *LockFreeTelemetryAggregator) RecordFinish(status SagaStatus, duration time.Duration) {
	a.activeCount.Add(-1)
	ns := uint64(duration.Nanoseconds())
	a.durationSumNs.Store(a.durationSumNs.Load() + ns)
	switch status {
	case StatusCommitted:
		a.totalCommitted.Add(1)
	case StatusCompensated:
		a.totalCompensated.Add(1)
	case StatusFailed:
		a.totalFailed.Add(1)
	}
	bIdx := ns / 1000000
	if bIdx > 16 {
		bIdx = 15
	}
	a.latencyBuckets[bIdx].Add(1)
}

func (a *LockFreeTelemetryAggregator) GetAverageDuration() time.Duration {
	completed := a.totalCommitted.Load() + a.totalCompensated.Load() + a.totalFailed.Load()
	if completed == 0 {
		return 0
	}
	avgNs := a.durationSumNs.Load() / completed
	return time.Duration(avgNs)
}

func (a *LockFreeTelemetryAggregator) GetPercentileDuration(pct float64) time.Duration {
	completed := a.totalCommitted.Load() + a.totalCompensated.Load() + a.totalFailed.Load()
	if completed == 0 {
		return 0
	}
	target := uint64(float64(completed) * pct)
	var accum uint64
	for i := 15; i >= 0; i-- {
		accum += a.latencyBuckets[i].Load()
		if accum >= target {
			return time.Duration((i + 1) * 1000000)
		}
	}
	return 0
}

func (a *LockFreeTelemetryAggregator) GetSuccessRate() float64 {
	completed := a.totalCommitted.Load() + a.totalCompensated.Load() + a.totalFailed.Load()
	if completed == 0 {
		return 0.0
	}
	return float64(a.totalCommitted.Load()) / float64(completed)
}

func (a *LockFreeTelemetryAggregator) GetActiveCount() int64 {
	return a.activeCount.Load()
}

func (a *LockFreeTelemetryAggregator) GetPeakConcurrent() int64 {
	return a.peakConcurrent.Load()
}

func (a *LockFreeTelemetryAggregator) Reset() {
	a.totalStarted.Store(0)
	a.totalCommitted.Store(0)
	a.totalCompensated.Store(0)
	a.totalFailed.Store(0)
	a.durationSumNs.Store(0)
	a.activeCount.Store(0)
	a.peakConcurrent.Store(0)
	for i := 0; i < 16; i++ {
		a.latencyBuckets[i].Store(0)
	}
}

type LockFreeStepDependencyValidator struct {
	validationErrors atomic.Uint64
	validatedCount   atomic.Uint64
}

func NewLockFreeStepDependencyValidator() *LockFreeStepDependencyValidator {
	return &LockFreeStepDependencyValidator{}
}

func (v *LockFreeStepDependencyValidator) ValidateDAG(steps []StepDefinition) error {
	v.validatedCount.Add(1)
	if len(steps) == 0 {
		v.validationErrors.Add(1)
		return ErrMissingDependency
	}
	ids := make(map[string]bool)
	for _, s := range steps {
		if ids[s.StepID] {
			v.validationErrors.Add(1)
			return ErrStepNotFound
		}
		ids[s.StepID] = true
	}
	for _, s := range steps {
		for _, dep := range s.DependsOn {
			if !ids[dep] {
				v.validationErrors.Add(1)
				return ErrMissingDependency
			}
		}
	}
	if v.DetectCycles(steps) {
		v.validationErrors.Add(1)
		return ErrCyclicDependency
	}
	return nil
}

func (v *LockFreeStepDependencyValidator) DetectCycles(steps []StepDefinition) bool {
	return false
}

func (v *LockFreeStepDependencyValidator) GetStats() (uint64, uint64) {
	return v.validatedCount.Load(), v.validationErrors.Load()
}

type LockFreeSnapshotReplayer struct {
	replayedCount    atomic.Uint64
	divergenceCount  atomic.Uint64
}

func NewLockFreeSnapshotReplayer() *LockFreeSnapshotReplayer {
	return &LockFreeSnapshotReplayer{}
}

func (r *LockFreeSnapshotReplayer) Replay(snapshots []SagaStateSnapshot) (*SagaStateSnapshot, error) {
	if len(snapshots) == 0 {
		return nil, nil
	}
	r.replayedCount.Add(1)
	latest := &snapshots[0]
	for i := 1; i < len(snapshots); i++ {
		cur := &snapshots[i]
		if cur.Version < latest.Version {
			latest = cur
			r.divergenceCount.Add(1)
		}
	}
	return latest, nil
}

func (r *LockFreeSnapshotReplayer) VerifyLinearizability(snaps []SagaStateSnapshot) bool {
	for i := 1; i < len(snaps); i++ {
		if snaps[i].Version <= snaps[i-1].Version {
			return false
		}
	}
	return true
}

func (r *LockFreeSnapshotReplayer) GetStats() (uint64, uint64) {
	return r.replayedCount.Load(), r.divergenceCount.Load()
}

type LockFreeLedgerPostingQueue struct {
	capacity      uint64
	mask          uint64
	entries       []Money
	isCredits     []bool
	head          atomic.Uint64
	tail          atomic.Uint64
	totalDebited  atomic.Int64
	totalCredited atomic.Int64
}

func NewLockFreeLedgerPostingQueue(capacity uint64) *LockFreeLedgerPostingQueue {
	capPow2 := uint64(1)
	for capPow2 < capacity {
		capPow2 <<= 1
	}
	return &LockFreeLedgerPostingQueue{
		capacity:  capPow2,
		mask:      capPow2 - 1,
		entries:   make([]Money, capPow2),
		isCredits: make([]bool, capPow2),
	}
}

func (q *LockFreeLedgerPostingQueue) PostEntry(amount Money, isCredit bool) bool {
	h := q.head.Load()
	t := q.tail.Load()
	if h-t >= q.capacity {
		return false
	}
	idx := h & q.mask
	q.entries[idx] = amount
	q.isCredits[idx] = isCredit
	q.head.Store(h + 1)
	if isCredit {
		q.totalCredited.Store(q.totalCredited.Load() + amount.AmountCents)
	} else {
		q.totalDebited.Store(q.totalDebited.Load() + amount.AmountCents)
	}
	return true
}

func (q *LockFreeLedgerPostingQueue) DrainBatch(maxBatch int) ([]Money, int) {
	t := q.tail.Load()
	h := q.head.Load()
	avail := int(h - t)
	if avail <= 0 {
		return nil, 0
	}
	if avail > maxBatch {
		avail = maxBatch
	}
	batch := make([]Money, avail)
	for i := 0; i < avail; i++ {
		idx := (t + uint64(i)) & q.mask
		batch[i] = q.entries[idx]
	}
	q.tail.Store(t + uint64(avail))
	return batch, avail
}

func (q *LockFreeLedgerPostingQueue) GetTotals() (int64, int64) {
	return q.totalDebited.Load(), q.totalCredited.Load()
}

func (q *LockFreeLedgerPostingQueue) PendingCount() uint64 {
	return q.head.Load() - q.tail.Load()
}

type LockFreeSagaCoordinationFence struct {
	requiredParticipants uint64
	arrivedCount          atomic.Uint64
	epoch                 atomic.Uint64
	timedOut              atomic.Bool
}

func NewLockFreeSagaCoordinationFence(required uint64) *LockFreeSagaCoordinationFence {
	return &LockFreeSagaCoordinationFence{
		requiredParticipants: required,
	}
}

func (f *LockFreeSagaCoordinationFence) Arrive(participantID string) bool {
	cur := f.arrivedCount.Add(1)
	return cur == f.requiredParticipants
}

func (f *LockFreeSagaCoordinationFence) WaitSpin(maxSpins int) bool {
	for i := 0; i < maxSpins; i++ {
		if f.arrivedCount.Load() >= f.requiredParticipants {
			return true
		}
	}
	f.timedOut.Store(true)
	return false
}

func (f *LockFreeSagaCoordinationFence) Reset(newRequired uint64) {
	f.requiredParticipants = newRequired
	f.arrivedCount.Store(0)
	f.timedOut.Store(false)
	f.epoch.Store(f.epoch.Load() + 1)
}

func (f *LockFreeSagaCoordinationFence) IsComplete() bool {
	return f.arrivedCount.Load() >= f.requiredParticipants
}

type LockFreeTransactionJournal struct {
	journalCap      uint64
	mask            uint64
	entries         []AuditLogEntry
	writeCursor     atomic.Uint64
	committedCursor atomic.Uint64
	flushedCursor   atomic.Uint64
	droppedEntries  atomic.Uint64
}

func NewLockFreeTransactionJournal(capacity uint64) *LockFreeTransactionJournal {
	capPow2 := uint64(1)
	for capPow2 < capacity {
		capPow2 <<= 1
	}
	return &LockFreeTransactionJournal{
		journalCap: capPow2,
		mask:       capPow2 - 1,
		entries:    make([]AuditLogEntry, capPow2),
	}
}

func (j *LockFreeTransactionJournal) AppendJournalEntry(entry AuditLogEntry) bool {
	cursor := j.writeCursor.Load()
	flushed := j.flushedCursor.Load()
	if cursor-flushed >= j.journalCap {
		j.droppedEntries.Add(1)
		return false
	}
	j.entries[cursor&j.mask] = entry
	j.writeCursor.Store(cursor + 1)
	return true
}

func (j *LockFreeTransactionJournal) CommitUpTo(cursor uint64) {
	if cursor <= j.committedCursor.Load() {
		j.committedCursor.Store(cursor)
	}
}

func (j *LockFreeTransactionJournal) FlushBatch(maxBatch int) ([]AuditLogEntry, int) {
	flushed := j.flushedCursor.Load()
	committed := j.committedCursor.Load()
	avail := int(committed - flushed)
	if avail <= 0 {
		return nil, 0
	}
	if avail > maxBatch {
		avail = maxBatch
	}
	batch := make([]AuditLogEntry, avail)
	for i := 0; i < avail; i++ {
		batch[i] = j.entries[(flushed+uint64(i))&j.mask]
	}
	j.flushedCursor.Store(flushed + uint64(avail))
	return batch, avail
}

func (j *LockFreeTransactionJournal) GetJournalStats() (uint64, uint64, uint64, uint64) {
	return j.writeCursor.Load(), j.committedCursor.Load(), j.flushedCursor.Load(), j.droppedEntries.Load()
}

func (j *LockFreeTransactionJournal) PendingToFlush() uint64 {
	return j.committedCursor.Load() - j.flushedCursor.Load()
}

`,

	TestCode: `package main

import (
	"bytes"
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLockFreeSaga_AntiCheat_NoLocksOrChannels(t *testing.T) {
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

func TestLockFreeSaga_SingleWorkflowExecution(t *testing.T) {
	engine := NewSagaEngine()
	money, err := NewMoney(150000, CurrencyUSD)
	if err != nil {
		t.Fatalf("failed to create money: %v", err)
	}
	saga, err := BuildPaymentTransferWorkflow("saga-sync-01", "tenant-alpha", "acc-100", "acc-200", money)
	if err != nil {
		t.Fatalf("failed to build workflow: %v", err)
	}
	if err := engine.SubmitSaga(saga); err != nil {
		t.Fatalf("failed to submit saga: %v", err)
	}
	ctx := context.Background()
	status, err := engine.ExecuteWorkflowSync(ctx, saga)
	if err != nil {
		t.Fatalf("workflow execution failed: %v", err)
	}
	if status != StatusCommitted {
		t.Fatalf("expected status COMMITTED, got %s", status)
	}
	snap := saga.GetSnapshot()
	if snap.Status != StatusCommitted {
		t.Fatalf("expected snapshot COMMITTED, got %s", snap.Status)
	}
	if snap.Version < 2 {
		t.Fatalf("expected state version >= 2, got %d", snap.Version)
	}
}

func TestLockFreeSaga_ConcurrentTerminalRace(t *testing.T) {
	engine := NewSagaEngine()
	money, _ := NewMoney(50000, CurrencyUSD)
	saga, err := BuildPaymentTransferWorkflow("saga-term-01", "tenant-test", "acc-1", "acc-2", money)
	if err != nil {
		t.Fatalf("failed to build workflow: %v", err)
	}
	saga.TransitionTo(StatusExecuting, "ready to race")

	const contenders = 50
	var commitWinners atomic.Int32
	var compensateWinners atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if engine.TriggerCommit(saga, "concurrent commit win") {
				commitWinners.Add(1)
			}
		}()
	}

	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if engine.TriggerCompensate(saga, "concurrent compensate win") {
				compensateWinners.Add(1)
			}
		}()
	}

	wg.Wait()
	totalWinners := commitWinners.Load() + compensateWinners.Load()
	if totalWinners != 1 {
		t.Fatalf("terminal invariant violation: exactly 1 winner expected, got %d (commit=%d, compensate=%d)",
			totalWinners, commitWinners.Load(), compensateWinners.Load())
	}
	if !saga.IsTerminal() {
		t.Fatal("saga should be terminal after race")
	}
}

func TestLockFreeSaga_DAGConcurrentMemoryVisibility(t *testing.T) {
	engine := NewSagaEngine()
	money, _ := NewMoney(100000, CurrencyEUR)
	saga, err := BuildCrossBorderSettlementWorkflow("saga-dag-01", "tenant-eur", CurrencyEUR, CurrencyUSD, money)
	if err != nil {
		t.Fatalf("failed to build workflow: %v", err)
	}
	saga.TransitionTo(StatusExecuting, "starting parallel branches")

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			node, ok := saga.dag.GetStep("step-fx-lock")
			if ok {
				res := engine.ExecuteStep(context.Background(), saga, node)
				if len(res.Payload) == 0 {
					t.Errorf("worker %d: empty payload on step completion", workerID)
				}
			}
		}(i)
	}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec, ok := saga.GetStepRecord("step-fx-lock")
			if ok {
				_ = rec.GetStatus()
				_ = rec.GetOutput()
			}
		}()
	}
	wg.Wait()
}

func TestLockFreeSaga_AuditTrailConcurrentSafety(t *testing.T) {
	trail := NewLockFreeAuditTrail()
	const numWriters = 50
	const logsPerWriter = 20
	var wg sync.WaitGroup

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for j := 0; j < logsPerWriter; j++ {
				trail.Append(AuditLogEntry{
					Timestamp: time.Now().UnixNano(),
					SagaID:    "saga-audit-01",
					StepID:    "step-audit",
					Action:    "LOG_TEST",
					Message:   "audit concurrent entry",
				})
			}
		}(i)
	}
	wg.Wait()

	if trail.Count() != numWriters*logsPerWriter {
		t.Fatalf("expected %d logs, got %d", numWriters*logsPerWriter, trail.Count())
	}
	snaps := trail.Snapshot()
	if len(snaps) != numWriters*logsPerWriter {
		t.Fatalf("expected %d snap logs, got %d", numWriters*logsPerWriter, len(snaps))
	}
}

func TestLockFreeSaga_RegistryConcurrency(t *testing.T) {
	reg := NewLockFreeSagaRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sagaID := "saga-reg-" + string(rune('A'+(id%26))) + string(rune('0'+(id/26)))
			money, _ := NewMoney(1000, CurrencyUSD)
			saga, err := BuildPaymentTransferWorkflow(sagaID, "tenant-reg", "acc-a", "acc-b", money)
			if err != nil {
				return
			}
			_ = reg.Put(saga)
			_, _ = reg.Get(sagaID)
		}(i)
	}
	wg.Wait()
	if reg.Count() == 0 {
		t.Fatal("registry count should be > 0 after concurrent puts")
	}
}`,

	TotalTests: 6,
	ReferenceSolution: `package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

var (
	ErrSagaNotFound        = errors.New("saga not found")
	ErrSagaAlreadyExists   = errors.New("saga already exists with same id")
	ErrInvalidTransition   = errors.New("invalid saga state transition")
	ErrSagaTerminal        = errors.New("saga is already in a terminal state")
	ErrStepNotFound        = errors.New("step not found in saga definition")
	ErrCyclicDependency   = errors.New("cyclic dependency detected in saga dag")
	ErrMissingDependency   = errors.New("step references non-existent dependency")
	ErrStepExecutionFailed = errors.New("step execution returned an error")
	ErrTimeoutExceeded     = errors.New("saga execution deadline exceeded")
	ErrStoreFull           = errors.New("lock-free saga store capacity exceeded")
	ErrNilSnapshot         = errors.New("nil state snapshot provided")
	ErrInvalidCurrency     = errors.New("unsupported currency code")
	ErrInvalidAmount       = errors.New("invalid monetary amount")
)

type SagaStatus uint32

const (
	StatusUnknown      SagaStatus = 0
	StatusPending      SagaStatus = 1
	StatusExecuting    SagaStatus = 2
	StatusCompensating SagaStatus = 3
	StatusCommitted    SagaStatus = 4
	StatusCompensated  SagaStatus = 5
	StatusFailed       SagaStatus = 6
)

func (s SagaStatus) String() string {
	switch s {
	case StatusPending:
		return "PENDING"
	case StatusExecuting:
		return "EXECUTING"
	case StatusCompensating:
		return "COMPENSATING"
	case StatusCommitted:
		return "COMMITTED"
	case StatusCompensated:
		return "COMPENSATED"
	case StatusFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

func (s SagaStatus) IsTerminal() bool {
	return s == StatusCommitted || s == StatusCompensated || s == StatusFailed
}

func (s SagaStatus) CanTransitionTo(target SagaStatus) bool {
	if s.IsTerminal() {
		return false
	}
	switch s {
	case StatusPending:
		return target == StatusExecuting || target == StatusFailed
	case StatusExecuting:
		return target == StatusCompensating || target == StatusCommitted || target == StatusFailed
	case StatusCompensating:
		return target == StatusCompensated || target == StatusFailed
	default:
		return false
	}
}

type StepStatus uint32

const (
	StepStatusPending      StepStatus = 1
	StepStatusExecuting    StepStatus = 2
	StepStatusCompleted    StepStatus = 3
	StepStatusFailed       StepStatus = 4
	StepStatusCompensating StepStatus = 5
	StepStatusCompensated  StepStatus = 6
	StepStatusSkipped      StepStatus = 7
)

func (ss StepStatus) String() string {
	switch ss {
	case StepStatusPending:
		return "PENDING"
	case StepStatusExecuting:
		return "EXECUTING"
	case StepStatusCompleted:
		return "COMPLETED"
	case StepStatusFailed:
		return "FAILED"
	case StepStatusCompensating:
		return "COMPENSATING"
	case StepStatusCompensated:
		return "COMPENSATED"
	case StepStatusSkipped:
		return "SKIPPED"
	default:
		return "UNKNOWN"
	}
}

func (ss StepStatus) IsFinished() bool {
	return ss == StepStatusCompleted || ss == StepStatusFailed || ss == StepStatusCompensated || ss == StepStatusSkipped
}

type ActionType string

const (
	ActionAuthorizePayment     ActionType = "AUTHORIZE_PAYMENT"
	ActionCapturePayment       ActionType = "CAPTURE_PAYMENT"
	ActionReserveInventory     ActionType = "RESERVE_INVENTORY"
	ActionReleaseInventory     ActionType = "RELEASE_INVENTORY"
	ActionEvaluateFraud        ActionType = "EVALUATE_FRAUD"
	ActionPostLedgerEntry      ActionType = "POST_LEDGER_ENTRY"
	ActionReverseLedgerEntry   ActionType = "REVERSE_LEDGER_ENTRY"
	ActionIssueTaxInvoice      ActionType = "ISSUE_TAX_INVOICE"
	ActionCancelTaxInvoice     ActionType = "CANCEL_TAX_INVOICE"
	ActionDispatchNotification ActionType = "DISPATCH_NOTIFICATION"
	ActionLockExchangeRate     ActionType = "LOCK_EXCHANGE_RATE"
	ActionReleaseExchangeRate  ActionType = "RELEASE_EXCHANGE_RATE"
	ActionCreditBeneficiary    ActionType = "CREDIT_BENEFICIARY"
	ActionDebitBeneficiary     ActionType = "DEBIT_BENEFICIARY"
)

type Currency string

const (
	CurrencyUSD Currency = "USD"
	CurrencyEUR Currency = "EUR"
	CurrencyGBP Currency = "GBP"
	CurrencyJPY Currency = "JPY"
	CurrencyCAD Currency = "CAD"
	CurrencyAUD Currency = "AUD"
	CurrencySGD Currency = "SGD"
)

func IsValidCurrency(c Currency) bool {
	switch c {
	case CurrencyUSD, CurrencyEUR, CurrencyGBP, CurrencyJPY, CurrencyCAD, CurrencyAUD, CurrencySGD:
		return true
	default:
		return false
	}
}

type Money struct {
	AmountCents int64
	Currency    Currency
}

func NewMoney(cents int64, cur Currency) (Money, error) {
	if !IsValidCurrency(cur) {
		return Money{}, ErrInvalidCurrency
	}
	if cents < 0 {
		return Money{}, ErrInvalidAmount
	}
	return Money{AmountCents: cents, Currency: cur}, nil
}

func (m Money) Add(other Money) (Money, error) {
	if m.Currency != other.Currency {
		return Money{}, fmt.Errorf("currency mismatch: %s vs %s", m.Currency, other.Currency)
	}
	if math.MaxInt64-m.AmountCents < other.AmountCents {
		return Money{}, errors.New("integer overflow during money addition")
	}
	return Money{AmountCents: m.AmountCents + other.AmountCents, Currency: m.Currency}, nil
}

func (m Money) Sub(other Money) (Money, error) {
	if m.Currency != other.Currency {
		return Money{}, fmt.Errorf("currency mismatch: %s vs %s", m.Currency, other.Currency)
	}
	if m.AmountCents < other.AmountCents {
		return Money{}, errors.New("insufficient funds for subtraction")
	}
	return Money{AmountCents: m.AmountCents - other.AmountCents, Currency: m.Currency}, nil
}

func (m Money) Multiply(factor float64) Money {
	if factor < 0 {
		factor = 0
	}
	result := float64(m.AmountCents) * factor
	return Money{AmountCents: int64(math.Round(result)), Currency: m.Currency}
}

func (m Money) String() string {
	dollars := m.AmountCents / 100
	cents := m.AmountCents % 100
	return fmt.Sprintf("%d.%02d %s", dollars, cents, m.Currency)
}

func (m Money) IsZero() bool {
	return m.AmountCents == 0
}

type CustomerAccount struct {
	AccountID   string
	TenantID    string
	Balance     Money
	Currency    Currency
	Status      string
	RiskScore   float64
	LastUpdated int64
}

func (ca CustomerAccount) CanDebit(amount Money) bool {
	if ca.Status != "ACTIVE" {
		return false
	}
	if ca.Currency != amount.Currency {
		return false
	}
	return ca.Balance.AmountCents >= amount.AmountCents
}

type StepDefinition struct {
	StepID         string
	Name           string
	ActionType     ActionType
	CompensateType ActionType
	DependsOn      []string
	TimeoutMs      int64
	MaxRetries     uint32
	IsCritical     bool
	Compensable    bool
}

func (sd StepDefinition) Validate() error {
	if strings.TrimSpace(sd.StepID) == "" {
		return errors.New("step id cannot be empty")
	}
	if strings.TrimSpace(sd.Name) == "" {
		return errors.New("step name cannot be empty")
	}
	if sd.TimeoutMs < 0 {
		return errors.New("step timeout cannot be negative")
	}
	return nil
}

type StepResult struct {
	StepID                 string
	Status                 StepStatus
	Payload                []byte
	ErrorMessage           string
	ExecutionDurationNanos int64
	AttemptCount           uint32
}

func (sr StepResult) IsSuccess() bool {
	return sr.Status == StepStatusCompleted
}

type SagaMetadata struct {
	SagaID        string
	TenantID      string
	CorrelationID string
	WorkflowType  string
	CreatedAt     int64
	DeadlineAt    int64
	MaxRetries    uint32
}

func (sm SagaMetadata) IsExpired() bool {
	if sm.DeadlineAt <= 0 {
		return false
	}
	return time.Now().UnixNano() > sm.DeadlineAt
}

type AuditLogEntry struct {
	Sequence   uint64
	Timestamp  int64
	SagaID     string
	StepID     string
	FromStatus SagaStatus
	ToStatus   SagaStatus
	Action     string
	Message    string
	Details    string
}

func (ale AuditLogEntry) Format() string {
	return fmt.Sprintf("[%d][%d] saga=%s step=%s %s->%s act=%s msg=%s",
		ale.Sequence, ale.Timestamp, ale.SagaID, ale.StepID, ale.FromStatus, ale.ToStatus, ale.Action, ale.Message)
}

type SagaStateSnapshot struct {
	Version          uint64
	Status           SagaStatus
	ActiveSteps      uint32
	CompletedSteps   uint32
	FailedSteps      uint32
	CompensatedSteps uint32
	TerminalReason   string
	LastUpdated      int64
}

func (s *SagaStateSnapshot) Clone() *SagaStateSnapshot {
	if s == nil {
		return nil
	}
	return &SagaStateSnapshot{
		Version:          s.Version,
		Status:           s.Status,
		ActiveSteps:      s.ActiveSteps,
		CompletedSteps:   s.CompletedSteps,
		FailedSteps:      s.FailedSteps,
		CompensatedSteps: s.CompensatedSteps,
		TerminalReason:   s.TerminalReason,
		LastUpdated:      s.LastUpdated,
	}
}

func (s *SagaStateSnapshot) String() string {
	if s == nil {
		return "<nil>"
	}
	return fmt.Sprintf("v%d status=%s active=%d completed=%d failed=%d compensated=%d reason=%s",
		s.Version, s.Status, s.ActiveSteps, s.CompletedSteps, s.FailedSteps, s.CompensatedSteps, s.TerminalReason)
}

type StepExecutionRecord struct {
	StepID      string
	status      atomic.Uint32
	output      atomic.Pointer[[]byte]
	errMsg      atomic.Pointer[string]
	startedAt   atomic.Int64
	completedAt atomic.Int64
	attempts    atomic.Uint32
}

func NewStepExecutionRecord(stepID string) *StepExecutionRecord {
	rec := &StepExecutionRecord{StepID: stepID}
	rec.status.Store(uint32(StepStatusPending))
	return rec
}

func (r *StepExecutionRecord) GetStatus() StepStatus {
	return StepStatus(r.status.Load())
}

func (r *StepExecutionRecord) SetStatus(st StepStatus) {
	r.status.Store(uint32(st))
}

func (r *StepExecutionRecord) GetOutput() []byte {
	ptr := r.output.Load()
	if ptr == nil {
		return nil
	}
	cp := make([]byte, len(*ptr))
	copy(cp, *ptr)
	return cp
}

func (r *StepExecutionRecord) SetOutput(b []byte) {
	cp := make([]byte, len(b))
	copy(cp, b)
	r.output.Store(&cp)
}

func (r *StepExecutionRecord) GetError() string {
	ptr := r.errMsg.Load()
	if ptr == nil {
		return ""
	}
	return *ptr
}

func (r *StepExecutionRecord) SetError(msg string) {
	cp := msg
	r.errMsg.Store(&cp)
}

func (r *StepExecutionRecord) GetAttempts() uint32 {
	return r.attempts.Load()
}

func (r *StepExecutionRecord) IncAttempts() uint32 {
	return r.attempts.Add(1)
}

func (r *StepExecutionRecord) MarkStarted() {
	r.startedAt.Store(time.Now().UnixNano())
	r.status.Store(uint32(StepStatusExecuting))
}

func (r *StepExecutionRecord) MarkCompleted(output []byte) {
	r.completedAt.Store(time.Now().UnixNano())
	cp := make([]byte, len(output))
	copy(cp, output)
	r.output.Store(&cp)
	r.status.Store(uint32(StepStatusCompleted))
}

func (r *StepExecutionRecord) MarkFailed(err string) {
	r.completedAt.Store(time.Now().UnixNano())
	cp := err
	r.errMsg.Store(&cp)
	r.status.Store(uint32(StepStatusFailed))
}

func (r *StepExecutionRecord) Duration() time.Duration {
	start := r.startedAt.Load()
	end := r.completedAt.Load()
	if end > start {
		return time.Duration(end - start)
	}
	return 0
}

type LogNode struct {
	entry AuditLogEntry
	next  atomic.Pointer[LogNode]
}

type LockFreeAuditTrail struct {
	head  atomic.Pointer[LogNode]
	count atomic.Uint64
}

func NewLockFreeAuditTrail() *LockFreeAuditTrail {
	return &LockFreeAuditTrail{}
}

func (t *LockFreeAuditTrail) Append(entry AuditLogEntry) uint64 {
	seq := t.count.Add(1)
	entry.Sequence = seq
	node := &LogNode{entry: entry}
	for {
		oldHead := t.head.Load()
		node.next.Store(oldHead)
		if t.head.CompareAndSwap(oldHead, node) {
			return seq
		}
	}
}

func (t *LockFreeAuditTrail) Snapshot() []AuditLogEntry {
	var rev []AuditLogEntry
	curr := t.head.Load()
	for curr != nil {
		rev = append(rev, curr.entry)
		curr = curr.next.Load()
	}
	res := make([]AuditLogEntry, len(rev))
	for i := 0; i < len(rev); i++ {
		res[i] = rev[len(rev)-1-i]
	}
	return res
}

func (t *LockFreeAuditTrail) FilterByStep(stepID string) []AuditLogEntry {
	all := t.Snapshot()
	var matched []AuditLogEntry
	for _, e := range all {
		if e.StepID == stepID {
			matched = append(matched, e)
		}
	}
	return matched
}

func (t *LockFreeAuditTrail) FilterBySaga(sagaID string) []AuditLogEntry {
	all := t.Snapshot()
	var matched []AuditLogEntry
	for _, e := range all {
		if e.SagaID == sagaID {
			matched = append(matched, e)
		}
	}
	return matched
}

func (t *LockFreeAuditTrail) Count() uint64 {
	return t.count.Load()
}

func (t *LockFreeAuditTrail) Latest() (AuditLogEntry, bool) {
	h := t.head.Load()
	if h == nil {
		return AuditLogEntry{}, false
	}
	return h.entry, true
}

type LockFreeDAGNode struct {
	step              StepDefinition
	dependencies      []*LockFreeDAGNode
	dependents        []*LockFreeDAGNode
	totalDependencies uint32
	satisfiedDeps     atomic.Uint32
}

func NewLockFreeDAGNode(step StepDefinition) *LockFreeDAGNode {
	return &LockFreeDAGNode{
		step:              step,
		totalDependencies: uint32(len(step.DependsOn)),
	}
}

func (n *LockFreeDAGNode) IsReady() bool {
	return n.satisfiedDeps.Load() >= n.totalDependencies
}

func (n *LockFreeDAGNode) SatisfyOne() bool {
	newVal := n.satisfiedDeps.Add(1)
	return newVal == n.totalDependencies
}

func (n *LockFreeDAGNode) Reset() {
	n.satisfiedDeps.Store(0)
}

type LockFreeDAGResolver struct {
	nodes    map[string]*LockFreeDAGNode
	nodeList []*LockFreeDAGNode
}

func NewLockFreeDAGResolver(steps []StepDefinition) (*LockFreeDAGResolver, error) {
	nodes := make(map[string]*LockFreeDAGNode, len(steps))
	nodeList := make([]*LockFreeDAGNode, 0, len(steps))
	for _, s := range steps {
		if err := s.Validate(); err != nil {
			return nil, err
		}
		if _, exists := nodes[s.StepID]; exists {
			return nil, fmt.Errorf("duplicate step id: %s", s.StepID)
		}
		node := NewLockFreeDAGNode(s)
		nodes[s.StepID] = node
		nodeList = append(nodeList, node)
	}
	for _, node := range nodeList {
		for _, depID := range node.step.DependsOn {
			depNode, ok := nodes[depID]
			if !ok {
				return nil, fmt.Errorf("%w: step %s depends on %s", ErrMissingDependency, node.step.StepID, depID)
			}
			node.dependencies = append(node.dependencies, depNode)
			depNode.dependents = append(depNode.dependents, node)
		}
	}
	r := &LockFreeDAGResolver{nodes: nodes, nodeList: nodeList}
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *LockFreeDAGResolver) Validate() error {
	visited := make(map[string]int, len(r.nodes))
	var dfs func(id string) error
	dfs = func(id string) error {
		state := visited[id]
		if state == 1 {
			return fmt.Errorf("%w at step %s", ErrCyclicDependency, id)
		}
		if state == 2 {
			return nil
		}
		visited[id] = 1
		node := r.nodes[id]
		for _, dep := range node.dependents {
			if err := dfs(dep.step.StepID); err != nil {
				return err
			}
		}
		visited[id] = 2
		return nil
	}
	for id := range r.nodes {
		if visited[id] == 0 {
			if err := dfs(id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *LockFreeDAGResolver) GetRootSteps() []*LockFreeDAGNode {
	var roots []*LockFreeDAGNode
	for _, n := range r.nodeList {
		if n.totalDependencies == 0 {
			roots = append(roots, n)
		}
	}
	return roots
}

func (r *LockFreeDAGResolver) GetStep(stepID string) (*LockFreeDAGNode, bool) {
	node, ok := r.nodes[stepID]
	return node, ok
}

func (r *LockFreeDAGResolver) MarkStepCompleted(stepID string) ([]*LockFreeDAGNode, error) {
	node, ok := r.nodes[stepID]
	if !ok {
		return nil, ErrStepNotFound
	}
	var newlyReady []*LockFreeDAGNode
	for _, dependent := range node.dependents {
		if dependent.SatisfyOne() {
			newlyReady = append(newlyReady, dependent)
		}
	}
	return newlyReady, nil
}

func (r *LockFreeDAGResolver) Reset() {
	for _, n := range r.nodeList {
		n.Reset()
	}
}

func (r *LockFreeDAGResolver) TotalSteps() int {
	return len(r.nodeList)
}

func (r *LockFreeDAGResolver) TopologicalSort() ([]string, error) {
	inDegree := make(map[string]int, len(r.nodes))
	for id, node := range r.nodes {
		inDegree[id] = int(node.totalDependencies)
	}
	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}
	var sorted []string
	for len(queue) > 0 {
		currID := queue[0]
		queue = queue[1:]
		sorted = append(sorted, currID)
		currNode := r.nodes[currID]
		for _, dep := range currNode.dependents {
			inDegree[dep.step.StepID]--
			if inDegree[dep.step.StepID] == 0 {
				queue = append(queue, dep.step.StepID)
			}
		}
	}
	if len(sorted) != len(r.nodes) {
		return nil, ErrCyclicDependency
	}
	return sorted, nil
}

type LockFreeSagaInstance struct {
	meta       SagaMetadata
	state      atomic.Pointer[SagaStateSnapshot]
	stepMap    map[string]*StepExecutionRecord
	stepList   []*StepExecutionRecord
	dag        *LockFreeDAGResolver
	audit      *LockFreeAuditTrail
	payloadBag atomic.Pointer[map[string]string]
}

func NewLockFreeSagaInstance(meta SagaMetadata, steps []StepDefinition) (*LockFreeSagaInstance, error) {
	dag, err := NewLockFreeDAGResolver(steps)
	if err != nil {
		return nil, err
	}
	stepMap := make(map[string]*StepExecutionRecord, len(steps))
	stepList := make([]*StepExecutionRecord, 0, len(steps))
	for _, s := range steps {
		rec := NewStepExecutionRecord(s.StepID)
		stepMap[s.StepID] = rec
		stepList = append(stepList, rec)
	}
	initialSnap := &SagaStateSnapshot{
		Version:        1,
		Status:         StatusPending,
		ActiveSteps:    0,
		CompletedSteps: 0,
		FailedSteps:     0,
		TerminalReason:  "",
		LastUpdated:    time.Now().UnixNano(),
	}
	bag := make(map[string]string)
	inst := &LockFreeSagaInstance{
		meta:     meta,
		stepMap:  stepMap,
		stepList: stepList,
		dag:      dag,
		audit:    NewLockFreeAuditTrail(),
	}
	inst.state.Store(initialSnap)
	inst.payloadBag.Store(&bag)
	return inst, nil
}

func (s *LockFreeSagaInstance) GetMetadata() SagaMetadata {
	return s.meta
}

func (s *LockFreeSagaInstance) GetSnapshot() *SagaStateSnapshot {
	return s.state.Load()
}

func (s *LockFreeSagaInstance) TransitionTo(to SagaStatus, reason string) bool {
	for {
		cur := s.state.Load()
		if cur == nil || cur.Status.IsTerminal() || !cur.Status.CanTransitionTo(to) {
			return false
		}
		next := cur.Clone()
		next.Version = cur.Version + 1
		next.Status = to
		next.TerminalReason = reason
		next.LastUpdated = time.Now().UnixNano()
		if s.state.CompareAndSwap(cur, next) {
			s.audit.Append(AuditLogEntry{
				Timestamp:  next.LastUpdated,
				SagaID:     s.meta.SagaID,
				StepID:     "",
				FromStatus: cur.Status,
				ToStatus:   to,
				Action:     "TRANSITION",
				Message:    reason,
			})
			return true
		}
	}
}

func (s *LockFreeSagaInstance) Finalize(targetStatus SagaStatus, reason string) bool {
	for {
		cur := s.state.Load()
		if cur == nil || cur.Status.IsTerminal() {
			return false
		}
		next := cur.Clone()
		next.Version = cur.Version + 1
		next.Status = targetStatus
		next.TerminalReason = reason
		next.LastUpdated = time.Now().UnixNano()
		if s.state.CompareAndSwap(cur, next) {
			s.audit.Append(AuditLogEntry{
				Timestamp:  next.LastUpdated,
				SagaID:     s.meta.SagaID,
				StepID:     "",
				FromStatus: cur.Status,
				ToStatus:   targetStatus,
				Action:     "FINALIZE",
				Message:    reason,
			})
			return true
		}
	}
}

func (s *LockFreeSagaInstance) IsTerminal() bool {
	snap := s.state.Load()
	if snap == nil {
		return false
	}
	return snap.Status.IsTerminal()
}

func (s *LockFreeSagaInstance) GetStepRecord(stepID string) (*StepExecutionRecord, bool) {
	rec, ok := s.stepMap[stepID]
	return rec, ok
}

func (s *LockFreeSagaInstance) RecordStepSuccess(stepID string, output []byte) error {
	rec, ok := s.stepMap[stepID]
	if !ok {
		return ErrStepNotFound
	}
	rec.MarkCompleted(output)
	s.audit.Append(AuditLogEntry{
		Timestamp:  time.Now().UnixNano(),
		SagaID:     s.meta.SagaID,
		StepID:     stepID,
		FromStatus: StatusExecuting,
		ToStatus:   StatusExecuting,
		Action:     "STEP_COMPLETED",
		Message:    fmt.Sprintf("step %s succeeded", stepID),
	})
	return nil
}

func (s *LockFreeSagaInstance) RecordStepFailure(stepID string, err string) error {
	rec, ok := s.stepMap[stepID]
	if !ok {
		return ErrStepNotFound
	}
	rec.MarkFailed(err)
	s.audit.Append(AuditLogEntry{
		Timestamp:  time.Now().UnixNano(),
		SagaID:     s.meta.SagaID,
		StepID:     stepID,
		FromStatus: StatusExecuting,
		ToStatus:   StatusCompensating,
		Action:     "STEP_FAILED",
		Message:    fmt.Sprintf("step %s failed: %s", stepID, err),
	})
	return nil
}

func (s *LockFreeSagaInstance) GetPayload(key string) (string, bool) {
	bagPtr := s.payloadBag.Load()
	if bagPtr == nil {
		return "", false
	}
	v, ok := (*bagPtr)[key]
	return v, ok
}

func (s *LockFreeSagaInstance) SetPayload(key, val string) {
	for {
		oldPtr := s.payloadBag.Load()
		newMap := make(map[string]string)
		if oldPtr != nil {
			for k, v := range *oldPtr {
				newMap[k] = v
			}
		}
		newMap[key] = val
		if s.payloadBag.CompareAndSwap(oldPtr, &newMap) {
			return
		}
	}
}

func (s *LockFreeSagaInstance) Summary() string {
	snap := s.state.Load()
	return fmt.Sprintf("Saga[%s] Type=%s Tenant=%s Status=%s Version=%d",
		s.meta.SagaID, s.meta.WorkflowType, s.meta.TenantID, snap.Status, snap.Version)
}

type RegistryBucketNode struct {
	saga *LockFreeSagaInstance
	next atomic.Pointer[RegistryBucketNode]
}

type LockFreeSagaRegistry struct {
	buckets [128]atomic.Pointer[RegistryBucketNode]
	count   atomic.Uint64
}

func NewLockFreeSagaRegistry() *LockFreeSagaRegistry {
	return &LockFreeSagaRegistry{}
}

func hashFNV(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func (r *LockFreeSagaRegistry) Put(saga *LockFreeSagaInstance) error {
	if saga == nil {
		return errors.New("cannot register nil saga")
	}
	id := saga.meta.SagaID
	bucketIdx := hashFNV(id) % 128
	newNode := &RegistryBucketNode{saga: saga}
	for {
		oldHead := r.buckets[bucketIdx].Load()
		curr := oldHead
		for curr != nil {
			if curr.saga.meta.SagaID == id {
				return ErrSagaAlreadyExists
			}
			curr = curr.next.Load()
		}
		newNode.next.Store(oldHead)
		if r.buckets[bucketIdx].CompareAndSwap(oldHead, newNode) {
			r.count.Add(1)
			return nil
		}
	}
}

func (r *LockFreeSagaRegistry) Get(sagaID string) (*LockFreeSagaInstance, bool) {
	bucketIdx := hashFNV(sagaID) % 128
	curr := r.buckets[bucketIdx].Load()
	for curr != nil {
		if curr.saga.meta.SagaID == sagaID {
			return curr.saga, true
		}
		curr = curr.next.Load()
	}
	return nil, false
}

func (r *LockFreeSagaRegistry) Count() int {
	return int(r.count.Load())
}

func (r *LockFreeSagaRegistry) ListAll() []*LockFreeSagaInstance {
	var list []*LockFreeSagaInstance
	for i := 0; i < 128; i++ {
		curr := r.buckets[i].Load()
		for curr != nil {
			list = append(list, curr.saga)
			curr = curr.next.Load()
		}
	}
	return list
}

func (r *LockFreeSagaRegistry) ListByTenant(tenantID string) []*LockFreeSagaInstance {
	all := r.ListAll()
	var res []*LockFreeSagaInstance
	for _, s := range all {
		if s.meta.TenantID == tenantID {
			res = append(res, s)
		}
	}
	return res
}

func (r *LockFreeSagaRegistry) ListActive() []*LockFreeSagaInstance {
	all := r.ListAll()
	var res []*LockFreeSagaInstance
	for _, s := range all {
		if !s.IsTerminal() {
			res = append(res, s)
		}
	}
	return res
}

type MetricsReport struct {
	TotalSagas         uint64
	TotalCommitted     uint64
	TotalCompensated   uint64
	TotalFailed        uint64
	TotalStepRuns      uint64
	TotalStepRetries   uint64
	TotalStepFailures  uint64
	TotalDurationNanos uint64
	AverageDurationMs  float64
	CommitRatePercent  float64
}

type SagaMetricsCollector struct {
	totalSagas         atomic.Uint64
	totalCommitted     atomic.Uint64
	totalCompensated   atomic.Uint64
	totalFailed        atomic.Uint64
	totalStepRuns      atomic.Uint64
	totalStepRetries   atomic.Uint64
	totalStepFailures  atomic.Uint64
	totalDurationNanos atomic.Uint64
}

func NewSagaMetricsCollector() *SagaMetricsCollector {
	return &SagaMetricsCollector{}
}

func (m *SagaMetricsCollector) RecordSagaStarted() {
	m.totalSagas.Add(1)
}

func (m *SagaMetricsCollector) RecordSagaCompleted(st SagaStatus, duration time.Duration) {
	m.totalDurationNanos.Add(uint64(duration.Nanoseconds()))
	switch st {
	case StatusCommitted:
		m.totalCommitted.Add(1)
	case StatusCompensated:
		m.totalCompensated.Add(1)
	case StatusFailed:
		m.totalFailed.Add(1)
	}
}

func (m *SagaMetricsCollector) RecordStepExecution(success bool, retried bool) {
	m.totalStepRuns.Add(1)
	if retried {
		m.totalStepRetries.Add(1)
	}
	if !success {
		m.totalStepFailures.Add(1)
	}
}

func (m *SagaMetricsCollector) Snapshot() MetricsReport {
	sagas := m.totalSagas.Load()
	commits := m.totalCommitted.Load()
	compensations := m.totalCompensated.Load()
	failures := m.totalFailed.Load()
	stepRuns := m.totalStepRuns.Load()
	stepRetries := m.totalStepRetries.Load()
	stepFails := m.totalStepFailures.Load()
	dur := m.totalDurationNanos.Load()

	avgMs := 0.0
	commitRate := 0.0
	totalFinished := commits + compensations + failures
	if totalFinished > 0 {
		avgMs = float64(dur) / float64(totalFinished) / 1e6
		commitRate = (float64(commits) / float64(totalFinished)) * 100.0
	}

	return MetricsReport{
		TotalSagas:         sagas,
		TotalCommitted:     commits,
		TotalCompensated:   compensations,
		TotalFailed:        failures,
		TotalStepRuns:      stepRuns,
		TotalStepRetries:   stepRetries,
		TotalStepFailures:  stepFails,
		TotalDurationNanos: dur,
		AverageDurationMs:  avgMs,
		CommitRatePercent:  commitRate,
	}
}

type StepActionFunc func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult
type StepCompensateFunc func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) error

type SagaEngine struct {
	registry      *LockFreeSagaRegistry
	metrics       *SagaMetricsCollector
	actions       map[ActionType]StepActionFunc
	compensations map[ActionType]StepCompensateFunc
}

func NewSagaEngine() *SagaEngine {
	e := &SagaEngine{
		registry:      NewLockFreeSagaRegistry(),
		metrics:       NewSagaMetricsCollector(),
		actions:       make(map[ActionType]StepActionFunc),
		compensations: make(map[ActionType]StepCompensateFunc),
	}
	e.installDefaultHandlers()
	return e
}

func (e *SagaEngine) RegisterAction(act ActionType, fn StepActionFunc) {
	e.actions[act] = fn
}

func (e *SagaEngine) RegisterCompensation(act ActionType, fn StepCompensateFunc) {
	e.compensations[act] = fn
}

func (e *SagaEngine) SubmitSaga(saga *LockFreeSagaInstance) error {
	e.metrics.RecordSagaStarted()
	return e.registry.Put(saga)
}

func (e *SagaEngine) ExecuteStep(ctx context.Context, saga *LockFreeSagaInstance, node *LockFreeDAGNode) StepResult {
	fn, ok := e.actions[node.step.ActionType]
	if !ok {
		return StepResult{
			StepID:       node.step.StepID,
			Status:       StepStatusFailed,
			ErrorMessage: fmt.Sprintf("no handler registered for action %s", node.step.ActionType),
		}
	}
	start := time.Now()
	var res StepResult
	var attempts uint32
	rec, _ := saga.GetStepRecord(node.step.StepID)
	if rec != nil {
		rec.MarkStarted()
	}
	for {
		attempts++
		if rec != nil {
			rec.IncAttempts()
		}
		res = fn(ctx, saga, node.step)
		res.StepID = node.step.StepID
		res.AttemptCount = attempts
		if res.IsSuccess() || attempts > node.step.MaxRetries {
			break
		}
		e.metrics.RecordStepExecution(false, true)
		runtime.Gosched()
	}
	res.ExecutionDurationNanos = time.Since(start).Nanoseconds()
	e.metrics.RecordStepExecution(res.IsSuccess(), attempts > 1)
	if res.IsSuccess() {
		saga.RecordStepSuccess(node.step.StepID, res.Payload)
	} else {
		saga.RecordStepFailure(node.step.StepID, res.ErrorMessage)
	}
	return res
}

func (e *SagaEngine) ExecuteCompensationWave(ctx context.Context, saga *LockFreeSagaInstance) error {
	order, err := saga.dag.TopologicalSort()
	if err != nil {
		return err
	}
	for i := len(order) - 1; i >= 0; i-- {
		stepID := order[i]
		node, ok := saga.dag.GetStep(stepID)
		if !ok || !node.step.Compensable {
			continue
		}
		rec, ok := saga.GetStepRecord(stepID)
		if !ok || rec.GetStatus() != StepStatusCompleted {
			continue
		}
		compFn, ok := e.compensations[node.step.CompensateType]
		if ok {
			rec.SetStatus(StepStatusCompensating)
			err := compFn(ctx, saga, node.step)
			if err != nil {
				rec.SetStatus(StepStatusFailed)
			} else {
				rec.SetStatus(StepStatusCompensated)
			}
		}
	}
	return nil
}

func (e *SagaEngine) TriggerCommit(saga *LockFreeSagaInstance, reason string) bool {
	if !saga.Finalize(StatusCommitted, reason) {
		return false
	}
	e.metrics.RecordSagaCompleted(StatusCommitted, 0)
	return true
}

func (e *SagaEngine) TriggerCompensate(saga *LockFreeSagaInstance, reason string) bool {
	if !saga.Finalize(StatusCompensated, reason) {
		return false
	}
	e.metrics.RecordSagaCompleted(StatusCompensated, 0)
	return true
}

func (e *SagaEngine) TriggerFail(saga *LockFreeSagaInstance, reason string) bool {
	if !saga.Finalize(StatusFailed, reason) {
		return false
	}
	e.metrics.RecordSagaCompleted(StatusFailed, 0)
	return true
}

func (e *SagaEngine) ExecuteWorkflowSync(ctx context.Context, saga *LockFreeSagaInstance) (SagaStatus, error) {
	if !saga.TransitionTo(StatusExecuting, "workflow execution started") {
		return saga.GetSnapshot().Status, ErrInvalidTransition
	}
	ready := saga.dag.GetRootSteps()
	for len(ready) > 0 {
		var nextRound []*LockFreeDAGNode
		for _, node := range ready {
			res := e.ExecuteStep(ctx, saga, node)
			if !res.IsSuccess() {
				if node.step.IsCritical {
					saga.TransitionTo(StatusCompensating, fmt.Sprintf("critical step %s failed", node.step.StepID))
					e.ExecuteCompensationWave(ctx, saga)
					e.TriggerCompensate(saga, "rolled back due to critical step failure")
					return StatusCompensated, nil
				}
			} else {
				unblocked, err := saga.dag.MarkStepCompleted(node.step.StepID)
				if err == nil {
					nextRound = append(nextRound, unblocked...)
				}
			}
		}
		ready = nextRound
	}
	e.TriggerCommit(saga, "all steps completed successfully")
	return StatusCommitted, nil
}

func (e *SagaEngine) installDefaultHandlers() {
	e.RegisterAction(ActionAuthorizePayment, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		saga.SetPayload("auth_id", "AUTH-99281")
		return StepResult{Status: StepStatusCompleted, Payload: []byte("AUTH_OK")}
	})
	e.RegisterAction(ActionCapturePayment, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		authID, ok := saga.GetPayload("auth_id")
		if !ok {
			return StepResult{Status: StepStatusFailed, ErrorMessage: "missing auth_id"}
		}
		saga.SetPayload("capture_id", "CAP-"+authID)
		return StepResult{Status: StepStatusCompleted, Payload: []byte("CAPTURE_OK")}
	})
	e.RegisterAction(ActionEvaluateFraud, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		saga.SetPayload("fraud_score", "0.12")
		return StepResult{Status: StepStatusCompleted, Payload: []byte("FRAUD_CHECK_PASS")}
	})
	e.RegisterAction(ActionReserveInventory, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		saga.SetPayload("res_id", "RES-55102")
		return StepResult{Status: StepStatusCompleted, Payload: []byte("INVENTORY_HELD")}
	})
	e.RegisterAction(ActionPostLedgerEntry, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		saga.SetPayload("ledger_id", "TX-883719")
		return StepResult{Status: StepStatusCompleted, Payload: []byte("LEDGER_POSTED")}
	})
	e.RegisterAction(ActionIssueTaxInvoice, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		saga.SetPayload("tax_inv", "INV-2026-001")
		return StepResult{Status: StepStatusCompleted, Payload: []byte("INVOICE_ISSUED")}
	})
	e.RegisterAction(ActionDispatchNotification, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		return StepResult{Status: StepStatusCompleted, Payload: []byte("NOTIFIED")}
	})
	e.RegisterAction(ActionLockExchangeRate, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		saga.SetPayload("fx_rate", "1.0850")
		return StepResult{Status: StepStatusCompleted, Payload: []byte("FX_LOCKED")}
	})
	e.RegisterAction(ActionCreditBeneficiary, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		return StepResult{Status: StepStatusCompleted, Payload: []byte("CREDITED")}
	})
	e.RegisterAction(ActionDebitBeneficiary, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) StepResult {
		return StepResult{Status: StepStatusCompleted, Payload: []byte("DEBITED")}
	})

	e.RegisterCompensation(ActionReleaseInventory, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) error {
		saga.SetPayload("res_id_cancelled", "true")
		return nil
	})
	e.RegisterCompensation(ActionReverseLedgerEntry, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) error {
		saga.SetPayload("ledger_reversed", "true")
		return nil
	})
	e.RegisterCompensation(ActionCancelTaxInvoice, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) error {
		saga.SetPayload("tax_cancelled", "true")
		return nil
	})
	e.RegisterCompensation(ActionReleaseExchangeRate, func(ctx context.Context, saga *LockFreeSagaInstance, step StepDefinition) error {
		saga.SetPayload("fx_released", "true")
		return nil
	})
}

func BuildPaymentTransferWorkflow(sagaID, tenantID, fromAcc, toAcc string, amount Money) (*LockFreeSagaInstance, error) {
	meta := SagaMetadata{
		SagaID:        sagaID,
		TenantID:      tenantID,
		CorrelationID: "CORR-" + sagaID,
		WorkflowType:  "PAYMENT_TRANSFER",
		CreatedAt:     time.Now().UnixNano(),
		MaxRetries:    3,
	}
	steps := []StepDefinition{
		{
			StepID:         "step-fraud-check",
			Name:           "Realtime Fraud Assessment",
			ActionType:     ActionEvaluateFraud,
			Compensable:    false,
			IsCritical:     true,
			MaxRetries:     1,
		},
		{
			StepID:         "step-auth-payment",
			Name:           "Authorize Account Payment",
			ActionType:     ActionAuthorizePayment,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "step-hold-inventory",
			Name:           "Reserve Physical Inventory",
			ActionType:     ActionReserveInventory,
			Compensable:    true,
			CompensateType: ActionReleaseInventory,
			DependsOn:      []string{"step-fraud-check", "step-auth-payment"},
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "step-capture-payment",
			Name:           "Capture Authorized Funds",
			ActionType:     ActionCapturePayment,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"step-hold-inventory"},
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "step-post-ledger",
			Name:           "Post Double-Entry Ledger",
			ActionType:     ActionPostLedgerEntry,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"step-capture-payment"},
			IsCritical:     true,
			MaxRetries:     3,
		},
		{
			StepID:         "step-issue-tax",
			Name:           "Generate Fiscal Tax Invoice",
			ActionType:     ActionIssueTaxInvoice,
			Compensable:    true,
			CompensateType: ActionCancelTaxInvoice,
			DependsOn:      []string{"step-post-ledger"},
			IsCritical:     false,
			MaxRetries:     2,
		},
		{
			StepID:         "step-notify-parties",
			Name:           "Dispatch Notification Receipts",
			ActionType:     ActionDispatchNotification,
			Compensable:    false,
			DependsOn:      []string{"step-issue-tax"},
			IsCritical:     false,
			MaxRetries:     1,
		},
	}
	saga, err := NewLockFreeSagaInstance(meta, steps)
	if err != nil {
		return nil, err
	}
	saga.SetPayload("from_acc", fromAcc)
	saga.SetPayload("to_acc", toAcc)
	saga.SetPayload("amount", amount.String())
	return saga, nil
}

func BuildCrossBorderSettlementWorkflow(sagaID, tenantID string, srcCur, dstCur Currency, amount Money) (*LockFreeSagaInstance, error) {
	meta := SagaMetadata{
		SagaID:        sagaID,
		TenantID:      tenantID,
		CorrelationID: "FX-" + sagaID,
		WorkflowType:  "CROSS_BORDER_FX",
		CreatedAt:     time.Now().UnixNano(),
		MaxRetries:    3,
	}
	steps := []StepDefinition{
		{
			StepID:         "step-fx-lock",
			Name:           "Lock FX Treasury Rate",
			ActionType:     ActionLockExchangeRate,
			Compensable:    true,
			CompensateType: ActionReleaseExchangeRate,
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "step-debit-nostro",
			Name:           "Debit Nostro Liquidity Account",
			ActionType:     ActionDebitBeneficiary,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"step-fx-lock"},
			IsCritical:     true,
			MaxRetries:     3,
		},
		{
			StepID:         "step-credit-vostro",
			Name:           "Credit Vostro Settlement Account",
			ActionType:     ActionCreditBeneficiary,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"step-debit-nostro"},
			IsCritical:     true,
			MaxRetries:     3,
		},
		{
			StepID:         "step-fx-notify",
			Name:           "Send SWIFT Settlement Advice",
			ActionType:     ActionDispatchNotification,
			Compensable:    false,
			DependsOn:      []string{"step-credit-vostro"},
			IsCritical:     false,
			MaxRetries:     1,
		},
	}
	inst, err := NewLockFreeSagaInstance(meta, steps)
	if err != nil {
		return nil, err
	}
	inst.SetPayload("src_cur", string(srcCur))
	inst.SetPayload("dst_cur", string(dstCur))
	inst.SetPayload("amount", amount.String())
	return inst, nil
}

type SagaDetailedReport struct {
	SagaID          string
	TenantID        string
	WorkflowType    string
	FinalStatus     string
	Version         uint64
	TotalSteps      int
	CompletedSteps  int
	FailedSteps     int
	AuditEntries    int
	ExecutionTimeMs int64
}

func GenerateSagaReport(saga *LockFreeSagaInstance) SagaDetailedReport {
	snap := saga.GetSnapshot()
	meta := saga.GetMetadata()
	auditLogs := saga.audit.Snapshot()
	completed := 0
	failed := 0
	for _, rec := range saga.stepList {
		if rec.GetStatus() == StepStatusCompleted {
			completed++
		} else if rec.GetStatus() == StepStatusFailed {
			failed++
		}
	}
	return SagaDetailedReport{
		SagaID:          meta.SagaID,
		TenantID:        meta.TenantID,
		WorkflowType:    meta.WorkflowType,
		FinalStatus:     snap.Status.String(),
		Version:         snap.Version,
		TotalSteps:      len(saga.stepList),
		CompletedSteps:  completed,
		FailedSteps:     failed,
		AuditEntries:    len(auditLogs),
		ExecutionTimeMs: (snap.LastUpdated - meta.CreatedAt) / 1e6,
	}
}

func (r SagaDetailedReport) String() string {
	return fmt.Sprintf("Report: ID=%s Type=%s Status=%s Steps=%d/%d Logs=%d Time=%dms",
		r.SagaID, r.WorkflowType, r.FinalStatus, r.CompletedSteps, r.TotalSteps, r.AuditEntries, r.ExecutionTimeMs)
}

func BuildECommerceOrderWorkflow(sagaID, tenantID string, total Money) (*LockFreeSagaInstance, error) {
	meta := SagaMetadata{
		SagaID:        sagaID,
		TenantID:      tenantID,
		CorrelationID: "ORD-" + sagaID,
		WorkflowType:  "ORDER_FULFILLMENT",
		CreatedAt:     time.Now().UnixNano(),
		MaxRetries:    2,
	}
	steps := []StepDefinition{
		{
			StepID:         "ord-reserve-stock",
			Name:           "Hold Warehouse SKU Stock",
			ActionType:     ActionReserveInventory,
			Compensable:    true,
			CompensateType: ActionReleaseInventory,
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "ord-auth-card",
			Name:           "Authorize Credit Card",
			ActionType:     ActionAuthorizePayment,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"ord-reserve-stock"},
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "ord-capture-funds",
			Name:           "Capture Card Settlement",
			ActionType:     ActionCapturePayment,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"ord-auth-card"},
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "ord-tax-invoice",
			Name:           "Generate Order Invoice",
			ActionType:     ActionIssueTaxInvoice,
			Compensable:    true,
			CompensateType: ActionCancelTaxInvoice,
			DependsOn:      []string{"ord-capture-funds"},
			IsCritical:     false,
			MaxRetries:     2,
		},
		{
			StepID:         "ord-notify-buyer",
			Name:           "Send Shipping Confirmation Email",
			ActionType:     ActionDispatchNotification,
			Compensable:    false,
			DependsOn:      []string{"ord-tax-invoice"},
			IsCritical:     false,
			MaxRetries:     1,
		},
	}
	inst, err := NewLockFreeSagaInstance(meta, steps)
	if err != nil {
		return nil, err
	}
	inst.SetPayload("total_amount", total.String())
	return inst, nil
}

func BuildSubscriptionBillingWorkflow(sagaID, tenantID string, monthlyFee Money) (*LockFreeSagaInstance, error) {
	meta := SagaMetadata{
		SagaID:        sagaID,
		TenantID:      tenantID,
		CorrelationID: "SUB-" + sagaID,
		WorkflowType:  "RECURRING_BILLING",
		CreatedAt:     time.Now().UnixNano(),
		MaxRetries:    3,
	}
	steps := []StepDefinition{
		{
			StepID:         "sub-auth-charge",
			Name:           "Authorize Recurring Charge",
			ActionType:     ActionAuthorizePayment,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "sub-capture-charge",
			Name:           "Capture Subscription Payment",
			ActionType:     ActionCapturePayment,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"sub-auth-charge"},
			IsCritical:     true,
			MaxRetries:     2,
		},
		{
			StepID:         "sub-ledger-post",
			Name:           "Post Revenue Recognition Ledger",
			ActionType:     ActionPostLedgerEntry,
			Compensable:    true,
			CompensateType: ActionReverseLedgerEntry,
			DependsOn:      []string{"sub-capture-charge"},
			IsCritical:     true,
			MaxRetries:     3,
		},
		{
			StepID:         "sub-send-invoice",
			Name:           "Email Invoice Receipt",
			ActionType:     ActionDispatchNotification,
			Compensable:    false,
			DependsOn:      []string{"sub-ledger-post"},
			IsCritical:     false,
			MaxRetries:     1,
		},
	}
	inst, err := NewLockFreeSagaInstance(meta, steps)
	if err != nil {
		return nil, err
	}
	inst.SetPayload("monthly_fee", monthlyFee.String())
	return inst, nil
}
type LockFreeIdempotencyGate struct {
	slots          []atomic.Uint64
	timestamps     []atomic.Int64
	mask           uint64
	capacity       uint64
	ttlNs          int64
	totalAccepted  atomic.Uint64
	totalRejected  atomic.Uint64
}

func NewLockFreeIdempotencyGate(capacity uint64, ttl time.Duration) *LockFreeIdempotencyGate {
	capPow2 := uint64(1)
	for capPow2 < capacity {
		capPow2 <<= 1
	}
	return &LockFreeIdempotencyGate{
		slots:      make([]atomic.Uint64, capPow2),
		timestamps: make([]atomic.Int64, capPow2),
		mask:       capPow2 - 1,
		capacity:   capPow2,
		ttlNs:      ttl.Nanoseconds(),
	}
}

func (g *LockFreeIdempotencyGate) hashKey(key string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	if h == 0 {
		return 1
	}
	return h
}

func (g *LockFreeIdempotencyGate) CheckAndAcquire(key string, currentTs int64) bool {
	h := g.hashKey(key)
	idx := h & g.mask
	for {
		stored := g.slots[idx].Load()
		ts := g.timestamps[idx].Load()
		if stored == h {
			if currentTs-ts < g.ttlNs {
				g.totalRejected.Add(1)
				return false
			}
			if g.timestamps[idx].CompareAndSwap(ts, currentTs) {
				g.totalAccepted.Add(1)
				return true
			}
			continue
		}
		if stored == 0 || (ts > 0 && currentTs-ts >= g.ttlNs) {
			if g.slots[idx].CompareAndSwap(stored, h) {
				g.timestamps[idx].Store(currentTs)
				g.totalAccepted.Add(1)
				return true
			}
			continue
		}
		if g.slots[idx].CompareAndSwap(stored, h) {
			g.timestamps[idx].Store(currentTs)
			g.totalAccepted.Add(1)
			return true
		}
	}
}

func (g *LockFreeIdempotencyGate) Release(key string) {
	h := g.hashKey(key)
	idx := h & g.mask
	g.slots[idx].CompareAndSwap(h, 0)
	g.timestamps[idx].Store(0)
}

func (g *LockFreeIdempotencyGate) EvictExpired(currentTs int64) int {
	evicted := 0
	for i := uint64(0); i < g.capacity; i++ {
		ts := g.timestamps[i].Load()
		if ts > 0 && currentTs-ts >= g.ttlNs {
			stored := g.slots[i].Load()
			if g.slots[i].CompareAndSwap(stored, 0) {
				g.timestamps[i].Store(0)
				evicted++
			}
		}
	}
	return evicted
}

func (g *LockFreeIdempotencyGate) GetStats() (uint64, uint64) {
	return g.totalAccepted.Load(), g.totalRejected.Load()
}

func (g *LockFreeIdempotencyGate) Reset() {
	for i := uint64(0); i < g.capacity; i++ {
		g.slots[i].Store(0)
		g.timestamps[i].Store(0)
	}
	g.totalAccepted.Store(0)
	g.totalRejected.Store(0)
}

type CompensationBackoffScheduler struct {
	baseDelayNs    int64
	maxDelayNs     int64
	factor         float64
	maxRetries     int32
	totalScheduled atomic.Uint64
	totalExhausted atomic.Uint64
}

func NewCompensationBackoffScheduler(base, max time.Duration, factor float64, maxRetries int32) *CompensationBackoffScheduler {
	return &CompensationBackoffScheduler{
		baseDelayNs: base.Nanoseconds(),
		maxDelayNs:  max.Nanoseconds(),
		factor:      factor,
		maxRetries:  maxRetries,
	}
}

func (s *CompensationBackoffScheduler) CalculateDelay(retryCount int32) time.Duration {
	if retryCount <= 0 {
		return time.Duration(s.baseDelayNs)
	}
	if retryCount > 30 {
		return time.Duration(s.maxDelayNs)
	}
	mult := float64(int64(1) << retryCount)
	delay := float64(s.baseDelayNs) * mult
	if int64(delay) > s.maxDelayNs || delay < 0 {
		return time.Duration(s.maxDelayNs)
	}
	return time.Duration(int64(delay))
}

func (s *CompensationBackoffScheduler) RecordScheduled() {
	s.totalScheduled.Add(1)
}

func (s *CompensationBackoffScheduler) RecordExhausted() {
	s.totalExhausted.Add(1)
}

func (s *CompensationBackoffScheduler) ShouldRetry(retryCount int32) bool {
	return retryCount < s.maxRetries
}

func (s *CompensationBackoffScheduler) GetStats() (uint64, uint64) {
	return s.totalScheduled.Load(), s.totalExhausted.Load()
}

type LockFreeTelemetryAggregator struct {
	totalStarted     atomic.Uint64
	totalCommitted   atomic.Uint64
	totalCompensated atomic.Uint64
	totalFailed      atomic.Uint64
	durationSumNs    atomic.Uint64
	latencyBuckets   [16]atomic.Uint64
	peakConcurrent   atomic.Int64
	activeCount      atomic.Int64
}

func NewLockFreeTelemetryAggregator() *LockFreeTelemetryAggregator {
	return &LockFreeTelemetryAggregator{}
}

func (a *LockFreeTelemetryAggregator) RecordStart() {
	a.totalStarted.Add(1)
	act := a.activeCount.Add(1)
	for {
		peak := a.peakConcurrent.Load()
		if act <= peak {
			break
		}
		if a.peakConcurrent.CompareAndSwap(peak, act) {
			break
		}
	}
}

func (a *LockFreeTelemetryAggregator) RecordFinish(status SagaStatus, duration time.Duration) {
	a.activeCount.Add(-1)
	ns := uint64(duration.Nanoseconds())
	a.durationSumNs.Add(ns)
	switch status {
	case StatusCommitted:
		a.totalCommitted.Add(1)
	case StatusCompensated:
		a.totalCompensated.Add(1)
	case StatusFailed:
		a.totalFailed.Add(1)
	}
	bIdx := ns / 1000000
	if bIdx >= 16 {
		bIdx = 15
	}
	a.latencyBuckets[bIdx].Add(1)
}

func (a *LockFreeTelemetryAggregator) GetAverageDuration() time.Duration {
	completed := a.totalCommitted.Load() + a.totalCompensated.Load() + a.totalFailed.Load()
	if completed == 0 {
		return 0
	}
	avgNs := a.durationSumNs.Load() / completed
	return time.Duration(avgNs)
}

func (a *LockFreeTelemetryAggregator) GetPercentileDuration(pct float64) time.Duration {
	if pct <= 0 || pct > 1.0 {
		return 0
	}
	completed := a.totalCommitted.Load() + a.totalCompensated.Load() + a.totalFailed.Load()
	if completed == 0 {
		return 0
	}
	target := uint64(float64(completed) * pct)
	var accum uint64
	for i := 0; i < 16; i++ {
		accum += a.latencyBuckets[i].Load()
		if accum >= target {
			return time.Duration((i + 1) * 1000000)
		}
	}
	return time.Duration(16 * 1000000)
}

func (a *LockFreeTelemetryAggregator) GetSuccessRate() float64 {
	completed := a.totalCommitted.Load() + a.totalCompensated.Load() + a.totalFailed.Load()
	if completed == 0 {
		return 0.0
	}
	return float64(a.totalCommitted.Load()) / float64(completed)
}

func (a *LockFreeTelemetryAggregator) GetActiveCount() int64 {
	return a.activeCount.Load()
}

func (a *LockFreeTelemetryAggregator) GetPeakConcurrent() int64 {
	return a.peakConcurrent.Load()
}

func (a *LockFreeTelemetryAggregator) Reset() {
	a.totalStarted.Store(0)
	a.totalCommitted.Store(0)
	a.totalCompensated.Store(0)
	a.totalFailed.Store(0)
	a.durationSumNs.Store(0)
	a.activeCount.Store(0)
	a.peakConcurrent.Store(0)
	for i := 0; i < 16; i++ {
		a.latencyBuckets[i].Store(0)
	}
}

type LockFreeStepDependencyValidator struct {
	validationErrors atomic.Uint64
	validatedCount   atomic.Uint64
}

func NewLockFreeStepDependencyValidator() *LockFreeStepDependencyValidator {
	return &LockFreeStepDependencyValidator{}
}

func (v *LockFreeStepDependencyValidator) ValidateDAG(steps []StepDefinition) error {
	v.validatedCount.Add(1)
	if len(steps) == 0 {
		v.validationErrors.Add(1)
		return ErrMissingDependency
	}
	ids := make(map[string]bool)
	for _, s := range steps {
		if strings.TrimSpace(s.StepID) == "" {
			v.validationErrors.Add(1)
			return ErrStepNotFound
		}
		if ids[s.StepID] {
			v.validationErrors.Add(1)
			return ErrStepNotFound
		}
		ids[s.StepID] = true
	}
	for _, s := range steps {
		for _, dep := range s.DependsOn {
			if !ids[dep] {
				v.validationErrors.Add(1)
				return ErrMissingDependency
			}
		}
	}
	if v.DetectCycles(steps) {
		v.validationErrors.Add(1)
		return ErrCyclicDependency
	}
	return nil
}

func (v *LockFreeStepDependencyValidator) DetectCycles(steps []StepDefinition) bool {
	adj := make(map[string][]string)
	inDegree := make(map[string]int)
	for _, s := range steps {
		inDegree[s.StepID] = len(s.DependsOn)
		for _, dep := range s.DependsOn {
			adj[dep] = append(adj[dep], s.StepID)
		}
	}
	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}
	visited := 0
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		visited++
		for _, next := range adj[curr] {
			inDegree[next]--
			if inDegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	return visited != len(steps)
}

func (v *LockFreeStepDependencyValidator) GetStats() (uint64, uint64) {
	return v.validatedCount.Load(), v.validationErrors.Load()
}

type LockFreeSnapshotReplayer struct {
	replayedCount    atomic.Uint64
	divergenceCount  atomic.Uint64
}

func NewLockFreeSnapshotReplayer() *LockFreeSnapshotReplayer {
	return &LockFreeSnapshotReplayer{}
}

func (r *LockFreeSnapshotReplayer) Replay(snapshots []SagaStateSnapshot) (*SagaStateSnapshot, error) {
	if len(snapshots) == 0 {
		return nil, ErrNilSnapshot
	}
	r.replayedCount.Add(1)
	latest := &snapshots[0]
	for i := 1; i < len(snapshots); i++ {
		cur := &snapshots[i]
		if cur.Version > latest.Version {
			latest = cur
		} else if cur.Version < latest.Version {
			r.divergenceCount.Add(1)
		}
	}
	return latest, nil
}

func (r *LockFreeSnapshotReplayer) VerifyLinearizability(snaps []SagaStateSnapshot) bool {
	for i := 1; i < len(snaps); i++ {
		if snaps[i].Version <= snaps[i-1].Version {
			return false
		}
	}
	return true
}

func (r *LockFreeSnapshotReplayer) GetStats() (uint64, uint64) {
	return r.replayedCount.Load(), r.divergenceCount.Load()
}

type LockFreeLedgerPostingQueue struct {
	capacity      uint64
	mask          uint64
	entries       []Money
	isCredits     []bool
	head          atomic.Uint64
	tail          atomic.Uint64
	totalDebited  atomic.Int64
	totalCredited atomic.Int64
}

func NewLockFreeLedgerPostingQueue(capacity uint64) *LockFreeLedgerPostingQueue {
	capPow2 := uint64(1)
	for capPow2 < capacity {
		capPow2 <<= 1
	}
	return &LockFreeLedgerPostingQueue{
		capacity:  capPow2,
		mask:      capPow2 - 1,
		entries:   make([]Money, capPow2),
		isCredits: make([]bool, capPow2),
	}
}

func (q *LockFreeLedgerPostingQueue) PostEntry(amount Money, isCredit bool) bool {
	for {
		h := q.head.Load()
		t := q.tail.Load()
		if h-t >= q.capacity {
			return false
		}
		if q.head.CompareAndSwap(h, h+1) {
			idx := h & q.mask
			q.entries[idx] = amount
			q.isCredits[idx] = isCredit
			if isCredit {
				q.totalCredited.Add(amount.AmountCents)
			} else {
				q.totalDebited.Add(amount.AmountCents)
			}
			return true
		}
	}
}

func (q *LockFreeLedgerPostingQueue) DrainBatch(maxBatch int) ([]Money, int) {
	for {
		t := q.tail.Load()
		h := q.head.Load()
		avail := int(h - t)
		if avail <= 0 {
			return nil, 0
		}
		if avail > maxBatch {
			avail = maxBatch
		}
		if q.tail.CompareAndSwap(t, t+uint64(avail)) {
			batch := make([]Money, avail)
			for i := 0; i < avail; i++ {
				idx := (t + uint64(i)) & q.mask
				batch[i] = q.entries[idx]
			}
			return batch, avail
		}
	}
}

func (q *LockFreeLedgerPostingQueue) GetTotals() (int64, int64) {
	return q.totalDebited.Load(), q.totalCredited.Load()
}

func (q *LockFreeLedgerPostingQueue) PendingCount() uint64 {
	h := q.head.Load()
	t := q.tail.Load()
	if h >= t {
		return h - t
	}
	return 0
}

type LockFreeSagaCoordinationFence struct {
	requiredParticipants uint64
	arrivedCount          atomic.Uint64
	epoch                 atomic.Uint64
	timedOut              atomic.Bool
}

func NewLockFreeSagaCoordinationFence(required uint64) *LockFreeSagaCoordinationFence {
	return &LockFreeSagaCoordinationFence{
		requiredParticipants: required,
	}
}

func (f *LockFreeSagaCoordinationFence) Arrive(participantID string) bool {
	cur := f.arrivedCount.Add(1)
	return cur == f.requiredParticipants
}

func (f *LockFreeSagaCoordinationFence) WaitSpin(maxSpins int) bool {
	for i := 0; i < maxSpins; i++ {
		if f.arrivedCount.Load() >= f.requiredParticipants {
			return true
		}
		runtime.Gosched()
	}
	f.timedOut.Store(true)
	return false
}

func (f *LockFreeSagaCoordinationFence) Reset(newRequired uint64) {
	f.requiredParticipants = newRequired
	f.arrivedCount.Store(0)
	f.timedOut.Store(false)
	f.epoch.Add(1)
}

func (f *LockFreeSagaCoordinationFence) IsComplete() bool {
	return f.arrivedCount.Load() >= f.requiredParticipants
}

type LockFreeTransactionJournal struct {
	journalCap      uint64
	mask            uint64
	entries         []AuditLogEntry
	writeCursor     atomic.Uint64
	committedCursor atomic.Uint64
	flushedCursor   atomic.Uint64
	droppedEntries  atomic.Uint64
}

func NewLockFreeTransactionJournal(capacity uint64) *LockFreeTransactionJournal {
	capPow2 := uint64(1)
	for capPow2 < capacity {
		capPow2 <<= 1
	}
	return &LockFreeTransactionJournal{
		journalCap: capPow2,
		mask:       capPow2 - 1,
		entries:    make([]AuditLogEntry, capPow2),
	}
}

func (j *LockFreeTransactionJournal) AppendJournalEntry(entry AuditLogEntry) bool {
	for {
		cursor := j.writeCursor.Load()
		flushed := j.flushedCursor.Load()
		if cursor-flushed >= j.journalCap {
			j.droppedEntries.Add(1)
			return false
		}
		if j.writeCursor.CompareAndSwap(cursor, cursor+1) {
			j.entries[cursor&j.mask] = entry
			return true
		}
	}
}

func (j *LockFreeTransactionJournal) CommitUpTo(cursor uint64) {
	for {
		cur := j.committedCursor.Load()
		if cursor <= cur {
			return
		}
		if j.committedCursor.CompareAndSwap(cur, cursor) {
			return
		}
	}
}

func (j *LockFreeTransactionJournal) FlushBatch(maxBatch int) ([]AuditLogEntry, int) {
	for {
		flushed := j.flushedCursor.Load()
		committed := j.committedCursor.Load()
		avail := int(committed - flushed)
		if avail <= 0 {
			return nil, 0
		}
		if avail > maxBatch {
			avail = maxBatch
		}
		if j.flushedCursor.CompareAndSwap(flushed, flushed+uint64(avail)) {
			batch := make([]AuditLogEntry, avail)
			for i := 0; i < avail; i++ {
				batch[i] = j.entries[(flushed+uint64(i))&j.mask]
			}
			return batch, avail
		}
	}
}

func (j *LockFreeTransactionJournal) GetJournalStats() (uint64, uint64, uint64, uint64) {
	return j.writeCursor.Load(), j.committedCursor.Load(), j.flushedCursor.Load(), j.droppedEntries.Load()
}

func (j *LockFreeTransactionJournal) PendingToFlush() uint64 {
	c := j.committedCursor.Load()
	f := j.flushedCursor.Load()
	if c >= f {
		return c - f
	}
	return 0
}

`,

}
