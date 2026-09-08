package tasks

import "benchmark/internal/swe"

var TaskT1InventoryCache = &swe.Task{
	ID:       "swe-t1-inventory-cache-01",
	Title:    "Concurrent SKU Inventory Cache with TTL Expiration and Reservation",
	Tier:     swe.TierJunior,
	Points:   20,
	Category: "concurrency_and_race",
	IssueBody: `### Bug Report: Data Races, Underflow, and Map Iteration Crashes in Inventory Store

**Environment:** Go 1.24+ real-time inventory management microservice.

**Expected Behavior:**
- Concurrent SKU stock operations (replenish, reserve, release, batch operations) must be completely thread-safe without data races (` + "`-race`" + ` zero warnings).
- Stock levels must never drop below zero under high concurrent reservations. If available stock is insufficient, ` + "`ErrInsufficientStock`" + ` must be returned immediately.
- Batch reservation must be atomic: if any item fails or has insufficient stock, none of the reservations in that batch should take effect.
- Background eviction of expired items (` + "`EvictExpired()`" + `) must safely clean up entries without panicking from concurrent map read/write or concurrent iteration.
- Items past their TTL must not be reservable or returned by queries, returning ` + "`ErrItemExpired`" + `.`,
	BrokenCode: `package main

import (
	"errors"
	"time"
)

var (
	ErrItemNotFound      = errors.New("item not found in inventory")
	ErrInsufficientStock = errors.New("insufficient stock available")
	ErrItemExpired       = errors.New("inventory item is expired")
	ErrInvalidQuantity   = errors.New("quantity must be greater than zero")
	ErrEmptyBatch        = errors.New("batch request cannot be empty")
)

type InventoryStatus string

const (
	StatusActive       InventoryStatus = "active"
	StatusOutOfStock   InventoryStatus = "out_of_stock"
	StatusDiscontinued InventoryStatus = "discontinued"
)

type StockEntry struct {
	SKU           string
	Name          string
	Quantity      int64
	ReservedCount int64
	Status        InventoryStatus
	CreatedAt     time.Time
	ExpiresAt     time.Time
	LastRestocked time.Time
	Version       int64
}

type ReserveRequest struct {
	SKU      string
	Quantity int64
}

type ReplenishRequest struct {
	SKU      string
	Quantity int64
}

type StoreStats struct {
	TotalSKUs       int
	ActiveSKUs      int
	OutOfStockSKUs  int
	TotalStockCount int64
}

type InventoryStore struct {
	items      map[string]*StockEntry
	defaultTTL time.Duration
	auditLog   []string
}

func NewInventoryStore(ttl time.Duration) *InventoryStore {
	return &InventoryStore{
		items:      make(map[string]*StockEntry),
		defaultTTL: ttl,
		auditLog:   make([]string, 0),
	}
}

func (s *InventoryStore) AddItem(sku string, name string, initialQty int64, ttl time.Duration) error {
	if initialQty < 0 {
		return ErrInvalidQuantity
	}
	if ttl <= 0 {
		ttl = s.defaultTTL
	}
	now := time.Now()
	s.items[sku] = &StockEntry{
		SKU:           sku,
		Name:          name,
		Quantity:      initialQty,
		ReservedCount: 0,
		Status:        StatusActive,
		CreatedAt:     now,
		ExpiresAt:     now.Add(ttl),
		LastRestocked: now,
		Version:       1,
	}
	return nil
}

func (s *InventoryStore) GetItem(sku string) (*StockEntry, error) {
	entry, exists := s.items[sku]
	if !exists {
		return nil, ErrItemNotFound
	}
	if time.Now().Before(entry.ExpiresAt) {
		return nil, ErrItemExpired
	}
	return entry, nil
}

func (s *InventoryStore) Replenish(sku string, qty int64) error {
	if qty <= 0 {
		return ErrInvalidQuantity
	}
	entry, exists := s.items[sku]
	if !exists {
		return ErrItemNotFound
	}
	entry.Quantity += qty
	entry.LastRestocked = time.Now()
	entry.Version++
	if entry.Quantity > 0 && entry.Status == StatusOutOfStock {
		entry.Status = StatusActive
	}
	return nil
}

func (s *InventoryStore) Reserve(sku string, qty int64) error {
	if qty <= 0 {
		return ErrInvalidQuantity
	}
	entry, exists := s.items[sku]
	if !exists {
		return ErrItemNotFound
	}
	if time.Now().Before(entry.ExpiresAt) {
		return ErrItemExpired
	}
	available := entry.Quantity - entry.ReservedCount
	if available < qty {
		return ErrInsufficientStock
	}
	entry.ReservedCount += qty
	entry.Version++
	if entry.Quantity-entry.ReservedCount == 0 {
		entry.Status = StatusOutOfStock
	}
	return nil
}

func (s *InventoryStore) ReleaseReservation(sku string, qty int64) error {
	if qty <= 0 {
		return ErrInvalidQuantity
	}
	entry, exists := s.items[sku]
	if !exists {
		return ErrItemNotFound
	}
	if entry.ReservedCount < qty {
		entry.ReservedCount = 0
	} else {
		entry.ReservedCount -= qty
	}
	entry.Version++
	if entry.Quantity-entry.ReservedCount > 0 && entry.Status == StatusOutOfStock {
		entry.Status = StatusActive
	}
	return nil
}

func (s *InventoryStore) CommitReservation(sku string, qty int64) error {
	if qty <= 0 {
		return ErrInvalidQuantity
	}
	entry, exists := s.items[sku]
	if !exists {
		return ErrItemNotFound
	}
	if entry.ReservedCount < qty {
		return ErrInsufficientStock
	}
	entry.ReservedCount -= qty
	entry.Quantity -= qty
	entry.Version++
	return nil
}

func (s *InventoryStore) BatchReserve(reqs []ReserveRequest) error {
	if len(reqs) == 0 {
		return ErrEmptyBatch
	}
	for _, req := range reqs {
		if err := s.Reserve(req.SKU, req.Quantity); err != nil {
			return err
		}
	}
	return nil
}

func (s *InventoryStore) BatchReplenish(reqs []ReplenishRequest) error {
	if len(reqs) == 0 {
		return ErrEmptyBatch
	}
	for _, req := range reqs {
		if err := s.Replenish(req.SKU, req.Quantity); err != nil {
			return err
		}
	}
	return nil
}

func (s *InventoryStore) EvictExpired() int {
	now := time.Now()
	evictedCount := 0
	for sku, entry := range s.items {
		if now.Before(entry.ExpiresAt) {
			delete(s.items, sku)
			evictedCount++
		}
	}
	return evictedCount
}

func (s *InventoryStore) GetStats() StoreStats {
	stats := StoreStats{}
	stats.TotalSKUs = len(s.items)
	for _, entry := range s.items {
		if entry.Status == StatusActive {
			stats.ActiveSKUs++
		} else if entry.Status == StatusOutOfStock {
			stats.OutOfStockSKUs++
		}
		stats.TotalStockCount += entry.Quantity
	}
	return stats
}

func (s *InventoryStore) Snapshot() map[string]StockEntry {
	result := make(map[string]StockEntry, len(s.items))
	for sku, entry := range s.items {
		result[sku] = *entry
	}
	return result
}
`,
	TestCode: `package main

import (
	"sync"
	"testing"
	"time"
)

func TestInventoryStore_ConcurrentStockRace(t *testing.T) {
	store := NewInventoryStore(1 * time.Hour)
	sku := "SKU-RACE-01"
	_ = store.AddItem(sku, "Concurrent Gadget", 1000, 1*time.Hour)

	const workers = 20
	const iterations = 50
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if id%2 == 0 {
					_ = store.Reserve(sku, 1)
				} else {
					_ = store.Replenish(sku, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	entry, err := store.GetItem(sku)
	if err != nil {
		t.Fatalf("unexpected error fetching item: %v", err)
	}
	if entry == nil {
		t.Fatal("entry is nil")
	}
}

func TestInventoryStore_NoStockUnderflow(t *testing.T) {
	store := NewInventoryStore(1 * time.Hour)
	sku := "SKU-LIMITED"
	_ = store.AddItem(sku, "Limited Item", 10, 1*time.Hour)

	const workers = 30
	var wg sync.WaitGroup
	var successCount int64
	var mu sync.Mutex

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := store.Reserve(sku, 1)
			if err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	if successCount != 10 {
		t.Fatalf("expected exactly 10 successful reservations, got %d", successCount)
	}
}

func TestInventoryStore_BatchReservationAtomicity(t *testing.T) {
	store := NewInventoryStore(1 * time.Hour)
	_ = store.AddItem("SKU-A", "Item A", 10, 1*time.Hour)
	_ = store.AddItem("SKU-B", "Item B", 2, 1*time.Hour)

	batch := []ReserveRequest{
		{SKU: "SKU-A", Quantity: 5},
		{SKU: "SKU-B", Quantity: 5}, // SKU-B has only 2, must fail
	}

	err := store.BatchReserve(batch)
	if err == nil {
		t.Fatal("expected batch reservation to fail due to SKU-B")
	}

	// Invariant: SKU-A must not have been reserved if batch failed
	itemA, _ := store.GetItem("SKU-A")
	if itemA.ReservedCount != 0 {
		t.Fatalf("atomicity violation: SKU-A reserved count is %d, expected 0", itemA.ReservedCount)
	}
}

func TestInventoryStore_SafeConcurrentEviction(t *testing.T) {
	store := NewInventoryStore(50 * time.Millisecond)
	for i := 0; i < 50; i++ {
		_ = store.AddItem("SKU-EV-"+string(rune('A'+i)), "Expiring Item", 5, 20*time.Millisecond)
	}

	time.Sleep(30 * time.Millisecond)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_ = store.EvictExpired()
	}()

	go func() {
		defer wg.Done()
		_ = store.Snapshot()
	}()

	wg.Wait()
}
`,
	TotalTests: 4,
	ReferenceSolution: `package main

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrItemNotFound      = errors.New("item not found in inventory")
	ErrInsufficientStock = errors.New("insufficient stock available")
	ErrItemExpired       = errors.New("inventory item is expired")
	ErrInvalidQuantity   = errors.New("quantity must be greater than zero")
	ErrEmptyBatch        = errors.New("batch request cannot be empty")
)

type InventoryStatus string

const (
	StatusActive       InventoryStatus = "active"
	StatusOutOfStock   InventoryStatus = "out_of_stock"
	StatusDiscontinued InventoryStatus = "discontinued"
)

type StockEntry struct {
	SKU           string
	Name          string
	Quantity      int64
	ReservedCount int64
	Status        InventoryStatus
	CreatedAt     time.Time
	ExpiresAt     time.Time
	LastRestocked time.Time
	Version       int64
}

type ReserveRequest struct {
	SKU      string
	Quantity int64
}

type ReplenishRequest struct {
	SKU      string
	Quantity int64
}

type StoreStats struct {
	TotalSKUs       int
	ActiveSKUs      int
	OutOfStockSKUs  int
	TotalStockCount int64
}

type InventoryStore struct {
	mu         sync.RWMutex
	items      map[string]*StockEntry
	defaultTTL time.Duration
	auditLog   []string
}

func NewInventoryStore(ttl time.Duration) *InventoryStore {
	return &InventoryStore{
		items:      make(map[string]*StockEntry),
		defaultTTL: ttl,
		auditLog:   make([]string, 0),
	}
}

func (s *InventoryStore) AddItem(sku string, name string, initialQty int64, ttl time.Duration) error {
	if initialQty < 0 {
		return ErrInvalidQuantity
	}
	if ttl <= 0 {
		ttl = s.defaultTTL
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[sku] = &StockEntry{
		SKU:           sku,
		Name:          name,
		Quantity:      initialQty,
		ReservedCount: 0,
		Status:        StatusActive,
		CreatedAt:     now,
		ExpiresAt:     now.Add(ttl),
		LastRestocked: now,
		Version:       1,
	}
	return nil
}

func (s *InventoryStore) GetItem(sku string) (*StockEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, exists := s.items[sku]
	if !exists {
		return nil, ErrItemNotFound
	}
	if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
		return nil, ErrItemExpired
	}
	copied := *entry
	return &copied, nil
}

func (s *InventoryStore) Replenish(sku string, qty int64) error {
	if qty <= 0 {
		return ErrInvalidQuantity
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, exists := s.items[sku]
	if !exists {
		return ErrItemNotFound
	}
	entry.Quantity += qty
	entry.LastRestocked = time.Now()
	entry.Version++
	if entry.Quantity-entry.ReservedCount > 0 && entry.Status == StatusOutOfStock {
		entry.Status = StatusActive
	}
	return nil
}

func (s *InventoryStore) Reserve(sku string, qty int64) error {
	if qty <= 0 {
		return ErrInvalidQuantity
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, exists := s.items[sku]
	if !exists {
		return ErrItemNotFound
	}
	if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
		return ErrItemExpired
	}
	available := entry.Quantity - entry.ReservedCount
	if available < qty {
		return ErrInsufficientStock
	}
	entry.ReservedCount += qty
	entry.Version++
	if entry.Quantity-entry.ReservedCount == 0 {
		entry.Status = StatusOutOfStock
	}
	return nil
}

func (s *InventoryStore) ReleaseReservation(sku string, qty int64) error {
	if qty <= 0 {
		return ErrInvalidQuantity
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, exists := s.items[sku]
	if !exists {
		return ErrItemNotFound
	}
	if entry.ReservedCount < qty {
		entry.ReservedCount = 0
	} else {
		entry.ReservedCount -= qty
	}
	entry.Version++
	if entry.Quantity-entry.ReservedCount > 0 && entry.Status == StatusOutOfStock {
		entry.Status = StatusActive
	}
	return nil
}

func (s *InventoryStore) CommitReservation(sku string, qty int64) error {
	if qty <= 0 {
		return ErrInvalidQuantity
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, exists := s.items[sku]
	if !exists {
		return ErrItemNotFound
	}
	if entry.ReservedCount < qty {
		return ErrInsufficientStock
	}
	entry.ReservedCount -= qty
	entry.Quantity -= qty
	entry.Version++
	return nil
}

func (s *InventoryStore) BatchReserve(reqs []ReserveRequest) error {
	if len(reqs) == 0 {
		return ErrEmptyBatch
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Verify all items are present and have enough stock
	for _, req := range reqs {
		if req.Quantity <= 0 {
			return ErrInvalidQuantity
		}
		entry, exists := s.items[req.SKU]
		if !exists {
			return ErrItemNotFound
		}
		if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
			return ErrItemExpired
		}
		available := entry.Quantity - entry.ReservedCount
		if available < req.Quantity {
			return ErrInsufficientStock
		}
	}

	// 2. Commit all reservations atomically
	for _, req := range reqs {
		entry := s.items[req.SKU]
		entry.ReservedCount += req.Quantity
		entry.Version++
		if entry.Quantity-entry.ReservedCount == 0 {
			entry.Status = StatusOutOfStock
		}
	}
	return nil
}

func (s *InventoryStore) BatchReplenish(reqs []ReplenishRequest) error {
	if len(reqs) == 0 {
		return ErrEmptyBatch
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, req := range reqs {
		if req.Quantity <= 0 {
			return ErrInvalidQuantity
		}
		entry, exists := s.items[req.SKU]
		if !exists {
			return ErrItemNotFound
		}
		entry.Quantity += req.Quantity
		entry.LastRestocked = time.Now()
		entry.Version++
		if entry.Quantity-entry.ReservedCount > 0 && entry.Status == StatusOutOfStock {
			entry.Status = StatusActive
		}
	}
	return nil
}

func (s *InventoryStore) EvictExpired() int {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	evictedCount := 0
	for sku, entry := range s.items {
		if !entry.ExpiresAt.IsZero() && now.After(entry.ExpiresAt) {
			delete(s.items, sku)
			evictedCount++
		}
	}
	return evictedCount
}

func (s *InventoryStore) GetStats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stats := StoreStats{}
	stats.TotalSKUs = len(s.items)
	for _, entry := range s.items {
		if entry.Status == StatusActive {
			stats.ActiveSKUs++
		} else if entry.Status == StatusOutOfStock {
			stats.OutOfStockSKUs++
		}
		stats.TotalStockCount += entry.Quantity
	}
	return stats
}

func (s *InventoryStore) Snapshot() map[string]StockEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]StockEntry, len(s.items))
	for sku, entry := range s.items {
		result[sku] = *entry
	}
	return result
}
`,
}
