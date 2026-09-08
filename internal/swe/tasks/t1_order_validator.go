package tasks

import "benchmark/internal/swe"

var TaskT1OrderValidator = &swe.Task{
	ID:       "swe-t1-order-validator-01",
	Title:    "E-Commerce Multi-Item Order Pricing and Tax Calculation Engine",
	Tier:     swe.TierJunior,
	Points:   20,
	Category: "concurrency_and_bounds",
	IssueBody: `### Bug Report: Multiple Runtime Panics and Inconsistent Calculations in Order Engine

**Environment:** Go 1.24+ high-concurrency order checkout service.

**Expected Behavior:**
- Order validation and price calculation must never panic under any input condition (nil pointers, missing coupons, empty bundles, invalid quantities).
- Category tax lookup cache must be fully safe for concurrent reads and writes without data races (` + "`-race`" + ` zero warnings).
- Tiered discount rules must safely handle bundles with fewer tiers than expected without slicing out of bounds.
- Subtotal calculations must handle large numbers correctly without negative integer overflow.
- If a discount code is expired or nil, calculation must continue gracefully with zero discount applied.`,
	BrokenCode: `package main

import (
	"errors"
	"math"
	"time"
)

var (
	ErrEmptyOrder      = errors.New("order contains no items")
	ErrNegativePrice   = errors.New("price cannot be negative")
	ErrInvalidQuantity = errors.New("quantity must be positive")
)

type ItemCategory string

const (
	CategoryStandard ItemCategory = "standard"
	CategoryDigital  ItemCategory = "digital"
	CategoryLuxury   ItemCategory = "luxury"
	CategoryPerished ItemCategory = "perished"
)

type DiscountCoupon struct {
	Code       string
	Percentage float64
	ExpiresAt  time.Time
	MaxLimit   int64
}

type OrderItem struct {
	ID         string
	Name       string
	Category   ItemCategory
	UnitPrice  int64
	Quantity   int64
	BundleTier []float64
}

type CustomerProfile struct {
	ID        string
	IsVIP     bool
	CreatedAt time.Time
}

type OrderRequest struct {
	OrderID   string
	Customer  CustomerProfile
	Items     []OrderItem
	Coupon    *DiscountCoupon
	CreatedAt time.Time
}

type OrderSummary struct {
	OrderID         string
	GrossSubtotal   int64
	TotalDiscount   int64
	TaxAmount       int64
	ShippingFee     int64
	NetTotal        int64
	ProcessingSteps int
}

type PricingEngine struct {
	categoryTaxCache map[ItemCategory]float64
	defaultTaxRate   float64
	vipDiscountRate  float64
	baseShippingFee  int64
}

func NewPricingEngine(defaultTax float64, vipRate float64, shipping int64) *PricingEngine {
	return &PricingEngine{
		categoryTaxCache: make(map[ItemCategory]float64),
		defaultTaxRate:   defaultTax,
		vipDiscountRate:  vipRate,
		baseShippingFee:  shipping,
	}
}

func (e *PricingEngine) RegisterTaxRate(cat ItemCategory, rate float64) {
	e.categoryTaxCache[cat] = rate
}

func (e *PricingEngine) GetTaxRate(cat ItemCategory) float64 {
	rate, ok := e.categoryTaxCache[cat]
	if !ok {
		return e.defaultTaxRate
	}
	return rate
}

func (e *PricingEngine) ValidateOrder(req *OrderRequest) error {
	if req == nil || len(req.Items) == 0 {
		return ErrEmptyOrder
	}
	for _, item := range req.Items {
		if item.UnitPrice < 0 {
			return ErrNegativePrice
		}
		if item.Quantity <= 0 {
			return ErrInvalidQuantity
		}
	}
	return nil
}

func (e *PricingEngine) CalculateItemSubtotal(item OrderItem) (int64, error) {
	if item.Quantity <= 0 {
		return 0, ErrInvalidQuantity
	}
	if item.UnitPrice < 0 {
		return 0, ErrNegativePrice
	}
	subtotal := item.UnitPrice * item.Quantity
	return subtotal, nil
}

func (e *PricingEngine) CalculateBundleDiscount(item OrderItem) int64 {
	if item.Quantity < 3 {
		return 0
	}
	tierIndex := int(item.Quantity / 3)
	rate := item.BundleTier[tierIndex]
	subtotal, _ := e.CalculateItemSubtotal(item)
	return int64(float64(subtotal) * rate)
}

func (e *PricingEngine) CalculateCouponDiscount(req *OrderRequest, grossSubtotal int64) int64 {
	coupon := req.Coupon
	if time.Now().After(coupon.ExpiresAt) {
		return 0
	}
	discount := int64(float64(grossSubtotal) * (coupon.Percentage / 100.0))
	if coupon.MaxLimit > 0 && discount > coupon.MaxLimit {
		discount = coupon.MaxLimit
	}
	return discount
}

func (e *PricingEngine) CalculateVIPDiscount(req *OrderRequest, currentSubtotal int64) int64 {
	if !req.Customer.IsVIP {
		return 0
	}
	return int64(float64(currentSubtotal) * (e.vipDiscountRate / 100.0))
}

func (e *PricingEngine) CalculateShipping(req *OrderRequest, grossSubtotal int64) int64 {
	if grossSubtotal > 100000 {
		return 0
	}
	allDigital := true
	for _, item := range req.Items {
		if item.Category != CategoryDigital {
			allDigital = false
			break
		}
	}
	if allDigital {
		return 0
	}
	return e.baseShippingFee
}

func (e *PricingEngine) CalculateTaxes(req *OrderRequest, taxableAmount int64) int64 {
	if taxableAmount <= 0 {
		return 0
	}
	var totalTax float64
	for _, item := range req.Items {
		rate := e.GetTaxRate(item.Category)
		sub, _ := e.CalculateItemSubtotal(item)
		totalTax += float64(sub) * (rate / 100.0)
	}
	return int64(math.Round(totalTax))
}

func (e *PricingEngine) ProcessOrder(req *OrderRequest) (*OrderSummary, error) {
	if err := e.ValidateOrder(req); err != nil {
		return nil, err
	}

	steps := 0
	var grossSubtotal int64
	for _, item := range req.Items {
		sub, err := e.CalculateItemSubtotal(item)
		if err != nil {
			return nil, err
		}
		grossSubtotal += sub
		steps++
	}

	var totalDiscount int64
	for _, item := range req.Items {
		bDisc := e.CalculateBundleDiscount(item)
		totalDiscount += bDisc
		steps++
	}

	cDisc := e.CalculateCouponDiscount(req, grossSubtotal)
	totalDiscount += cDisc
	steps++

	afterDiscount := grossSubtotal - totalDiscount
	if afterDiscount < 0 {
		afterDiscount = 0
	}

	vipDisc := e.CalculateVIPDiscount(req, afterDiscount)
	totalDiscount += vipDisc
	afterDiscount -= vipDisc
	if afterDiscount < 0 {
		afterDiscount = 0
	}
	steps++

	tax := e.CalculateTaxes(req, afterDiscount)
	steps++

	shipping := e.CalculateShipping(req, grossSubtotal)
	steps++

	netTotal := afterDiscount + tax + shipping
	steps++

	return &OrderSummary{
		OrderID:         req.OrderID,
		GrossSubtotal:   grossSubtotal,
		TotalDiscount:   totalDiscount,
		TaxAmount:       tax,
		ShippingFee:     shipping,
		NetTotal:        netTotal,
		ProcessingSteps: steps,
	}, nil
}
`,
	TestCode: `package main

import (
	"sync"
	"testing"
)

func TestOrderEngine_NilCouponHandling(t *testing.T) {
	engine := NewPricingEngine(10.0, 5.0, 1500)
	req := &OrderRequest{
		OrderID: "ORD-NIL-COUPON",
		Customer: CustomerProfile{
			ID:    "CUST-01",
			IsVIP: false,
		},
		Items: []OrderItem{
			{
				ID:        "ITM-01",
				Name:      "Standard Widget",
				Category:  CategoryStandard,
				UnitPrice: 5000,
				Quantity:  2,
			},
		},
		Coupon: nil,
	}

	summary, err := engine.ProcessOrder(req)
	if err != nil {
		t.Fatalf("unexpected error processing order with nil coupon: %v", err)
	}
	if summary.GrossSubtotal != 10000 {
		t.Fatalf("expected gross subtotal 10000, got %d", summary.GrossSubtotal)
	}
}

func TestOrderEngine_ConcurrentCategoryTax(t *testing.T) {
	engine := NewPricingEngine(10.0, 5.0, 1500)
	const workers = 30
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cat := CategoryStandard
			if id%2 == 0 {
				cat = CategoryLuxury
			}
			engine.RegisterTaxRate(cat, float64(10+id%5))
			_ = engine.GetTaxRate(cat)
		}(i)
	}
	wg.Wait()
}

func TestOrderEngine_BundleTierBoundsSafety(t *testing.T) {
	engine := NewPricingEngine(10.0, 5.0, 1500)
	req := &OrderRequest{
		OrderID: "ORD-BUNDLE-BOUNDS",
		Customer: CustomerProfile{
			ID:    "CUST-02",
			IsVIP: true,
		},
		Items: []OrderItem{
			{
				ID:         "ITM-02",
				Name:       "Bulk Item",
				Category:   CategoryStandard,
				UnitPrice:  1000,
				Quantity:   20,
				BundleTier: []float64{0.05},
			},
		},
	}

	summary, err := engine.ProcessOrder(req)
	if err != nil {
		t.Fatalf("unexpected error on bundle tier bounds: %v", err)
	}
	if summary == nil {
		t.Fatal("expected summary, got nil")
	}
}

func TestOrderEngine_SubtotalOverflowProtection(t *testing.T) {
	engine := NewPricingEngine(10.0, 5.0, 1500)
	largeItem := OrderItem{
		ID:        "ITM-LARGE",
		UnitPrice: 9000000000000000000,
		Quantity:  5,
	}
	_, err := engine.CalculateItemSubtotal(largeItem)
	if err == nil {
		t.Fatal("expected overflow error on huge multiplication, got nil")
	}
}
`,
	TotalTests: 4,
	ReferenceSolution: `package main

import (
	"errors"
	"math"
	"sync"
	"time"
)

var (
	ErrEmptyOrder      = errors.New("order contains no items")
	ErrNegativePrice   = errors.New("price cannot be negative")
	ErrInvalidQuantity = errors.New("quantity must be positive")
	ErrSubtotalOverflow = errors.New("subtotal calculation overflow")
)

type ItemCategory string

const (
	CategoryStandard ItemCategory = "standard"
	CategoryDigital  ItemCategory = "digital"
	CategoryLuxury   ItemCategory = "luxury"
	CategoryPerished ItemCategory = "perished"
)

type DiscountCoupon struct {
	Code       string
	Percentage float64
	ExpiresAt  time.Time
	MaxLimit   int64
}

type OrderItem struct {
	ID         string
	Name       string
	Category   ItemCategory
	UnitPrice  int64
	Quantity   int64
	BundleTier []float64
}

type CustomerProfile struct {
	ID        string
	IsVIP     bool
	CreatedAt time.Time
}

type OrderRequest struct {
	OrderID   string
	Customer  CustomerProfile
	Items     []OrderItem
	Coupon    *DiscountCoupon
	CreatedAt time.Time
}

type OrderSummary struct {
	OrderID         string
	GrossSubtotal   int64
	TotalDiscount   int64
	TaxAmount       int64
	ShippingFee     int64
	NetTotal        int64
	ProcessingSteps int
}

type PricingEngine struct {
	mu               sync.RWMutex
	categoryTaxCache map[ItemCategory]float64
	defaultTaxRate   float64
	vipDiscountRate  float64
	baseShippingFee  int64
}

func NewPricingEngine(defaultTax float64, vipRate float64, shipping int64) *PricingEngine {
	return &PricingEngine{
		categoryTaxCache: make(map[ItemCategory]float64),
		defaultTaxRate:   defaultTax,
		vipDiscountRate:  vipRate,
		baseShippingFee:  shipping,
	}
}

func (e *PricingEngine) RegisterTaxRate(cat ItemCategory, rate float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.categoryTaxCache[cat] = rate
}

func (e *PricingEngine) GetTaxRate(cat ItemCategory) float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	rate, ok := e.categoryTaxCache[cat]
	if !ok {
		return e.defaultTaxRate
	}
	return rate
}

func (e *PricingEngine) ValidateOrder(req *OrderRequest) error {
	if req == nil || len(req.Items) == 0 {
		return ErrEmptyOrder
	}
	for _, item := range req.Items {
		if item.UnitPrice < 0 {
			return ErrNegativePrice
		}
		if item.Quantity <= 0 {
			return ErrInvalidQuantity
		}
	}
	return nil
}

func (e *PricingEngine) CalculateItemSubtotal(item OrderItem) (int64, error) {
	if item.Quantity <= 0 {
		return 0, ErrInvalidQuantity
	}
	if item.UnitPrice < 0 {
		return 0, ErrNegativePrice
	}
	if item.Quantity != 0 && item.UnitPrice > math.MaxInt64/item.Quantity {
		return 0, ErrSubtotalOverflow
	}
	subtotal := item.UnitPrice * item.Quantity
	return subtotal, nil
}

func (e *PricingEngine) CalculateBundleDiscount(item OrderItem) int64 {
	if item.Quantity < 3 || len(item.BundleTier) == 0 {
		return 0
	}
	tierIndex := int(item.Quantity / 3)
	if tierIndex >= len(item.BundleTier) {
		tierIndex = len(item.BundleTier) - 1
	}
	rate := item.BundleTier[tierIndex]
	subtotal, err := e.CalculateItemSubtotal(item)
	if err != nil {
		return 0
	}
	return int64(float64(subtotal) * rate)
}

func (e *PricingEngine) CalculateCouponDiscount(req *OrderRequest, grossSubtotal int64) int64 {
	if req.Coupon == nil {
		return 0
	}
	coupon := req.Coupon
	if !coupon.ExpiresAt.IsZero() && time.Now().After(coupon.ExpiresAt) {
		return 0
	}
	discount := int64(float64(grossSubtotal) * (coupon.Percentage / 100.0))
	if coupon.MaxLimit > 0 && discount > coupon.MaxLimit {
		discount = coupon.MaxLimit
	}
	return discount
}

func (e *PricingEngine) CalculateVIPDiscount(req *OrderRequest, currentSubtotal int64) int64 {
	if !req.Customer.IsVIP {
		return 0
	}
	return int64(float64(currentSubtotal) * (e.vipDiscountRate / 100.0))
}

func (e *PricingEngine) CalculateShipping(req *OrderRequest, grossSubtotal int64) int64 {
	if grossSubtotal > 100000 {
		return 0
	}
	allDigital := true
	for _, item := range req.Items {
		if item.Category != CategoryDigital {
			allDigital = false
			break
		}
	}
	if allDigital {
		return 0
	}
	return e.baseShippingFee
}

func (e *PricingEngine) CalculateTaxes(req *OrderRequest, taxableAmount int64) int64 {
	if taxableAmount <= 0 {
		return 0
	}
	var totalTax float64
	for _, item := range req.Items {
		rate := e.GetTaxRate(item.Category)
		sub, err := e.CalculateItemSubtotal(item)
		if err != nil {
			continue
		}
		totalTax += float64(sub) * (rate / 100.0)
	}
	return int64(math.Round(totalTax))
}

func (e *PricingEngine) ProcessOrder(req *OrderRequest) (*OrderSummary, error) {
	if err := e.ValidateOrder(req); err != nil {
		return nil, err
	}

	steps := 0
	var grossSubtotal int64
	for _, item := range req.Items {
		sub, err := e.CalculateItemSubtotal(item)
		if err != nil {
			return nil, err
		}
		grossSubtotal += sub
		steps++
	}

	var totalDiscount int64
	for _, item := range req.Items {
		bDisc := e.CalculateBundleDiscount(item)
		totalDiscount += bDisc
		steps++
	}

	cDisc := e.CalculateCouponDiscount(req, grossSubtotal)
	totalDiscount += cDisc
	steps++

	afterDiscount := grossSubtotal - totalDiscount
	if afterDiscount < 0 {
		afterDiscount = 0
	}

	vipDisc := e.CalculateVIPDiscount(req, afterDiscount)
	totalDiscount += vipDisc
	afterDiscount -= vipDisc
	if afterDiscount < 0 {
		afterDiscount = 0
	}
	steps++

	tax := e.CalculateTaxes(req, afterDiscount)
	steps++

	shipping := e.CalculateShipping(req, grossSubtotal)
	steps++

	netTotal := afterDiscount + tax + shipping
	steps++

	return &OrderSummary{
		OrderID:         req.OrderID,
		GrossSubtotal:   grossSubtotal,
		TotalDiscount:   totalDiscount,
		TaxAmount:       tax,
		ShippingFee:     shipping,
		NetTotal:        netTotal,
		ProcessingSteps: steps,
	}, nil
}
`,
}
