package metrics

import (
	"sync/atomic"
)

// Metrics manages thread-safe in-memory counters for benchmark server observability.
type Metrics struct {
	RequestsTotal    atomic.Int64 `json:"requestsTotal"`    // Total request diterima
	RequestsAccepted atomic.Int64 `json:"requestsAccepted"` // Request yang diterima (202)
	RequestsRejected atomic.Int64 `json:"requestsRejected"` // Request yang ditolak (4xx/5xx)
	JobsCompleted    atomic.Int64 `json:"jobsCompleted"`    // Job selesai sukses
	JobsFailed       atomic.Int64 `json:"jobsFailed"`       // Job gagal
	JobsTimeout      atomic.Int64 `json:"jobsTimeout"`      // Job timeout
	ActiveJobs       atomic.Int32 `json:"activeJobs"`       // Job yang sedang berjalan
	CallbackErrors   atomic.Int64 `json:"callbackErrors"`   // Callback gagal dikirim
}

// Snapshot provides a point-in-time value snapshot of atomic counters for serialization.
type Snapshot struct {
	RequestsTotal    int64 `json:"requestsTotal"`
	RequestsAccepted int64 `json:"requestsAccepted"`
	RequestsRejected int64 `json:"requestsRejected"`
	JobsCompleted    int64 `json:"jobsCompleted"`
	JobsFailed       int64 `json:"jobsFailed"`
	JobsTimeout      int64 `json:"jobsTimeout"`
	ActiveJobs       int32 `json:"activeJobs"`
	CallbackErrors   int64 `json:"callbackErrors"`
}

// Default is the default global metrics registry.
var Default = New()

// New creates a new zero-initialized Metrics registry.
func New() *Metrics {
	return &Metrics{}
}

// Snapshot captures the current atomic values into a serializable Snapshot.
func (m *Metrics) Snapshot() Snapshot {
	if m == nil {
		return Snapshot{}
	}
	return Snapshot{
		RequestsTotal:    m.RequestsTotal.Load(),
		RequestsAccepted: m.RequestsAccepted.Load(),
		RequestsRejected: m.RequestsRejected.Load(),
		JobsCompleted:    m.JobsCompleted.Load(),
		JobsFailed:       m.JobsFailed.Load(),
		JobsTimeout:      m.JobsTimeout.Load(),
		ActiveJobs:       m.ActiveJobs.Load(),
		CallbackErrors:   m.CallbackErrors.Load(),
	}
}

// Reset clears all counters back to zero.
func (m *Metrics) Reset() {
	if m == nil {
		return
	}
	m.RequestsTotal.Store(0)
	m.RequestsAccepted.Store(0)
	m.RequestsRejected.Store(0)
	m.JobsCompleted.Store(0)
	m.JobsFailed.Store(0)
	m.JobsTimeout.Store(0)
	m.ActiveJobs.Store(0)
	m.CallbackErrors.Store(0)
}
