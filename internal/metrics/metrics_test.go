package metrics

import (
	"sync"
	"testing"
)

func TestMetrics_AtomicOperations(t *testing.T) {
	m := New()

	var wg sync.WaitGroup
	workers := 20
	iterations := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				m.RequestsTotal.Add(1)
				m.RequestsAccepted.Add(1)
				m.RequestsRejected.Add(1)
				m.JobsCompleted.Add(1)
				m.JobsFailed.Add(1)
				m.JobsTimeout.Add(1)
				m.ActiveJobs.Add(1)
				m.CallbackErrors.Add(1)
			}
		}()
	}

	wg.Wait()

	expected := int64(workers * iterations)
	snap := m.Snapshot()

	if snap.RequestsTotal != expected {
		t.Errorf("expected RequestsTotal %d, got %d", expected, snap.RequestsTotal)
	}
	if snap.RequestsAccepted != expected {
		t.Errorf("expected RequestsAccepted %d, got %d", expected, snap.RequestsAccepted)
	}
	if snap.RequestsRejected != expected {
		t.Errorf("expected RequestsRejected %d, got %d", expected, snap.RequestsRejected)
	}
	if snap.JobsCompleted != expected {
		t.Errorf("expected JobsCompleted %d, got %d", expected, snap.JobsCompleted)
	}
	if snap.JobsFailed != expected {
		t.Errorf("expected JobsFailed %d, got %d", expected, snap.JobsFailed)
	}
	if snap.JobsTimeout != expected {
		t.Errorf("expected JobsTimeout %d, got %d", expected, snap.JobsTimeout)
	}
	if snap.ActiveJobs != int32(expected) {
		t.Errorf("expected ActiveJobs %d, got %d", expected, snap.ActiveJobs)
	}
	if snap.CallbackErrors != expected {
		t.Errorf("expected CallbackErrors %d, got %d", expected, snap.CallbackErrors)
	}

	// Test Reset
	m.Reset()
	snapReset := m.Snapshot()
	if snapReset.RequestsTotal != 0 || snapReset.ActiveJobs != 0 {
		t.Errorf("expected all counters 0 after reset, got %+v", snapReset)
	}
}

func TestMetrics_NilSafety(t *testing.T) {
	var m *Metrics
	snap := m.Snapshot()
	if snap.RequestsTotal != 0 {
		t.Errorf("expected 0 for nil metrics snapshot, got %d", snap.RequestsTotal)
	}
	m.Reset() // should not panic
}
