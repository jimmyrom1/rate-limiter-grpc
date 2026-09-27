package circuitbreaker

import (
	"sync"
	"testing"
	"time"
)

func TestCircuitBreakerLifecycle(t *testing.T) {
	cfg := Config{
		FailureThreshold:      3,
		RecoveryTimeout:       100 * time.Millisecond,
		ProbeSuccessThreshold: 2,
	}
	cb := New("test-service", cfg)

	// Initial state: CLOSED
	allowed, state := cb.Allow()
	if !allowed || state != StateClosed {
		t.Fatalf("expected CLOSED and allowed, got allowed=%v state=%v", allowed, state)
	}

	// 2 failures: still CLOSED
	cb.RecordFailure()
	cb.RecordFailure()
	snap := cb.Snapshot()
	if snap.State != StateClosed || snap.FailureCount != 2 {
		t.Fatalf("expected CLOSED with 2 failures, got state=%v failures=%d", snap.State, snap.FailureCount)
	}

	// 3rd failure: trips to OPEN
	cb.RecordFailure()
	snap = cb.Snapshot()
	if snap.State != StateOpen || snap.TotalTrips != 1 {
		t.Fatalf("expected OPEN with 1 trip, got state=%v trips=%d", snap.State, snap.TotalTrips)
	}

	// Immediate calls in OPEN are denied
	allowed, state = cb.Allow()
	if allowed || state != StateOpen {
		t.Fatalf("expected OPEN and rejected, got allowed=%v state=%v", allowed, state)
	}

	// Wait for recovery timeout
	time.Sleep(120 * time.Millisecond)

	// Next call should transition to HALF_OPEN
	allowed, state = cb.Allow()
	if !allowed || state != StateHalfOpen {
		t.Fatalf("expected HALF_OPEN and allowed probe, got allowed=%v state=%v", allowed, state)
	}

	// 1st probe success
	st := cb.RecordSuccess()
	if st != StateHalfOpen {
		t.Fatalf("expected still HALF_OPEN after 1 success, got %v", st)
	}

	// 2nd probe success: recovers to CLOSED
	st = cb.RecordSuccess()
	if st != StateClosed {
		t.Fatalf("expected recovered to CLOSED after 2 successes, got %v", st)
	}

	snap = cb.Snapshot()
	if snap.State != StateClosed || snap.FailureCount != 0 {
		t.Fatalf("expected clean CLOSED state, got %+v", snap)
	}
}

func TestCircuitBreakerProbeFailure(t *testing.T) {
	cfg := Config{
		FailureThreshold:      2,
		RecoveryTimeout:       50 * time.Millisecond,
		ProbeSuccessThreshold: 2,
	}
	cb := New("flaky-service", cfg)

	// Trip to OPEN
	cb.RecordFailure()
	cb.RecordFailure()

	time.Sleep(60 * time.Millisecond)

	// Transitions to HALF_OPEN on first call
	allowed, state := cb.Allow()
	if !allowed || state != StateHalfOpen {
		t.Fatalf("expected probe allowed in HALF_OPEN")
	}

	// Probe fails -> trips immediately back to OPEN
	st := cb.RecordFailure()
	if st != StateOpen {
		t.Fatalf("expected immediate transition back to OPEN, got %v", st)
	}

	allowed, _ = cb.Allow()
	if allowed {
		t.Fatalf("expected call to be denied immediately after probe failure")
	}
}

func TestCircuitBreakerManagerConcurrency(t *testing.T) {
	mgr := NewManager()

	var wg sync.WaitGroup
	workers := 30
	opsPerWorker := 50

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			svcName := "svc-a"
			if workerId%2 == 0 {
				svcName = "svc-b"
			}

			cb := mgr.GetOrCreate(svcName, 10, 100*time.Millisecond, 2)
			for j := 0; j < opsPerWorker; j++ {
				cb.Allow()
				if j%5 == 0 {
					cb.RecordFailure()
				} else {
					cb.RecordSuccess()
				}
			}
		}(i)
	}

	wg.Wait()

	if mgr.Count() != 2 {
		t.Fatalf("expected 2 managed services, got %d", mgr.Count())
	}

	cbA, exists := mgr.Get("svc-a")
	if !exists || cbA == nil {
		t.Fatalf("expected to get svc-a")
	}

	// Test Reset on Manager
	if !mgr.Reset("svc-a") {
		t.Fatalf("expected Reset('svc-a') to succeed")
	}
	if mgr.Reset("nonexistent") {
		t.Fatalf("expected Reset on nonexistent to return false")
	}

	snapA := cbA.Snapshot()
	if snapA.State != StateClosed || snapA.FailureCount != 0 {
		t.Fatalf("expected svc-a reset to closed state, got %+v", snapA)
	}

	trips := mgr.TotalTrips()
	if trips < 0 {
		t.Fatalf("expected valid non-negative total trips, got %d", trips)
	}
}

