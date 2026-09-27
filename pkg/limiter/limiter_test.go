package limiter

import (
	"sync"
	"testing"
	"time"
)

func TestTokenBucketBasic(t *testing.T) {
	// 5 tokens capacity, 2 tokens per second
	tb, err := NewTokenBucket(5, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Consume 5 tokens
	for i := 0; i < 5; i++ {
		d := tb.Allow(1)
		if !d.Allowed {
			t.Fatalf("expected request %d to be allowed", i+1)
		}
	}

	// 6th request should fail
	d := tb.Allow(1)
	if d.Allowed {
		t.Fatalf("expected request beyond capacity to be rejected")
	}
	if d.RetryAfter <= 0 {
		t.Errorf("expected positive RetryAfter, got %v", d.RetryAfter)
	}

	// Wait for replenishment (0.6s should give > 1 token at 2 tokens/sec)
	time.Sleep(600 * time.Millisecond)

	d = tb.Allow(1)
	if !d.Allowed {
		t.Fatalf("expected request after replenishment to be allowed")
	}

	// Test Reset
	tb.Allow(5) // exhaust
	tb.Reset()
	d = tb.Allow(5)
	if !d.Allowed {
		t.Fatalf("expected request after Reset to be allowed")
	}
}

func TestTokenBucketConcurrency(t *testing.T) {
	capacity := 1000.0
	rate := 100.0
	tb, err := NewTokenBucket(capacity, rate)
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	var wg sync.WaitGroup
	workers := 50
	requestsPerWorker := 20

	var allowedCount int64
	var mu sync.Mutex

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requestsPerWorker; j++ {
				d := tb.Allow(1)
				if d.Allowed {
					mu.Lock()
					allowedCount++
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	if allowedCount > 1050 { // initial 1000 + slight replenishment during test
		t.Errorf("allowed count %d exceeded expected capacity", allowedCount)
	}
}

func TestSlidingWindowCounter(t *testing.T) {
	limit := 10.0
	window := 200 * time.Millisecond
	sw, err := NewSlidingWindowCounter(limit, window)
	if err != nil {
		t.Fatalf("failed to create sliding window: %v", err)
	}

	// Allow 10
	for i := 0; i < 10; i++ {
		d := sw.Allow(1)
		if !d.Allowed {
			t.Fatalf("request %d should have been allowed", i+1)
		}
	}

	// 11th should be denied
	d := sw.Allow(1)
	if d.Allowed {
		t.Fatalf("11th request should have been denied")
	}

	// Wait for window to advance
	time.Sleep(250 * time.Millisecond)

	d = sw.Allow(5)
	if !d.Allowed {
		t.Fatalf("request in new window should have been allowed")
	}
}

func TestLeakyBucketBasic(t *testing.T) {
	capacity := 5.0
	rate := 10.0 // 10 units leaked per second (1 unit every 100ms)
	lb, err := NewLeakyBucket(capacity, rate)
	if err != nil {
		t.Fatalf("failed to create leaky bucket: %v", err)
	}

	// Fill bucket
	for i := 0; i < 5; i++ {
		d := lb.Allow(1)
		if !d.Allowed {
			t.Fatalf("request %d should have been allowed", i+1)
		}
	}

	// Overflows
	d := lb.Allow(1)
	if d.Allowed {
		t.Fatalf("overflow request should be denied")
	}

	// Wait for 200ms (should leak ~2 units)
	time.Sleep(200 * time.Millisecond)

	d = lb.Allow(1)
	if !d.Allowed {
		t.Fatalf("request after leaking should be allowed")
	}
}

func TestManagerConcurrentAccessAndEviction(t *testing.T) {
	mgr := NewManager()

	var wg sync.WaitGroup
	keys := 20
	workersPerKey := 10

	for k := 0; k < keys; k++ {
		for w := 0; w < workersPerKey; w++ {
			wg.Add(1)
			go func(keyIdx int) {
				defer wg.Done()
				keyName := string(rune('a' + (keyIdx % 26)))
				limiter, err := mgr.GetOrCreate(keyName, AlgorithmTokenBucket, 100, 10, 1000)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}
				_ = limiter.Allow(1)
			}(k)
		}
	}

	wg.Wait()

	count := mgr.Count()
	if count != keys {
		t.Errorf("expected %d limiters in manager, got %d", keys, count)
	}

	// Test Reset
	res := mgr.Reset("a")
	if !res {
		t.Errorf("expected Reset('a') to return true")
	}

	// Eviction test with small TTL
	time.Sleep(50 * time.Millisecond)
	evicted := mgr.CleanupStale(10 * time.Millisecond)
	if evicted != keys {
		t.Errorf("expected all %d keys evicted, got %d", keys, evicted)
	}
	if mgr.Count() != 0 {
		t.Errorf("expected count 0 after cleanup, got %d", mgr.Count())
	}
}
