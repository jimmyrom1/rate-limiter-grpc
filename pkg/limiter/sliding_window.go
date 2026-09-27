package limiter

import (
	"math"
	"sync"
	"time"
)

// SlidingWindowCounter implements a memory-efficient Sliding Window Counter algorithm.
// It avoids burst boundaries common in fixed windows by blending counts from the previous
// and current time frames.
type SlidingWindowCounter struct {
	mu                 sync.Mutex
	limit              float64
	window             time.Duration
	currentWindowStart time.Time
	prevCount          float64
	currCount          float64
	lastAccessed       time.Time
}

// NewSlidingWindowCounter creates a new sliding window counter with a given capacity limit and window duration.
func NewSlidingWindowCounter(limit float64, window time.Duration) (*SlidingWindowCounter, error) {
	if limit <= 0 {
		return nil, ErrInvalidCapacity
	}
	if window <= 0 {
		return nil, ErrInvalidWindow
	}

	now := time.Now()
	windowStart := now.Truncate(window)
	return &SlidingWindowCounter{
		limit:              limit,
		window:             window,
		currentWindowStart: windowStart,
		prevCount:          0,
		currCount:          0,
		lastAccessed:       now,
	}, nil
}

// Allow evaluates if the cost can be accepted within the current sliding window.
func (sw *SlidingWindowCounter) Allow(cost int64) Decision {
	if cost <= 0 {
		cost = 1
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	sw.lastAccessed = now

	// Advance window if elapsed
	elapsedWindows := now.Sub(sw.currentWindowStart) / sw.window
	if elapsedWindows >= 1 {
		if elapsedWindows == 1 {
			sw.prevCount = sw.currCount
		} else {
			sw.prevCount = 0
		}
		sw.currCount = 0
		sw.currentWindowStart = now.Truncate(sw.window)
	}

	// Calculate weighted estimate
	weightInCurrent := float64(now.Sub(sw.currentWindowStart)) / float64(sw.window)
	if weightInCurrent > 1.0 {
		weightInCurrent = 1.0
	} else if weightInCurrent < 0 {
		weightInCurrent = 0
	}

	weightPrev := 1.0 - weightInCurrent
	estimatedCount := (sw.prevCount * weightPrev) + sw.currCount
	reqCost := float64(cost)

	if estimatedCount+reqCost <= sw.limit {
		sw.currCount += reqCost
		remaining := math.Max(0, sw.limit-(estimatedCount+reqCost))
		return Decision{
			Allowed:    true,
			Remaining:  remaining,
			RetryAfter: 0,
			Reason:     "",
		}
	}

	remaining := math.Max(0, sw.limit-estimatedCount)
	retryAfter := sw.currentWindowStart.Add(sw.window).Sub(now)
	if retryAfter <= 0 {
		retryAfter = time.Millisecond
	}

	return Decision{
		Allowed:    false,
		Remaining:  remaining,
		RetryAfter: retryAfter,
		Reason:     "RATE_LIMIT_EXCEEDED",
	}
}

// Reset clears both window counts.
func (sw *SlidingWindowCounter) Reset() {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	sw.currentWindowStart = now.Truncate(sw.window)
	sw.prevCount = 0
	sw.currCount = 0
	sw.lastAccessed = now
}

// LastAccessed returns the timestamp of the most recent evaluation.
func (sw *SlidingWindowCounter) LastAccessed() time.Time {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.lastAccessed
}
