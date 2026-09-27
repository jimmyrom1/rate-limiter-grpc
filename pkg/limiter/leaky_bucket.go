package limiter

import (
	"math"
	"sync"
	"time"
)

// LeakyBucket implements the Leaky Bucket algorithm.
// Incoming requests add volume to the bucket; requests leak at a smooth, constant rate.
// Useful for traffic shaping and preventing downstream service saturation.
type LeakyBucket struct {
	mu           sync.Mutex
	capacity     float64
	rate         float64 // leak rate per second
	waterLevel   float64
	lastLeak     time.Time
	lastAccessed time.Time
}

// NewLeakyBucket creates a new LeakyBucket with capacity and leak rate per second.
func NewLeakyBucket(capacity float64, rate float64) (*LeakyBucket, error) {
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}
	if rate <= 0 {
		return nil, ErrInvalidRate
	}

	now := time.Now()
	return &LeakyBucket{
		capacity:     capacity,
		rate:         rate,
		waterLevel:   0,
		lastLeak:     now,
		lastAccessed: now,
	}, nil
}

// Allow processes an incoming request volume. If the bucket overflows, it is rejected.
func (lb *LeakyBucket) Allow(cost int64) Decision {
	if cost <= 0 {
		cost = 1
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	now := time.Now()
	lb.lastAccessed = now

	// Calculate water that leaked out since last arrival
	elapsed := now.Sub(lb.lastLeak).Seconds()
	leaked := elapsed * lb.rate
	lb.waterLevel = math.Max(0, lb.waterLevel-leaked)
	lb.lastLeak = now

	reqCost := float64(cost)
	if lb.waterLevel+reqCost <= lb.capacity {
		lb.waterLevel += reqCost
		remaining := math.Max(0, lb.capacity-lb.waterLevel)
		return Decision{
			Allowed:    true,
			Remaining:  remaining,
			RetryAfter: 0,
			Reason:     "",
		}
	}

	remaining := math.Max(0, lb.capacity-lb.waterLevel)
	overflow := (lb.waterLevel + reqCost) - lb.capacity
	secondsToLeak := overflow / lb.rate
	retryAfter := time.Duration(secondsToLeak * float64(time.Second))
	if retryAfter < time.Millisecond {
		retryAfter = time.Millisecond
	}

	return Decision{
		Allowed:    false,
		Remaining:  remaining,
		RetryAfter: retryAfter,
		Reason:     "RATE_LIMIT_EXCEEDED",
	}
}

// Reset empties the bucket.
func (lb *LeakyBucket) Reset() {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	now := time.Now()
	lb.waterLevel = 0
	lb.lastLeak = now
	lb.lastAccessed = now
}

// LastAccessed returns the timestamp of the most recent evaluation.
func (lb *LeakyBucket) LastAccessed() time.Time {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.lastAccessed
}
