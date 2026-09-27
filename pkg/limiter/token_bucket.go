package limiter

import (
	"math"
	"sync"
	"time"
)

// TokenBucket implements the Token Bucket rate limiting algorithm.
// It allows bursts up to capacity and replenishes tokens smoothly over time.
type TokenBucket struct {
	mu           sync.Mutex
	capacity     float64
	rate         float64 // tokens per second
	tokens       float64
	lastRefill   time.Time
	lastAccessed time.Time
}

// NewTokenBucket creates a new TokenBucket with given capacity and refill rate (tokens/sec).
func NewTokenBucket(capacity float64, rate float64) (*TokenBucket, error) {
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}
	if rate <= 0 {
		return nil, ErrInvalidRate
	}

	now := time.Now()
	return &TokenBucket{
		capacity:     capacity,
		rate:         rate,
		tokens:       capacity,
		lastRefill:   now,
		lastAccessed: now,
	}, nil
}

// Allow checks if cost tokens are available and consumes them if so.
func (tb *TokenBucket) Allow(cost int64) Decision {
	if cost <= 0 {
		cost = 1
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	tb.lastAccessed = now

	// Refill tokens based on elapsed time
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens = math.Min(tb.capacity, tb.tokens+(elapsed*tb.rate))
	tb.lastRefill = now

	reqCost := float64(cost)
	if tb.tokens >= reqCost {
		tb.tokens -= reqCost
		return Decision{
			Allowed:    true,
			Remaining:  tb.tokens,
			RetryAfter: 0,
			Reason:     "",
		}
	}

	// Calculate wait time until enough tokens are replenished
	needed := reqCost - tb.tokens
	secondsToWait := needed / tb.rate
	retryAfter := time.Duration(secondsToWait * float64(time.Second))
	if retryAfter < time.Millisecond {
		retryAfter = time.Millisecond
	}

	return Decision{
		Allowed:    false,
		Remaining:  tb.tokens,
		RetryAfter: retryAfter,
		Reason:     "RATE_LIMIT_EXCEEDED",
	}
}

// Reset restores the bucket to full capacity.
func (tb *TokenBucket) Reset() {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	tb.tokens = tb.capacity
	tb.lastRefill = now
	tb.lastAccessed = now
}

// LastAccessed returns the timestamp of the most recent evaluation.
func (tb *TokenBucket) LastAccessed() time.Time {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.lastAccessed
}
