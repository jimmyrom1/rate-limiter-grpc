package limiter

import (
	"errors"
	"time"
)

var (
	ErrInvalidCapacity = errors.New("capacity must be greater than zero")
	ErrInvalidRate     = errors.New("rate must be greater than zero")
	ErrInvalidWindow   = errors.New("window duration must be greater than zero")
	ErrUnknownAlgorithm = errors.New("unknown rate limiting algorithm")
)

// Decision represents the outcome of a rate limit check.
type Decision struct {
	Allowed    bool
	Remaining  float64
	RetryAfter time.Duration
	Reason     string
}

// RateLimiter defines the common interface for rate limiting algorithms.
type RateLimiter interface {
	// Allow evaluates whether an incoming request with the given cost is permitted.
	Allow(cost int64) Decision

	// Reset resets the limiter internal state to maximum capacity.
	Reset()

	// LastAccessed returns the last time this limiter was queried.
	LastAccessed() time.Time
}
