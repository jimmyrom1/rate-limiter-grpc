package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

type State int

const (
	StateClosed State = iota + 1
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

var (
	ErrCircuitOpen = errors.New("circuit breaker is OPEN")
)

type Config struct {
	FailureThreshold      int64         // Number of consecutive failures to trip to OPEN
	RecoveryTimeout       time.Duration // Time to wait before attempting recovery in HALF_OPEN
	ProbeSuccessThreshold int64         // Consecutive successes in HALF_OPEN to close the circuit
}

// DefaultConfig provides production-ready defaults.
func DefaultConfig() Config {
	return Config{
		FailureThreshold:      5,
		RecoveryTimeout:       5 * time.Second,
		ProbeSuccessThreshold: 2,
	}
}

// CircuitBreaker manages fault tolerance and fail-fast behavior.
type CircuitBreaker struct {
	mu             sync.RWMutex
	name           string
	config         Config
	state          State
	failureCount   int64
	successCount   int64
	lastTransition time.Time
	totalTrips     int64
}

// New creates a new CircuitBreaker with the specified configuration.
func New(name string, cfg Config) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.RecoveryTimeout <= 0 {
		cfg.RecoveryTimeout = 5 * time.Second
	}
	if cfg.ProbeSuccessThreshold <= 0 {
		cfg.ProbeSuccessThreshold = 2
	}

	return &CircuitBreaker{
		name:           name,
		config:         cfg,
		state:          StateClosed,
		lastTransition: time.Now(),
	}
}

// Allow evaluates if a request should be dispatched to the downstream service.
func (cb *CircuitBreaker) Allow() (bool, State) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case StateClosed:
		return true, StateClosed

	case StateOpen:
		// Check if recovery timeout has elapsed to probe with HALF_OPEN
		if now.Sub(cb.lastTransition) >= cb.config.RecoveryTimeout {
			cb.state = StateHalfOpen
			cb.successCount = 0
			cb.lastTransition = now
			return true, StateHalfOpen
		}
		return false, StateOpen

	case StateHalfOpen:
		// In HALF_OPEN, permit probe execution
		return true, StateHalfOpen

	default:
		return true, StateClosed
	}
}

// RecordSuccess registers a successful downstream operation.
func (cb *CircuitBreaker) RecordSuccess() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	if cb.state == StateOpen && now.Sub(cb.lastTransition) >= cb.config.RecoveryTimeout {
		cb.state = StateHalfOpen
		cb.successCount = 0
		cb.lastTransition = now
	}

	switch cb.state {
	case StateHalfOpen:
		cb.successCount++
		if cb.successCount >= cb.config.ProbeSuccessThreshold {
			// Recovered back to CLOSED
			cb.state = StateClosed
			cb.failureCount = 0
			cb.successCount = 0
			cb.lastTransition = now
		}
	case StateClosed:
		// Reset failure streak on success
		cb.failureCount = 0
	}

	return cb.state
}

// RecordFailure registers a downstream failure.
func (cb *CircuitBreaker) RecordFailure() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	if cb.state == StateOpen && now.Sub(cb.lastTransition) >= cb.config.RecoveryTimeout {
		cb.state = StateHalfOpen
		cb.successCount = 0
		cb.lastTransition = now
	}

	switch cb.state {
	case StateClosed:
		cb.failureCount++
		if cb.failureCount >= cb.config.FailureThreshold {
			// Trip to OPEN
			cb.state = StateOpen
			cb.totalTrips++
			cb.lastTransition = now
		}
	case StateHalfOpen:
		// Immediate trip back to OPEN on any probe failure
		cb.state = StateOpen
		cb.totalTrips++
		cb.successCount = 0
		cb.lastTransition = now
	}

	return cb.state
}


// Reset returns the circuit breaker to CLOSED state and zeroes counters.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state = StateClosed
	cb.failureCount = 0
	cb.successCount = 0
	cb.lastTransition = time.Now()
}

// Snapshot returns the current status metrics of the circuit breaker.
type Snapshot struct {
	Name           string
	State          State
	FailureCount   int64
	SuccessCount   int64
	TotalTrips     int64
	LastTransition time.Time
}

// Snapshot captures the current state thread-safely.
func (cb *CircuitBreaker) Snapshot() Snapshot {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return Snapshot{
		Name:           cb.name,
		State:          cb.state,
		FailureCount:   cb.failureCount,
		SuccessCount:   cb.successCount,
		TotalTrips:     cb.totalTrips,
		LastTransition: cb.lastTransition,
	}
}
