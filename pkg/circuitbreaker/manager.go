package circuitbreaker

import (
	"sync"
	"time"
)

// Manager manages a thread-safe registry of named CircuitBreakers.
type Manager struct {
	mu       sync.RWMutex
	breakers map[string]*CircuitBreaker
}

// NewManager creates a new circuit breaker manager.
func NewManager() *Manager {
	return &Manager{
		breakers: make(map[string]*CircuitBreaker),
	}
}

// GetOrCreate retrieves an existing CircuitBreaker or registers a new one.
func (m *Manager) GetOrCreate(
	name string,
	failureThreshold int64,
	recoveryTimeout time.Duration,
	probeSuccessThreshold int64,
) *CircuitBreaker {
	if name == "" {
		name = "default"
	}

	m.mu.RLock()
	cb, exists := m.breakers[name]
	m.mu.RUnlock()
	if exists {
		return cb
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if cb, exists = m.breakers[name]; exists {
		return cb
	}

	cfg := Config{
		FailureThreshold:      failureThreshold,
		RecoveryTimeout:       recoveryTimeout,
		ProbeSuccessThreshold: probeSuccessThreshold,
	}
	cb = New(name, cfg)
	m.breakers[name] = cb
	return cb
}

// Get returns the CircuitBreaker for a given name if it exists.
func (m *Manager) Get(name string) (*CircuitBreaker, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cb, exists := m.breakers[name]
	return cb, exists
}

// Reset clears the state of a named circuit breaker.
func (m *Manager) Reset(name string) bool {
	m.mu.RLock()
	cb, exists := m.breakers[name]
	m.mu.RUnlock()

	if !exists {
		return false
	}
	cb.Reset()
	return true
}

// Count returns the number of registered circuit breakers.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.breakers)
}

// TotalTrips aggregates all trip events across all registered breakers.
func (m *Manager) TotalTrips() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var total int64
	for _, cb := range m.breakers {
		total += cb.Snapshot().TotalTrips
	}
	return total
}
