package limiter

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

type AlgorithmType int

const (
	AlgorithmTokenBucket   AlgorithmType = 1
	AlgorithmSlidingWindow AlgorithmType = 2
	AlgorithmLeakyBucket   AlgorithmType = 3
)

const defaultShardCount = 64

type shard struct {
	mu      sync.RWMutex
	entries map[string]RateLimiter
}

// Manager manages partitioned rate limiters across concurrent shards.
// It supports automatic background eviction of stale limiters.
type Manager struct {
	shardCount uint32
	shards     []*shard
}

// NewManager initializes a sharded rate limiter manager.
func NewManager() *Manager {
	m := &Manager{
		shardCount: defaultShardCount,
		shards:     make([]*shard, defaultShardCount),
	}
	for i := range m.shards {
		m.shards[i] = &shard{
			entries: make(map[string]RateLimiter),
		}
	}
	return m
}

func (m *Manager) getShard(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	index := h.Sum32() % m.shardCount
	return m.shards[index]
}

// GetOrCreate retrieves an existing limiter for the key or instantiates a new one.
func (m *Manager) GetOrCreate(
	key string,
	algo AlgorithmType,
	capacity int64,
	rate float64,
	windowMs int64,
) (RateLimiter, error) {
	if key == "" {
		key = "default"
	}
	if capacity <= 0 {
		capacity = 60
	}
	if rate <= 0 {
		rate = 10.0
	}
	if windowMs <= 0 {
		windowMs = 1000
	}

	s := m.getShard(key)

	// Fast path: read lock
	s.mu.RLock()
	existing, found := s.entries[key]
	s.mu.RUnlock()
	if found {
		return existing, nil
	}

	// Slow path: write lock
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock
	if existing, found = s.entries[key]; found {
		return existing, nil
	}

	var limiter RateLimiter
	var err error

	switch algo {
	case AlgorithmTokenBucket:
		limiter, err = NewTokenBucket(float64(capacity), rate)
	case AlgorithmSlidingWindow:
		limiter, err = NewSlidingWindowCounter(float64(capacity), time.Duration(windowMs)*time.Millisecond)
	case AlgorithmLeakyBucket:
		limiter, err = NewLeakyBucket(float64(capacity), rate)
	default:
		// Default to TokenBucket
		limiter, err = NewTokenBucket(float64(capacity), rate)
	}

	if err != nil {
		return nil, fmt.Errorf("failed creating limiter: %w", err)
	}

	s.entries[key] = limiter
	return limiter, nil
}

// Reset resets the state of a limiter for the given key.
func (m *Manager) Reset(key string) bool {
	s := m.getShard(key)
	s.mu.RLock()
	limiter, found := s.entries[key]
	s.mu.RUnlock()

	if !found {
		return false
	}
	limiter.Reset()
	return true
}

// Delete removes a key from its shard.
func (m *Manager) Delete(key string) bool {
	s := m.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, found := s.entries[key]; found {
		delete(s.entries, key)
		return true
	}
	return false
}

// Count returns the total number of active limiters across all shards.
func (m *Manager) Count() int {
	total := 0
	for _, s := range m.shards {
		s.mu.RLock()
		total += len(s.entries)
		s.mu.RUnlock()
	}
	return total
}

// CleanupStale evicts entries that have not been accessed for longer than ttl.
func (m *Manager) CleanupStale(ttl time.Duration) int {
	cutoff := time.Now().Add(-ttl)
	evicted := 0

	for _, s := range m.shards {
		s.mu.Lock()
		for key, l := range s.entries {
			if l.LastAccessed().Before(cutoff) {
				delete(s.entries, key)
				evicted++
			}
		}
		s.mu.Unlock()
	}

	return evicted
}

// StartAutoCleanup starts a periodic background sweep to remove idle limiters.
func (m *Manager) StartAutoCleanup(ctx context.Context, interval, ttl time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.CleanupStale(ttl)
			}
		}
	}()
}
