package service

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/jimmyrom1/rate-limiter-grpc/pkg/circuitbreaker"
	"github.com/jimmyrom1/rate-limiter-grpc/pkg/limiter"
	pb "github.com/jimmyrom1/rate-limiter-grpc/proto/limiter/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ServerMetrics struct {
	TotalRequests   int64
	AllowedRequests int64
	DeniedRequests  int64
}

// LimiterServer implements pb.RateLimiterServiceServer.
type LimiterServer struct {
	pb.UnimplementedRateLimiterServiceServer

	limiterMgr *limiter.Manager
	breakerMgr *circuitbreaker.Manager
	metrics    ServerMetrics
}

// NewLimiterServer initializes the gRPC service implementation.
func NewLimiterServer(limiterMgr *limiter.Manager, breakerMgr *circuitbreaker.Manager) *LimiterServer {
	return &LimiterServer{
		limiterMgr: limiterMgr,
		breakerMgr: breakerMgr,
	}
}

// Check evaluates incoming requests against rate limiting rules and downstream circuit breakers.
func (s *LimiterServer) Check(ctx context.Context, req *pb.CheckRequest) (*pb.CheckResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	atomic.AddInt64(&s.metrics.TotalRequests, 1)

	var currentBreakerState pb.BreakerState = pb.BreakerState_BREAKER_STATE_UNSPECIFIED

	// 1. Check Circuit Breaker if service_name is present
	if req.ServiceName != "" {
		cb := s.breakerMgr.GetOrCreate(req.ServiceName, 5, 5*time.Second, 2)
		allowed, state := cb.Allow()

		switch state {
		case circuitbreaker.StateClosed:
			currentBreakerState = pb.BreakerState_BREAKER_STATE_CLOSED
		case circuitbreaker.StateOpen:
			currentBreakerState = pb.BreakerState_BREAKER_STATE_OPEN
		case circuitbreaker.StateHalfOpen:
			currentBreakerState = pb.BreakerState_BREAKER_STATE_HALF_OPEN
		}

		if !allowed {
			atomic.AddInt64(&s.metrics.DeniedRequests, 1)
			return &pb.CheckResponse{
				Allowed:      false,
				Reason:       "CIRCUIT_BREAKER_OPEN",
				Remaining:    0,
				RetryAfterMs: 5000,
				BreakerState: currentBreakerState,
			}, nil
		}
	}

	// 2. Check Rate Limiting
	algoType := limiter.AlgorithmTokenBucket
	switch req.Algorithm {
	case pb.Algorithm_ALGORITHM_SLIDING_WINDOW:
		algoType = limiter.AlgorithmSlidingWindow
	case pb.Algorithm_ALGORITHM_LEAKY_BUCKET:
		algoType = limiter.AlgorithmLeakyBucket
	case pb.Algorithm_ALGORITHM_TOKEN_BUCKET:
		algoType = limiter.AlgorithmTokenBucket
	}

	l, err := s.limiterMgr.GetOrCreate(
		req.Key,
		algoType,
		req.Capacity,
		req.Rate,
		req.WindowMs,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get or create rate limiter: %v", err)
	}

	cost := req.Cost
	if cost <= 0 {
		cost = 1
	}

	decision := l.Allow(cost)

	if decision.Allowed {
		atomic.AddInt64(&s.metrics.AllowedRequests, 1)
		return &pb.CheckResponse{
			Allowed:      true,
			Reason:       "",
			Remaining:    decision.Remaining,
			RetryAfterMs: 0,
			BreakerState: currentBreakerState,
		}, nil
	}

	atomic.AddInt64(&s.metrics.DeniedRequests, 1)
	return &pb.CheckResponse{
		Allowed:      false,
		Reason:       decision.Reason,
		Remaining:    decision.Remaining,
		RetryAfterMs: decision.RetryAfter.Milliseconds(),
		BreakerState: currentBreakerState,
	}, nil
}

// RecordResult updates the circuit breaker state based on external call outcomes.
func (s *LimiterServer) RecordResult(ctx context.Context, req *pb.RecordResultRequest) (*pb.RecordResultResponse, error) {
	if req == nil || req.ServiceName == "" {
		return nil, status.Error(codes.InvalidArgument, "service_name is required")
	}

	recoveryTimeout := 5 * time.Second
	if req.RecoveryTimeoutMs > 0 {
		recoveryTimeout = time.Duration(req.RecoveryTimeoutMs) * time.Millisecond
	}

	cb := s.breakerMgr.GetOrCreate(
		req.ServiceName,
		req.ConsecutiveFailuresThreshold,
		recoveryTimeout,
		req.ProbeSuccessThreshold,
	)

	prevSnapshot := cb.Snapshot()
	var nextState circuitbreaker.State
	if req.Success {
		nextState = cb.RecordSuccess()
	} else {
		nextState = cb.RecordFailure()
	}
	newSnapshot := cb.Snapshot()

	return &pb.RecordResultResponse{
		ServiceName:   req.ServiceName,
		PreviousState: toProtoBreakerState(prevSnapshot.State),
		CurrentState:  toProtoBreakerState(nextState),
		FailureCount:  newSnapshot.FailureCount,
		SuccessCount:  newSnapshot.SuccessCount,
	}, nil
}

// GetBreakerStatus queries the current state of a circuit breaker.
func (s *LimiterServer) GetBreakerStatus(ctx context.Context, req *pb.BreakerStatusRequest) (*pb.BreakerStatusResponse, error) {
	if req == nil || req.ServiceName == "" {
		return nil, status.Error(codes.InvalidArgument, "service_name is required")
	}

	cb, exists := s.breakerMgr.Get(req.ServiceName)
	if !exists {
		return nil, status.Errorf(codes.NotFound, "circuit breaker for service %q not found", req.ServiceName)
	}

	snap := cb.Snapshot()
	return &pb.BreakerStatusResponse{
		ServiceName:             req.ServiceName,
		State:                   toProtoBreakerState(snap.State),
		FailureCount:            snap.FailureCount,
		SuccessCount:            snap.SuccessCount,
		LastStateChangeUnixMs:   snap.LastTransition.UnixMilli(),
	}, nil
}

// Reset clears limiter quotas or resets circuit breakers.
func (s *LimiterServer) Reset(ctx context.Context, req *pb.ResetRequest) (*pb.ResetResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	limiterReset := false
	breakerReset := false

	if req.Key != "" {
		limiterReset = s.limiterMgr.Reset(req.Key)
	}
	if req.ServiceName != "" {
		breakerReset = s.breakerMgr.Reset(req.ServiceName)
	}

	return &pb.ResetResponse{
		LimiterReset: limiterReset,
		BreakerReset: breakerReset,
	}, nil
}

// GetMetrics returns aggregate server runtime statistics.
func (s *LimiterServer) GetMetrics(ctx context.Context, req *pb.MetricsRequest) (*pb.MetricsResponse, error) {
	return &pb.MetricsResponse{
		TotalRequests:         atomic.LoadInt64(&s.metrics.TotalRequests),
		AllowedRequests:       atomic.LoadInt64(&s.metrics.AllowedRequests),
		DeniedRequests:        atomic.LoadInt64(&s.metrics.DeniedRequests),
		ActiveLimiters:        int64(s.limiterMgr.Count()),
		ActiveCircuitBreakers: int64(s.breakerMgr.Count()),
		BreakerTrips:          s.breakerMgr.TotalTrips(),
	}, nil
}

func toProtoBreakerState(st circuitbreaker.State) pb.BreakerState {
	switch st {
	case circuitbreaker.StateClosed:
		return pb.BreakerState_BREAKER_STATE_CLOSED
	case circuitbreaker.StateOpen:
		return pb.BreakerState_BREAKER_STATE_OPEN
	case circuitbreaker.StateHalfOpen:
		return pb.BreakerState_BREAKER_STATE_HALF_OPEN
	default:
		return pb.BreakerState_BREAKER_STATE_UNSPECIFIED
	}
}
