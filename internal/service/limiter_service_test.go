package service

import (
	"context"
	"testing"

	"github.com/jimmyrom1/rate-limiter-grpc/pkg/circuitbreaker"
	"github.com/jimmyrom1/rate-limiter-grpc/pkg/limiter"
	pb "github.com/jimmyrom1/rate-limiter-grpc/proto/limiter/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func setupTestServer() *LimiterServer {
	limiterMgr := limiter.NewManager()
	breakerMgr := circuitbreaker.NewManager()
	return NewLimiterServer(limiterMgr, breakerMgr)
}

func TestCheckTokenBucket(t *testing.T) {
	srv := setupTestServer()
	ctx := context.Background()

	req := &pb.CheckRequest{
		Key:       "user-123",
		Algorithm: pb.Algorithm_ALGORITHM_TOKEN_BUCKET,
		Capacity:  3,
		Rate:      1.0,
		Cost:      1,
	}

	// First 3 requests should be allowed
	for i := 0; i < 3; i++ {
		resp, err := srv.Check(ctx, req)
		if err != nil {
			t.Fatalf("unexpected error on request %d: %v", i+1, err)
		}
		if !resp.Allowed {
			t.Fatalf("expected request %d to be allowed", i+1)
		}
	}

	// 4th request must be denied
	resp, err := srv.Check(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error on 4th request: %v", err)
	}
	if resp.Allowed {
		t.Fatalf("expected 4th request to be denied")
	}
	if resp.Reason != "RATE_LIMIT_EXCEEDED" {
		t.Errorf("expected reason RATE_LIMIT_EXCEEDED, got %s", resp.Reason)
	}
	if resp.RetryAfterMs <= 0 {
		t.Errorf("expected positive RetryAfterMs, got %d", resp.RetryAfterMs)
	}
}

func TestCheckSlidingWindowAndLeakyBucket(t *testing.T) {
	srv := setupTestServer()
	ctx := context.Background()

	// Sliding window check
	swReq := &pb.CheckRequest{
		Key:       "sliding-key",
		Algorithm: pb.Algorithm_ALGORITHM_SLIDING_WINDOW,
		Capacity:  2,
		WindowMs:  500,
	}
	res1, _ := srv.Check(ctx, swReq)
	res2, _ := srv.Check(ctx, swReq)
	res3, _ := srv.Check(ctx, swReq)

	if !res1.Allowed || !res2.Allowed || res3.Allowed {
		t.Fatalf("unexpected sliding window outcomes: %v, %v, %v", res1.Allowed, res2.Allowed, res3.Allowed)
	}

	// Leaky bucket check
	lbReq := &pb.CheckRequest{
		Key:       "leaky-key",
		Algorithm: pb.Algorithm_ALGORITHM_LEAKY_BUCKET,
		Capacity:  2,
		Rate:      10,
	}
	l1, _ := srv.Check(ctx, lbReq)
	l2, _ := srv.Check(ctx, lbReq)
	l3, _ := srv.Check(ctx, lbReq)

	if !l1.Allowed || !l2.Allowed || l3.Allowed {
		t.Fatalf("unexpected leaky bucket outcomes: %v, %v, %v", l1.Allowed, l2.Allowed, l3.Allowed)
	}
}

func TestCheckWithCircuitBreaker(t *testing.T) {
	srv := setupTestServer()
	ctx := context.Background()

	svcName := "payment-gateway"

	// Trip circuit breaker by recording failures
	for i := 0; i < 5; i++ {
		_, err := srv.RecordResult(ctx, &pb.RecordResultRequest{
			ServiceName:                  svcName,
			Success:                      false,
			ConsecutiveFailuresThreshold: 5,
			RecoveryTimeoutMs:            100,
		})
		if err != nil {
			t.Fatalf("failed recording result: %v", err)
		}
	}

	// Verify Check is blocked by breaker
	req := &pb.CheckRequest{
		Key:         "client-1",
		Capacity:    100,
		Rate:        100,
		ServiceName: svcName,
	}
	resp, err := srv.Check(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Allowed {
		t.Fatalf("expected request blocked by open circuit breaker")
	}
	if resp.Reason != "CIRCUIT_BREAKER_OPEN" {
		t.Errorf("expected reason CIRCUIT_BREAKER_OPEN, got %s", resp.Reason)
	}
	if resp.BreakerState != pb.BreakerState_BREAKER_STATE_OPEN {
		t.Errorf("expected state OPEN, got %v", resp.BreakerState)
	}
}

func TestGetBreakerStatusAndReset(t *testing.T) {
	srv := setupTestServer()
	ctx := context.Background()

	// Non-existent service status returns NotFound
	_, err := srv.GetBreakerStatus(ctx, &pb.BreakerStatusRequest{ServiceName: "ghost"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound error, got %v", err)
	}

	// Create and trip service
	srv.RecordResult(ctx, &pb.RecordResultRequest{
		ServiceName: "auth-service",
		Success:     false,
	})

	statResp, err := srv.GetBreakerStatus(ctx, &pb.BreakerStatusRequest{ServiceName: "auth-service"})
	if err != nil {
		t.Fatalf("failed getting breaker status: %v", err)
	}
	if statResp.FailureCount != 1 {
		t.Errorf("expected 1 failure count, got %d", statResp.FailureCount)
	}

	// Reset
	resetResp, err := srv.Reset(ctx, &pb.ResetRequest{
		ServiceName: "auth-service",
	})
	if err != nil || !resetResp.BreakerReset {
		t.Fatalf("expected successful breaker reset: %v", err)
	}
}

func TestGetMetrics(t *testing.T) {
	srv := setupTestServer()
	ctx := context.Background()

	// Make some checks
	srv.Check(ctx, &pb.CheckRequest{Key: "metric-key", Capacity: 1, Rate: 1})
	srv.Check(ctx, &pb.CheckRequest{Key: "metric-key", Capacity: 1, Rate: 1}) // denied

	metrics, err := srv.GetMetrics(ctx, &pb.MetricsRequest{})
	if err != nil {
		t.Fatalf("failed getting metrics: %v", err)
	}
	if metrics.TotalRequests != 2 {
		t.Errorf("expected 2 total requests, got %d", metrics.TotalRequests)
	}
	if metrics.AllowedRequests != 1 {
		t.Errorf("expected 1 allowed request, got %d", metrics.AllowedRequests)
	}
	if metrics.DeniedRequests != 1 {
		t.Errorf("expected 1 denied request, got %d", metrics.DeniedRequests)
	}
	if metrics.ActiveLimiters != 1 {
		t.Errorf("expected 1 active limiter, got %d", metrics.ActiveLimiters)
	}
}

func TestValidationErrors(t *testing.T) {
	srv := setupTestServer()
	ctx := context.Background()

	if _, err := srv.Check(ctx, nil); status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument on nil Check request, got %v", err)
	}

	if _, err := srv.RecordResult(ctx, nil); status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument on nil RecordResult, got %v", err)
	}

	if _, err := srv.GetBreakerStatus(ctx, nil); status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument on nil GetBreakerStatus, got %v", err)
	}

	if _, err := srv.Reset(ctx, nil); status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument on nil Reset, got %v", err)
	}
}
