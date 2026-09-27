package service

import (
	"context"
	"fmt"
	"testing"

	pb "github.com/jimmyrom1/rate-limiter-grpc/proto/limiter/v1"
)

func BenchmarkCheckTokenBucket_Parallel(b *testing.B) {
	srv := setupTestServer()
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pbRunner *testing.PB) {
		i := 0
		for pbRunner.Next() {
			i++
			key := fmt.Sprintf("tenant-%d", i%100)
			_, _ = srv.Check(ctx, &pb.CheckRequest{
				Key:       key,
				Algorithm: pb.Algorithm_ALGORITHM_TOKEN_BUCKET,
				Capacity:  1000000,
				Rate:      100000,
				Cost:      1,
			})
		}
	})
}

func BenchmarkCheckSlidingWindow_Parallel(b *testing.B) {
	srv := setupTestServer()
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pbRunner *testing.PB) {
		i := 0
		for pbRunner.Next() {
			i++
			key := fmt.Sprintf("tenant-%d", i%100)
			_, _ = srv.Check(ctx, &pb.CheckRequest{
				Key:       key,
				Algorithm: pb.Algorithm_ALGORITHM_SLIDING_WINDOW,
				Capacity:  1000000,
				WindowMs:  60000,
				Cost:      1,
			})
		}
	})
}

func BenchmarkCheckLeakyBucket_Parallel(b *testing.B) {
	srv := setupTestServer()
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pbRunner *testing.PB) {
		i := 0
		for pbRunner.Next() {
			i++
			key := fmt.Sprintf("tenant-%d", i%100)
			_, _ = srv.Check(ctx, &pb.CheckRequest{
				Key:       key,
				Algorithm: pb.Algorithm_ALGORITHM_LEAKY_BUCKET,
				Capacity:  1000000,
				Rate:      100000,
				Cost:      1,
			})
		}
	})
}
