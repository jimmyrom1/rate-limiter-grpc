package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/jimmyrom1/rate-limiter-grpc/internal/config"
	"github.com/jimmyrom1/rate-limiter-grpc/internal/service"
	"github.com/jimmyrom1/rate-limiter-grpc/pkg/circuitbreaker"
	"github.com/jimmyrom1/rate-limiter-grpc/pkg/limiter"
	pb "github.com/jimmyrom1/rate-limiter-grpc/proto/limiter/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	cfg := config.Load()
	log.Printf("[SERVER] Starting Rate Limiter & Circuit Breaker gRPC service...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limiterMgr := limiter.NewManager()
	breakerMgr := circuitbreaker.NewManager()

	// Start background cleanup for inactive limiters
	limiterMgr.StartAutoCleanup(ctx, cfg.CleanupInterval, cfg.StaleTTL)

	// Create gRPC Server
	limiterServer := service.NewLimiterServer(limiterMgr, breakerMgr)
	grpcServer := grpc.NewServer()

	// Register services
	pb.RegisterRateLimiterServiceServer(grpcServer, limiterServer)

	// Enable gRPC Health Checking
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("limiter.v1.RateLimiterService", healthpb.HealthCheckResponse_SERVING)

	// Enable gRPC Server Reflection for grpcurl / Postman
	reflection.Register(grpcServer)

	// Start HTTP Metrics & Health endpoint
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK\n"))
		})
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			metrics, err := limiterServer.GetMetrics(r.Context(), &pb.MetricsRequest{})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(metrics)
		})

		metricsAddr := ":" + cfg.MetricsPort
		log.Printf("[METRICS] HTTP server listening at %s", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, mux); err != nil && err != http.ErrServerClosed {
			log.Printf("[METRICS] Error: %v", err)
		}
	}()

	// Listen on gRPC port
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		log.Fatalf("[SERVER] Failed to listen on port %s: %v", cfg.GRPCPort, err)
	}

	go func() {
		log.Printf("[SERVER] gRPC service listening at :%s", cfg.GRPCPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("[SERVER] gRPC server failed: %v", err)
		}
	}()

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("[SERVER] Shutting down gracefully...")
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	grpcServer.GracefulStop()
	cancel()
	fmt.Println("[SERVER] Server stopped.")
}
