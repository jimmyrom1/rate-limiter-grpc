package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/jimmyrom1/rate-limiter-grpc/proto/limiter/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	serverAddr := flag.String("addr", "localhost:50051", "The gRPC server address")
	mode := flag.String("mode", "demo", "Mode: demo, burst, breaker, metrics")
	flag.Parse()

	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server at %s: %v", *serverAddr, err)
	}
	defer conn.Close()

	client := pb.NewRateLimiterServiceClient(conn)
	ctx := context.Background()

	switch *mode {
	case "burst":
		runBurstDemo(ctx, client)
	case "breaker":
		runBreakerDemo(ctx, client)
	case "metrics":
		runMetrics(ctx, client)
	case "demo":
		fallthrough
	default:
		runFullDemo(ctx, client)
	}
}

func runFullDemo(ctx context.Context, client pb.RateLimiterServiceClient) {
	fmt.Println("================================================================")
	fmt.Println("🚀 RATE LIMITER & CIRCUIT BREAKER gRPC - INTERACTIVE LIVE DEMO")
	fmt.Println("================================================================")

	// 1. Token Bucket Burst
	fmt.Println("\n[1] DEMO: Token Bucket (Capacidad: 5, Tasa: 2 tokens/seg)")
	fmt.Println("----------------------------------------------------------------")
	keyTB := "tenant-alpha"
	for i := 1; i <= 7; i++ {
		res, err := client.Check(ctx, &pb.CheckRequest{
			Key:       keyTB,
			Algorithm: pb.Algorithm_ALGORITHM_TOKEN_BUCKET,
			Capacity:  5,
			Rate:      2.0,
			Cost:      1,
		})
		if err != nil {
			log.Fatalf("Error calling Check: %v", err)
		}
		if res.Allowed {
			fmt.Printf("  Petición %d: ✅ PERMITIDA | Tokens restantes: %.2f\n", i, res.Remaining)
		} else {
			fmt.Printf("  Petición %d: ❌ DENEGADA  | Motivo: %s | Reintentar en: %d ms\n", i, res.Reason, res.RetryAfterMs)
		}
	}

	fmt.Println("\n⏳ Esperando 1.2 segundos para rellenar tokens...")
	time.Sleep(1200 * time.Millisecond)

	resAfterWait, _ := client.Check(ctx, &pb.CheckRequest{
		Key:       keyTB,
		Algorithm: pb.Algorithm_ALGORITHM_TOKEN_BUCKET,
		Capacity:  5,
		Rate:      2.0,
		Cost:      1,
	})
	fmt.Printf("  Petición tras espera: %s | Tokens restantes: %.2f\n",
		formatAllowed(resAfterWait.Allowed), resAfterWait.Remaining)

	// 2. Sliding Window
	fmt.Println("\n[2] DEMO: Sliding Window Counter (Límite: 3 en ventana de 1s)")
	fmt.Println("----------------------------------------------------------------")
	keySW := "client-sliding"
	for i := 1; i <= 4; i++ {
		res, err := client.Check(ctx, &pb.CheckRequest{
			Key:       keySW,
			Algorithm: pb.Algorithm_ALGORITHM_SLIDING_WINDOW,
			Capacity:  3,
			WindowMs:  1000,
			Cost:      1,
		})
		if err != nil {
			log.Fatalf("Error: %v", err)
		}
		if res.Allowed {
			fmt.Printf("  Petición %d: ✅ PERMITIDA | Capacidad restante estimada: %.2f\n", i, res.Remaining)
		} else {
			fmt.Printf("  Petición %d: ❌ DENEGADA  | Motivo: %s | Reintentar en: %d ms\n", i, res.Reason, res.RetryAfterMs)
		}
	}

	// 3. Circuit Breaker Lifecycle
	fmt.Println("\n[3] DEMO: Circuit Breaker Lifecycle (Servicio: payment-gateway)")
	fmt.Println("----------------------------------------------------------------")
	svc := "payment-gateway"

	fmt.Println("  Simulando 3 fallos consecutivos en downstream...")
	for f := 1; f <= 3; f++ {
		rec, _ := client.RecordResult(ctx, &pb.RecordResultRequest{
			ServiceName:                  svc,
			Success:                      false,
			ConsecutiveFailuresThreshold: 3,
			RecoveryTimeoutMs:            1500,
			ProbeSuccessThreshold:        2,
		})
		fmt.Printf("  Fallo %d registrado -> Estado del breaker: %s\n", f, rec.CurrentState)
	}

	// Probar petición cuando el circuito está abierto
	resBreaker, _ := client.Check(ctx, &pb.CheckRequest{
		Key:         "client-checkout",
		Capacity:    100,
		Rate:        100,
		ServiceName: svc,
	})
	fmt.Printf("  Intento de Checkout con Breaker OPEN: %s | Motivo: %s | Estado: %s\n",
		formatAllowed(resBreaker.Allowed), resBreaker.Reason, resBreaker.BreakerState)

	fmt.Println("  ⏳ Esperando tiempo de recuperación (1.6s) para pasar a HALF_OPEN...")
	time.Sleep(1600 * time.Millisecond)

	// Sonda 1 de recuperación
	fmt.Println("  Enviando sonda 1 de prueba...")
	recProbe1, _ := client.RecordResult(ctx, &pb.RecordResultRequest{
		ServiceName:           svc,
		Success:               true,
		ProbeSuccessThreshold: 2,
	})
	fmt.Printf("  Sonda 1 exitosa -> Estado del breaker: %s (Éxitos: %d)\n", recProbe1.CurrentState, recProbe1.SuccessCount)

	// Sonda 2 de recuperación
	fmt.Println("  Enviando sonda 2 de prueba...")
	recProbe2, _ := client.RecordResult(ctx, &pb.RecordResultRequest{
		ServiceName:           svc,
		Success:               true,
		ProbeSuccessThreshold: 2,
	})
	fmt.Printf("  Sonda 2 exitosa -> Estado del breaker: %s (¡CIRCUITO RECUPERADO!)\n", recProbe2.CurrentState)

	// Checkout ahora permitido
	resRecovered, _ := client.Check(ctx, &pb.CheckRequest{
		Key:         "client-checkout",
		Capacity:    100,
		Rate:        100,
		ServiceName: svc,
	})
	fmt.Printf("  Intento de Checkout tras recuperación: %s | Breaker: %s\n",
		formatAllowed(resRecovered.Allowed), resRecovered.BreakerState)

	// 4. Métricas finales
	fmt.Println("\n[4] MÉTRICAS DEL SERVIDOR")
	fmt.Println("----------------------------------------------------------------")
	runMetrics(ctx, client)
	fmt.Println("================================================================")
	fmt.Println("✨ DEMOSTRACIÓN COMPLETADA CON ÉXITO")
	fmt.Println("================================================================")
}

func runBurstDemo(ctx context.Context, client pb.RateLimiterServiceClient) {
	fmt.Println("🚀 Ejecutando ráfaga concurrente de 50 peticiones simultáneas...")
	var wg sync.WaitGroup
	var allowedCount int
	var deniedCount int
	var mu sync.Mutex

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			res, err := client.Check(ctx, &pb.CheckRequest{
				Key:       "burst-tenant",
				Algorithm: pb.Algorithm_ALGORITHM_TOKEN_BUCKET,
				Capacity:  20,
				Rate:      10,
				Cost:      1,
			})
			if err == nil {
				mu.Lock()
				if res.Allowed {
					allowedCount++
				} else {
					deniedCount++
				}
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("Resultado de ráfaga: %d permitidas, %d rechazadas (Capacidad 20)\n", allowedCount, deniedCount)
}

func runBreakerDemo(ctx context.Context, client pb.RateLimiterServiceClient) {
	svc := "external-api"
	fmt.Printf("Disparando Circuit Breaker para servicio: %s\n", svc)
	for i := 1; i <= 5; i++ {
		res, _ := client.RecordResult(ctx, &pb.RecordResultRequest{
			ServiceName:                  svc,
			Success:                      false,
			ConsecutiveFailuresThreshold: 4,
		})
		fmt.Printf("Fallo %d: Estado = %s\n", i, res.CurrentState)
	}
}

func runMetrics(ctx context.Context, client pb.RateLimiterServiceClient) {
	m, err := client.GetMetrics(ctx, &pb.MetricsRequest{})
	if err != nil {
		log.Printf("Error obteniendo métricas: %v", err)
		return
	}
	fmt.Printf("  Total Peticiones:        %d\n", m.TotalRequests)
	fmt.Printf("  Peticiones Permitidas:   %d\n", m.AllowedRequests)
	fmt.Printf("  Peticiones Denegadas:    %d\n", m.DeniedRequests)
	fmt.Printf("  Limitadores Activos:     %d\n", m.ActiveLimiters)
	fmt.Printf("  Circuit Breakers Activos:%d\n", m.ActiveCircuitBreakers)
	fmt.Printf("  Disparos de Breaker:     %d\n", m.BreakerTrips)
}

func formatAllowed(allowed bool) string {
	if allowed {
		return "✅ PERMITIDO"
	}
	return "❌ DENEGADO"
}
