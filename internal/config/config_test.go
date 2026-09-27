package config

import (
	"os"
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	cfg := Load()
	if cfg.GRPCPort != "50051" {
		t.Errorf("expected default GRPCPort 50051, got %s", cfg.GRPCPort)
	}
	if cfg.MetricsPort != "9090" {
		t.Errorf("expected default MetricsPort 9090, got %s", cfg.MetricsPort)
	}
	if cfg.DefaultCapacity != 60 {
		t.Errorf("expected default capacity 60, got %d", cfg.DefaultCapacity)
	}
	if cfg.DefaultRate != 10.0 {
		t.Errorf("expected default rate 10.0, got %f", cfg.DefaultRate)
	}
	if cfg.CleanupInterval != 1*time.Minute {
		t.Errorf("expected default cleanup interval 1m, got %v", cfg.CleanupInterval)
	}
}

func TestConfigEnvOverrides(t *testing.T) {
	os.Setenv("GRPC_PORT", "50052")
	os.Setenv("METRICS_PORT", "9091")
	os.Setenv("DEFAULT_CAPACITY", "100")
	os.Setenv("DEFAULT_RATE", "25.5")
	os.Setenv("DEFAULT_WINDOW_MS", "2000")
	os.Setenv("CLEANUP_INTERVAL", "30s")
	os.Setenv("STALE_TTL", "5m")

	defer func() {
		os.Unsetenv("GRPC_PORT")
		os.Unsetenv("METRICS_PORT")
		os.Unsetenv("DEFAULT_CAPACITY")
		os.Unsetenv("DEFAULT_RATE")
		os.Unsetenv("DEFAULT_WINDOW_MS")
		os.Unsetenv("CLEANUP_INTERVAL")
		os.Unsetenv("STALE_TTL")
	}()

	cfg := Load()
	if cfg.GRPCPort != "50052" {
		t.Errorf("expected port 50052, got %s", cfg.GRPCPort)
	}
	if cfg.MetricsPort != "9091" {
		t.Errorf("expected metrics port 9091, got %s", cfg.MetricsPort)
	}
	if cfg.DefaultCapacity != 100 {
		t.Errorf("expected capacity 100, got %d", cfg.DefaultCapacity)
	}
	if cfg.DefaultRate != 25.5 {
		t.Errorf("expected rate 25.5, got %f", cfg.DefaultRate)
	}
	if cfg.DefaultWindowMs != 2000 {
		t.Errorf("expected window 2000, got %d", cfg.DefaultWindowMs)
	}
	if cfg.CleanupInterval != 30*time.Second {
		t.Errorf("expected cleanup 30s, got %v", cfg.CleanupInterval)
	}
	if cfg.StaleTTL != 5*time.Minute {
		t.Errorf("expected stale TTL 5m, got %v", cfg.StaleTTL)
	}
}
