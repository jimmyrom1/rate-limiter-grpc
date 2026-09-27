package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	GRPCPort        string
	MetricsPort     string
	DefaultCapacity int64
	DefaultRate     float64
	DefaultWindowMs int64
	CleanupInterval time.Duration
	StaleTTL        time.Duration
}

func Load() *Config {
	return &Config{
		GRPCPort:        getEnv("GRPC_PORT", "50051"),
		MetricsPort:     getEnv("METRICS_PORT", "9090"),
		DefaultCapacity: getEnvInt64("DEFAULT_CAPACITY", 60),
		DefaultRate:     getEnvFloat64("DEFAULT_RATE", 10.0),
		DefaultWindowMs: getEnvInt64("DEFAULT_WINDOW_MS", 1000),
		CleanupInterval: getEnvDuration("CLEANUP_INTERVAL", 1*time.Minute),
		StaleTTL:        getEnvDuration("STALE_TTL", 15*time.Minute),
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt64(key string, defaultVal int64) int64 {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvFloat64(key string, defaultVal float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}
