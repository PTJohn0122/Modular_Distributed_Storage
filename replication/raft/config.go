package raft

import (
	"fmt"
	"time"
)

// Config defines the various settings for a Raft cluster.
type Config struct {
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	Storage          StableStore
}

// DefaultConfig returns the default config for a Raft cluster. By default
// config uses in-memory storage
func DefaultConfig() Config {
	return Config{
		ElectionTimeout:  time.Millisecond * 150,
		HeartbeatTimeout: time.Millisecond * 50,
		Storage:          NewMemoryStore(),
	}
}

// CheckConfig checks if a provided Raft config is valid.
func CheckConfig(config Config) error {
	if config.HeartbeatTimeout < 5*time.Millisecond {
		return fmt.Errorf("Heartbeat timeout is too low")
	}

	if config.ElectionTimeout < 5*time.Millisecond {
		return fmt.Errorf("Election timeout is too low")
	}

	if config.ElectionTimeout < config.HeartbeatTimeout {
		return fmt.Errorf(
			"The election timeout (%v) is less than the heartbeat timeout (%v)",
			config.ElectionTimeout,
			config.HeartbeatTimeout,
		)
	}

	return nil
}
