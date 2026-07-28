package pipeline

import (
	"errors"
	"fmt"
	"os"
	"time"
)

// Config holds all tunable parameters for the image processing pipeline.
// All fields are immutable after construction via NewConfig or DefaultConfig.
//
// Design rationale for defaults:
//   - WorkerCount=4:   matches a typical 4-core machine without over-subscribing.
//   - QueueCapacity=64: provides ~64 jobs of buffering before backpressure kicks in,
//     keeping memory bounded (each queued job holds only a multipart.FileHeader reference,
//     not the decoded pixel buffer).
//   - MaxRetries=3:    enough to survive transient I/O errors without infinite loops.
//   - ShutdownTimeout=10s: gives in-flight resizes enough time to finish a large image.
type Config struct {
	// WorkerCount is the number of concurrent worker goroutines in the pool.
	// Must be >= 1.
	WorkerCount int

	// QueueCapacity is the bounded channel buffer size.
	// When the channel is full, the HTTP handler returns 429 Too Many Requests.
	// Must be >= 1.
	QueueCapacity int

	// OutputDir is the directory where resized images are written.
	// Created automatically if it does not exist.
	OutputDir string

	// MaxRetries is the maximum number of processing attempts per job
	// before the job is moved to the Dead-Letter Queue.
	// Must be >= 0 (0 means no retries, fail immediately).
	MaxRetries int

	// ShutdownTimeout is the maximum duration to wait for in-flight jobs
	// to complete during graceful shutdown. If exceeded, the process exits
	// with any remaining jobs abandoned.
	ShutdownTimeout time.Duration

	// DLQPath is the file path for the Dead-Letter Queue log.
	// Failed jobs are appended as JSON lines. If empty, DLQ entries
	// are written to stderr.
	DLQPath string

	// ServerAddr is the listen address for the HTTP server (e.g., ":8080").
	ServerAddr string

	// MaxUploadSize is the maximum allowed upload body size in bytes.
	// Requests exceeding this are rejected with 413 Payload Too Large.
	// Default: 32 MiB.
	MaxUploadSize int64
}

// DefaultConfig returns a Config populated with production-ready defaults.
func DefaultConfig() Config {
	return Config{
		WorkerCount:     4,
		QueueCapacity:   64,
		OutputDir:       "./output",
		MaxRetries:      3,
		ShutdownTimeout: 10 * time.Second,
		DLQPath:         "./dlq.log",
		ServerAddr:      ":8080",
		MaxUploadSize:   32 << 20, // 32 MiB
	}
}

// Validate checks all configuration invariants and returns a descriptive error
// if any constraint is violated. Call this once at startup before constructing
// the pipeline.
func (c Config) Validate() error {
	var errs []error

	if c.WorkerCount < 1 {
		errs = append(errs, fmt.Errorf("WorkerCount must be >= 1, got %d", c.WorkerCount))
	}
	if c.QueueCapacity < 1 {
		errs = append(errs, fmt.Errorf("QueueCapacity must be >= 1, got %d", c.QueueCapacity))
	}
	if c.OutputDir == "" {
		errs = append(errs, errors.New("OutputDir must not be empty"))
	}
	if c.MaxRetries < 0 {
		errs = append(errs, fmt.Errorf("MaxRetries must be >= 0, got %d", c.MaxRetries))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, fmt.Errorf("ShutdownTimeout must be positive, got %v", c.ShutdownTimeout))
	}
	if c.ServerAddr == "" {
		errs = append(errs, errors.New("ServerAddr must not be empty"))
	}
	if c.MaxUploadSize <= 0 {
		errs = append(errs, fmt.Errorf("MaxUploadSize must be positive, got %d", c.MaxUploadSize))
	}

	if len(errs) > 0 {
		return fmt.Errorf("invalid pipeline config: %w", errors.Join(errs...))
	}
	return nil
}

// EnsureOutputDir creates the output directory (and any parents) if it does
// not already exist. Returns an error if the path exists but is not a directory,
// or if creation fails.
func (c Config) EnsureOutputDir() error {
	info, err := os.Stat(c.OutputDir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("output path %q exists but is not a directory", c.OutputDir)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("stat output dir: %w", err)
	}
	if err := os.MkdirAll(c.OutputDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	return nil
}
