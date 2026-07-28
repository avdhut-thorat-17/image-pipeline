package pipeline

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// DLQEntry is the JSON-serializable record written for each dead-lettered job.
type DLQEntry struct {
	JobID        string    `json:"job_id"`
	FileName     string    `json:"file_name"`
	Attempts     int       `json:"attempts"`
	LastError    string    `json:"last_error"`
	TargetWidth  uint      `json:"target_width"`
	TargetHeight uint      `json:"target_height"`
	CreatedAt    time.Time `json:"created_at"`
	DeadAt       time.Time `json:"dead_at"`
}

// DLQ is a thread-safe Dead-Letter Queue that appends failed job metadata
// as JSON lines to a file. If no file path is configured, entries are
// logged to stderr.
//
// The DLQ uses an append-only file opened with O_APPEND, which guarantees
// atomic writes on POSIX systems for writes ≤ PIPE_BUF (typically 4096 bytes).
// A mutex serialises access to avoid interleaved output.
type DLQ struct {
	mu     sync.Mutex
	file   *os.File
	logger *log.Logger
}

// NewDLQ opens (or creates) the DLQ log file at the given path.
// If path is empty, entries are written to stderr instead.
func NewDLQ(path string) (*DLQ, error) {
	d := &DLQ{}

	if path == "" {
		d.logger = log.New(os.Stderr, "[DLQ] ", log.LstdFlags)
		return d, nil
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open DLQ file %q: %w", path, err)
	}
	d.file = f
	d.logger = log.New(f, "", 0) // No prefix; we write structured JSON lines.
	return d, nil
}

// Push records a dead-lettered job. It constructs a DLQEntry from the job's
// current state and writes it as a single JSON line.
func (d *DLQ) Push(job *Job) {
	attempts, lastErr, _ := job.Snapshot()

	entry := DLQEntry{
		JobID:        job.ID,
		FileName:     job.FileName,
		Attempts:     attempts,
		TargetWidth:  job.TargetWidth,
		TargetHeight: job.TargetHeight,
		CreatedAt:    job.CreatedAt,
		DeadAt:       time.Now(),
	}
	if lastErr != nil {
		entry.LastError = lastErr.Error()
	}

	data, err := json.Marshal(entry)
	if err != nil {
		// Marshalling a DLQEntry should never fail (no channels, funcs, etc.),
		// but if it does, fall back to a plain log line.
		log.Printf("[DLQ] failed to marshal entry for job %s: %v", job.ID, err)
		return
	}

	d.mu.Lock()
	d.logger.Output(2, string(data)) //nolint:errcheck // best-effort logging
	d.mu.Unlock()
}

// Close flushes and closes the underlying DLQ file, if any.
func (d *DLQ) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file != nil {
		return d.file.Close()
	}
	return nil
}

// RetryPolicy determines whether a failed job should be retried or dead-lettered.
type RetryPolicy struct {
	MaxRetries int
}

// NewRetryPolicy creates a RetryPolicy with the specified maximum retry count.
func NewRetryPolicy(maxRetries int) RetryPolicy {
	return RetryPolicy{MaxRetries: maxRetries}
}

// ShouldRetry returns true if the job has not yet exhausted its retry budget.
// It checks the current attempt count (thread-safe via Snapshot).
func (rp RetryPolicy) ShouldRetry(job *Job) bool {
	attempts, _, _ := job.Snapshot()
	return attempts < rp.MaxRetries
}

// ExecuteWithRetry runs the processor on the job, retrying on failure up to
// MaxRetries times. If all attempts fail, the job is pushed to the DLQ.
//
// Panic recovery: if the processor panics (e.g., nil pointer in a corrupt
// image decoder), the panic is caught, converted to an error, and counted
// as a failed attempt.
func ExecuteWithRetry(job *Job, proc *Processor, policy RetryPolicy, dlq *DLQ) {
	job.SetStatus(StatusProcessing)

	for {
		result, err := safeProcess(proc, job)
		if err == nil {
			job.SetResult(result)
			return
		}

		job.RecordAttempt(err)
		job.SetStatus(StatusFailed)

		if !policy.ShouldRetry(job) {
			job.SetStatus(StatusDeadLettered)
			dlq.Push(job)
			return
		}

		// No backoff delay: retries are immediate since failures are typically
		// deterministic (corrupt file) rather than transient (network blip).
		// A jittered backoff could be added here for transient failure modes.
	}
}

// safeProcess wraps Processor.Process with panic recovery, converting any
// panic into an error. This ensures a single corrupt image cannot crash
// the entire worker pool.
func safeProcess(proc *Processor, job *Job) (result JobResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic during processing of job %s: %v", job.ID, r)
		}
	}()
	return proc.Process(job)
}
