// Package pipeline implements a bounded, concurrent image processing pipeline
// with backpressure, retry semantics, and graceful shutdown.
package pipeline

import (
	"fmt"
	"mime/multipart"
	"sync"
	"sync/atomic"
	"time"
)

// JobStatus represents the discrete lifecycle states of an image processing job.
// Transitions are monotonically forward: Queued → Processing → Completed|Failed|DeadLettered.
type JobStatus int32

const (
	// StatusQueued indicates the job is waiting in the bounded channel.
	StatusQueued JobStatus = iota
	// StatusProcessing indicates a worker has claimed the job.
	StatusProcessing
	// StatusCompleted indicates successful resize and disk write.
	StatusCompleted
	// StatusFailed indicates the job failed but may still be retried.
	StatusFailed
	// StatusDeadLettered indicates all retries exhausted; moved to DLQ.
	StatusDeadLettered
)

// String returns a human-readable label for the job status.
func (s JobStatus) String() string {
	switch s {
	case StatusQueued:
		return "QUEUED"
	case StatusProcessing:
		return "PROCESSING"
	case StatusCompleted:
		return "COMPLETED"
	case StatusFailed:
		return "FAILED"
	case StatusDeadLettered:
		return "DEAD_LETTERED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int32(s))
	}
}

// Job encapsulates all state for a single image processing request.
//
// Concurrency model:
//   - The status field is accessed atomically via Status/SetStatus.
//   - The mu mutex guards mutable fields that are read/written by
//     multiple goroutines (Attempts, LastError, Result).
//   - Immutable fields (ID, FileName, TargetWidth, TargetHeight, CreatedAt,
//     FileHeader) are set once at construction and never modified.
type Job struct {
	// --- Immutable fields (set once at construction) ---

	// ID is a unique identifier for this job, assigned at ingestion time.
	ID string

	// FileName is the original upload filename, used for logging and DLQ records.
	FileName string

	// TargetWidth is the desired output width in pixels.
	TargetWidth uint

	// TargetHeight is the desired output height in pixels.
	TargetHeight uint

	// CreatedAt records when the job was first submitted.
	CreatedAt time.Time

	// FileHeader holds the multipart file handle for lazy reading.
	// The HTTP handler keeps the multipart reader alive until the job
	// is consumed by a worker, at which point the file is read and
	// the header is no longer referenced.
	FileHeader *multipart.FileHeader

	// --- Atomic field ---

	// status is the current lifecycle state, accessed via atomic load/store
	// to avoid locking on the hot read path (e.g., status checks from
	// monitoring or logging goroutines).
	status atomic.Int32

	// --- Guarded mutable fields ---

	mu        sync.Mutex
	Attempts  int       // Number of processing attempts so far.
	LastError error     // Most recent processing error, if any.
	Result    JobResult // Populated on successful completion.
}

// JobResult holds the output metadata for a successfully processed job.
type JobResult struct {
	// OutputPath is the absolute filesystem path to the resized image.
	OutputPath string

	// Duration is the wall-clock time spent processing (decode + resize + write).
	Duration time.Duration

	// OriginalSize is the raw byte count of the uploaded file.
	OriginalSize int64

	// ResizedSize is the byte count of the output file after resize and encode.
	ResizedSize int64
}

// NewJob constructs a Job in the Queued state with all immutable fields populated.
// The caller must supply a unique id (e.g., UUID or monotonic counter).
func NewJob(id string, fileName string, targetWidth, targetHeight uint, header *multipart.FileHeader) *Job {
	j := &Job{
		ID:           id,
		FileName:     fileName,
		TargetWidth:  targetWidth,
		TargetHeight: targetHeight,
		CreatedAt:    time.Now(),
		FileHeader:   header,
	}
	j.status.Store(int32(StatusQueued))
	return j
}

// Status returns the current job status without acquiring a lock.
func (j *Job) Status() JobStatus {
	return JobStatus(j.status.Load())
}

// SetStatus atomically updates the job status.
func (j *Job) SetStatus(s JobStatus) {
	j.status.Store(int32(s))
}

// RecordAttempt increments the attempt counter and stores the latest error.
// It is called by the retry engine before each processing attempt.
func (j *Job) RecordAttempt(err error) {
	j.mu.Lock()
	j.Attempts++
	j.LastError = err
	j.mu.Unlock()
}

// SetResult stores the successful processing result and transitions
// the job to the Completed status.
func (j *Job) SetResult(r JobResult) {
	j.mu.Lock()
	j.Result = r
	j.mu.Unlock()
	j.SetStatus(StatusCompleted)
}

// Snapshot returns a safe copy of the mutable fields for logging or inspection.
// This avoids exposing the mutex to callers.
func (j *Job) Snapshot() (attempts int, lastErr error, result JobResult) {
	j.mu.Lock()
	attempts = j.Attempts
	lastErr = j.LastError
	result = j.Result
	j.mu.Unlock()
	return
}
