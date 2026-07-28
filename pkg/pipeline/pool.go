package pipeline

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerPool manages a fixed set of goroutines that consume jobs from a
// bounded channel, process them through the image pipeline, and handle
// retries/DLQ on failure.
//
// Lifecycle:
//  1. NewWorkerPool() — allocates the pool, opens the job channel.
//  2. Start(ctx)       — spawns N worker goroutines.
//  3. Submit(job)       — enqueues a job (returns false if channel is full → backpressure).
//  4. Shutdown()        — closes the channel and waits for all workers to drain.
//
// Concurrency guarantees:
//   - The job channel is the sole communication path between producers (HTTP handler)
//     and consumers (workers). No shared mutable state outside of Job's own mutex.
//   - The WaitGroup tracks only worker goroutines, not individual jobs. Each worker
//     processes jobs sequentially, so the WaitGroup count equals WorkerCount.
//   - shutdownOnce ensures the channel is closed exactly once, even if Shutdown is
//     called concurrently from multiple signal handlers.
type WorkerPool struct {
	cfg    Config
	jobs   chan *Job
	wg     sync.WaitGroup
	proc   *Processor
	policy RetryPolicy
	dlq    *DLQ
	events *EventBus // nil-safe: no events emitted if nil

	shutdownOnce sync.Once

	// Atomic stats for real-time monitoring.
	activeWorkers atomic.Int32
	totalJobs     atomic.Int64
	completedJobs atomic.Int64
	dlqJobs       atomic.Int64
}

// NewWorkerPool constructs a WorkerPool from the given configuration.
// The DLQ, Processor, and EventBus are injected to support testing with mocks.
// Pass nil for events to disable event emission (useful in tests).
func NewWorkerPool(cfg Config, proc *Processor, policy RetryPolicy, dlq *DLQ, events *EventBus) *WorkerPool {
	return &WorkerPool{
		cfg:    cfg,
		jobs:   make(chan *Job, cfg.QueueCapacity),
		proc:   proc,
		policy: policy,
		dlq:    dlq,
		events: events,
	}
}

// Start spawns cfg.WorkerCount goroutines, each pulling jobs from the
// bounded channel until it is closed. The provided context controls
// cancellation: if ctx is cancelled, workers stop processing new jobs
// (but finish any in-flight job).
func (wp *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < wp.cfg.WorkerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i)
	}
	log.Printf("[pool] started %d workers (queue capacity: %d)", wp.cfg.WorkerCount, wp.cfg.QueueCapacity)
}

// Submit attempts to enqueue a job into the bounded channel.
// Returns true if the job was accepted, false if the channel is full
// (backpressure signal → caller should return HTTP 429).
//
// This method is non-blocking: it uses a select with a default case
// to avoid parking the HTTP handler goroutine.
func (wp *WorkerPool) Submit(job *Job) bool {
	select {
	case wp.jobs <- job:
		wp.totalJobs.Add(1)
		wp.emit(EventJobQueued, map[string]any{
			"job_id":        job.ID,
			"file_name":     job.FileName,
			"target_width":  job.TargetWidth,
			"target_height": job.TargetHeight,
			"queue_len":     len(wp.jobs),
			"queue_cap":     cap(wp.jobs),
		})
		return true
	default:
		wp.emit(EventBackpressure, map[string]any{
			"queue_len": len(wp.jobs),
			"queue_cap": cap(wp.jobs),
		})
		return false
	}
}

// Shutdown closes the job channel (so workers see no more work) and
// blocks until all worker goroutines have exited. It is safe to call
// concurrently from multiple goroutines; the channel close happens
// exactly once.
func (wp *WorkerPool) Shutdown() {
	wp.shutdownOnce.Do(func() {
		close(wp.jobs)
		log.Println("[pool] job channel closed, waiting for workers to drain")
	})
	wp.wg.Wait()
	log.Println("[pool] all workers stopped")
}

// QueueLen returns the current number of jobs waiting in the channel.
// Useful for metrics/health endpoints.
func (wp *WorkerPool) QueueLen() int {
	return len(wp.jobs)
}

// QueueCap returns the total capacity of the job channel.
func (wp *WorkerPool) QueueCap() int {
	return cap(wp.jobs)
}

// PoolStats holds a snapshot of pool metrics for the dashboard / SSE.
type PoolStats struct {
	QueueLen      int   `json:"queue_len"`
	QueueCap      int   `json:"queue_cap"`
	WorkerCount   int   `json:"worker_count"`
	ActiveWorkers int32 `json:"active_workers"`
	TotalJobs     int64 `json:"total_jobs"`
	CompletedJobs int64 `json:"completed_jobs"`
	DLQJobs       int64 `json:"dlq_jobs"`
}

// Stats returns a consistent snapshot of pool metrics.
func (wp *WorkerPool) Stats() PoolStats {
	return PoolStats{
		QueueLen:      len(wp.jobs),
		QueueCap:      cap(wp.jobs),
		WorkerCount:   wp.cfg.WorkerCount,
		ActiveWorkers: wp.activeWorkers.Load(),
		TotalJobs:     wp.totalJobs.Load(),
		CompletedJobs: wp.completedJobs.Load(),
		DLQJobs:       wp.dlqJobs.Load(),
	}
}

// emit publishes an event if the EventBus is configured (non-nil).
func (wp *WorkerPool) emit(t EventType, data map[string]any) {
	if wp.events != nil {
		wp.events.Publish(Event{Type: t, Timestamp: time.Now(), Data: data})
	}
}

// worker is the main loop for a single pool worker. It reads jobs from
// the channel until the channel is closed or the context is cancelled.
func (wp *WorkerPool) worker(ctx context.Context, id int) {
	defer wp.wg.Done()
	log.Printf("[worker-%d] started", id)

	for job := range wp.jobs {
		// Check if context has been cancelled before starting work.
		select {
		case <-ctx.Done():
			log.Printf("[worker-%d] context cancelled, discarding job %s", id, job.ID)
			job.RecordAttempt(ctx.Err())
			job.SetStatus(StatusDeadLettered)
			wp.dlq.Push(job)
			wp.dlqJobs.Add(1)
			wp.emit(EventJobDeadLettered, map[string]any{
				"worker_id": id, "job_id": job.ID,
				"file_name": job.FileName, "attempts": 0,
				"error": ctx.Err().Error(),
			})
			continue
		default:
		}

		// Mark worker busy and emit processing event.
		wp.activeWorkers.Add(1)
		wp.emit(EventJobProcessing, map[string]any{
			"worker_id":     id,
			"job_id":        job.ID,
			"file_name":     job.FileName,
			"target_width":  job.TargetWidth,
			"target_height": job.TargetHeight,
		})

		log.Printf("[worker-%d] processing job %s (%s → %dx%d)", id, job.ID, job.FileName, job.TargetWidth, job.TargetHeight)
		ExecuteWithRetry(job, wp.proc, wp.policy, wp.dlq)

		wp.activeWorkers.Add(-1)

		// Emit outcome event based on final job status.
		switch job.Status() {
		case StatusCompleted:
			wp.completedJobs.Add(1)
			_, _, result := job.Snapshot()
			wp.emit(EventJobCompleted, map[string]any{
				"worker_id":     id,
				"job_id":        job.ID,
				"file_name":     job.FileName,
				"output_path":   result.OutputPath,
				"duration_ms":   result.Duration.Milliseconds(),
				"original_size": result.OriginalSize,
				"resized_size":  result.ResizedSize,
			})
			log.Printf("[worker-%d] job %s completed in %v", id, job.ID, result.Duration)
		case StatusDeadLettered:
			wp.dlqJobs.Add(1)
			attempts, lastErr, _ := job.Snapshot()
			errStr := ""
			if lastErr != nil {
				errStr = lastErr.Error()
			}
			wp.emit(EventJobDeadLettered, map[string]any{
				"worker_id": id, "job_id": job.ID,
				"file_name": job.FileName,
				"attempts":  attempts, "error": errStr,
			})
			log.Printf("[worker-%d] job %s dead-lettered after %d attempts", id, job.ID, attempts)
		default:
			log.Printf("[worker-%d] job %s finished with status %s", id, job.ID, job.Status())
		}
	}

	log.Printf("[worker-%d] stopped (channel closed)", id)
}
