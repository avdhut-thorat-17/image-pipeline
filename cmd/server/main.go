// Package main is the entry point for the image processing pipeline server.
//
// It configures the HTTP server, binds the upload endpoint, starts the
// worker pool, serves the real-time dashboard, and orchestrates graceful
// shutdown on OS signals.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"image-pipeline/pkg/pipeline"
	"image-pipeline/web"
)

func main() {
	// --- Flag Parsing ---
	var cfg pipeline.Config
	flag.IntVar(&cfg.WorkerCount, "workers", 4, "Number of worker goroutines")
	flag.IntVar(&cfg.QueueCapacity, "queue", 64, "Bounded job channel capacity")
	flag.StringVar(&cfg.OutputDir, "output", "./output", "Output directory for resized images")
	flag.IntVar(&cfg.MaxRetries, "retries", 3, "Max retries before DLQ")
	flag.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", 10*time.Second, "Graceful shutdown timeout")
	flag.StringVar(&cfg.DLQPath, "dlq", "./dlq.log", "Dead-letter queue log file path")
	flag.StringVar(&cfg.ServerAddr, "addr", ":8080", "HTTP listen address")
	flag.Int64Var(&cfg.MaxUploadSize, "max-upload", 32<<20, "Max upload body size in bytes")
	flag.Parse()

	if err := cfg.Validate(); err != nil {
		log.Fatalf("configuration error: %v", err)
	}
	if err := cfg.EnsureOutputDir(); err != nil {
		log.Fatalf("output directory error: %v", err)
	}

	log.Printf("[main] config: workers=%d queue=%d retries=%d addr=%s output=%s",
		cfg.WorkerCount, cfg.QueueCapacity, cfg.MaxRetries, cfg.ServerAddr, cfg.OutputDir)

	// --- Initialise Pipeline Components ---
	proc := pipeline.NewProcessor(cfg.OutputDir)
	policy := pipeline.NewRetryPolicy(cfg.MaxRetries)
	eventBus := pipeline.NewEventBus()

	dlq, err := pipeline.NewDLQ(cfg.DLQPath)
	if err != nil {
		log.Fatalf("failed to open DLQ: %v", err)
	}
	defer dlq.Close()

	pool := pipeline.NewWorkerPool(cfg, proc, policy, dlq, eventBus)

	// --- Signal Handling ---
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool.Start(ctx)

	// --- Job ID Generator ---
	var jobCounter atomic.Int64

	// --- Shutdown Tracking ---
	var shuttingDown atomic.Int32

	// --- HTTP Routes ---
	mux := http.NewServeMux()

	// Dashboard (embedded static files) — catch-all for unmatched paths.
	mux.Handle("/", http.FileServer(http.FS(web.Assets)))

	// Health endpoint for load balancers / readiness probes.
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if shuttingDown.Load() != 0 {
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		stats := pool.Stats()
		json.NewEncoder(w).Encode(map[string]any{
			"status":         "ok",
			"queue_len":      stats.QueueLen,
			"queue_cap":      stats.QueueCap,
			"workers":        stats.WorkerCount,
			"active_workers": stats.ActiveWorkers,
			"total_jobs":     stats.TotalJobs,
			"completed_jobs": stats.CompletedJobs,
			"dlq_jobs":       stats.DLQJobs,
		})
	})

	// Upload endpoint — the core ingestion path.
	mux.HandleFunc("POST /upload", func(w http.ResponseWriter, r *http.Request) {
		if shuttingDown.Load() != 0 {
			http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, cfg.MaxUploadSize)

		if err := r.ParseMultipartForm(10 << 20); err != nil {
			http.Error(w, fmt.Sprintf("parse multipart form: %v", err), http.StatusBadRequest)
			return
		}

		_, header, err := r.FormFile("image")
		if err != nil {
			http.Error(w, fmt.Sprintf("missing or invalid 'image' field: %v", err), http.StatusBadRequest)
			return
		}

		width, err := parseUint(r.FormValue("width"))
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid 'width': %v", err), http.StatusBadRequest)
			return
		}
		height, err := parseUint(r.FormValue("height"))
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid 'height': %v", err), http.StatusBadRequest)
			return
		}
		if width == 0 && height == 0 {
			http.Error(w, "at least one of 'width' or 'height' must be > 0", http.StatusBadRequest)
			return
		}

		id := strconv.FormatInt(jobCounter.Add(1), 10)
		job := pipeline.NewJob(id, header.Filename, width, height, header)

		if !pool.Submit(job) {
			http.Error(w, "server is at capacity, try again later", http.StatusTooManyRequests)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]any{
			"job_id":  id,
			"status":  "queued",
			"message": "job accepted for processing",
		})
	})

	// SSE endpoint — real-time event stream for the dashboard.
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no") // Disable nginx buffering

		// Subscribe to pipeline events.
		subID, eventCh := eventBus.Subscribe(128)
		defer eventBus.Unsubscribe(subID)

		// Send initial state snapshot.
		sendSSE(w, flusher, "init", pool.Stats())

		// Periodic stats ticker for live gauge updates.
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case evt, ok := <-eventCh:
				if !ok {
					return
				}
				sendSSE(w, flusher, string(evt.Type), evt.Data)
			case <-ticker.C:
				sendSSE(w, flusher, "pool_stats", pool.Stats())
			}
		}
	})

	// --- HTTP Server ---
	srv := &http.Server{
		Addr:              cfg.ServerAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	srvErrCh := make(chan error, 1)
	go func() {
		log.Printf("[main] HTTP server listening on %s", cfg.ServerAddr)
		log.Printf("[main] Dashboard: http://localhost%s", cfg.ServerAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			srvErrCh <- err
		}
		close(srvErrCh)
	}()

	// --- Wait for Shutdown Signal ---
	select {
	case err := <-srvErrCh:
		log.Fatalf("[main] HTTP server error: %v", err)
	case <-ctx.Done():
		log.Println("[main] received shutdown signal")
	}

	// --- Graceful Shutdown Sequence ---
	shuttingDown.Store(1)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[main] HTTP server shutdown error: %v", err)
	}
	log.Println("[main] HTTP server stopped")

	pool.Shutdown()

	log.Println("[main] shutdown complete")
}

// sendSSE writes a single Server-Sent Event to the response writer.
func sendSSE(w http.ResponseWriter, flusher http.Flusher, eventType string, data any) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventType, jsonData)
	flusher.Flush()
}

// parseUint parses a string as a non-negative unsigned integer.
func parseUint(s string) (uint, error) {
	if s == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("expected non-negative integer, got %q: %w", s, err)
	}
	return uint(v), nil
}
