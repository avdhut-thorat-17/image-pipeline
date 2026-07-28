package pipeline_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"image-pipeline/pkg/pipeline"
)

// ============================================================================
// Test Helpers
// ============================================================================

// createTestImage generates a minimal valid PNG in-memory.
func createTestImage(t testing.TB, width, height int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	// Fill with a non-trivial colour to ensure encode/decode round-trips.
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Set(x, y, color.RGBA{R: uint8(x % 256), G: uint8(y % 256), B: 128, A: 255})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode test image: %v", err)
	}
	return buf.Bytes()
}

// createMultipartBody builds a multipart form body with an image file and resize dimensions.
func createMultipartBody(t testing.TB, imageData []byte, width, height uint) (*bytes.Buffer, string) {
	t.Helper()
	var body bytes.Buffer
	w := multipart.NewWriter(&body)

	part, err := w.CreateFormFile("image", "test.png")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if _, err := part.Write(imageData); err != nil {
		t.Fatalf("write image data: %v", err)
	}
	if err := w.WriteField("width", strconv.FormatUint(uint64(width), 10)); err != nil {
		t.Fatalf("write width field: %v", err)
	}
	if err := w.WriteField("height", strconv.FormatUint(uint64(height), 10)); err != nil {
		t.Fatalf("write height field: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	return &body, w.FormDataContentType()
}

// setupPipeline initialises a full pipeline with a temporary output directory.
// Returns the config, pool, temp dir, and a cleanup function.
func setupPipeline(t testing.TB, workers, queueCap, maxRetries int) (pipeline.Config, *pipeline.WorkerPool, string) {
	t.Helper()
	tmpDir := t.TempDir()
	outputDir := filepath.Join(tmpDir, "output")
	dlqPath := filepath.Join(tmpDir, "dlq.log")

	cfg := pipeline.Config{
		WorkerCount:     workers,
		QueueCapacity:   queueCap,
		OutputDir:       outputDir,
		MaxRetries:      maxRetries,
		ShutdownTimeout: 5 * time.Second,
		DLQPath:         dlqPath,
		ServerAddr:      ":0",
		MaxUploadSize:   32 << 20,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	if err := cfg.EnsureOutputDir(); err != nil {
		t.Fatalf("ensure output dir: %v", err)
	}

	proc := pipeline.NewProcessor(outputDir)
	policy := pipeline.NewRetryPolicy(maxRetries)
	dlq, err := pipeline.NewDLQ(dlqPath)
	if err != nil {
		t.Fatalf("open DLQ: %v", err)
	}
	t.Cleanup(func() { dlq.Close() })

	pool := pipeline.NewWorkerPool(cfg, proc, policy, dlq, nil)
	return cfg, pool, tmpDir
}

// setupHTTPServer creates a test HTTP server wired to a real pipeline.
// Returns the test server and a shutdown function.
func setupHTTPServer(t testing.TB, workers, queueCap int) (*httptest.Server, func()) {
	t.Helper()
	cfg, pool, tmpDir := setupPipeline(t, workers, queueCap, 3)

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	var jobCounter atomic.Int64
	var shuttingDown atomic.Int32

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if shuttingDown.Load() != 0 {
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":    "ok",
			"queue_len": pool.QueueLen(),
			"queue_cap": pool.QueueCap(),
			"workers":   cfg.WorkerCount,
		})
	})

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
			http.Error(w, fmt.Sprintf("missing 'image': %v", err), http.StatusBadRequest)
			return
		}

		width, _ := parseUint(r.FormValue("width"))
		height, _ := parseUint(r.FormValue("height"))
		if width == 0 && height == 0 {
			http.Error(w, "at least one of 'width' or 'height' must be > 0", http.StatusBadRequest)
			return
		}

		id := strconv.FormatInt(jobCounter.Add(1), 10)
		job := pipeline.NewJob(id, header.Filename, width, height, header)

		if !pool.Submit(job) {
			http.Error(w, "server is at capacity", http.StatusTooManyRequests)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]any{
			"job_id": id,
			"status": "queued",
		})
	})

	ts := httptest.NewServer(mux)

	shutdown := func() {
		shuttingDown.Store(1)
		cancel()
		pool.Shutdown()
		ts.Close()
		_ = tmpDir // tmpDir is cleaned up by t.TempDir
	}

	return ts, shutdown
}

func parseUint(s string) (uint, error) {
	if s == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint(v), nil
}

// ============================================================================
// Unit Tests
// ============================================================================

func TestConfigValidation(t *testing.T) {
	t.Run("default config is valid", func(t *testing.T) {
		cfg := pipeline.DefaultConfig()
		if err := cfg.Validate(); err != nil {
			t.Fatalf("default config should be valid: %v", err)
		}
	})

	t.Run("zero workers rejected", func(t *testing.T) {
		cfg := pipeline.DefaultConfig()
		cfg.WorkerCount = 0
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error for WorkerCount=0")
		}
	})

	t.Run("negative retries rejected", func(t *testing.T) {
		cfg := pipeline.DefaultConfig()
		cfg.MaxRetries = -1
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error for MaxRetries=-1")
		}
	})

	t.Run("empty output dir rejected", func(t *testing.T) {
		cfg := pipeline.DefaultConfig()
		cfg.OutputDir = ""
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error for empty OutputDir")
		}
	})
}

func TestJobStatusString(t *testing.T) {
	tests := []struct {
		status pipeline.JobStatus
		want   string
	}{
		{pipeline.StatusQueued, "QUEUED"},
		{pipeline.StatusProcessing, "PROCESSING"},
		{pipeline.StatusCompleted, "COMPLETED"},
		{pipeline.StatusFailed, "FAILED"},
		{pipeline.StatusDeadLettered, "DEAD_LETTERED"},
		{pipeline.JobStatus(99), "UNKNOWN(99)"},
	}
	for _, tt := range tests {
		if got := tt.status.String(); got != tt.want {
			t.Errorf("JobStatus(%d).String() = %q, want %q", int(tt.status), got, tt.want)
		}
	}
}

func TestJobConcurrentAccess(t *testing.T) {
	// Verify Job's thread-safety under concurrent reads and writes.
	job := pipeline.NewJob("test-1", "image.png", 100, 100, nil)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			job.RecordAttempt(fmt.Errorf("error %d", i))
		}()
		go func() {
			defer wg.Done()
			_, _, _ = job.Snapshot()
			_ = job.Status()
		}()
	}
	wg.Wait()

	attempts, _, _ := job.Snapshot()
	if attempts != 100 {
		t.Errorf("expected 100 attempts, got %d", attempts)
	}
}

func TestProcessValidImage(t *testing.T) {
	tmpDir := t.TempDir()
	proc := pipeline.NewProcessor(tmpDir)
	imgData := createTestImage(t, 64, 64)

	// Build a multipart file header from the test image.
	body, contentType := createMultipartBody(t, imgData, 32, 32)
	req := httptest.NewRequest(http.MethodPost, "/", body)
	req.Header.Set("Content-Type", contentType)
	if err := req.ParseMultipartForm(10 << 20); err != nil {
		t.Fatalf("parse multipart: %v", err)
	}
	_, header, err := req.FormFile("image")
	if err != nil {
		t.Fatalf("form file: %v", err)
	}

	job := pipeline.NewJob("proc-1", header.Filename, 32, 32, header)
	result, err := proc.Process(job)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if result.OutputPath == "" {
		t.Error("expected non-empty OutputPath")
	}
	if result.ResizedSize == 0 {
		t.Error("expected non-zero ResizedSize")
	}
	if result.Duration == 0 {
		t.Error("expected non-zero Duration")
	}

	// Verify the output file exists and is a valid PNG.
	f, err := os.Open(result.OutputPath)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer f.Close()
	outImg, _, err := image.Decode(f)
	if err != nil {
		t.Fatalf("decode output: %v", err)
	}
	bounds := outImg.Bounds()
	if bounds.Dx() != 32 || bounds.Dy() != 32 {
		t.Errorf("expected 32x32, got %dx%d", bounds.Dx(), bounds.Dy())
	}
}

func TestProcessCorruptImage(t *testing.T) {
	tmpDir := t.TempDir()
	proc := pipeline.NewProcessor(tmpDir)

	// Create a multipart body with garbage data (not a valid image).
	body, contentType := createMultipartBody(t, []byte("not an image"), 32, 32)
	req := httptest.NewRequest(http.MethodPost, "/", body)
	req.Header.Set("Content-Type", contentType)
	if err := req.ParseMultipartForm(10 << 20); err != nil {
		t.Fatalf("parse multipart: %v", err)
	}
	_, header, err := req.FormFile("image")
	if err != nil {
		t.Fatalf("form file: %v", err)
	}

	job := pipeline.NewJob("corrupt-1", header.Filename, 32, 32, header)
	_, err = proc.Process(job)
	if err == nil {
		t.Fatal("expected error for corrupt image")
	}
}

func TestRetryExhaustion(t *testing.T) {
	tmpDir := t.TempDir()
	dlqPath := filepath.Join(tmpDir, "dlq.log")
	dlq, err := pipeline.NewDLQ(dlqPath)
	if err != nil {
		t.Fatalf("open DLQ: %v", err)
	}
	defer dlq.Close()

	proc := pipeline.NewProcessor(tmpDir)
	policy := pipeline.NewRetryPolicy(2)

	// Use corrupt data so every attempt fails.
	body, contentType := createMultipartBody(t, []byte("corrupt"), 32, 32)
	req := httptest.NewRequest(http.MethodPost, "/", body)
	req.Header.Set("Content-Type", contentType)
	if err := req.ParseMultipartForm(10 << 20); err != nil {
		t.Fatalf("parse multipart: %v", err)
	}
	_, header, err := req.FormFile("image")
	if err != nil {
		t.Fatalf("form file: %v", err)
	}

	job := pipeline.NewJob("retry-1", header.Filename, 32, 32, header)
	pipeline.ExecuteWithRetry(job, proc, policy, dlq)

	if job.Status() != pipeline.StatusDeadLettered {
		t.Errorf("expected DEAD_LETTERED, got %s", job.Status())
	}

	attempts, _, _ := job.Snapshot()
	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}

	// Verify DLQ file has an entry.
	dlq.Close() // Flush.
	data, err := os.ReadFile(dlqPath)
	if err != nil {
		t.Fatalf("read DLQ: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected DLQ entry, got empty file")
	}

	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(data), &entry); err != nil {
		t.Fatalf("unmarshal DLQ entry: %v", err)
	}
	if entry["job_id"] != "retry-1" {
		t.Errorf("DLQ job_id = %v, want retry-1", entry["job_id"])
	}
}

func TestWorkerPoolSubmitAndDrain(t *testing.T) {
	_, pool, tmpDir := setupPipeline(t, 2, 4, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	imgData := createTestImage(t, 16, 16)
	submitted := 0

	for i := 0; i < 4; i++ {
		body, contentType := createMultipartBody(t, imgData, 8, 8)
		req := httptest.NewRequest(http.MethodPost, "/", body)
		req.Header.Set("Content-Type", contentType)
		if err := req.ParseMultipartForm(10 << 20); err != nil {
			t.Fatalf("parse multipart: %v", err)
		}
		_, header, err := req.FormFile("image")
		if err != nil {
			t.Fatalf("form file: %v", err)
		}

		job := pipeline.NewJob(fmt.Sprintf("pool-%d", i), header.Filename, 8, 8, header)
		if pool.Submit(job) {
			submitted++
		}
	}

	if submitted != 4 {
		t.Errorf("expected 4 submissions, got %d", submitted)
	}

	// Drain: close channel and wait for workers.
	pool.Shutdown()

	// Check that output files were written.
	entries, err := os.ReadDir(filepath.Join(tmpDir, "output"))
	if err != nil {
		t.Fatalf("read output dir: %v", err)
	}
	if len(entries) != 4 {
		t.Errorf("expected 4 output files, got %d", len(entries))
	}
}

func TestBackpressurePoolLevel(t *testing.T) {
	// Test backpressure deterministically at the pool level: don't start
	// any workers so the channel fills up and Submit returns false.
	_, pool, _ := setupPipeline(t, 2, 2, 1)
	// Intentionally NOT calling pool.Start — channel won't drain.

	imgData := createTestImage(t, 8, 8)

	// Submit 2 jobs to fill the queue (capacity 2).
	for i := 0; i < 2; i++ {
		body, contentType := createMultipartBody(t, imgData, 4, 4)
		req := httptest.NewRequest(http.MethodPost, "/", body)
		req.Header.Set("Content-Type", contentType)
		if err := req.ParseMultipartForm(10 << 20); err != nil {
			t.Fatalf("parse multipart: %v", err)
		}
		_, header, err := req.FormFile("image")
		if err != nil {
			t.Fatalf("form file: %v", err)
		}
		job := pipeline.NewJob(fmt.Sprintf("bp-%d", i), header.Filename, 4, 4, header)
		if !pool.Submit(job) {
			t.Fatalf("job %d should have been accepted", i)
		}
	}

	// Third submission must be rejected — channel is full.
	body, contentType := createMultipartBody(t, imgData, 4, 4)
	req := httptest.NewRequest(http.MethodPost, "/", body)
	req.Header.Set("Content-Type", contentType)
	if err := req.ParseMultipartForm(10 << 20); err != nil {
		t.Fatalf("parse multipart: %v", err)
	}
	_, header, err := req.FormFile("image")
	if err != nil {
		t.Fatalf("form file: %v", err)
	}
	overflowJob := pipeline.NewJob("bp-overflow", header.Filename, 4, 4, header)
	if pool.Submit(overflowJob) {
		t.Error("expected Submit to return false (backpressure), but got true")
	}

	// Start workers and drain so TempDir cleanup succeeds.
	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	cancel()
	pool.Shutdown()
}

func TestBackpressureHTTP429(t *testing.T) {
	// Use a small queue and 1 worker. Rapidly submit requests;
	// at least one must get 429 when the queue is full.
	ts, shutdown := setupHTTPServer(t, 1, 1)
	defer shutdown()

	imgData := createTestImage(t, 8, 8)
	const rapidSubmissions = 10
	var got429 bool

	for i := 0; i < rapidSubmissions; i++ {
		body, ct := createMultipartBody(t, imgData, 4, 4)
		resp, err := http.Post(ts.URL+"/upload", ct, body)
		if err != nil {
			t.Fatalf("upload %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			got429 = true
		}
	}

	if !got429 {
		t.Error("expected at least one 429 response under rapid submission to queue of capacity 1")
	}
}

func TestHTTPUploadAccepted(t *testing.T) {
	ts, shutdown := setupHTTPServer(t, 4, 16)
	defer shutdown()

	imgData := createTestImage(t, 32, 32)
	body, contentType := createMultipartBody(t, imgData, 16, 16)

	resp, err := http.Post(ts.URL+"/upload", contentType, body)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 202, got %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result["status"] != "queued" {
		t.Errorf("expected status=queued, got %v", result["status"])
	}
}

func TestHTTPHealthEndpoint(t *testing.T) {
	ts, shutdown := setupHTTPServer(t, 2, 8)
	defer shutdown()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result["status"] != "ok" {
		t.Errorf("expected status=ok, got %v", result["status"])
	}
}

func TestHTTPBadRequest(t *testing.T) {
	ts, shutdown := setupHTTPServer(t, 2, 8)
	defer shutdown()

	t.Run("missing image field", func(t *testing.T) {
		var body bytes.Buffer
		w := multipart.NewWriter(&body)
		w.WriteField("width", "32")
		w.WriteField("height", "32")
		w.Close()

		resp, err := http.Post(ts.URL+"/upload", w.FormDataContentType(), &body)
		if err != nil {
			t.Fatalf("upload: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", resp.StatusCode)
		}
	})

	t.Run("zero dimensions", func(t *testing.T) {
		imgData := createTestImage(t, 8, 8)
		body, ct := createMultipartBody(t, imgData, 0, 0)
		resp, err := http.Post(ts.URL+"/upload", ct, body)
		if err != nil {
			t.Fatalf("upload: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", resp.StatusCode)
		}
	})
}

// ============================================================================
// Concurrency Stress Test (designed for -race)
// ============================================================================

func TestConcurrentSubmissions(t *testing.T) {
	ts, shutdown := setupHTTPServer(t, 8, 128)
	defer shutdown()

	imgData := createTestImage(t, 16, 16)
	const concurrency = 100

	var wg sync.WaitGroup
	var accepted, rejected atomic.Int32

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body, ct := createMultipartBody(t, imgData, 8, 8)
			resp, err := http.Post(ts.URL+"/upload", ct, body)
			if err != nil {
				t.Errorf("[%d] upload error: %v", idx, err)
				return
			}
			resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusAccepted:
				accepted.Add(1)
			case http.StatusTooManyRequests:
				rejected.Add(1)
			default:
				t.Errorf("[%d] unexpected status: %d", idx, resp.StatusCode)
			}
		}(i)
	}

	wg.Wait()
	t.Logf("concurrent submissions: accepted=%d rejected=%d total=%d",
		accepted.Load(), rejected.Load(), concurrency)

	if accepted.Load() == 0 {
		t.Error("expected at least some accepted submissions")
	}
}

func TestGracefulShutdown(t *testing.T) {
	_, pool, _ := setupPipeline(t, 2, 4, 1)
	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	// Submit a job.
	imgData := createTestImage(t, 16, 16)
	body, contentType := createMultipartBody(t, imgData, 8, 8)
	req := httptest.NewRequest(http.MethodPost, "/", body)
	req.Header.Set("Content-Type", contentType)
	if err := req.ParseMultipartForm(10 << 20); err != nil {
		t.Fatalf("parse multipart: %v", err)
	}
	_, header, err := req.FormFile("image")
	if err != nil {
		t.Fatalf("form file: %v", err)
	}
	job := pipeline.NewJob("shutdown-1", header.Filename, 8, 8, header)
	pool.Submit(job)

	// Cancel context and shut down.
	cancel()

	done := make(chan struct{})
	go func() {
		pool.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		// Shutdown completed.
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not complete within 5 seconds")
	}
}

// ============================================================================
// Benchmarks
// ============================================================================

// BenchmarkWorkerPoolThroughput measures job processing throughput across
// varying pool sizes. Each sub-benchmark processes a batch of 100 small
// images through the full pipeline (decode → resize → encode → disk write).
func BenchmarkWorkerPoolThroughput(b *testing.B) {
	poolSizes := []int{1, 4, 8, 16, 32}

	for _, n := range poolSizes {
		b.Run(fmt.Sprintf("workers=%d", n), func(b *testing.B) {
			tmpDir := b.TempDir()
			outputDir := filepath.Join(tmpDir, "output")
			dlqPath := filepath.Join(tmpDir, "dlq.log")

			cfg := pipeline.Config{
				WorkerCount:     n,
				QueueCapacity:   256,
				OutputDir:       outputDir,
				MaxRetries:      1,
				ShutdownTimeout: 10 * time.Second,
				DLQPath:         dlqPath,
				ServerAddr:      ":0",
				MaxUploadSize:   32 << 20,
			}
			if err := cfg.EnsureOutputDir(); err != nil {
				b.Fatalf("ensure output dir: %v", err)
			}

			proc := pipeline.NewProcessor(outputDir)
			policy := pipeline.NewRetryPolicy(1)
			dlq, err := pipeline.NewDLQ(dlqPath)
			if err != nil {
				b.Fatalf("open DLQ: %v", err)
			}
			defer dlq.Close()

			// Pre-generate test image data (32x32 PNG is ~200 bytes).
			imgData := createTestImage(b, 32, 32)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				pool := pipeline.NewWorkerPool(cfg, proc, policy, dlq, nil)
				ctx, cancel := context.WithCancel(context.Background())
				pool.Start(ctx)

				const batchSize = 100
				for j := 0; j < batchSize; j++ {
					body, contentType := createMultipartBody(b, imgData, 16, 16)
					req := httptest.NewRequest(http.MethodPost, "/", body)
					req.Header.Set("Content-Type", contentType)
					if err := req.ParseMultipartForm(10 << 20); err != nil {
						b.Fatalf("parse multipart: %v", err)
					}
					_, header, err := req.FormFile("image")
					if err != nil {
						b.Fatalf("form file: %v", err)
					}

					job := pipeline.NewJob(fmt.Sprintf("bench-%d-%d", i, j), header.Filename, 16, 16, header)
					pool.Submit(job)
				}

				cancel()
				pool.Shutdown()
			}
		})
	}
}
