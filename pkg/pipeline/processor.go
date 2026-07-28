package pipeline

import (
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nfnt/resize"
)

// Processor handles the decode → resize → encode pipeline for a single job.
// It is stateless and safe for concurrent use by multiple workers.
type Processor struct {
	outputDir string
}

// NewProcessor creates a Processor that writes output files to outputDir.
func NewProcessor(outputDir string) *Processor {
	return &Processor{outputDir: outputDir}
}

// Process decodes the uploaded image, resizes it to the target dimensions,
// encodes the result, and writes it to disk.
//
// It returns a JobResult on success or an error describing the failure point.
// The caller is responsible for closing the multipart file if needed.
func (p *Processor) Process(job *Job) (JobResult, error) {
	start := time.Now()

	// Open the uploaded file from the multipart header.
	src, err := job.FileHeader.Open()
	if err != nil {
		return JobResult{}, fmt.Errorf("open uploaded file %q: %w", job.FileName, err)
	}
	defer src.Close()

	// Decode the image and detect its format.
	img, format, err := image.Decode(src)
	if err != nil {
		return JobResult{}, fmt.Errorf("decode image %q: %w", job.FileName, err)
	}

	// Determine original file size for the result metadata.
	originalSize, err := fileSize(src)
	if err != nil {
		return JobResult{}, fmt.Errorf("determine original size %q: %w", job.FileName, err)
	}

	// Resize using Lanczos3 interpolation (high-quality downsampling).
	resized := resize.Resize(job.TargetWidth, job.TargetHeight, img, resize.Lanczos3)

	// Build the output path: <outputDir>/<jobID>_<width>x<height>.<ext>
	ext := formatExtension(format)
	outName := fmt.Sprintf("%s_%dx%d%s", job.ID, job.TargetWidth, job.TargetHeight, ext)
	outPath := filepath.Join(p.outputDir, outName)

	// Write the resized image to disk.
	resizedSize, err := p.writeImage(outPath, resized, format)
	if err != nil {
		return JobResult{}, fmt.Errorf("write resized image: %w", err)
	}

	return JobResult{
		OutputPath:   outPath,
		Duration:     time.Since(start),
		OriginalSize: originalSize,
		ResizedSize:  resizedSize,
	}, nil
}

// writeImage encodes the image to disk in the original format.
// Returns the number of bytes written.
func (p *Processor) writeImage(path string, img image.Image, format string) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, fmt.Errorf("create output file %q: %w", path, err)
	}
	// Use a deferred close with error capture to avoid silent write failures
	// (e.g., filesystem full detected only at Close).
	var closeErr error
	defer func() {
		if cerr := f.Close(); cerr != nil && closeErr == nil {
			closeErr = cerr
		}
	}()

	switch strings.ToLower(format) {
	case "jpeg", "jpg":
		err = jpeg.Encode(f, img, &jpeg.Options{Quality: 85})
	case "png":
		err = png.Encode(f, img)
	default:
		// Fall back to PNG for unknown/unsupported formats (gif, webp decoded
		// by registered decoders produce valid image.Image values).
		err = png.Encode(f, img)
	}

	if err != nil {
		// Attempt cleanup of partial file on encode failure.
		_ = os.Remove(path)
		return 0, fmt.Errorf("encode %s to %q: %w", format, path, err)
	}

	// Sync to ensure bytes are flushed to stable storage before reporting success.
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("sync %q: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat output %q: %w", path, err)
	}

	if closeErr != nil {
		return 0, fmt.Errorf("close output %q: %w", path, closeErr)
	}

	return info.Size(), nil
}

// fileSize returns the size in bytes of the underlying file behind a
// multipart.File. It seeks to the end, records the position, and seeks
// back to the start so the reader can be reused.
func fileSize(r io.ReadSeeker) (int64, error) {
	// Seek to end to determine total bytes.
	size, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	// Reset to start — the caller may still need the data.
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	return size, nil
}

// formatExtension maps a decoded image format string to its canonical
// file extension (including the leading dot).
func formatExtension(format string) string {
	switch strings.ToLower(format) {
	case "jpeg", "jpg":
		return ".jpg"
	case "png":
		return ".png"
	case "gif":
		return ".gif"
	default:
		return ".png"
	}
}
