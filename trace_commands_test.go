package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
)

func TestWriteTraceAttachmentCanRetryAfterInterruptedDownload(t *testing.T) {
	destination := filepath.Join(t.TempDir(), "attachment")
	partial := io.MultiReader(strings.NewReader("partial"), iotest.ErrReader(io.ErrUnexpectedEOF))
	if err := writeTraceAttachment(destination, partial); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("interrupted download: %v", err)
	}
	if err := writeTraceAttachment(destination, strings.NewReader("complete")); err != nil {
		t.Fatalf("retry download: %v", err)
	}
	if err := writeTraceAttachment(destination, strings.NewReader("replacement")); !errors.Is(err, os.ErrExist) {
		t.Fatalf("download to an existing path: %v", err)
	}
	data, err := os.ReadFile(destination)
	if err != nil || string(data) != "complete" {
		t.Fatalf("saved file = %q, error = %v", data, err)
	}
}
