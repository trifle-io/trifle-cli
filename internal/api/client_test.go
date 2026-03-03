package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBootstrapCreateDatabaseMultipart(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	sqlitePath := filepath.Join(tempDir, "metrics.sqlite")
	if err := os.WriteFile(sqlitePath, []byte("sqlite"), 0o600); err != nil {
		t.Fatalf("write sqlite file: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != "/api/v1/bootstrap/databases" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data;") {
			t.Fatalf("content-type = %s", r.Header.Get("Content-Type"))
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer user-token" {
			t.Fatalf("authorization = %s", auth)
		}

		if err := r.ParseMultipartForm(10 << 20); err != nil {
			t.Fatalf("parse multipart: %v", err)
		}

		if got := r.FormValue("display_name"); got != "SQLite Upload" {
			t.Fatalf("display_name = %q", got)
		}
		if got := r.FormValue("driver"); got != "sqlite" {
			t.Fatalf("driver = %q", got)
		}

		file, header, err := r.FormFile("sqlite_file")
		if err != nil {
			t.Fatalf("form file: %v", err)
		}
		defer file.Close()

		if header.Filename != "metrics.sqlite" {
			t.Fatalf("filename = %q", header.Filename)
		}

		body, err := io.ReadAll(file)
		if err != nil {
			t.Fatalf("read uploaded file: %v", err)
		}
		if string(body) != "sqlite" {
			t.Fatalf("uploaded body = %q", string(body))
		}

		response := map[string]any{
			"data": map[string]any{
				"source": map[string]any{
					"id":   "db_123",
					"type": "database",
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client, err := New(server.URL, "user-token", 5*time.Second)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var response map[string]any
	err = client.BootstrapCreateDatabaseMultipart(
		context.Background(),
		map[string]string{
			"display_name": "SQLite Upload",
			"driver":       "sqlite",
		},
		"sqlite_file",
		sqlitePath,
		&response,
	)
	if err != nil {
		t.Fatalf("BootstrapCreateDatabaseMultipart error: %v", err)
	}

	data, ok := response["data"].(map[string]any)
	if !ok {
		t.Fatalf("response missing data: %#v", response)
	}
	source, ok := data["source"].(map[string]any)
	if !ok || source["id"] != "db_123" {
		t.Fatalf("response source mismatch: %#v", response)
	}
}

func TestBootstrapCreateDatabaseMultipartRequiresFilePath(t *testing.T) {
	t.Parallel()

	client, err := New("https://app.trifle.io", "user-token", 5*time.Second)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	err = client.BootstrapCreateDatabaseMultipart(
		context.Background(),
		map[string]string{"driver": "sqlite"},
		"sqlite_file",
		"",
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "missing sqlite file path") {
		t.Fatalf("expected missing sqlite file path error, got %v", err)
	}
}
