package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNewClientRequiresSourceID(t *testing.T) {
	t.Parallel()

	_, err := newClient(&commonOptions{
		BaseURL: "https://app.trifle.io",
		Token:   "token",
		Timeout: time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "missing source ID") {
		t.Fatalf("expected missing source ID error, got %v", err)
	}
}

func TestNewClientSendsSourceIDHeader(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer global-token" {
			t.Errorf("authorization = %q", got)
		}
		if got := r.Header.Get("X-Trifle-Source-Id"); got != "source-123" {
			t.Errorf("source header = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{}}`))
	}))
	defer server.Close()

	client, err := newClient(&commonOptions{
		BaseURL:  server.URL,
		Token:    "global-token",
		SourceID: "  source-123  ",
		Timeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var response map[string]any
	if err := client.GetSource(context.Background(), &response); err != nil {
		t.Fatalf("get source: %v", err)
	}
}
