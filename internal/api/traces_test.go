package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestSearchTracesSendsExactRepeatedTags(t *testing.T) {
	tags := []string{"customer:42", "queue,default"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/traces" {
			t.Errorf("requested %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query()["tags[]"]; !slices.Equal(got, tags) {
			t.Errorf("tags = %q, want %q", got, tags)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"data":[],"pagination":{"next_cursor":"page+2/="}}`)
	}))
	defer server.Close()

	client, err := New(server.URL, "token", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.SetSourceID("source-123")
	var page struct {
		Pagination struct {
			NextCursor string `json:"next_cursor"`
		} `json:"pagination"`
	}
	if err := client.SearchTraces(context.Background(), url.Values{"tags[]": tags}, &page); err != nil {
		t.Fatal(err)
	}
	if got := page.Pagination.NextCursor; got != "page+2/=" {
		t.Fatalf("next cursor = %q", got)
	}
}

func TestDownloadTraceAttachmentUsesBinaryResponse(t *testing.T) {
	const reference = "job/42?attempt=#2%"
	const attachmentID = "attachment_1"
	const binary = "\x00\xff\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wantPath := "/api/v1/traces/job%2F42%3Fattempt=%232%25/attachments/"
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.EscapedPath(), wantPath) || r.URL.RawQuery != "" {
			t.Errorf("requested %s %s", r.Method, r.URL)
		}
		for key, want := range map[string]string{
			"Accept":             "application/octet-stream",
			"Authorization":      "Bearer token",
			"X-Trifle-Source-Id": "source-123",
		} {
			if got := r.Header.Get(key); got != want {
				t.Errorf("%s = %q, want %q", key, got, want)
			}
		}
		if r.URL.EscapedPath() != wantPath+attachmentID {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"errors":{"detail":"Attachment not found"}}`)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		io.WriteString(w, binary)
	}))
	defer server.Close()

	client, err := New(server.URL, "token", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.SetSourceID("source-123")
	body, err := client.DownloadTraceAttachment(context.Background(), reference, attachmentID)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil || string(data) != binary {
		t.Fatalf("attachment = %q, error = %v", data, err)
	}

	_, err = client.DownloadTraceAttachment(context.Background(), reference, "missing")
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound || !strings.Contains(apiErr.Body, "Attachment not found") {
		t.Fatalf("missing attachment: %v", err)
	}
}
