package api

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
)

const tracesPath = apiBasePath + "/traces"

func (c *Client) SearchTraces(ctx context.Context, params url.Values, out any) error {
	return c.doSourceJSON(ctx, http.MethodGet, tracesPath, params, out)
}

func (c *Client) GetTrace(ctx context.Context, reference string, out any) error {
	return c.doSourceJSON(ctx, http.MethodGet, tracePath(reference), nil, out)
}

func (c *Client) GetTraceEntries(ctx context.Context, reference string, params url.Values, out any) error {
	return c.doSourceJSON(ctx, http.MethodGet, tracePath(reference)+"/entries", params, out)
}

func (c *Client) GetTraceAttachments(ctx context.Context, reference string, params url.Values, out any) error {
	return c.doSourceJSON(ctx, http.MethodGet, tracePath(reference)+"/attachments", params, out)
}

// DownloadTraceAttachment opens a binary response. The caller must close it.
func (c *Client) DownloadTraceAttachment(ctx context.Context, reference, attachmentID string) (io.ReadCloser, error) {
	path := tracePath(reference) + "/attachments/" + url.PathEscape(attachmentID)
	if err := c.requireSourceID(path); err != nil {
		return nil, err
	}
	req, err := c.newRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/octet-stream")

	resp, err := c.openResponse(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, &Error{StatusCode: resp.StatusCode}
	}
	contentType := resp.Header.Get("Content-Type")
	if mediaType, _, _ := mime.ParseMediaType(contentType); mediaType != "application/octet-stream" {
		resp.Body.Close()
		return nil, fmt.Errorf("expected a binary attachment, got Content-Type %q", contentType)
	}
	return resp.Body, nil
}

func tracePath(reference string) string {
	return tracesPath + "/" + url.PathEscape(reference)
}
