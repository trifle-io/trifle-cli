package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultTimeout = 30 * time.Second
	apiBasePath    = "/api/v1"
	cliUserAgent   = "trifle-cli"
)

type Client struct {
	baseURL  string
	token    string
	sourceID string
	host     string
	http     *http.Client
}

type Error struct {
	StatusCode int
	Body       string
}

func (e *Error) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("api request failed with status %d", e.StatusCode)
	}

	return fmt.Sprintf("api request failed with status %d: %s", e.StatusCode, e.Body)
}

func New(baseURL, token string, timeout time.Duration) (*Client, error) {
	normalized := normalizeBaseURL(baseURL)
	if normalized == "" {
		return nil, fmt.Errorf("missing base URL")
	}

	if _, err := url.ParseRequestURI(normalized); err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}

	if timeout <= 0 {
		timeout = defaultTimeout
	}

	return &Client{
		baseURL: normalized,
		token:   token,
		host:    clientHost(),
		http: &http.Client{
			Timeout: timeout,
		},
	}, nil
}

func (c *Client) SetToken(token string) {
	c.token = token
}

func (c *Client) SetSourceID(sourceID string) {
	c.sourceID = strings.TrimSpace(sourceID)
}

func (c *Client) GetMetrics(ctx context.Context, params map[string]string, out any) error {
	return c.doSourceJSON(ctx, http.MethodGet, apiBasePath+"/metrics", params, out)
}

func (c *Client) PostMetrics(ctx context.Context, payload any, out any) error {
	return c.doSourceJSON(ctx, http.MethodPost, apiBasePath+"/metrics", payload, out)
}

func (c *Client) QueryMetrics(ctx context.Context, payload any, out any) error {
	return c.doSourceJSON(ctx, http.MethodPost, apiBasePath+"/metrics/query", payload, out)
}

func (c *Client) GetSource(ctx context.Context, out any) error {
	return c.doSourceJSON(ctx, http.MethodGet, apiBasePath+"/source", nil, out)
}

func (c *Client) GetTransponders(ctx context.Context, out any) error {
	return c.doSourceJSON(ctx, http.MethodGet, apiBasePath+"/transponders", nil, out)
}

func (c *Client) CreateTransponder(ctx context.Context, payload any, out any) error {
	return c.doSourceJSON(ctx, http.MethodPost, apiBasePath+"/transponders", payload, out)
}

func (c *Client) UpdateTransponder(ctx context.Context, id string, payload any, out any) error {
	return c.doSourceJSON(ctx, http.MethodPut, apiBasePath+"/transponders/"+id, payload, out)
}

func (c *Client) DeleteTransponder(ctx context.Context, id string, out any) error {
	return c.doSourceJSON(ctx, http.MethodDelete, apiBasePath+"/transponders/"+id, nil, out)
}

func (c *Client) BootstrapSignup(ctx context.Context, payload any, out any) error {
	return c.doJSON(ctx, http.MethodPost, apiBasePath+"/bootstrap/signup", payload, out)
}

func (c *Client) BootstrapLogin(ctx context.Context, payload any, out any) error {
	return c.doJSON(ctx, http.MethodPost, apiBasePath+"/bootstrap/login", payload, out)
}

func (c *Client) BootstrapMe(ctx context.Context, out any) error {
	return c.doJSON(ctx, http.MethodGet, apiBasePath+"/bootstrap/me", nil, out)
}

func (c *Client) BootstrapCreateOrganization(ctx context.Context, payload any, out any) error {
	return c.doJSON(ctx, http.MethodPost, apiBasePath+"/bootstrap/organizations", payload, out)
}

func (c *Client) BootstrapListSources(ctx context.Context, out any) error {
	return c.doJSON(ctx, http.MethodGet, apiBasePath+"/bootstrap/sources", nil, out)
}

func (c *Client) BootstrapCreateDatabase(ctx context.Context, payload any, out any) error {
	return c.doJSON(ctx, http.MethodPost, apiBasePath+"/bootstrap/databases", payload, out)
}

func (c *Client) BootstrapCreateDatabaseMultipart(
	ctx context.Context,
	fields map[string]string,
	fileField string,
	filePath string,
	out any,
) error {
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return fmt.Errorf("missing sqlite file path")
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	for key, value := range fields {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			continue
		}
		if err := writer.WriteField(key, value); err != nil {
			return fmt.Errorf("encode field %s: %w", key, err)
		}
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open sqlite file: %w", err)
	}
	defer file.Close()

	part, err := writer.CreateFormFile(fileField, filepath.Base(filePath))
	if err != nil {
		return fmt.Errorf("create sqlite file field: %w", err)
	}

	if _, err := io.Copy(part, file); err != nil {
		return fmt.Errorf("copy sqlite file: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("finalize multipart payload: %w", err)
	}

	req, err := c.newRequest(
		ctx,
		http.MethodPost,
		apiBasePath+"/bootstrap/databases",
		&body,
	)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())

	return c.doRequest(req, out)
}

func (c *Client) BootstrapSetupDatabase(ctx context.Context, id string, out any) error {
	return c.doJSON(ctx, http.MethodPost, apiBasePath+"/bootstrap/databases/"+id+"/setup", map[string]any{}, out)
}

func (c *Client) BootstrapCreateProject(ctx context.Context, payload any, out any) error {
	return c.doJSON(ctx, http.MethodPost, apiBasePath+"/bootstrap/projects", payload, out)
}

func (c *Client) BootstrapListTokens(ctx context.Context, out any) error {
	return c.doJSON(ctx, http.MethodGet, apiBasePath+"/bootstrap/tokens", nil, out)
}

func (c *Client) BootstrapCreateToken(ctx context.Context, payload any, out any) error {
	return c.doJSON(ctx, http.MethodPost, apiBasePath+"/bootstrap/tokens", payload, out)
}

func (c *Client) BootstrapUpdateToken(ctx context.Context, id string, payload any, out any) error {
	return c.doJSON(ctx, http.MethodPut, apiBasePath+"/bootstrap/tokens/"+id, payload, out)
}

func (c *Client) BootstrapDeleteToken(ctx context.Context, id string, out any) error {
	return c.doJSON(ctx, http.MethodDelete, apiBasePath+"/bootstrap/tokens/"+id, nil, out)
}

func (c *Client) doSourceJSON(ctx context.Context, method, path string, payload any, out any) error {
	if err := c.requireSourceID(path); err != nil {
		return err
	}
	return c.doJSON(ctx, method, path, payload, out)
}

func (c *Client) requireSourceID(path string) error {
	if c.sourceID == "" {
		return fmt.Errorf("source ID is required for %s", path)
	}
	return nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, payload any, out any) error {
	var body io.Reader
	if method == http.MethodGet {
		var query url.Values
		switch params := payload.(type) {
		case url.Values:
			query = params
		case map[string]string:
			query = make(url.Values, len(params))
			for key, value := range params {
				if value != "" {
					query.Set(key, value)
				}
			}
		}
		if encoded := query.Encode(); encoded != "" {
			path += "?" + encoded
		}
	} else if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("encode payload: %w", err)
		}
		body = bytes.NewReader(encoded)
	}

	req, err := c.newRequest(ctx, method, path, body)
	if err != nil {
		return err
	}
	if method != http.MethodGet {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.doRequest(req, out)
}

func (c *Client) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("User-Agent", cliUserAgent)
	if c.host != "" {
		req.Header.Set("X-Trifle-Client-Host", c.host)
	}
	if c.sourceID != "" {
		req.Header.Set("X-Trifle-Source-Id", c.sourceID)
	}
	return req, nil
}

func (c *Client) doRequest(req *http.Request, out any) error {
	resp, err := c.openResponse(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if out == nil {
		return nil
	}

	if len(responseBody) == 0 {
		return nil
	}

	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	decoder.UseNumber()
	if err := decoder.Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}

func (c *Client) openResponse(req *http.Request) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	if resp.StatusCode < 400 {
		return resp, nil
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	return nil, &Error{StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(body))}
}

func normalizeBaseURL(baseURL string) string {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return ""
	}

	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}

	return strings.TrimRight(baseURL, "/")
}

func clientHost() string {
	host, err := os.Hostname()
	if err != nil {
		return ""
	}

	return strings.TrimSpace(host)
}
