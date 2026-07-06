package goldenbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client black-box drives the target service, recording each response as
// {HTTPCode, Headers, Body}.
type Client interface {
	// Get issues a GET to path (path may include its own query string).
	Get(ctx context.Context, path string, opts ...RequestOption) (Response, error)
	// Post sends body as a JSON request to path (body nil = no request body).
	Post(ctx context.Context, path string, body any, opts ...RequestOption) (Response, error)
	// Send is the low-level call for any method; body nil sends no request body.
	Send(ctx context.Context, method, path string, body any, opts ...RequestOption) (Response, error)
}

// RequestOption adjusts the request before it is sent (e.g. attaching auth headers / cookies).
type RequestOption func(*http.Request)

// WithCookie appends a cookie (e.g. an auth/session cookie).
func WithCookie(name, value string) RequestOption {
	return func(r *http.Request) {
		r.AddCookie(&http.Cookie{Name: name, Value: value})
	}
}

// WithHeader sets a request header. Per-request options are applied after the
// client's defaults, so they can override them.
func WithHeader(key, value string) RequestOption {
	return func(r *http.Request) {
		r.Header.Set(key, value)
	}
}

type httpClient struct {
	baseURL  string
	defaults []RequestOption
	hc       *http.Client
}

var _ Client = (*httpClient)(nil)

// NewHTTPClient creates a client targeting baseURL (no redirect follow, 30s
// timeout). defaults apply to every request before per-call options.
func NewHTTPClient(baseURL string, defaults ...RequestOption) Client {
	return &httpClient{baseURL: baseURL, defaults: defaults, hc: &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
}

func (c *httpClient) Get(ctx context.Context, path string, opts ...RequestOption) (Response, error) {
	return c.Send(ctx, http.MethodGet, path, nil, opts...)
}

func (c *httpClient) Post(ctx context.Context, path string, body any, opts ...RequestOption) (Response, error) {
	return c.Send(ctx, http.MethodPost, path, body, opts...)
}

func (c *httpClient) Send(ctx context.Context, method, path string, body any, opts ...RequestOption) (Response, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return Response{}, fmt.Errorf("marshal body: %w", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return Response{}, fmt.Errorf("new request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for _, opt := range c.defaults {
		opt(req)
	}
	for _, opt := range opts {
		opt(req)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return Response{}, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return Response{}, fmt.Errorf("read response: %w", err)
	}
	return Response{
		HTTPCode: resp.StatusCode,
		Headers:  captureHeaders(resp.Header),
		Body:     decodeBody(raw),
	}, nil
}

// Response is what the target returned. The body is JSON-parsed into a
// map/slice/scalar or kept as a raw string; header values are normalized by the
// same scrubbers as the body (keyed by header name).
type Response struct {
	HTTPCode int               `yaml:"httpcode"`
	Headers  map[string]string `yaml:"headers,omitempty"`
	Body     any               `yaml:"body,omitempty"`
}

// hopByHopHeaders are the RFC 7230 §6.1 connection-management headers: dropped
// from the recorded Response since they govern a hop, not the response meaning.
var hopByHopHeaders = map[string]bool{
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

// captureHeaders flattens end-to-end headers (multi-values joined), dropping
// hop-by-hop ones; nil when empty.
func captureHeaders(h http.Header) map[string]string {
	out := map[string]string{}
	for k, v := range h {
		if hopByHopHeaders[http.CanonicalHeaderKey(k)] {
			continue
		}
		out[k] = strings.Join(v, ", ")
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// decodeBody parses a JSON body into a map/slice/scalar, or returns the raw
// string when the body is not JSON. An empty body decodes to nil.
func decodeBody(raw []byte) any {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err == nil {
		return v
	}
	return string(raw)
}
