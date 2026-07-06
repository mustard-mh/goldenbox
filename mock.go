package goldenbox

// The mock is SHARED by every parallel case, so Calls/HitCount are process-wide
// tallies and a SetFault hits every case's traffic unless narrowed with
// Fault.Match on a case-owned marker.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"
)

// Mock is a started mock upstream server.
type Mock struct {
	srv     *http.Server
	ln      net.Listener
	mux     *http.ServeMux
	baseURL string

	mu        sync.Mutex
	calls     map[string][]MockCall // served requests per URL path, in arrival order
	faults    map[string]*Fault     // active fault per URL path
	unmatched http.Handler          // nil = the mux's default 404
}

// MockCall is one request the mock served, captured for verification.
type MockCall struct {
	Method string
	Path   string
	Query  url.Values
	Header http.Header
	Body   []byte
}

// Fault is an injected failure for requests to one URL path.
type Fault struct {
	// Delay sleeps before the route runs. Alone (no StatusCode/DropConn) it is
	// pure latency injection: the real route still runs afterwards.
	Delay time.Duration

	// StatusCode, when non-zero, short-circuits the route with this code and
	// Body verbatim.
	StatusCode int
	Body       string

	// DropConn aborts the connection with no response — a network-level
	// failure, not an HTTP error.
	DropConn bool

	// Times limits the fault to the first N matching requests, then clears it.
	// 0 = active until ClearFault.
	Times int

	// Match narrows the fault (nil = every request to the path). Match on a
	// case-owned marker so parallel cases stay healthy.
	Match func(*http.Request) bool
}

// StartMock starts the mock server on a random local port.
func StartMock() (*Mock, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("mock listen: %w", err)
	}

	m := &Mock{
		ln:      ln,
		baseURL: "http://" + ln.Addr().String(),
		mux:     http.NewServeMux(),
		calls:   map[string][]MockCall{},
		faults:  map[string]*Fault{},
	}

	m.srv = &http.Server{Handler: m.intercept()}
	go func() { _ = m.srv.Serve(ln) }()

	return m, nil
}

// intercept wraps the mux: capture each request, apply any active fault, and
// route unmatched requests to the configured unmatched handler.
func (m *Mock) intercept() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(body))

		m.mu.Lock()
		m.calls[r.URL.Path] = append(m.calls[r.URL.Path], MockCall{
			Method: r.Method,
			Path:   r.URL.Path,
			Query:  r.URL.Query(),
			Header: r.Header.Clone(),
			Body:   body,
		})
		var fault *Fault
		if f := m.faults[r.URL.Path]; f != nil && (f.Match == nil || f.Match(r)) {
			cp := *f
			fault = &cp
			if f.Times > 0 {
				f.Times--
				if f.Times == 0 {
					delete(m.faults, r.URL.Path)
				}
			}
		}
		unmatched := m.unmatched
		m.mu.Unlock()

		if fault != nil {
			if fault.Delay > 0 {
				time.Sleep(fault.Delay)
			}
			if fault.DropConn {
				panic(http.ErrAbortHandler)
			}
			if fault.StatusCode != 0 {
				w.WriteHeader(fault.StatusCode)
				_, _ = io.WriteString(w, fault.Body)
				return
			}
		}

		if unmatched != nil {
			if _, pattern := m.mux.Handler(r); pattern == "" {
				unmatched.ServeHTTP(w, r)
				return
			}
		}
		m.mux.ServeHTTP(w, r)
	})
}

// Calls returns a copy of the requests served for the given path, in arrival
// order. Process-wide: filter on a case-owned marker under t.Parallel().
func (m *Mock) Calls(path string) []MockCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]MockCall, len(m.calls[path]))
	copy(out, m.calls[path])
	return out
}

// HitCount returns how many requests the mock has served for the given URL path.
func (m *Mock) HitCount(path string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.calls[path]))
}

// ResetCalls clears every recorded call (faults stay).
func (m *Mock) ResetCalls() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = map[string][]MockCall{}
}

// SetFault injects f for the given path (matched exactly against r.URL.Path),
// replacing any previous fault there.
func (m *Mock) SetFault(path string, f Fault) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.faults[path] = &f
}

// ClearFault removes the fault on the given URL path, if any.
func (m *Mock) ClearFault(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.faults, path)
}

// FailUnmatched makes requests matching no route fail with HTTP 502 instead of
// the mux's silent 404, so a missing upstream seam surfaces immediately.
func (m *Mock) FailUnmatched() {
	m.setUnmatched(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("goldenbox mock: no route registered for %s %s", r.Method, r.URL.Path),
		})
	}))
}

// PassthroughUnmatched forwards unmatched requests to the real upstream at
// base. Use sparingly: passthrough reintroduces the network, breaking
// hermeticity and determinism.
func (m *Mock) PassthroughUnmatched(base string) error {
	u, err := url.Parse(base)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("mock passthrough: invalid base URL %q", base)
	}
	m.setUnmatched(httputil.NewSingleHostReverseProxy(u))
	return nil
}

func (m *Mock) setUnmatched(h http.Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unmatched = h
}

func (m *Mock) BaseURL() string { return m.baseURL }

func (m *Mock) Close() {
	_ = m.srv.Close()
	_ = m.ln.Close()
}

// HandleFunc registers an upstream mock route. Register all routes before the
// target under test starts hitting the mock.
func (m *Mock) HandleFunc(pattern string, handler http.HandlerFunc) {
	m.mux.HandleFunc(pattern, handler)
}

// WriteJSON writes body as JSON with HTTP 200.
func WriteJSON(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(body)
}
