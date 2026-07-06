package goldenbox

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func startTestMock(t *testing.T) *Mock {
	t.Helper()
	m, err := StartMock()
	if err != nil {
		t.Fatalf("StartMock: %v", err)
	}
	t.Cleanup(m.Close)
	return m
}

func get(t *testing.T, url string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, string(b)
}

func TestMockCallCapture(t *testing.T) {
	m := startTestMock(t)
	m.HandleFunc("/api/echo", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		WriteJSON(w, map[string]string{"got": string(body)})
	})

	resp, err := http.Post(m.BaseURL()+"/api/echo?uid=42", "application/json", strings.NewReader(`{"k":"v"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), `{\"k\":\"v\"}`) && !strings.Contains(string(body), `{"k":"v"}`) {
		t.Fatalf("handler did not see restored body: %s", body)
	}

	calls := m.Calls("/api/echo")
	if len(calls) != 1 {
		t.Fatalf("Calls = %d, want 1", len(calls))
	}
	c := calls[0]
	if c.Method != http.MethodPost || c.Path != "/api/echo" {
		t.Errorf("call = %s %s", c.Method, c.Path)
	}
	if c.Query.Get("uid") != "42" {
		t.Errorf("query uid = %q, want 42", c.Query.Get("uid"))
	}
	if string(c.Body) != `{"k":"v"}` {
		t.Errorf("body = %q", c.Body)
	}
	if got := m.HitCount("/api/echo"); got != 1 {
		t.Errorf("HitCount = %d, want 1", got)
	}

	m.ResetCalls()
	if got := m.HitCount("/api/echo"); got != 0 {
		t.Errorf("HitCount after ResetCalls = %d, want 0", got)
	}
}

func TestMockFaultStatusAndTimes(t *testing.T) {
	m := startTestMock(t)
	m.HandleFunc("/api/ok", func(w http.ResponseWriter, r *http.Request) { WriteJSON(w, "ok") })

	m.SetFault("/api/ok", Fault{StatusCode: http.StatusInternalServerError, Body: "boom", Times: 1})

	resp, body := get(t, m.BaseURL()+"/api/ok")
	if resp.StatusCode != http.StatusInternalServerError || body != "boom" {
		t.Fatalf("faulted call = %d %q, want 500 boom", resp.StatusCode, body)
	}
	resp, _ = get(t, m.BaseURL()+"/api/ok")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("call after Times exhausted = %d, want 200", resp.StatusCode)
	}
}

func TestMockFaultMatchAndClear(t *testing.T) {
	m := startTestMock(t)
	m.HandleFunc("/api/user", func(w http.ResponseWriter, r *http.Request) { WriteJSON(w, "ok") })

	m.SetFault("/api/user", Fault{
		StatusCode: http.StatusTooManyRequests,
		Match:      func(r *http.Request) bool { return r.URL.Query().Get("uid") == "victim" },
	})

	resp, _ := get(t, m.BaseURL()+"/api/user?uid=healthy")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("non-matching request = %d, want 200", resp.StatusCode)
	}
	resp, _ = get(t, m.BaseURL()+"/api/user?uid=victim")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("matching request = %d, want 429", resp.StatusCode)
	}

	m.ClearFault("/api/user")
	resp, _ = get(t, m.BaseURL()+"/api/user?uid=victim")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("after ClearFault = %d, want 200", resp.StatusCode)
	}
}

func TestMockFaultDelay(t *testing.T) {
	m := startTestMock(t)
	m.HandleFunc("/api/slow", func(w http.ResponseWriter, r *http.Request) { WriteJSON(w, "ok") })

	m.SetFault("/api/slow", Fault{Delay: 80 * time.Millisecond})
	start := time.Now()
	resp, _ := get(t, m.BaseURL()+"/api/slow")
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("elapsed = %s, want >= 80ms", elapsed)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delay-only fault must still run the route, got %d", resp.StatusCode)
	}
}

func TestMockFaultDropConn(t *testing.T) {
	m := startTestMock(t)
	m.HandleFunc("/api/drop", func(w http.ResponseWriter, r *http.Request) { WriteJSON(w, "ok") })

	m.SetFault("/api/drop", Fault{DropConn: true})
	if _, err := http.Get(m.BaseURL() + "/api/drop"); err == nil {
		t.Fatal("DropConn: want a transport error, got a response")
	}
}

func TestMockUnmatchedDefault404(t *testing.T) {
	m := startTestMock(t)
	resp, _ := get(t, m.BaseURL()+"/no/such/route")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("default unmatched = %d, want 404", resp.StatusCode)
	}
}

func TestMockFailUnmatched(t *testing.T) {
	m := startTestMock(t)
	m.HandleFunc("/api/known", func(w http.ResponseWriter, r *http.Request) { WriteJSON(w, "ok") })
	m.FailUnmatched()

	resp, body := get(t, m.BaseURL()+"/no/such/route")
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("unmatched = %d, want 502", resp.StatusCode)
	}
	if !strings.Contains(body, "GET /no/such/route") {
		t.Fatalf("error must name the request, got %q", body)
	}
	resp, _ = get(t, m.BaseURL()+"/api/known")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("registered route = %d, want 200", resp.StatusCode)
	}
}

func TestMockPassthroughUnmatched(t *testing.T) {
	real := startTestMock(t)
	real.HandleFunc("/upstream/thing", func(w http.ResponseWriter, r *http.Request) { WriteJSON(w, "real") })

	m := startTestMock(t)
	m.HandleFunc("/api/known", func(w http.ResponseWriter, r *http.Request) { WriteJSON(w, "mocked") })
	if err := m.PassthroughUnmatched(real.BaseURL()); err != nil {
		t.Fatalf("PassthroughUnmatched: %v", err)
	}

	_, body := get(t, m.BaseURL()+"/upstream/thing")
	if !strings.Contains(body, "real") {
		t.Fatalf("passthrough body = %q, want proxied response", body)
	}
	_, body = get(t, m.BaseURL()+"/api/known")
	if !strings.Contains(body, "mocked") {
		t.Fatalf("registered route body = %q, want mock response", body)
	}

	if err := m.PassthroughUnmatched("not a url"); err == nil {
		t.Fatal("invalid base URL must error")
	}
}
