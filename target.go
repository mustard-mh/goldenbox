package goldenbox

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Target is the lifecycle abstraction for one service instance under test.
// Alternative launch methods register via RegisterTarget and are selected with
// GOLDENBOX_TARGET=<name>, each keeping its own per-target golden files.
type Target interface {
	// Prepare builds the target's artifacts once per process (e.g. `go build`).
	Prepare(ctx context.Context) error
	// Start boots the service against the case's stores and returns after the
	// readiness probe succeeds.
	Start(ctx context.Context, c *Case) error
	// BaseURL returns the base URL of the started service.
	BaseURL() string
	// Stop terminates the service.
	Stop(ctx context.Context) error
	// StopGraceful terminates via SIGTERM and waits, so server-side shutdown
	// work (buffer flushes, coverage counters) runs before exit.
	StopGraceful(ctx context.Context) error
}

// DefaultTargetName is the built-in launch method.
const DefaultTargetName = "exec"

var targetRegistry = map[string]func(h *Harness, args ...string) Target{
	DefaultTargetName: newExecTarget,
}

// RegisterTarget adds a launch method so GOLDENBOX_TARGET=<name> can select it.
func RegisterTarget(name string, factory func(h *Harness, args ...string) Target) {
	targetRegistry[name] = factory
}

// TargetName returns the selected launch method: GOLDENBOX_TARGET when set, else
// DefaultTargetName. It is also the golden-file infix for non-default targets.
func TargetName() string {
	if v := os.Getenv(envPrefix + "_TARGET"); v != "" {
		return v
	}
	return DefaultTargetName
}

// NewTarget returns the selected target with launcher-specific extra argv.
func (h *Harness) NewTarget(args ...string) Target {
	name := TargetName()
	factory, ok := targetRegistry[name]
	if !ok {
		known := make([]string, 0, len(targetRegistry))
		for k := range targetRegistry {
			known = append(known, k)
		}
		sort.Strings(known)
		panic(fmt.Sprintf("unknown %s_TARGET %q (registered: %s)", envPrefix, name, strings.Join(known, ", ")))
	}
	return factory(h, args...)
}

// envPrefix namespaces the harness's control env vars (GOLDENBOX_TARGET / _LOGS /
// _REUSE / _COVERAGE / _COVERAGE_DIR).
const envPrefix = "GOLDENBOX"

// targetLogWriter returns os.Stderr when log piping is on, io.Discard when off.
func targetLogWriter() io.Writer {
	if logsEnabled() {
		return os.Stderr
	}
	return io.Discard
}

// logsEnabled reports whether to pipe the target's logs. Default on to keep
// startup failures visible; GOLDENBOX_LOGS=0/false/off/no silences it.
func logsEnabled() bool {
	switch strings.ToLower(os.Getenv(envPrefix + "_LOGS")) {
	case "0", "false", "off", "no":
		return false
	default:
		return true
	}
}

// --- exec target ---

// buildEnv runs the configured Env hook for a TargetContext (nil hook = empty).
func (h *Harness) buildEnv(c *Case, port string) ([]string, error) {
	if h.opts.Target.Env == nil {
		return nil, nil
	}
	return h.opts.Target.Env(TargetContext{Case: c, Port: port, Mock: h.mock.BaseURL()})
}

// coverageDir returns the GOCOVERDIR subprocesses write coverage counters into,
// or "" when coverage is off (GOLDENBOX_COVERAGE unset). Requires binaries built
// with -cover; counter files are written only on clean exit.
func coverageDir() string {
	if os.Getenv(envPrefix+"_COVERAGE") == "" {
		return ""
	}
	if d := os.Getenv(envPrefix + "_COVERAGE_DIR"); d != "" {
		return d
	}
	return filepath.Join(os.TempDir(), "goldenbox-covdata")
}

// coverageEnv returns the GOCOVERDIR env entry for a subprocess (empty when
// coverage is off), creating the directory on first use.
func coverageEnv() []string {
	dir := coverageDir()
	if dir == "" {
		return nil
	}
	_ = os.MkdirAll(dir, 0o755)
	return []string{"GOCOVERDIR=" + dir}
}

// RunJob runs a one-shot CLI against the case's isolated stores, returning when
// it exits. args is the full command; the environment comes from
// Options.Target.Env with an empty Port. This drives a background job through
// the exact production command path, not a test RPC.
func (h *Harness) RunJob(ctx context.Context, c *Case, args ...string) error {
	if len(args) == 0 {
		return fmt.Errorf("RunJob: args is required (the command to run)")
	}
	env, err := h.buildEnv(c, "") // no server port for a one-shot job
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Dir = h.opts.Target.Dir
	cmd.Stdout = targetLogWriter()
	cmd.Stderr = targetLogWriter()
	cmd.Env = append(os.Environ(), env...)
	cmd.Env = append(cmd.Env, coverageEnv()...)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run job %v: %w", args, err)
	}
	return nil
}

type execTarget struct {
	h    *Harness
	args []string // extra argv appended after RunArgs
	port string
	proc *procHandle
}

func newExecTarget(h *Harness, args ...string) Target { return &execTarget{h: h, args: args} }

func (t *execTarget) BaseURL() string { return "http://127.0.0.1:" + t.port }

func (t *execTarget) Prepare(ctx context.Context) error { return nil }

func (t *execTarget) Start(ctx context.Context, c *Case) error {
	if len(t.h.opts.Target.RunArgs) == 0 {
		return fmt.Errorf("exec target: Options.Target.RunArgs is required (the command that starts your service)")
	}
	port, err := freePort()
	if err != nil {
		return fmt.Errorf("pick port: %w", err)
	}
	t.port = strconv.Itoa(port)

	env, err := t.h.buildEnv(c, t.port)
	if err != nil {
		return err
	}
	argv := append(append([]string{}, t.h.opts.Target.RunArgs...), t.args...)

	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = t.h.opts.Target.Dir
	cmd.Stdout = targetLogWriter()
	cmd.Stderr = targetLogWriter()
	cmd.Env = append(os.Environ(), env...)
	cmd.Env = append(cmd.Env, coverageEnv()...)

	proc, err := startProc(cmd)
	if err != nil {
		return fmt.Errorf("start target: %w", err)
	}
	t.proc = proc
	return proc.waitReady(ctx, "target",
		probeHTTP(t.BaseURL()+t.h.opts.Target.ReadinessPath), 120*time.Second)
}

func (t *execTarget) Stop(ctx context.Context) error {
	if t.proc == nil {
		return nil
	}
	if coverageDir() != "" {
		// SIGTERM instead of SIGKILL: the coverage runtime writes counter files
		// only on clean exit.
		t.proc.stopGraceful(ctx, t.port, 20*time.Second)
		t.proc = nil
		return nil
	}
	t.proc.stop(ctx, t.port)
	return nil
}

// StopGraceful always SIGTERMs and waits, so server-side shutdown work runs.
func (t *execTarget) StopGraceful(ctx context.Context) error {
	if t.proc == nil {
		return nil
	}
	t.proc.stopGraceful(ctx, t.port, 20*time.Second)
	t.proc = nil // already reaped; make a following Stop() a no-op
	return nil
}

// --- subprocess management ---

// procHandle manages a subprocess run in its own process group.
type procHandle struct {
	cmd    *exec.Cmd
	exited chan error
}

// startProc starts cmd in its own process group, so the whole group (including
// any children it spawns) can be killed, and Waits on it in the background.
func startProc(cmd *exec.Cmd) (*procHandle, error) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	h := &procHandle{cmd: cmd, exited: make(chan error, 1)}
	go func() { h.exited <- cmd.Wait() }()
	return h, nil
}

// waitReady polls probe until it succeeds. If the process exits early (e.g. port
// already in use) it returns immediately rather than waiting out the timeout.
func (h *procHandle) waitReady(ctx context.Context, what string, probe func(context.Context) error, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-h.exited:
			return fmt.Errorf("%s exited before ready: %w", what, err)
		default:
		}
		if probe(ctx) == nil {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("%s not ready within %s", what, timeout)
}

// stop kills the entire process group and waits for it to exit; when port is
// non-empty it also waits for the port to be released, avoiding a next-startup collision.
func (h *procHandle) stop(ctx context.Context, port string) {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-h.cmd.Process.Pid, syscall.SIGKILL)
	<-h.exited
	if port != "" {
		_ = waitPortFree(ctx, port, 10*time.Second)
	}
}

// stopGraceful SIGTERMs the process group and waits up to grace for a clean exit
// (so shutdown hooks like coverage-counter writes run, which SIGKILL would
// discard), falling back to SIGKILL. Then waits for the port to be released.
func (h *procHandle) stopGraceful(ctx context.Context, port string, grace time.Duration) {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-h.cmd.Process.Pid, syscall.SIGTERM)
	select {
	case <-h.exited:
	case <-time.After(grace):
		_ = syscall.Kill(-h.cmd.Process.Pid, syscall.SIGKILL)
		<-h.exited
	}
	if port != "" {
		_ = waitPortFree(ctx, port, 10*time.Second)
	}
}

// probeHTTP returns a probe whose readiness condition is "GET url returns any response".
func probeHTTP(url string) func(context.Context) error {
	return func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		return nil
	}
}

// freePort has the kernel allocate a free TCP port and returns it. The tiny race
// window between release and reuse is acceptable for the single-process e2e scenario.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// waitPortFree polls until nothing is listening on the port or it times out. Used
// after a service stops to avoid an "address already in use" collision on the
// immediately following startup.
func waitPortFree(ctx context.Context, port string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	addr := net.JoinHostPort("127.0.0.1", port)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			return nil // cannot connect = already released
		}
		conn.Close()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("port %s still in use after %s", port, timeout)
}
