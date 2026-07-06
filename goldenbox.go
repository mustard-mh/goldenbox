// Package goldenbox is a black-box golden-testing harness that boots your real
// service binary against containerized stores, drives its API with a
// deterministic script, and after every step pins the normalized store delta
// into a reviewed YAML golden file. State changes you did not think to assert
// still land in the diff and still fail the golden.
//
// One Harness per test process (create it in TestMain). Cases are isolated
// slices of the shared containers (own database / Redis logical DB), so tests
// can run under t.Parallel().
package goldenbox

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
)

// Harness owns the shared infrastructure of one test process.
type Harness struct {
	opts   Options
	stores []StoreDriver
	mock   *Mock

	mu      sync.Mutex
	caseSeq int
}

// New starts each store and the mock server. Call once from TestMain and Close
// when done. One Harness per process is the supported model: the -update-golden
// flag and the launch-method registry are process-wide, like the flag package.
func New(ctx context.Context, opts Options, stores ...StoreDriver) (*Harness, error) {
	h := &Harness{opts: opts.withDefaults(), stores: stores}

	// Store names key the golden sections and LookupStore, so they must be unique.
	seen := map[string]bool{}
	for _, s := range h.stores {
		if seen[s.Name()] {
			return nil, fmt.Errorf("duplicate store name %q: give each store a distinct Config.Name", s.Name())
		}
		seen[s.Name()] = true
	}

	started := 0
	for _, s := range h.stores {
		if err := s.Start(ctx, h); err != nil {
			for _, prev := range h.stores[:started] {
				_ = prev.Stop(ctx)
			}
			return nil, fmt.Errorf("start store %s: %w", s.Name(), err)
		}
		started++
	}

	mock, err := StartMock()
	if err != nil {
		_ = h.Close(ctx)
		return nil, fmt.Errorf("start mock: %w", err)
	}
	h.mock = mock
	return h, nil
}

// Options returns the resolved options (defaults applied).
func (h *Harness) Options() Options { return h.opts }

// Mock returns the harness's upstream mock server.
func (h *Harness) Mock() *Mock { return h.mock }

// Close stops the mock and every store driver.
func (h *Harness) Close(ctx context.Context) error {
	var firstErr error
	if h.mock != nil {
		h.mock.Close()
	}
	for _, s := range h.stores {
		if err := s.Stop(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ReuseEnabled reports whether GOLDENBOX_REUSE=1, so store drivers reuse fixed-name
// containers across runs. Local-dev speedup for one interactive loop only:
// concurrent processes sharing reused containers collide on case names, so keep
// it off in CI and flake checks.
func (h *Harness) ReuseEnabled() bool { return os.Getenv(envPrefix+"_REUSE") == "1" }

// Case is one test case's isolated slice of every configured store.
type Case struct {
	h      *Harness
	id     int
	stores []StoreCase // parallel to h.stores
}

// NewCase allocates an isolated per-case slice of every store, then runs
// Options.SetupCase to prepare it. Close the case to release them (use t.Cleanup).
func (h *Harness) NewCase(ctx context.Context) (*Case, error) {
	h.mu.Lock()
	h.caseSeq++
	id := h.caseSeq
	h.mu.Unlock()

	c := &Case{h: h, id: id}
	for _, s := range h.stores {
		sc, err := s.NewCase(ctx, id)
		if err != nil {
			_ = c.Close(ctx)
			return nil, fmt.Errorf("store %s case: %w", s.Name(), err)
		}
		c.stores = append(c.stores, sc)
	}
	if h.opts.SetupCase != nil {
		if err := h.opts.SetupCase(ctx, c); err != nil {
			_ = c.Close(ctx)
			return nil, fmt.Errorf("setup case: %w", err)
		}
	}
	return c, nil
}

// LookupStore returns this case's slice of the named store (nil if absent).
// Drivers build their typed accessors on it, e.g. mysql.Of(c, "primary").
func (c *Case) LookupStore(name string) StoreCase {
	for i, s := range c.h.stores {
		if s.Name() == name {
			return c.stores[i]
		}
	}
	return nil
}

// StateSnapshot is one quiesced dump of every store, keyed by driver name.
type StateSnapshot map[string]Snapshot

// Snapshot dumps every store's state, all normalized through the same Normalizer
// so one real id maps to the same placeholder across stores and responses.
func (c *Case) Snapshot(ctx context.Context, n *Normalizer) (StateSnapshot, error) {
	out := StateSnapshot{}
	for i, sc := range c.stores {
		snap, err := sc.Snapshot(ctx, n)
		if err != nil {
			return nil, fmt.Errorf("snapshot %s: %w", c.h.stores[i].Name(), err)
		}
		out[c.h.stores[i].Name()] = snap
	}
	return out, nil
}

// Diff computes each store's prev→cur delta, keyed by store Name. Only non-empty
// deltas appear, so an empty map means the step changed no store state.
func (c *Case) Diff(prev, cur StateSnapshot) map[string]any {
	out := map[string]any{}
	for _, s := range c.h.stores {
		var prevSnap Snapshot
		if prev != nil {
			prevSnap = prev[s.Name()]
		}
		if curSnap, ok := cur[s.Name()]; ok {
			if delta, empty := curSnap.Diff(prevSnap); !empty {
				out[s.Name()] = delta
			}
		}
	}
	return out
}

// Stamp concatenates every store's cheap write-state fingerprint. Two equal
// stamps mean no write landed in any store in between.
func (c *Case) Stamp(ctx context.Context) (string, error) {
	var b []byte
	for i, sc := range c.stores {
		s, err := sc.Stamp(ctx)
		if err != nil {
			return "", fmt.Errorf("stamp %s: %w", c.h.stores[i].Name(), err)
		}
		b = append(b, c.h.stores[i].Name()...)
		b = append(b, '{')
		b = append(b, s...)
		b = append(b, '}')
	}
	return string(b), nil
}

// Reset wipes every store slice's data without re-provisioning, for mid-test wipes.
func (c *Case) Reset(ctx context.Context) error {
	for i, sc := range c.stores {
		if err := sc.Reset(ctx); err != nil {
			return fmt.Errorf("reset %s: %w", c.h.stores[i].Name(), err)
		}
	}
	return nil
}

// Close releases every store slice. Safe with partially-constructed cases.
func (c *Case) Close(ctx context.Context) error {
	var firstErr error
	for _, sc := range c.stores {
		if err := sc.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// SQLStore is implemented by store cases backed by database/sql.
type SQLStore interface{ DB() *sql.DB }

// DB returns the first SQL-backed store case's connection pool (nil if the harness
// has no SQL store), the escape hatch for direct fixture inserts and assertions.
func (c *Case) DB() *sql.DB {
	for _, sc := range c.stores {
		if s, ok := sc.(SQLStore); ok {
			return s.DB()
		}
	}
	return nil
}

// Options configures a Harness. Every field has a sensible default except
// Target.RunArgs, which is required to start a service.
type Options struct {
	// SetupCase prepares a freshly provisioned case before the target starts
	// (schema, migrations, seed fixtures). nil = no per-case setup.
	SetupCase func(ctx context.Context, c *Case) error

	// GoldenDir is where golden files live. Default "testdata".
	GoldenDir string

	// ReuseNamePrefix names the fixed containers used by GOLDENBOX_REUSE=1.
	// Default "goldenbox".
	ReuseNamePrefix string

	// Target configures the built-in "exec" launch method. Alternative launch
	// methods register with RegisterTarget and are selected via GOLDENBOX_TARGET.
	Target ExecTargetOptions

	// Normalize holds the normalization rules: an ordered Scrubber chain plus the
	// array-collapse threshold. Zero value = no scrubbers and the default threshold.
	Normalize Rules
}

// ExecTargetOptions configures the built-in "exec" target: run a service command
// as one subprocess per case. There is no build step; supply a ready command.
type ExecTargetOptions struct {
	// Dir is the working directory for the run command.
	Dir string

	// RunArgs is the static command that starts one service instance; extra
	// per-target args from NewTarget are appended. Required.
	RunArgs []string

	// Env builds the process environment for each service/job from a resolved
	// TargetContext. Anything env cannot express (a rendered config file) is a
	// side effect you do here before returning. nil = an empty environment.
	Env func(tc TargetContext) ([]string, error)

	// ReadinessPath is an unauthenticated GET the harness probes until the
	// service is up. Default "/healthz".
	ReadinessPath string
}

// TargetContext carries what an exec target needs to configure one process: the
// case, the picked listen Port ("" for one-shot jobs) and the mock server URL.
type TargetContext struct {
	Case *Case
	Port string
	Mock string
}

func (o Options) withDefaults() Options {
	if o.GoldenDir == "" {
		o.GoldenDir = "testdata"
	}
	if o.ReuseNamePrefix == "" {
		o.ReuseNamePrefix = "goldenbox"
	}
	// No RunArgs default: the run command is required, the consumer's choice.
	if o.Target.ReadinessPath == "" {
		o.Target.ReadinessPath = "/healthz"
	}
	o.Normalize = o.Normalize.withDefaults()
	return o
}
