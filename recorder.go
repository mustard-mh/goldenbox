package goldenbox

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mustard-mh/goldenbox/golden"
)

// Step is one recorded step: name, normalized request/response, and per-store
// deltas inlined under each store's Name. Unchanged parts are omitted.
type Step struct {
	Name     string         `yaml:"name"`
	Request  any            `yaml:"request,omitempty"`
	Response *Response      `yaml:"response,omitempty"`
	Diffs    map[string]any `yaml:",inline"`
}

// Golden is a whole script's golden: an ordered sequence of steps.
type Golden struct {
	Steps []Step `yaml:"steps"`
}

// Recorder records steps for ONE golden. All snapshots and responses share one
// Normalizer, so the same real id maps to the same placeholder across every
// store and step.
type Recorder struct {
	t     *testing.T
	ctx   context.Context
	c     *Case
	norm  *Normalizer
	steps []Step
	prev  StateSnapshot
}

// NewRecorder creates a recorder for one golden and takes a baseline quiesced
// snapshot, so startup side effects aren't attributed to the first step's delta.
func NewRecorder(t *testing.T, c *Case) *Recorder {
	t.Helper()
	r := &Recorder{t: t, ctx: context.Background(), c: c, norm: NewNormalizer(c.h.opts.Normalize)}
	r.prev = r.stableSnapshot(1)
	return r
}

// stableSnapshot polls the case's cheap state stamp until it is unchanged for
// wantStable consecutive reads (use more than 1 for steps fanning out several
// fire-and-forget writes), then takes ONE canonical snapshot through r.norm.
// One settled dump gives deterministic id numbering; numbering off a
// partially-landed state would make placeholders depend on async landing order.
func (r *Recorder) stableSnapshot(wantStable int) StateSnapshot {
	r.t.Helper()
	stamp := func() string {
		s, err := r.c.Stamp(r.ctx)
		if err != nil {
			r.t.Fatalf("state stamp: %v", err)
		}
		return s
	}
	prev := stamp()
	stable := 0
	for i := 0; i < 100 && stable < wantStable; i++ {
		time.Sleep(60 * time.Millisecond)
		cur := stamp()
		if cur == prev {
			stable++
		} else {
			stable = 0
		}
		prev = cur
	}
	snap, err := r.c.Snapshot(r.ctx, r.norm)
	if err != nil {
		r.t.Fatalf("snapshot: %v", err)
	}
	return snap
}

// Record records one step: normalized req/resp plus a quiesced snapshot diffed
// against the previous step.
func (r *Recorder) Record(name string, req any, resp *Response) {
	r.t.Helper()
	r.RecordN(name, req, resp, 1)
}

// RecordN is Record with a configurable snapshot-stability requirement (see
// stableSnapshot).
func (r *Recorder) RecordN(name string, req any, resp *Response, wantStable int) {
	r.t.Helper()
	cur := r.stableSnapshot(wantStable)
	s := Step{Name: name}
	if req != nil {
		s.Request = r.norm.Request(req)
	}
	if resp != nil {
		normResp := r.norm.Response(*resp)
		s.Response = &normResp
	}
	if diffs := r.c.Diff(r.prev, cur); len(diffs) > 0 {
		s.Diffs = diffs
	}
	r.prev = cur
	r.steps = append(r.steps, s)
}

// RecordWhen is Record for steps whose interesting effect is ASYNCHRONOUS: it
// polls cond over throwaway snapshots until true (waiting for a SPECIFIC write
// rather than global quiescence), then records normally. Fails if cond is not
// met within timeout.
func (r *Recorder) RecordWhen(name string, req any, resp *Response, timeout time.Duration, cond func(StateSnapshot) bool) {
	r.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		// Throwaway normalizer: polling must not consume id numbers from the
		// golden's shared one.
		snap, err := r.c.Snapshot(r.ctx, NewNormalizer(r.c.h.opts.Normalize))
		if err != nil {
			r.t.Fatalf("RecordWhen %s: snapshot: %v", name, err)
		}
		if cond(snap) {
			break
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("RecordWhen %s: condition not met within %s", name, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
	r.Record(name, req, resp)
}

// Golden returns the recorded steps, ready for Harness.Assert.
func (r *Recorder) Golden() Golden { return Golden{Steps: r.steps} }

// --- golden glue: core -> golden subpackage ---

// targetGoldenPath returns the golden path for the current target. The default
// target uses the plain path; a non-default target gets a per-target infix
// ("dir/e2e.golden.<target>.yaml") so alternative implementations can render
// different response shapes while sharing the per-store diff contract.
func targetGoldenPath(goldenPath string) string {
	if TargetName() == DefaultTargetName {
		return goldenPath
	}
	const suffix = ".golden.yaml"
	if base, ok := strings.CutSuffix(goldenPath, suffix); ok {
		return base + ".golden." + TargetName() + ".yaml"
	}
	return goldenPath
}

// Assert compares value against the golden named name under the harness's
// GoldenDir (non-default targets get a per-target infix; see targetGoldenPath).
func (h *Harness) Assert(t *testing.T, name string, value any) {
	t.Helper()
	golden.AssertAny(t, targetGoldenPath(filepath.Join(h.opts.GoldenDir, name)), value)
}

// CleanObsoleteGoldens deletes orphan goldens under the harness's GoldenDir —
// files no -update-golden run rewrote.
func (h *Harness) CleanObsoleteGoldens() []string { return golden.CleanObsolete(h.opts.GoldenDir) }
