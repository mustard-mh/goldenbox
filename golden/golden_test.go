package golden

// golden_test.go is the TRUST ROOT of the repo's test pyramid: every other
// test asserts through this package (dogfooding), so a comparator or
// update-flow bug here would silently green the whole suite. These tests
// therefore use plain assertions against temp dirs and a fake testing.TB —
// never golden files. Do not "simplify" them into golden-based tests.

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeT records failures instead of failing the real test, so failure paths
// (mismatch, missing golden, CI refusal) can be observed positively.
type fakeT struct {
	testing.TB
	failed bool
	fatal  bool
	msgs   []string
}

var errFakeFatal = errors.New("fakeT.Fatalf")

func (f *fakeT) Helper()                         {}
func (f *fakeT) Logf(format string, args ...any) {}
func (f *fakeT) Errorf(format string, args ...any) {
	f.failed = true
	f.msgs = append(f.msgs, fmt.Sprintf(format, args...))
}
func (f *fakeT) Fatalf(format string, args ...any) {
	f.failed, f.fatal = true, true
	f.msgs = append(f.msgs, fmt.Sprintf(format, args...))
	panic(errFakeFatal) // a real Fatalf stops the test; unwound by run()
}

// run executes fn with a fresh fakeT, converting Fatalf's panic into a return.
func run(fn func(ft *fakeT)) (ft *fakeT) {
	ft = &fakeT{}
	defer func() {
		if r := recover(); r != nil && r != errFakeFatal {
			panic(r)
		}
	}()
	fn(ft)
	return ft
}

// setUpdate pins the -update-golden flag for one test, so these tests behave
// identically under plain `go test` and `go test -update-golden`.
func setUpdate(t *testing.T, v bool) {
	t.Helper()
	old := *update
	*update = v
	t.Cleanup(func() { *update = old })
}

func (f *fakeT) allMsgs() string { return strings.Join(f.msgs, "\n") }

type pair struct {
	A int    `yaml:"a"`
	B string `yaml:"b"`
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestSnapshotNamesGoldenByCase pins the Snapshot contract: a case named N
// asserts against dir/N.golden.yaml. Verified through the -update path (writes
// the file) so the name→path mapping is observed on disk, not just asserted.
func TestSnapshotNamesGoldenByCase(t *testing.T) {
	t.Setenv("CI", "")
	setUpdate(t, true)
	dir := t.TempDir()

	Snapshot(t, dir, []Case[int]{
		{Name: "doubled", In: 21},
	}, func(t *testing.T, in int) pair {
		return pair{A: in * 2, B: "ok"}
	})

	data, err := os.ReadFile(filepath.Join(dir, "doubled.golden.yaml"))
	if err != nil {
		t.Fatalf("golden not written at dir/<name>.golden.yaml: %v", err)
	}
	if got, want := string(data), GeneratedHeader+"a: 42\nb: ok\n"; got != want {
		t.Errorf("golden content = %q, want %q", got, want)
	}
}

func TestAssertMatch(t *testing.T) {
	setUpdate(t, false)
	path := filepath.Join(t.TempDir(), "x.golden.yaml")
	writeFile(t, path, "a: 1\nb: two\n")

	ft := run(func(ft *fakeT) { Assert(ft, path, pair{A: 1, B: "two"}) })
	if ft.failed {
		t.Fatalf("matching value reported failure:\n%s", ft.allMsgs())
	}
}

func TestAssertMismatch(t *testing.T) {
	setUpdate(t, false)
	path := filepath.Join(t.TempDir(), "x.golden.yaml")
	writeFile(t, path, "a: 1\nb: two\n")

	ft := run(func(ft *fakeT) { Assert(ft, path, pair{A: 2, B: "two"}) })
	if !ft.failed || ft.fatal {
		t.Fatalf("mismatch must Errorf (failed=%v fatal=%v)", ft.failed, ft.fatal)
	}
	if !strings.Contains(ft.allMsgs(), "-want +got") {
		t.Errorf("mismatch message must carry the diff, got:\n%s", ft.allMsgs())
	}
}

func TestAssertMissingGoldenMentionsUpdateFlag(t *testing.T) {
	setUpdate(t, false)
	path := filepath.Join(t.TempDir(), "missing.golden.yaml")

	ft := run(func(ft *fakeT) { Assert(ft, path, pair{}) })
	if !ft.fatal {
		t.Fatal("missing golden must Fatalf")
	}
	if !strings.Contains(ft.allMsgs(), "-update-golden") {
		t.Errorf("missing-golden message must point at -update-golden, got:\n%s", ft.allMsgs())
	}
}

func TestAssertAnyIgnoresKeyOrderAndFormatting(t *testing.T) {
	setUpdate(t, false)
	path := filepath.Join(t.TempDir(), "x.golden.yaml")
	writeFile(t, path, "{b: two, a: 1}\n") // flow style, reversed key order

	ft := run(func(ft *fakeT) { AssertAny(ft, path, map[string]any{"a": 1, "b": "two"}) })
	if ft.failed {
		t.Fatalf("AssertAny must ignore key order / formatting churn:\n%s", ft.allMsgs())
	}
}

func TestUpdateWritesAndTracksGolden(t *testing.T) {
	t.Setenv("CI", "")
	setUpdate(t, true)
	path := filepath.Join(t.TempDir(), "new.golden.yaml")

	ft := run(func(ft *fakeT) { Assert(ft, path, pair{A: 1, B: "two"}) })
	if ft.failed {
		t.Fatalf("update run reported failure:\n%s", ft.allMsgs())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("golden not written: %v", err)
	}
	if got, want := string(data), GeneratedHeader+"a: 1\nb: two\n"; got != want {
		t.Errorf("golden content = %q, want %q", got, want)
	}
	mu.Lock()
	tracked := written[path]
	mu.Unlock()
	if !tracked {
		t.Error("written golden must be tracked for CleanObsolete")
	}
}

func TestUpdateRefusedInCI(t *testing.T) {
	t.Setenv("CI", "true")
	setUpdate(t, true)
	path := filepath.Join(t.TempDir(), "new.golden.yaml")

	ft := run(func(ft *fakeT) { Assert(ft, path, pair{A: 1}) })
	if !ft.fatal || !strings.Contains(ft.allMsgs(), "refusing") {
		t.Fatalf("CI update must be refused loudly (fatal=%v):\n%s", ft.fatal, ft.allMsgs())
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("refused update must not write the golden")
	}
}

func TestCleanObsolete(t *testing.T) {
	t.Setenv("CI", "")
	dir := t.TempDir()
	orphan := filepath.Join(dir, "orphan.golden.yaml")
	perTarget := filepath.Join(dir, "orphan.golden.other.yaml") // per-target infix form
	kept := filepath.Join(dir, "kept.golden.yaml")
	writeFile(t, orphan, "stale: true\n")
	writeFile(t, perTarget, "stale: true\n")

	setUpdate(t, false)
	if removed := CleanObsolete(dir); removed != nil {
		t.Fatalf("without -update-golden CleanObsolete must be a no-op, removed %v", removed)
	}

	if f := flag.Lookup("test.run"); f != nil && f.Value.String() != "" {
		t.Skip("CleanObsolete is a deliberate no-op under -run filters")
	}
	setUpdate(t, true)
	run(func(ft *fakeT) { Assert(ft, kept, pair{A: 1}) }) // lands in the written set

	removed := CleanObsolete(dir)
	if len(removed) != 2 {
		t.Fatalf("removed = %v, want the two orphans", removed)
	}
	if _, err := os.Stat(kept); err != nil {
		t.Errorf("kept golden must survive: %v", err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Error("orphan golden must be deleted")
	}
}
