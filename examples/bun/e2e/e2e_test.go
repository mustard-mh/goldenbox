package e2e_test

import (
	"context"
	"testing"

	"github.com/mustard-mh/goldenbox"
)

func TestUserLifecycle(t *testing.T) {
	h := newHarness(t)
	ctx := t.Context()
	c, err := h.NewCase(ctx)
	if err != nil {
		t.Fatalf("new case: %v", err)
	}
	// Teardown uses a fresh context: t.Context() is already canceled by cleanup time.
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	tg := h.NewTarget()
	if err := tg.Start(ctx, c); err != nil {
		t.Fatalf("start service: %v", err)
	}
	t.Cleanup(func() { _ = tg.StopGraceful(context.Background()) })

	client := goldenbox.NewHTTPClient(tg.BaseURL())
	rec := goldenbox.NewRecorder(t, c)

	// Two users → alice <ID_1>, bob <ID_2>, correlated across response, audit FK
	// and cache; the audit rows and cache writes land in the golden unasserted.
	alice, err := client.Post(ctx, "/users", map[string]any{"name": "alice"})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	rec.Record("create_alice", map[string]any{"name": "alice"}, &alice)

	bob, err := client.Post(ctx, "/users", map[string]any{"name": "bob"})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	rec.Record("create_bob", map[string]any{"name": "bob"}, &bob)

	// Missing user → 404, empty diff.
	missing, err := client.Post(ctx, "/users/999/deactivate", nil)
	if err != nil {
		t.Fatalf("deactivate missing: %v", err)
	}
	rec.Record("deactivate_missing", map[string]any{"id": 999}, &missing)

	// Deactivate alice → cell-level status change + audit row + cache invalidation.
	deactivate, err := client.Post(ctx, "/users/1/deactivate", nil)
	if err != nil {
		t.Fatalf("deactivate alice: %v", err)
	}
	rec.Record("deactivate_alice", map[string]any{"id": 1}, &deactivate)

	// Already inactive → 409, empty diff (a leaked write here would fail the golden).
	again, err := client.Post(ctx, "/users/1/deactivate", nil)
	if err != nil {
		t.Fatalf("deactivate alice again: %v", err)
	}
	rec.Record("deactivate_again", map[string]any{"id": 1}, &again)

	h.Assert(t, "user_lifecycle.golden.yaml", rec.Golden())
}
