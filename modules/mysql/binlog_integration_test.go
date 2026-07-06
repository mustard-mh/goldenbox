package mysql_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mustard-mh/goldenbox"
	mysqldrv "github.com/mustard-mh/goldenbox/modules/mysql"
)

// TestBinlogUpdatedDiff exercises the full mysql driver against a real MySQL 8
// container: the binlog tap's event-count Stamp, the dirty-table incremental
// snapshot, and the PK-paired `updated` rendering. Skips when Docker is
// unavailable (unit CI); the downstream e2e suite is the primary coverage.
func TestBinlogUpdatedDiff(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test needs a Docker daemon")
	}

	schema := filepath.Join(t.TempDir(), "schema.sql")
	if err := os.WriteFile(schema, []byte(
		"CREATE TABLE user (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(64), status VARCHAR(32));"), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	store := mysqldrv.With(mysqldrv.Config{Image: "mysql:8"})
	h, err := goldenbox.New(ctx, goldenbox.Options{
		SetupCase: func(ctx context.Context, c *goldenbox.Case) error {
			return store.Of(c).ApplySchema(schema)
		},
	}, store)
	if err != nil {
		t.Skipf("cannot start harness (Docker unavailable?): %v", err)
	}
	t.Cleanup(func() { _ = h.Close(ctx) })

	c, err := h.NewCase(ctx)
	if err != nil {
		t.Fatalf("new case: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(ctx) })

	db := store.Of(c).DB()
	norm := goldenbox.NewNormalizer(goldenbox.DefaultRules())

	// Baseline: empty table.
	base, err := c.Snapshot(ctx, norm)
	if err != nil {
		t.Fatalf("baseline snapshot: %v", err)
	}

	if _, err := db.ExecContext(ctx, "INSERT INTO user (name, status) VALUES ('alice','pending')"); err != nil {
		t.Fatal(err)
	}
	waitBinlog(t, ctx, c)
	afterInsert, err := c.Snapshot(ctx, norm)
	if err != nil {
		t.Fatalf("snapshot after insert: %v", err)
	}
	if _, ok := c.Diff(base, afterInsert)["mysql"]; !ok {
		t.Fatalf("insert produced no mysql diff")
	}

	if _, err := db.ExecContext(ctx, "UPDATE user SET status='active' WHERE name='alice'"); err != nil {
		t.Fatal(err)
	}
	waitBinlog(t, ctx, c)
	afterUpdate, err := c.Snapshot(ctx, norm)
	if err != nil {
		t.Fatalf("snapshot after update: %v", err)
	}
	updDelta := c.Diff(afterInsert, afterUpdate)

	// The in-place update must render as a cell-level `updated` entry, not a
	// removed+added pair.
	delta, ok := updDelta["mysql"].(mysqldrv.DBDelta)
	if !ok {
		t.Fatalf("db_diff is %T, want DBDelta", updDelta["mysql"])
	}
	if len(delta.Added) != 0 || len(delta.Removed) != 0 {
		t.Errorf("update leaked into added/removed: %+v", delta)
	}
	ups := delta.Updated["user"]
	if len(ups) != 1 {
		t.Fatalf("want 1 updated row, got %d: %+v", len(ups), ups)
	}
	if ch := ups[0].Changes["status"]; ch.From != "pending" || ch.To != "active" {
		t.Errorf("want status pending→active, got %+v", ups[0].Changes)
	}
	if ups[0].Key["name"] != "alice" {
		t.Errorf("want name=alice in key, got %+v", ups[0].Key)
	}
}

// waitBinlog polls the case stamp until it stabilizes, giving the async binlog
// event time to land (mirrors the recorder's quiescence loop).
func waitBinlog(t *testing.T, ctx context.Context, c *goldenbox.Case) {
	t.Helper()
	var prev string
	for range 50 {
		s, err := c.Stamp(ctx)
		if err != nil {
			t.Fatalf("stamp: %v", err)
		}
		if s == prev && s != "" {
			return
		}
		prev = s
		time.Sleep(60 * time.Millisecond)
	}
}
