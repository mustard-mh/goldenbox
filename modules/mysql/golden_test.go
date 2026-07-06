package mysql

import (
	"fmt"
	"os"
	"testing"

	"github.com/mustard-mh/goldenbox/golden"
)

// Code-driven golden tests for the containerless diff logic: cases live here in
// Go, only the expected OUTPUT lives in testdata/mysql (a subdir keeps these
// goldens clear of core's orphan sweep). Regenerate with `go test -update-golden`.

const goldenDir = "../../testdata/mysql"

func TestMain(m *testing.M) {
	code := m.Run()
	if code == 0 {
		for _, f := range golden.CleanObsolete(goldenDir) {
			fmt.Println("removed orphan golden:", f)
		}
	}
	os.Exit(code)
}

type dbDiffInput struct{ prev, cur DBSnapshot }

func TestDiffDBGolden(t *testing.T) {
	golden.Snapshot(t, goldenDir, []golden.Case[dbDiffInput]{
		// In-place update: same PK, one column changes → a cell-level `updated`
		// entry (key = unchanged columns, changes = from→to), never removed+added.
		{Name: "diff_update", In: dbDiffInput{
			prev: DBSnapshot{"user": {
				row("7", map[string]any{"name": "alice", "status": "pending"}),
				row("8", map[string]any{"name": "bob", "status": "active"}),
			}},
			cur: DBSnapshot{"user": {
				row("7", map[string]any{"name": "alice", "status": "active"}),
				row("8", map[string]any{"name": "bob", "status": "active"}),
			}},
		}},
		// Distinct PKs: a deleted row and a fresh insert stay removed + added.
		{Name: "diff_insert_delete", In: dbDiffInput{
			prev: DBSnapshot{"user": {row("1", map[string]any{"name": "gone", "status": "active"})}},
			cur:  DBSnapshot{"user": {row("2", map[string]any{"name": "fresh", "status": "pending"})}},
		}},
		// No id column (row() with "" pk): a changed row cannot pair, so it falls
		// back to a plain multiset added + removed.
		{Name: "diff_nopk", In: dbDiffInput{
			prev: DBSnapshot{"kv": {row("", map[string]any{"k": "theme", "v": "dark"})}},
			cur:  DBSnapshot{"kv": {row("", map[string]any{"k": "theme", "v": "light"})}},
		}},
		// All three deltas across two tables: inserted order, updated order,
		// deleted order, plus a separately updated user.
		{Name: "diff_mixed", In: dbDiffInput{
			prev: DBSnapshot{
				"order": {
					row("10", map[string]any{"sku": "A1", "status": "new"}),
					row("11", map[string]any{"sku": "B2", "status": "paid"}),
					row("12", map[string]any{"sku": "C3", "status": "cancelled"}),
				},
				"user": {row("3", map[string]any{"name": "carol", "tier": "free"})},
			},
			cur: DBSnapshot{
				"order": {
					row("10", map[string]any{"sku": "A1", "status": "paid"}),
					row("12", map[string]any{"sku": "C3", "status": "cancelled"}),
					row("13", map[string]any{"sku": "D4", "status": "new"}),
				},
				"user": {row("3", map[string]any{"name": "carol", "tier": "pro"})},
			},
		}},
	}, func(t *testing.T, in dbDiffInput) DBDelta {
		return DiffDB(in.prev, in.cur)
	})
}
