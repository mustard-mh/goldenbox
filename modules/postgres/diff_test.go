package postgres

import (
	"strings"
	"testing"

	"github.com/mustard-mh/goldenbox"
)

// row builds a snapshot row like dumpTable does: id → <ROWID> with the real
// primary key riding under hiddenPKCol ("" = table without an id column).
func row(pk string, cols map[string]any) map[string]any {
	r := map[string]any{}
	for k, v := range cols {
		r[k] = v
	}
	if pk != "" {
		r["id"] = "<ROWID>"
		r[hiddenPKCol] = pk
	}
	return r
}

func TestDiffDBPairsUpdatesByPK(t *testing.T) {
	prev := DBSnapshot{"users": {
		row("7", map[string]any{"name": "alice", "status": "pending"}),
		row("8", map[string]any{"name": "bob", "status": "active"}),
	}}
	cur := DBSnapshot{"users": {
		row("7", map[string]any{"name": "alice", "status": "active"}),
		row("8", map[string]any{"name": "bob", "status": "active"}),
	}}

	d := DiffDB(prev, cur)
	if len(d.Added) != 0 || len(d.Removed) != 0 {
		t.Fatalf("update must not surface as added/removed: %+v", d)
	}
	ups := d.Updated["users"]
	if len(ups) != 1 {
		t.Fatalf("want 1 updated row, got %d: %+v", len(ups), ups)
	}
	u := ups[0]
	if got := u.Key["name"]; got != "alice" {
		t.Errorf("key should hold unchanged columns, got %+v", u.Key)
	}
	if _, ok := u.Key["id"]; ok {
		t.Errorf("id (<ROWID>) must not appear in key: %+v", u.Key)
	}
	ch, ok := u.Changes["status"]
	if !ok || ch.From != "pending" || ch.To != "active" {
		t.Errorf("want status pending→active, got %+v", u.Changes)
	}
}

func TestDiffDBInsertDeleteStayAddedRemoved(t *testing.T) {
	prev := DBSnapshot{"users": {row("1", map[string]any{"name": "gone"})}}
	cur := DBSnapshot{"users": {row("2", map[string]any{"name": "new"})}}

	d := DiffDB(prev, cur)
	if len(d.Updated) != 0 {
		t.Fatalf("distinct PKs must not pair as updated: %+v", d.Updated)
	}
	if len(d.Added["users"]) != 1 || d.Added["users"][0]["name"] != "new" {
		t.Errorf("added: %+v", d.Added)
	}
	if len(d.Removed["users"]) != 1 || d.Removed["users"][0]["name"] != "gone" {
		t.Errorf("removed: %+v", d.Removed)
	}
}

func TestDiffDBStripsHiddenPK(t *testing.T) {
	cur := DBSnapshot{"users": {
		row("7", map[string]any{"name": "alice", "status": "pending"}),
		row("8", map[string]any{"name": "bob", "status": "active"}),
	}}
	next := DBSnapshot{"users": {
		row("8", map[string]any{"name": "bob", "status": "banned"}),
	}}

	for _, d := range []DBDelta{DiffDB(nil, cur), DiffDB(cur, next)} {
		if s := goldenbox.CanonicalJSON(d); strings.Contains(s, `\u0000`) {
			t.Errorf("hidden PK leaked into delta: %s", s)
		}
	}
}

func TestDiffDBNoPKRowsFallBackToMultiset(t *testing.T) {
	prev := DBSnapshot{"kv": {row("", map[string]any{"k": "a", "v": "1"})}}
	cur := DBSnapshot{"kv": {row("", map[string]any{"k": "a", "v": "2"})}}

	d := DiffDB(prev, cur)
	if len(d.Updated) != 0 {
		t.Fatalf("rows without an id column must not pair: %+v", d.Updated)
	}
	if len(d.Added["kv"]) != 1 || len(d.Removed["kv"]) != 1 {
		t.Errorf("want plain added+removed, got %+v", d)
	}
}

func TestDiffDBUpdatedOnlyDeltaIsNotEmpty(t *testing.T) {
	prev := DBSnapshot{"users": {row("7", map[string]any{"s": "a"})}}
	cur := DBSnapshot{"users": {row("7", map[string]any{"s": "b"})}}

	delta, empty := cur.Diff(prev)
	if empty {
		t.Fatal("updated-only delta reported empty")
	}
	if d := delta.(DBDelta); len(d.Updated["users"]) != 1 {
		t.Fatalf("want updated entry, got %+v", d)
	}
}
