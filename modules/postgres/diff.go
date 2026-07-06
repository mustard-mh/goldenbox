package postgres

import (
	"sort"

	"github.com/mustard-mh/goldenbox"
)

// Self-contained copy of the mysql driver's diff logic (same delta shape), so a
// Postgres consumer pulls no MySQL dependency. Hoist to a shared module if a
// third SQL driver lands.

// DBSnapshot maps table name -> rows (each row is column -> normalized value).
type DBSnapshot map[string][]map[string]any

// hiddenPKCol carries each row's real primary-key value inside snapshots so
// DiffDB can pair a before/after row of the same PK into an updated entry.
// The NUL prefix cannot collide with a real column name; the column is
// stripped from every delta before serialization.
const hiddenPKCol = "\x00pk"

// FieldChange is one column's before/after value in an updated row.
type FieldChange struct {
	From any `yaml:"from"`
	To   any `yaml:"to"`
}

// UpdatedRow pins one in-place row update (same PK across the step). Key holds
// the unchanged columns — identifying the row without freezing its
// auto-increment id — and Changes the columns that changed.
type UpdatedRow struct {
	Key     map[string]any         `yaml:"key"`
	Changes map[string]FieldChange `yaml:"changes"`
}

// DBDelta is the prev→cur database delta (by table). Unchanged tables and
// empty Added/Updated/Removed fields are omitted.
type DBDelta struct {
	Added   DBSnapshot              `yaml:"added,omitempty"`
	Updated map[string][]UpdatedRow `yaml:"updated,omitempty"`
	Removed DBSnapshot              `yaml:"removed,omitempty"`
}

// IsEmpty reports whether the delta contains no change.
func (d DBDelta) IsEmpty() bool {
	return len(d.Added) == 0 && len(d.Updated) == 0 && len(d.Removed) == 0
}

// MarshalYAML renders the delta compactly: each row of scalar columns and each
// {from,to} pair collapses to one flow-style line (see goldenbox.CompactNode).
func (d DBDelta) MarshalYAML() (any, error) {
	type plain DBDelta
	return goldenbox.CompactNode(plain(d))
}

// Diff implements the goldenbox.Snapshot interface for DBSnapshot (prev may be nil).
func (s DBSnapshot) Diff(prev goldenbox.Snapshot) (any, bool) {
	var p DBSnapshot
	if prev != nil {
		p = prev.(DBSnapshot)
	}
	d := DiffDB(p, s)
	return d, d.IsEmpty()
}

// DiffDB computes the prev→cur difference table by table: a multiset diff, then
// added/removed rows sharing a real PK are folded into cell-level updated
// entries. A delete+reinsert of the same PK within one step therefore also
// renders as updated — PK identity wins, matching how the database sees the row.
func DiffDB(prev, cur DBSnapshot) DBDelta {
	delta := DBDelta{Added: DBSnapshot{}, Updated: map[string][]UpdatedRow{}, Removed: DBSnapshot{}}
	for _, table := range unionTableNames(prev, cur) {
		added, removed := multisetDiffRows(prev[table], cur[table])
		updated, added, removed := pairUpdates(added, removed)
		if len(added) > 0 {
			delta.Added[table] = stripHiddenPK(added)
		}
		if len(updated) > 0 {
			delta.Updated[table] = updated
		}
		if len(removed) > 0 {
			delta.Removed[table] = stripHiddenPK(removed)
		}
	}
	if len(delta.Added) == 0 {
		delta.Added = nil
	}
	if len(delta.Updated) == 0 {
		delta.Updated = nil
	}
	if len(delta.Removed) == 0 {
		delta.Removed = nil
	}
	return delta
}

// pairUpdates folds added/removed rows sharing a hiddenPKCol value into
// UpdatedRow entries and returns the leftovers. Order follows added's (stable)
// order; rows without a real PK (no id column) are left as added/removed.
func pairUpdates(added, removed []map[string]any) ([]UpdatedRow, []map[string]any, []map[string]any) {
	removedByPK := map[string]int{}
	for i, r := range removed {
		if pk, ok := r[hiddenPKCol].(string); ok {
			removedByPK[pk] = i
		}
	}
	var updated []UpdatedRow
	paired := make([]bool, len(removed))
	var restAdded []map[string]any
	for _, a := range added {
		if pk, ok := a[hiddenPKCol].(string); ok {
			if i, hit := removedByPK[pk]; hit && !paired[i] {
				paired[i] = true
				updated = append(updated, makeUpdatedRow(removed[i], a))
				continue
			}
		}
		restAdded = append(restAdded, a)
	}
	var restRemoved []map[string]any
	for i, r := range removed {
		if !paired[i] {
			restRemoved = append(restRemoved, r)
		}
	}
	return updated, restAdded, restRemoved
}

func makeUpdatedRow(before, after map[string]any) UpdatedRow {
	u := UpdatedRow{Key: map[string]any{}, Changes: map[string]FieldChange{}}
	for col, av := range after {
		if col == hiddenPKCol {
			continue
		}
		if bv := before[col]; goldenbox.CanonicalJSON(av) != goldenbox.CanonicalJSON(bv) {
			u.Changes[col] = FieldChange{From: bv, To: av}
		} else if col != "id" { // id is always <ROWID>; it identifies nothing
			u.Key[col] = av
		}
	}
	return u
}

// stripHiddenPK returns rows without hiddenPKCol (copying only rows that carry it).
func stripHiddenPK(rows []map[string]any) []map[string]any {
	out := make([]map[string]any, len(rows))
	for i, r := range rows {
		if _, ok := r[hiddenPKCol]; !ok {
			out[i] = r
			continue
		}
		c := make(map[string]any, len(r)-1)
		for k, v := range r {
			if k != hiddenPKCol {
				c[k] = v
			}
		}
		out[i] = c
	}
	return out
}

func unionTableNames(a, b DBSnapshot) []string {
	seen := map[string]bool{}
	for t := range a {
		seen[t] = true
	}
	for t := range b {
		seen[t] = true
	}
	names := make([]string, 0, len(seen))
	for t := range seen {
		names = append(names, t)
	}
	sort.Strings(names)
	return names
}

// multisetDiffRows returns the rows cur has more of than prev (added) and the rows prev has more of than cur (removed).
func multisetDiffRows(prev, cur []map[string]any) (added, removed []map[string]any) {
	prevCount := map[string]int{}
	for _, r := range prev {
		prevCount[goldenbox.CanonicalJSON(r)]++
	}
	for _, r := range cur {
		k := goldenbox.CanonicalJSON(r)
		if prevCount[k] > 0 {
			prevCount[k]--
			continue
		}
		added = append(added, r)
	}
	curCount := map[string]int{}
	for _, r := range cur {
		curCount[goldenbox.CanonicalJSON(r)]++
	}
	for _, r := range prev {
		k := goldenbox.CanonicalJSON(r)
		if curCount[k] > 0 {
			curCount[k]--
			continue
		}
		removed = append(removed, r)
	}
	return added, removed
}
