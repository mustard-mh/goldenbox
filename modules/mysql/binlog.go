package mysql

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/mustard-mh/goldenbox"
)

// One canal replica per process tails the shared container's ROW-format binlog
// and fans row/DDL events to per-case trackers, powering the O(1) event-count
// Stamp and dirty-table incremental snapshot. Optimization with a full fallback:
// if canal cannot start or dies mid-run, cases revert to CHECKSUM stamps and
// full dumps.
//
// Event delivery is asynchronous: a committed write whose event has not yet
// arrived is invisible to the count and dirty set — the same settling assumption
// the recorder's stamp polling makes (replication lag << poll interval).

type binlogWatcher struct {
	canal.DummyEventHandler
	canal *canal.Canal
	ok    atomic.Bool

	mu    sync.Mutex
	cases map[string]*caseTracker // database name → tracker
}

// caseTracker accumulates one case's write evidence: a monotone event count
// (the Stamp fingerprint) and the tables touched since the last snapshot.
type caseTracker struct {
	count atomic.Int64
	mu    sync.Mutex
	dirty map[string]bool
}

func (t *caseTracker) mark(table string) {
	t.mu.Lock()
	t.dirty[table] = true
	t.mu.Unlock()
	t.count.Add(1)
}

// takeDirty returns and resets the dirty-table set. Take it BEFORE reading
// table rows: an event landing during the read then re-marks the table (a
// harmless re-dump), whereas the reverse order could discard an unread write.
func (t *caseTracker) takeDirty() map[string]bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	d := t.dirty
	t.dirty = map[string]bool{}
	return d
}

// startBinlogWatcher connects a canal replica and begins tailing from the
// current master position. Returns nil (full fallback) on any setup failure.
func startBinlogWatcher(addr, user, password string) *binlogWatcher {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = addr
	cfg.User = user
	cfg.Password = password
	// ServerID must be unique among replicas of this server; the library default
	// (seconds-seeded random) collides for processes started in the same second
	// against a reused container.
	cfg.ServerID = uint32(os.Getpid()%9999) + 50001
	cfg.IncludeTableRegex = []string{`^` + mysqlBaseDB + `_c\d+\..*`}
	// A case's database is DROPPED at Close while its last events may still be
	// in flight; discard events whose table meta is gone instead of erroring.
	cfg.DiscardNoMetaRowEvent = true
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg.Dump.ExecutionPath = "" // tail from a position; never mysqldump

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil
	}
	w := &binlogWatcher{canal: c, cases: map[string]*caseTracker{}}
	c.SetEventHandler(w)
	pos, err := c.GetMasterPos()
	if err != nil {
		c.Close()
		return nil
	}
	w.ok.Store(true)
	go func() {
		_ = c.RunFrom(pos) // blocks; returns nil after Close, an error if the tap dies
		w.ok.Store(false)  // every case falls back to CHECKSUM / full dumps
	}()
	return w
}

func (w *binlogWatcher) Close() { w.canal.Close() }

func (w *binlogWatcher) register(database string) *caseTracker {
	t := &caseTracker{dirty: map[string]bool{}}
	w.mu.Lock()
	w.cases[database] = t
	w.mu.Unlock()
	return t
}

func (w *binlogWatcher) unregister(database string) {
	w.mu.Lock()
	delete(w.cases, database)
	w.mu.Unlock()
}

func (w *binlogWatcher) tracker(database string) *caseTracker {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cases[database]
}

// --- canal.EventHandler (events for unregistered databases are ignored) ---

var _ canal.EventHandler = (*binlogWatcher)(nil)

func (w *binlogWatcher) OnRow(e *canal.RowsEvent) error {
	if t := w.tracker(e.Table.Schema); t != nil {
		t.mark(e.Table.Name)
	}
	return nil
}

// OnTableChanged covers DDL that changes table content without emitting row
// events (TRUNCATE, DROP, ALTER).
func (w *binlogWatcher) OnTableChanged(_ *replication.EventHeader, schemaName, table string) error {
	if t := w.tracker(schemaName); t != nil {
		t.mark(table)
	}
	return nil
}

// OnTableNotFound returns nil to skip the event; the embedded DummyEventHandler
// would error here, killing the canal loop when a closed case's in-flight event
// races its DROP DATABASE.
func (w *binlogWatcher) OnTableNotFound(*replication.EventHeader, *replication.RowsEvent) error {
	return nil
}

func (w *binlogWatcher) String() string { return "goldenbox" }

// snapshotIncremental maintains a per-case cache of normalized rows owned by the
// first Normalizer that snapshots the case: dirty tables are re-dumped, clean
// tables reuse cached rows. A different Normalizer must not mix numbering spaces,
// so the caller falls back to a full dump (ok=false) and the cache is untouched.
func (c *mysqlCase) snapshotIncremental(ctx context.Context, n *goldenbox.Normalizer) (DBSnapshot, bool, error) {
	c.snapMu.Lock()
	defer c.snapMu.Unlock()
	if c.snapCache != nil && c.snapNorm != n {
		return nil, false, nil
	}
	tables, err := listBaseTables(ctx, c.pool)
	if err != nil {
		return nil, true, err
	}
	dirty := c.track.takeDirty()
	if c.snapCache == nil {
		c.snapCache = DBSnapshot{}
		c.snapNorm = n
		for _, t := range tables {
			dirty[t] = true
		}
	}
	live := map[string]bool{}
	for _, t := range tables {
		live[t] = true
	}
	for t := range c.snapCache {
		if !live[t] {
			delete(c.snapCache, t)
		}
	}
	for _, t := range tables {
		if _, cached := c.snapCache[t]; cached && !dirty[t] {
			continue
		}
		rows, err := dumpTable(ctx, c.pool, t, n)
		if err != nil {
			return nil, true, err
		}
		c.snapCache[t] = rows // empty tables cached too, so they are not re-dumped
	}
	// Fresh map every call: the recorder retains the previous snapshot for
	// diffing, so later cache mutations must not alias into it.
	out := DBSnapshot{}
	for t, rows := range c.snapCache {
		if len(rows) > 0 {
			out[t] = rows
		}
	}
	return out, true, nil
}

// invalidateSnapshotCache forces the next snapshot to be a full dump (Reset's
// TRUNCATEs must never leave stale cached rows behind).
func (c *mysqlCase) invalidateSnapshotCache() {
	c.snapMu.Lock()
	c.snapCache, c.snapNorm = nil, nil
	c.snapMu.Unlock()
	if c.track != nil {
		c.track.takeDirty()
	}
}
