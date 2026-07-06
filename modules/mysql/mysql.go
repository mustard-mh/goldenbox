// Package mysql is the goldenbox MySQL store driver: one MySQL testcontainer
// per process, one fresh empty database per case, full-table snapshots diffed
// into the golden under the store's name (default "mysql").
//
//	goldenbox.New(ctx, opts, mysql.With(mysql.Config{Image: "mysql:8"}))
//
// Keep the value With returns and reach a case's connection with Of. Two stores
// are told apart by object, not by name.
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/mustard-mh/goldenbox"
)

const (
	mysqlUser     = "root"
	mysqlPassword = "root"
	// mysqlBaseDB is the base name; cases use "<base>_c<n>".
	mysqlBaseDB = "case"
	defaultName = "mysql"
)

// Config is the MySQL store's typed configuration, passed via With.
type Config struct {
	// Name labels this store instance — its golden section key and the key for
	// mysql.Of. Default "mysql"; set it to run several MySQL stores in one harness.
	Name string
	// Image is the container image, e.g. "mysql:8" — required (no default).
	Image string
}

// Store is a configured MySQL store: pass it to goldenbox.New, and keep the
// reference to reach each case's connection with Of.
type Store interface {
	goldenbox.StoreDriver
	// Of returns this store's connection for case c (panics if c has no slice
	// of this store — a config error).
	Of(c *goldenbox.Case) Handle
}

// With builds a configured MySQL store. Keep the returned value to reach a
// case's connection later with Of.
func With(cfg Config) Store {
	if cfg.Name == "" {
		cfg.Name = defaultName
	}
	return &mysqlStore{cfg: cfg}
}

type mysqlStore struct {
	cfg       Config
	container *mysql.MySQLContainer
	host      string
	port      string
	// adminDB is a pool with NO default database, for CREATE/DROP DATABASE.
	adminDB *sql.DB
	reused  bool
	// watch is the process's binlog tap (nil = unavailable; cases fall back
	// to CHECKSUM stamps and full dumps).
	watch *binlogWatcher
}

func (s *mysqlStore) Name() string { return s.cfg.Name }

func (s *mysqlStore) Start(ctx context.Context, h *goldenbox.Harness) error {
	s.reused = h.ReuseEnabled()

	opts := []testcontainers.ContainerCustomizer{
		mysql.WithDatabase(mysqlBaseDB),
		mysql.WithUsername(mysqlUser),
		mysql.WithPassword(mysqlPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("ready for connections").
				WithOccurrence(2).
				WithStartupTimeout(60 * time.Second),
		),
	}
	if s.reused {
		// Ryuk (the reaper) would terminate the container ~10s after this
		// session ends, defeating reuse. Must be set before the first container starts.
		os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
		opts = append(opts, testcontainers.WithReuseByName(h.Options().ReuseNamePrefix+"-"+s.cfg.Name))
	}

	if s.cfg.Image == "" {
		return fmt.Errorf(`mysql: Config.Image is required (e.g. "mysql:8")`)
	}
	c, err := mysql.Run(ctx, s.cfg.Image, opts...)
	if err != nil {
		return fmt.Errorf("start mysql container: %w", err)
	}
	s.container = c
	if s.host, err = c.Host(ctx); err != nil {
		return fmt.Errorf("mysql host: %w", err)
	}
	port, err := c.MappedPort(ctx, "3306")
	if err != nil {
		return fmt.Errorf("mysql port: %w", err)
	}
	s.port = port.Port()

	s.adminDB, err = sql.Open("mysql", s.dsn(""))
	if err != nil {
		return fmt.Errorf("open mysql: %w", err)
	}
	// The mysql container restarts once during initialization, so the port may
	// briefly close even after the wait strategy passes. Poll until connectable.
	if err := pingUntilReady(ctx, s.adminDB, 60*time.Second); err != nil {
		return fmt.Errorf("mysql not ready: %w", err)
	}
	s.watch = startBinlogWatcher(net.JoinHostPort(s.host, s.port), mysqlUser, mysqlPassword)
	return nil
}

// dsn builds a DSN for database ("" = none, for admin operations).
func (s *mysqlStore) dsn(database string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&multiStatements=true&loc=UTC",
		mysqlUser, mysqlPassword, net.JoinHostPort(s.host, s.port), database)
}

func (s *mysqlStore) NewCase(ctx context.Context, caseID int) (goldenbox.StoreCase, error) {
	name := fmt.Sprintf("%s_c%d", mysqlBaseDB, caseID)
	// Drop leftovers from a crashed prior run against reused containers.
	if _, err := s.adminDB.ExecContext(ctx, fmt.Sprintf(
		"DROP DATABASE IF EXISTS `%s`; CREATE DATABASE `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", name, name)); err != nil {
		return nil, fmt.Errorf("create case database %s: %w", name, err)
	}
	pool, err := sql.Open("mysql", s.dsn(name))
	if err != nil {
		return nil, fmt.Errorf("open case database %s: %w", name, err)
	}
	// Left empty; the harness runs Options.SetupCase next to apply schema or seed.
	c := &mysqlCase{store: s, database: name, pool: pool}
	if s.watch != nil {
		c.track = s.watch.register(name)
	}
	return c, nil
}

// Handle is a typed view of a MySQL store's per-case connection, returned by Of.
type Handle struct {
	Host, Port, Database, User, Password string
	pool                                 *sql.DB
}

// DB returns the case-scoped connection pool.
func (h Handle) DB() *sql.DB { return h.pool }

// DSN returns a go-sql-driver DSN for the case database (add your own params).
func (h Handle) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", h.User, h.Password, net.JoinHostPort(h.Host, h.Port), h.Database)
}

// ApplySchema executes each SQL file in order on the case database.
func (h Handle) ApplySchema(files ...string) error {
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read schema %s: %w", f, err)
		}
		if _, err := h.pool.Exec(string(b)); err != nil {
			return fmt.Errorf("apply schema %s: %w", f, err)
		}
	}
	return nil
}

// Of returns this store's connection for case c.
func (s *mysqlStore) Of(c *goldenbox.Case) Handle {
	mc, ok := c.LookupStore(s.cfg.Name).(*mysqlCase)
	if !ok {
		panic(fmt.Sprintf("mysql: store %q is not in this case", s.cfg.Name))
	}
	return Handle{
		Host:     mc.store.host,
		Port:     mc.store.port,
		Database: mc.database,
		User:     mysqlUser,
		Password: mysqlPassword,
		pool:     mc.pool,
	}
}

func (s *mysqlStore) Stop(ctx context.Context) error {
	if s.watch != nil {
		s.watch.Close()
	}
	if s.adminDB != nil {
		_ = s.adminDB.Close()
	}
	if s.reused || s.container == nil {
		// Reuse mode: leave the container running for the next invocation.
		return nil
	}
	return s.container.Terminate(ctx)
}

type mysqlCase struct {
	store    *mysqlStore
	database string
	pool     *sql.DB
	track    *caseTracker // nil = no binlog tap; CHECKSUM/full-dump fallback

	// Incremental-snapshot cache (see snapshotIncremental in binlog.go).
	snapMu    sync.Mutex
	snapNorm  *goldenbox.Normalizer
	snapCache DBSnapshot
}

var _ goldenbox.SQLStore = (*mysqlCase)(nil)

func (c *mysqlCase) DB() *sql.DB { return c.pool }

func (c *mysqlCase) Snapshot(ctx context.Context, n *goldenbox.Normalizer) (goldenbox.Snapshot, error) {
	if c.track != nil && c.store.watch.ok.Load() {
		if snap, ok, err := c.snapshotIncremental(ctx, n); ok {
			return snap, err
		}
	}
	return collectDBSnapshot(ctx, c.pool, n)
}

// Stamp is the O(1) binlog event count for this case's database, falling back
// to CHECKSUM TABLE (row-order independent, no row data transferred).
func (c *mysqlCase) Stamp(ctx context.Context) (string, error) {
	if c.track != nil && c.store.watch.ok.Load() {
		return fmt.Sprintf("<binlog:%d>", c.track.count.Load()), nil
	}
	tables, err := listBaseTables(ctx, c.pool)
	if err != nil {
		return "", err
	}
	if len(tables) == 0 {
		return "", nil
	}
	rows, err := c.pool.QueryContext(ctx, "CHECKSUM TABLE `"+strings.Join(tables, "`, `")+"`")
	if err != nil {
		return "", fmt.Errorf("checksum tables: %w", err)
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var tbl string
		var sum sql.NullInt64
		if err := rows.Scan(&tbl, &sum); err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "%s=%d;", tbl, sum.Int64)
	}
	return b.String(), rows.Err()
}

// Reset TRUNCATEs all the case's tables. FK checks are disabled around the
// truncates; all statements run on ONE pinned connection (SET is session-scoped).
func (c *mysqlCase) Reset(ctx context.Context) error {
	// TRUNCATE must never leave stale cached rows behind.
	c.invalidateSnapshotCache()
	tables, err := listBaseTables(ctx, c.pool)
	if err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	if len(tables) == 0 {
		return nil
	}
	conn, err := c.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=0"); err != nil {
		return fmt.Errorf("disable fk checks: %w", err)
	}
	// Re-enable via defer: a failed TRUNCATE must not leak a pooled connection
	// with constraints silently disabled.
	defer func() { _, _ = conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=1") }()
	for _, t := range tables {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE `%s`", t)); err != nil {
			return fmt.Errorf("truncate %s: %w", t, err)
		}
	}
	return nil
}

func (c *mysqlCase) Close(ctx context.Context) error {
	if c.track != nil {
		c.store.watch.unregister(c.database)
	}
	var firstErr error
	if err := c.pool.Close(); err != nil {
		firstErr = err
	}
	if _, err := c.store.adminDB.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+c.database+"`"); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// collectDBSnapshot dumps all rows of every non-empty base table.
func collectDBSnapshot(ctx context.Context, db *sql.DB, n *goldenbox.Normalizer) (DBSnapshot, error) {
	tables, err := listBaseTables(ctx, db)
	if err != nil {
		return nil, err
	}
	out := DBSnapshot{}
	for _, table := range tables {
		rows, err := dumpTable(ctx, db, table, n)
		if err != nil {
			return nil, fmt.Errorf("dump %s: %w", table, err)
		}
		if len(rows) > 0 {
			out[table] = rows
		}
	}
	return out, nil
}

// listBaseTables lists the test database's base tables, sorted by name. To skip
// a large static seed table, exclude it here (NOT IN (...)) so the first-step
// diff doesn't count the whole seed as Added and burn through thousands of ids.
func listBaseTables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' ORDER BY 1`)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func dumpTable(ctx context.Context, db *sql.DB, table string, n *goldenbox.Normalizer) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM `%s`", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	// Scan raw first: id numbers are assigned only after the content sort below.
	var raw [][]sql.NullString
	for rows.Next() {
		cells := make([]sql.NullString, len(cols))
		dest := make([]any, len(cols))
		for i := range cells {
			dest[i] = &cells[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		raw = append(raw, cells)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Sort by content so first-sight id numbering is deterministic.
	sort.SliceStable(raw, func(i, j int) bool {
		return rowSortKey(n, cols, raw[i]) < rowSortKey(n, cols, raw[j])
	})

	out := make([]map[string]any, 0, len(raw))
	for _, cells := range raw {
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			if col == "id" {
				// Run-dependent PK shown as <ROWID>; real value rides under
				// hiddenPKCol for prev/cur pairing, then stripped before the golden.
				row[col] = "<ROWID>"
				if cells[i].Valid {
					row[hiddenPKCol] = cells[i].String
				}
				continue
			}
			row[col] = normalizeCell(col, cells[i], n)
		}
		out = append(out, row)
	}
	return out, nil
}

// rowSortKey keys on the run-stable columns only, excluding the id and any cell
// a scrubber normalizes (ids, timestamps).
func rowSortKey(n *goldenbox.Normalizer, cols []string, cells []sql.NullString) string {
	var b strings.Builder
	for i, col := range cols {
		if col == "id" || (cells[i].Valid && n.IsVolatile(col, cells[i].String)) {
			continue
		}
		b.WriteString(col)
		b.WriteByte('=')
		if cells[i].Valid {
			b.WriteString(cells[i].String)
		} else {
			b.WriteString("\x00")
		}
		b.WriteByte(';')
	}
	return b.String()
}

// normalizeCell: NULL → nil, empty → "", else through the golden's scrubbers
// (same as a response value).
func normalizeCell(col string, v sql.NullString, n *goldenbox.Normalizer) any {
	if !v.Valid {
		return nil
	}
	if v.String == "" {
		return ""
	}
	return n.Value(col, v.String)
}

// DBSnapshot maps table name -> rows (each row is column -> normalized value).
type DBSnapshot map[string][]map[string]any

// hiddenPKCol carries each row's real primary-key value inside snapshots so
// DiffDB can pair a before/after row of the same PK into an updated entry. The
// NUL prefix cannot collide with a real column name; stripped before serialization.
const hiddenPKCol = "\x00pk"

// FieldChange is one column's before/after value in an updated row.
type FieldChange struct {
	From any `yaml:"from"`
	To   any `yaml:"to"`
}

// UpdatedRow pins one in-place row update (same primary key across the step).
// Key holds the unchanged columns — identifying the row without freezing its
// auto-increment id — and Changes the columns that did change.
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

// MarshalYAML renders the delta compactly (see goldenbox.CompactNode).
func (d DBDelta) MarshalYAML() (any, error) {
	type plain DBDelta
	return goldenbox.CompactNode(plain(d))
}

// DiffDB computes the prev→cur difference table by table: a multiset diff, then
// added/removed rows sharing a real primary key are folded into cell-level
// updated entries (Dolt-style, changed columns only). A delete+reinsert of the
// same PK within one step therefore also renders as updated — PK identity wins.
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
// UpdatedRow entries and returns the leftovers. Rows without a real PK are left
// as added/removed.
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

// Diff implements the Snapshot interface for DBSnapshot (prev may be nil).
func (s DBSnapshot) Diff(prev goldenbox.Snapshot) (any, bool) {
	var p DBSnapshot
	if prev != nil {
		p = prev.(DBSnapshot)
	}
	d := DiffDB(p, s)
	return d, d.IsEmpty()
}

// pingUntilReady polls by pinging the database until it succeeds or times out.
func pingUntilReady(ctx context.Context, db *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		lastErr = db.PingContext(pingCtx)
		cancel()
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
	return fmt.Errorf("ping timed out after %s: %w", timeout, lastErr)
}
