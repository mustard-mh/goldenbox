// Package postgres is the goldenbox PostgreSQL store driver: one Postgres
// testcontainer per process, a fresh empty database per case, full-table
// snapshots diffed into the golden under the store's name (default "postgres").
//
// Quiescence uses a synchronous per-table content fingerprint (Stamp): Postgres
// has no binlog, so the logical-replication event feed is left for later.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/mustard-mh/goldenbox"
)

const (
	pgUser     = "postgres"
	pgPassword = "postgres"
	// pgAdminDB is the maintenance database CREATE/DROP DATABASE connect to.
	pgAdminDB = "postgres"
	// pgBaseDB is the base name; cases use "<base>_c<n>".
	pgBaseDB = "case"
	// defaultName labels the store when Config.Name is empty.
	defaultName = "postgres"
)

// Config is the PostgreSQL store's typed configuration, passed via With.
type Config struct {
	// Name labels this store — its golden section key and the key for
	// postgres.Of. Default "postgres".
	Name string
	// Image is the container image, e.g. "postgres:16" — required (no default).
	Image string
}

// Store is a configured PostgreSQL store; pass it to goldenbox.New.
type Store interface {
	goldenbox.StoreDriver
	// Of returns this store's connection for case c (panics if c has no slice
	// of this store).
	Of(c *goldenbox.Case) Handle
}

// With builds a configured PostgreSQL store. Keep the returned value to reach a
// case's connection later via Of.
func With(cfg Config) Store {
	if cfg.Name == "" {
		cfg.Name = defaultName
	}
	return &pgStore{cfg: cfg}
}

type pgStore struct {
	cfg       Config
	container *tcpostgres.PostgresContainer
	host      string
	port      string
	// adminDB connects to the maintenance database, for CREATE/DROP DATABASE.
	adminDB *sql.DB
	reused  bool
}

func (s *pgStore) Name() string { return s.cfg.Name }

func (s *pgStore) Start(ctx context.Context, h *goldenbox.Harness) error {
	s.reused = h.ReuseEnabled()

	opts := []testcontainers.ContainerCustomizer{
		tcpostgres.WithDatabase(pgAdminDB),
		tcpostgres.WithUsername(pgUser),
		tcpostgres.WithPassword(pgPassword),
		testcontainers.WithWaitStrategy(
			// Postgres logs the ready line twice (init run, then real start);
			// wait for the second before probing.
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60 * time.Second),
		),
	}
	if s.reused {
		// Ryuk would reap the container shortly after this session ends,
		// defeating reuse — disable it for reuse mode only.
		os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
		opts = append(opts, testcontainers.WithReuseByName(h.Options().ReuseNamePrefix+"-"+s.cfg.Name))
	}

	if s.cfg.Image == "" {
		return fmt.Errorf(`postgres: Config.Image is required (e.g. "postgres:16")`)
	}
	c, err := tcpostgres.Run(ctx, s.cfg.Image, opts...)
	if err != nil {
		return fmt.Errorf("start postgres container: %w", err)
	}
	s.container = c
	if s.host, err = c.Host(ctx); err != nil {
		return fmt.Errorf("postgres host: %w", err)
	}
	port, err := c.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return fmt.Errorf("postgres port: %w", err)
	}
	s.port = port.Port()

	s.adminDB, err = sql.Open("postgres", s.dsn(pgAdminDB))
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	if err := pingUntilReady(ctx, s.adminDB, 60*time.Second); err != nil {
		return fmt.Errorf("postgres not ready: %w", err)
	}
	return nil
}

func (s *pgStore) dsn(database string) string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		pgUser, pgPassword, net.JoinHostPort(s.host, s.port), database)
}

func (s *pgStore) NewCase(ctx context.Context, caseID int) (goldenbox.StoreCase, error) {
	name := fmt.Sprintf("%s_c%d", pgBaseDB, caseID)
	// CREATE DATABASE cannot run inside a transaction, so DROP and CREATE are
	// separate autocommit statements. WITH (FORCE) terminates connections a
	// crashed prior run left on a reused container.
	if _, err := s.adminDB.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, ident(name))); err != nil {
		return nil, fmt.Errorf("drop stale case database %s: %w", name, err)
	}
	if _, err := s.adminDB.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE %s`, ident(name))); err != nil {
		return nil, fmt.Errorf("create case database %s: %w", name, err)
	}
	pool, err := sql.Open("postgres", s.dsn(name))
	if err != nil {
		return nil, fmt.Errorf("open case database %s: %w", name, err)
	}
	return &pgCase{store: s, database: name, pool: pool}, nil
}

// Handle is a typed view of a Postgres store's per-case connection, from Of.
type Handle struct {
	Host, Port, Database, User, Password string
	pool                                 *sql.DB
}

// DB returns the case-scoped connection pool.
func (h Handle) DB() *sql.DB { return h.pool }

// URL returns a libpq connection URL for the case database.
func (h Handle) URL() string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", h.User, h.Password, net.JoinHostPort(h.Host, h.Port), h.Database)
}

// ApplySchema executes each SQL file in order on the case database (lib/pq runs
// a whole multi-statement DDL file in one Exec).
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
func (s *pgStore) Of(c *goldenbox.Case) Handle {
	pc, ok := c.LookupStore(s.cfg.Name).(*pgCase)
	if !ok {
		panic(fmt.Sprintf("postgres: store %q is not in this case", s.cfg.Name))
	}
	return Handle{
		Host:     pc.store.host,
		Port:     pc.store.port,
		Database: pc.database,
		User:     pgUser,
		Password: pgPassword,
		pool:     pc.pool,
	}
}

func (s *pgStore) Stop(ctx context.Context) error {
	if s.adminDB != nil {
		_ = s.adminDB.Close()
	}
	if s.reused || s.container == nil {
		return nil
	}
	return s.container.Terminate(ctx)
}

type pgCase struct {
	store    *pgStore
	database string
	pool     *sql.DB
}

var _ goldenbox.SQLStore = (*pgCase)(nil)

func (c *pgCase) DB() *sql.DB { return c.pool }

func (c *pgCase) Snapshot(ctx context.Context, n *goldenbox.Normalizer) (goldenbox.Snapshot, error) {
	return collectDBSnapshot(ctx, c.pool, n)
}

// Stamp fingerprints the case's write state without an event feed: one
// order-independent md5 content hash per table, computed server-side. Equal
// stamps ⇒ no write landed in between.
func (c *pgCase) Stamp(ctx context.Context) (string, error) {
	tables, err := listBaseTables(ctx, c.pool)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, t := range tables {
		var h sql.NullString
		q := fmt.Sprintf(`SELECT md5(coalesce(string_agg(x::text, '' ORDER BY x::text), '')) FROM %s x`, ident(t))
		if err := c.pool.QueryRowContext(ctx, q).Scan(&h); err != nil {
			return "", fmt.Errorf("fingerprint %s: %w", t, err)
		}
		fmt.Fprintf(&b, "%s=%s;", t, h.String)
	}
	return b.String(), nil
}

// Reset TRUNCATEs every case table; CASCADE follows foreign keys and RESTART
// IDENTITY resets sequences, so a wiped case is byte-identical to a fresh one.
func (c *pgCase) Reset(ctx context.Context) error {
	tables, err := listBaseTables(ctx, c.pool)
	if err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	if len(tables) == 0 {
		return nil
	}
	quoted := make([]string, len(tables))
	for i, t := range tables {
		quoted[i] = ident(t)
	}
	if _, err := c.pool.ExecContext(ctx,
		"TRUNCATE "+strings.Join(quoted, ", ")+" RESTART IDENTITY CASCADE"); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	return nil
}

func (c *pgCase) Close(ctx context.Context) error {
	var firstErr error
	// Close the pool before DROP DATABASE — Postgres refuses to drop a database
	// with open sessions (WITH (FORCE) covers any stragglers).
	if err := c.pool.Close(); err != nil {
		firstErr = err
	}
	if _, err := c.store.adminDB.ExecContext(ctx,
		fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, ident(c.database))); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// collectDBSnapshot dumps all rows of every non-empty base table, normalizing
// cells through n.
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

// listBaseTables lists the public-schema base tables, sorted by name. Exclude a
// large static seed table here if its churn is pinned elsewhere, so the diff
// doesn't count the whole seed as Added and burn through thousands of ids.
func listBaseTables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY 1`)
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
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", ident(table)))
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
				// Run-dependent PK: shown as <ROWID>; the real value rides under
				// hiddenPKCol for prev/cur pairing, then is stripped before the golden.
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
// a scrubber normalizes.
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

// normalizeCell: NULL → nil, empty → "", else through the golden's scrubbers.
func normalizeCell(col string, v sql.NullString, n *goldenbox.Normalizer) any {
	if !v.Valid {
		return nil
	}
	if v.String == "" {
		return ""
	}
	return n.Value(col, v.String)
}

// ident double-quotes a Postgres identifier, escaping embedded quotes, so a
// snapshot survives reserved words and mixed-case names.
func ident(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

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
