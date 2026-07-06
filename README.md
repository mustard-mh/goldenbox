# goldenbox

**Black-box golden testing with full state diffs.**

goldenbox boots your real service binary against containerized stores (MySQL, PostgreSQL, Redis — pluggable), drives its HTTP API with a deterministic script, and after **every step** snapshots each store, diffs it against the previous step, normalizes dynamic values, and pins the delta into a reviewed YAML golden file:

```yaml
- name: create_user
  request: {name: alice}
  response: {httpcode: 200, headers: {Content-Type: application/json}, body: {id: <ID_1>}}
  mysql:
    added:
      user:
        - {id: <ROWID>, name: alice, status: pending, create_time: <TIME:YYYY-MM-DDTHH:mm:ssZ>}
- name: activate_user
  request: {id: <ID_1>}
  response: {httpcode: 200, body: {id: <ID_1>, status: active}}
  mysql:
    updated:
      user:
        - key: {name: alice}                       # unchanged columns identify the row
          changes: {status: {from: pending, to: active}}
```

Each delta is keyed by the store's name (`mysql`), so two MySQL stores appear as separate sections. An in-place row change renders as a cell-level `updated` entry (Dolt-style, PK-paired: only the changed columns, with `from`/`to`) — a one-column write is one line, not two rows to diff by eye.

## Features

- **Full state diffs, not just responses** — every step pins what changed in your databases and cache, so a write you did not think to assert still fails the golden.
- **Deterministic under async writes** — event-driven quiescence (MySQL binlog, Redis keyspace notifications) settles fire-and-forget writes before snapshotting; goldens stay byte-reproducible.
- **Readable deltas** — dynamic values normalize to stable placeholders (`<ID_n>`, `<TIME:…>`, TTL buckets); in-place row changes render as PK-paired cell-level `updated` entries.
- **Pluggable stores** — MySQL, PostgreSQL and Redis drivers ship as separate modules; add your own with a `With(Config)` constructor. You choose the infra, engine versions, and run command — the library assumes none.
- **Language-agnostic target** — you supply the build (optional) and run commands; Go, Node, a prebuilt binary, anything.
- **Standalone golden engine** — the `golden/` subpackage works on its own for code-driven golden tests, no harness or containers required.

## Why not an existing tool?

- **Response-snapshot libraries** (go-snaps, cupaloy, goldie, approval-tests) pin what your API *returned* — not what it *did* to your database and cache.
- **Scenario runners** (Venom, runn) can query MySQL/Redis after a step, but every assertion is hand-written per value: a state change you did not think to assert passes silently.

goldenbox pins **everything that changed**. Event-driven quiescence absorbs fire-and-forget async writes — the MySQL driver tails the binlog (canal), the Redis driver subscribes to keyspace notifications — so a step settles when its change feed goes quiet. Where no feed exists, it falls back to fingerprint polling (`CHECKSUM TABLE`, per-DB Redis scans).

## Install

Requires **Go 1.26+** and a Docker daemon (drivers use [Testcontainers](https://golang.testcontainers.org/)).

```bash
go get github.com/mustard-mh/goldenbox
# plus the store drivers you use:
go get github.com/mustard-mh/goldenbox/modules/mysql
go get github.com/mustard-mh/goldenbox/modules/postgres
go get github.com/mustard-mh/goldenbox/modules/redis
```

Drivers are separate Go modules (testcontainers-go style), so you pull only the dependencies of the drivers you use:

| Module | Contents |
| --- | --- |
| `github.com/mustard-mh/goldenbox` | core: stdlib + yaml + go-cmp |
| `github.com/mustard-mh/goldenbox/golden` | golden-file engine (usable standalone) |
| `github.com/mustard-mh/goldenbox/modules/mysql` | MySQL driver (go-sql-driver, go-mysql/canal, testcontainers) |
| `github.com/mustard-mh/goldenbox/modules/postgres` | PostgreSQL driver (lib/pq, testcontainers) |
| `github.com/mustard-mh/goldenbox/modules/redis` | Redis driver (go-redis, testcontainers) |

Configure each store with `With` and pass it to `New`:

```go
goldenbox.New(ctx, opts,
    mysql.With(mysql.Config{Image: "mysql:8"}),
    redis.With(redis.Config{Image: "redis:7"}),
)
```

`Image` is **required** — no default, so the engine version is always your choice. Run two of a kind (primary + replica) by giving each a `Config.Name`; `primary.Of(c)` / `replica.Of(c)` tell them apart by object, no name strings. MySQL and Postgres produce the same delta shape, so switching SQL backends doesn't change your goldens.

## Quickstart

```go
func TestMain(m *testing.M) {
    // Keep the store values With returns — you reference them directly (no name
    // strings) to reach each case's connection.
    db := mysql.With(mysql.Config{Image: "mysql:8"}) // pin your engine versions
    cache := redis.With(redis.Config{Image: "redis:7"})

    h, err = goldenbox.New(ctx, goldenbox.Options{
        // Prepare each fresh case DB — schema, migrations, seed data.
        SetupCase: func(ctx context.Context, c *goldenbox.Case) error {
            return db.Of(c).ApplySchema("path/to/your/schema.sql")
        },
        Target: goldenbox.ExecTargetOptions{
            Dir:     "path/to/your/service", // run working directory
            RunArgs: []string{"./server", "serve"}, // build beforehand; no language assumed
            // Build the process env from typed accessors — YOUR env-var names,
            // values from tc / db.Of(...). No magic string keys.
            Env: func(tc goldenbox.TargetContext) ([]string, error) {
                my := db.Of(tc.Case)
                rd := cache.Of(tc.Case)
                return []string{
                    "PORT=" + tc.Port,
                    "DB_IP=" + my.Host, "DB_DATABASE=" + my.Database,
                    "REDIS_HOST=" + rd.Host, "REDIS_DB_INDEX=" + strconv.Itoa(rd.DB),
                    "UPSTREAM_API_URL=" + tc.Mock, // point your upstreams at the mock
                }, nil
            },
        },
    }, db, cache)
    // ... m.Run(), h.CleanObsoleteGoldens(), h.Close(ctx)
}

func TestE2EUser(t *testing.T) {
    t.Parallel()
    c, _ := h.NewCase(ctx)            // isolated DB + Redis logical DB
    tg := h.NewTarget()               // one service process per case
    _ = tg.Prepare(ctx); _ = tg.Start(ctx, c)
    client := goldenbox.NewHTTPClient(tg.BaseURL())
    rec := goldenbox.NewRecorder(t, c)

    resp, _ := client.Post(ctx, "/api/v1/user", map[string]any{"name": "alice"})
    rec.Record("create_user", map[string]any{"name": "alice"}, &resp)

    h.Assert(t, "e2e.golden.yaml", rec.Golden())
}
```

Regenerate goldens with `go test -update-golden` (refused in CI; a full update run auto-deletes orphan goldens).

## Examples

[`examples/bun`](examples/bun) — a **Bun** user API over MySQL + Redis, zero dependencies, showing goldenbox drives any language. The flow asserts only HTTP responses, yet the golden pins every incidental write — audit rows, a write-through cache entry and its invalidation, the cell-level `status` change — that a hand-written test would never assert.

## How it works

- **One Harness per process** (`TestMain`), containers shared; **one Case per test** — an isolated slice (own MySQL database, own Redis logical DB), so tests run under `t.Parallel()` (16 concurrent cases max, capped by Redis logical DBs).
- **Explicit, pluggable infrastructure**: stores are typed, driver-provided configs passed to `New` (`mysql.With(...)`; add your own the same way). The built-in `"exec"` target runs one service command per case; register alternatives via `RegisterTarget` and select with `GOLDENBOX_TARGET`, each keeping per-target goldens.
- **Normalization is explicit, not magic** (`Options.Normalize`) — no auto-detection, no baked-in field names. Rules are a chain of `Scrubber`s (`func(n *Normalizer, key string, value any) (any, bool)`):
  - `LabelKeys("ID", "id", "user_id")` → stable `<ID_n>` placeholders (same value → same placeholder across responses, DB and cache; integer or UUID).
  - `TimeKeys("created_at")` → `<TIME:layout wall=now±Nh>` tags (revealing the producer's timezone); `TimeValues()` does the same by value for common ISO-8601 / SQL formats without a key list; `FormatKeys(fn, keys…)` plugs in your own formatter.
  - Oversized arrays collapse to `<ARRAY len=N>`. State lives on a per-golden `Normalizer`, not globals.
- **Determinism**: after settling, one canonical labeled dump, then a multiset diff — added/removed rows sharing a real primary key fold into cell-level `updated` entries. The binlog feed doubles as a dirty-table index, so a settled snapshot re-dumps only changed tables.
- **Mock upstream server** — a plain mux, no built-in routes; register seams via `Mock.HandleFunc` (with the `WriteJSON` helper). Also: request verification (`Mock.Calls` / `HitCount`), fault injection (`Mock.SetFault` — latency, forced status+body, connection drop; `Times`, `Match`), and unmatched-request policy (`FailUnmatched()` for a loud 502, `PassthroughUnmatched(base)` to proxy a real upstream).
- **Extras**: `GOLDENBOX_REUSE=1` container reuse, `GOLDENBOX_COVERAGE=1` black-box coverage (`-cover` binaries; injects `GOCOVERDIR`), one-shot CLI jobs via `Harness.RunJob`, async-effect steps via `Recorder.RecordWhen`.

## Golden files

The `golden/` subpackage is the golden-file engine — it works standalone, no harness required:

- `Snapshot[F, T](t, dir, cases, fn)` — code-driven: you write cases in Go (`[]golden.Case[F]{{Name, In}}`), each result pinned to `dir/<name>.golden.yaml`. Only the expected OUTPUT lives on disk; inputs stay in the test source. Each case is a subtest, so `-run` selects one by name.
- `Assert[T]` for typed assertions; `AssertAny` re-parses both sides to `any` before comparing (state goldens use it); plus `CleanObsolete`.
- The update lifecycle lives here: `-update-golden` is refused in CI, and updated files are tracked so a full update run can delete orphan goldens. The core package delegates to this subpackage.

Golden files carry a `# Code generated ... DO NOT EDIT.` header and are marked generated in `.gitattributes`; regenerate them rather than editing by hand.

## Development

```bash
just check   # build + vet + fmt-check across all modules
just test    # unit tests across all modules (containerless)
just itest   # driver integration tests against real containers (needs Docker)
```

One Harness per process is the supported model (the `-update-golden` flag and launch-method registry are process-wide, like the `flag` package). Normalization rules are not global — they live on a per-golden `Normalizer`.
