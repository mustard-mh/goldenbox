# goldenbox example: Bun + MySQL + Redis

A minimal user API on **[Bun](https://bun.sh)**, backed by **MySQL** and **Redis**, black-box tested with goldenbox — showing goldenbox drives any language, not just Go.

Bun ships a built-in HTTP server (`Bun.serve`), Redis client (`RedisClient`) and SQL client (`Bun.SQL`, MySQL adapter), so this example has **zero dependencies**: no `node_modules`, no `package.json`, nothing to install beyond the Bun runtime. Just `bun server.ts`.

## What it shows

goldenbox pins **everything each step changed** — including writes you'd never think to assert by hand. Each handler has incidental side effects (an audit row, a write-through cache, a cache invalidation), and the golden catches them all.

## The service (`server.ts`)

- `POST /users {name}` — inserts a user (with a `created_at` timestamp), **and** writes an `audit_log` row and a write-through `user:<id>` cache entry.
- `POST /users/:id/deactivate` — flips the user's status, **and** writes another audit row and invalidates the cache. Rejects a missing user (404) or an already-inactive one (409), writing nothing in either case.

Config (DB/Redis connection, port) comes entirely from the environment, filled by goldenbox's `Env` hook. No test-only code in the service.

## The test (`e2e/e2e_test.go`)

`newHarness(t)` boots `server.ts` against a real MySQL 8 + Redis 7 (Testcontainers). The test is a short script — create alice and bob, deactivate a nonexistent user, deactivate alice, deactivate alice again, `Assert` — asserting only HTTP responses while the golden pins each step's whole state delta. Normalization is explicit: `LabelKeys("ID", "id", "user_id")` correlates the user id across response, audit FK and cache; `TimeKeys("created_at")` tags the per-run timestamp; a one-line custom scrubber pins the volatile `Date` header.

```yaml
- name: create_alice
  response: {httpcode: 200, headers: {Content-Type: application/json, Date: <HTTP-DATE>, ...}, body: {id: <ID_1>, ...}}
  mysql:
    added:
      users:     [{id: <ROWID>, name: alice, status: active, created_at: '<TIME:YYYY-MM-DDTHH:mm:ssZ>'}]
      audit_log: [{action: create, id: <ROWID>, user_id: <ID_1>, created_at: '<TIME:…>'}]  # you didn't assert this — and the FK correlates
  redis:
    added:
      - {key: 'user:1', value: '{"id":"<ID_1>",...}', ttl: <TTL_NONE>}   # ...or this write-through cache
# ... create_bob → <ID_2> ...
- name: deactivate_missing        # POST /users/999/deactivate
  response: {httpcode: 404, body: {error: user not found}}
  # no mysql/redis diff — the rejected request wrote NOTHING, and the golden pins that too
- name: deactivate_alice
  response: {httpcode: 200, body: {id: <ID_1>, status: inactive}}
  mysql:
    updated:
      users: [{key: {name: alice}, changes: {status: {from: active, to: inactive}}}]  # cell-level
    added:
      audit_log: [{action: deactivate, id: <ROWID>, user_id: <ID_1>}]
  redis:
    removed:
      - {key: 'user:1', value: '...', ttl: <TTL_NONE>}             # cache invalidation, caught
- name: deactivate_again
  response: {httpcode: 409, body: {error: user already inactive}}
  # again no diff — the already-inactive rejection wrote NOTHING
```

None of the audit rows, cache write or invalidation were asserted, yet change any and the golden fails. In-place updates render as an `updated` entry showing only what changed (`status: active → inactive`), not a removed+added pair. The two rejected requests (404, 409) pin an *empty* diff — an error path leaking a stray write would break the golden.

## Extending it

Both normalization and stores are pluggable — this example shows the seams:

- **Normalization** is a chain of `Scrubber`s (`Options.Normalize`). Ready-made: `LabelKeys(cat, keys…)` for id correlation (integer or UUID), `TimeKeys(keys…)` / `TimeValues()` for ISO-8601 / SQL times, `FormatKeys(fn, keys…)` for your own tag. A raw `func(n, key, value) (any, bool)` handles anything else (the `Date`-header scrubber above).
- **Stores** are driver modules built with a typed config and passed to `New` (`mysql.With(...)`, `redis.With(...)`). Add a backend (Kafka, MongoDB, …) by implementing the small `StoreDriver` / `StoreCase` / `Snapshot` interfaces and a `With(Config)` constructor; no registration, no blank import.

## Run it

Needs a Docker daemon and the [Bun runtime](https://bun.sh) (`curl -fsSL https://bun.sh/install | bash`).

```bash
cd examples/bun/e2e
go test ./...                  # assert against the committed golden
go test ./... -update-golden   # regenerate the golden after intended changes
```
