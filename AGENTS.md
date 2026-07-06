# goldenbox — agent guide

Black-box golden testing library with full state diffs. Read `README.md` for the concept; this file is about working on the codebase.

## Layout (multi-module, testcontainers-go style)

- `/` — core module (`github.com/mustard-mh/goldenbox`). Deps: stdlib + yaml.v3 + go-cmp ONLY. Never import a database/container client here — that is what driver modules are for.
  - `goldenbox.go` — Harness/Case lifecycle + Options; `Options.SetupCase` seeds each fresh case. Stores are passed to `New` as `...StoreDriver`.
  - `store.go` — StoreDriver/StoreCase/Snapshot interfaces (no registry; drivers built via their own `With` and passed to `New`).
  - `target.go` — launch-method registry (`RegisterTarget`) + built-in exec target (static `RunArgs` per case, env from `Options.Target.Env(TargetContext)`, no build step) + subprocess plumbing.
  - `recorder.go` — step recorder (quiesce → snapshot → diff) + golden glue (delegates to `golden/`).
  - `normalize.go` — `Rules` = ordered `[]Scrubber` chain + array-collapse threshold. NO baked-in field names, NO auto-detection: the consumer says what to normalize (`LabelKeys`/`TimeKeys`/`TimeValues`/`FormatKeys` or a raw `Scrubber`). One `Normalizer` per golden, shared across DB/Redis/response so one real id → the same placeholder everywhere; no global rule state. Drivers run cells through `Value` and use `IsVolatile` to keep per-run cells out of the row sort key.
  - `client.go` — HTTP client (`Get`/`Post`/`Send`) + `Response{HTTPCode, Headers, Body}`: no envelope assumption (body → `any` or raw string); headers normalized by the same scrubbers as the body (only RFC 7230 hop-by-hop dropped).
  - `mock.go` — mock upstream: generic mux, no built-in routes. Consumers register via `Mock.HandleFunc` (+ `WriteJSON`). Built-in: `Calls`/`HitCount`, fault injection (`SetFault`), unmatched policy (`FailUnmatched`/`PassthroughUnmatched`). Shared across parallel cases — narrow per-case via `Fault.Match`. Never bake consumer-specific upstream behavior into the library.
- `golden/` — golden-file subpackage (standalone: code-driven `Snapshot`/`Case`, `Assert`/`AssertAny`, `CleanObsolete`). Owns the `-update-golden` flag, CI guard and orphan tracking; core delegates, never redefines the flag.
- `modules/{mysql,postgres,redis}` — driver modules with their own `go.mod` (heavy deps here). Each exposes a typed `Config` + `With(Config) goldenbox.StoreDriver`; `With` returns a `Store` whose `.Of(c) Handle` reads a case's connection (consumer keeps the `With` value, no name strings — two of a kind told apart by object). Deltas are keyed by `Config.Name`. Quiescence: mysql binlog (canal) → `CHECKSUM TABLE` fallback; redis keyspace notifications → scan fallback; postgres fingerprint-only (no binlog). SQL drivers fold PK-paired row changes into `updated` entries (real PK rides under a hidden NUL column, never reaches the golden). mysql/postgres share NO code (per-driver `diff.go` copies, so a consumer pulls only its backend's deps) but pin an IDENTICAL delta shape — hoist the shared diff into a common module only if a third SQL driver lands.
- `examples/bun` — runnable Bun service (zero deps) + its e2e module; the cleanest showcase of "pins what you didn't assert". Skips without Docker + the Bun runtime.
- `go.work` — local dev across the modules (re-evaluate before publishing).

## Commands

- `just check` — build + vet + gofmt across all modules (no containers)
- `just test` — unit tests across all modules

## Conventions

- Golden-file format is a contract: changes that alter existing consumers' golden bytes must be called out loudly in the change description.
- The library dogfoods its own golden engine — core/driver unit tests are code-driven golden tests (`golden.Snapshot`, output under `./testdata/<component>`), regenerated via `go test -update-golden`. EXCEPTION: `golden/golden_test.go` is the trust root and uses plain assertions — never assert the golden engine through itself. Behavioral/timing tests and func-valued inputs a golden can't express also stay plain.
- Everything exported is API. Pre-1.0 but has a real consumer — prefer adding Options fields (zero value = old behavior) over changing signatures.
- No mutable process globals — do not add any. `New()` sets nothing process-wide.
- Driver modules may depend on core; core must never depend on driver modules.
- Comments sparse: only non-obvious constraints or intent; don't narrate the code or explain a change (that's the commit message).
- Markdown is not hard-wrapped; keep each paragraph/bullet on one line.

## Open work

See `TODO.local.md` for the prioritized backlog.
