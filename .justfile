# goldenbox — multi-module dev commands

default:
  just -l

# build + vet + gofmt check across all modules (no containers)
check:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in . modules/mysql modules/redis modules/postgres examples/bun/e2e; do
        echo "==> $m"
        (cd "$m" && go build ./... && go vet ./...)
    done
    fmt_out=$(gofmt -l . 2>/dev/null | grep -v '^\.git' || true)
    if [ -n "$fmt_out" ]; then echo "gofmt needed:"; echo "$fmt_out"; exit 1; fi

# unit tests across all modules (containerless; -short skips driver integration tests)
test:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in . modules/mysql modules/redis modules/postgres; do
        echo "==> $m"
        (cd "$m" && go test ./... -short -count=1)
    done

# driver + example integration tests against real containers (needs a Docker
# daemon; the Bun example also needs the Bun runtime — https://bun.sh)
itest:
    #!/usr/bin/env bash
    set -euo pipefail
    for m in modules/mysql modules/redis modules/postgres examples/bun/e2e; do
        echo "==> $m"
        (cd "$m" && go test ./... -count=1 -timeout 400s)
    done

# format all Go files
fmt:
    gofmt -w . 2>/dev/null || true
