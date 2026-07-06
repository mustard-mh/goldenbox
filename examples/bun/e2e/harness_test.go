package e2e_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/mustard-mh/goldenbox"
	"github.com/mustard-mh/goldenbox/modules/mysql"
	"github.com/mustard-mh/goldenbox/modules/redis"
)

// newHarness boots a MySQL + Redis harness driving ../server.ts and registers
// its teardown on t. Skips when Bun or Docker is unavailable.
func newHarness(t *testing.T) *goldenbox.Harness {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test needs a Docker daemon and Bun")
	}
	if _, err := exec.LookPath("bun"); err != nil {
		t.Skip("install the Bun runtime first (https://bun.sh)")
	}
	svcDir, _ := filepath.Abs("..")
	db := mysql.With(mysql.Config{Image: "mysql:8"})
	cache := redis.With(redis.Config{Image: "redis:7"})
	h, err := goldenbox.New(t.Context(), goldenbox.Options{
		SetupCase: func(ctx context.Context, c *goldenbox.Case) error {
			return db.Of(c).ApplySchema(filepath.Join(svcDir, "schema.sql"))
		},
		// Choose exactly what to normalize: correlate the user id, tag created_at,
		// and pin the volatile Date header. (Swap TimeKeys for FormatKeys, or add
		// TimeValues(), to customize.)
		Normalize: goldenbox.Rules{Scrubbers: []goldenbox.Scrubber{
			goldenbox.LabelKeys("ID", "id", "user_id"),
			goldenbox.TimeKeys("created_at"),
			func(n *goldenbox.Normalizer, key string, v any) (any, bool) {
				if key == "Date" {
					return "<HTTP-DATE>", true
				}
				return nil, false
			},
		}},
		Target: goldenbox.ExecTargetOptions{
			Dir:     svcDir,
			RunArgs: []string{"bun", "server.ts"},
			Env: func(tc goldenbox.TargetContext) ([]string, error) {
				my, rd := db.Of(tc.Case), cache.Of(tc.Case)
				return []string{
					"PORT=" + tc.Port,
					"DB_HOST=" + my.Host, "DB_PORT=" + my.Port, "DB_USER=" + my.User,
					"DB_PASS=" + my.Password, "DB_NAME=" + my.Database,
					"REDIS_HOST=" + rd.Host, "REDIS_PORT=" + rd.Port, "REDIS_DB=" + strconv.Itoa(rd.DB),
				}, nil
			},
		},
	}, db, cache)
	if err != nil {
		t.Skipf("cannot start harness (Docker unavailable?): %v", err)
	}
	// t.Context() is already canceled by cleanup time, so teardown uses a fresh one.
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	return h
}
