package redis

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/mustard-mh/goldenbox"
	"github.com/mustard-mh/goldenbox/golden"
)

// Code-driven golden tests for the containerless Redis normalization and diff
// logic. Regenerate with `go test -update-golden`. Embedded timestamps are
// timezone-qualified (Z) so the <TIME:…> tags stay deterministic without a
// pinned run clock.

const goldenDir = "../../testdata/redis"

func TestMain(m *testing.M) {
	code := m.Run()
	if code == 0 {
		for _, f := range golden.CleanObsolete(goldenDir) {
			fmt.Println("removed orphan golden:", f)
		}
	}
	os.Exit(code)
}

func TestRedisNormalizeGolden(t *testing.T) {
	golden.Snapshot(t, goldenDir, []golden.Case[[]rawRedisEntry]{
		// Embedded-JSON values are recursively normalized by the configured
		// scrubbers (id/uid fields → placeholders, create_time → <TIME:…>,
		// re-serialized compactly); plain strings pass through. The same real id
		// in two keys maps to one placeholder.
		{Name: "norm_embedded_json", In: []rawRedisEntry{
			{key: "user:session", val: `{"id":12345,"uid":999,"create_time":"2026-06-24T15:30:00Z","name":"alice"}`, ttl: 30 * time.Minute},
			{key: "cache:greeting", val: "hello", ttl: noExpiry},
			{key: "user:profile", val: `{"id":12345,"tier":"pro"}`, ttl: noExpiry},
		}},
		// TTLs collapse to stable buckets; date segments in key names → <DATE>.
		{Name: "norm_ttl_and_datekey", In: []rawRedisEntry{
			{key: "counter:2026-06-24", val: "42", ttl: 200 * time.Second}, // <TTL_SHORT>
			{key: "counter:20260624", val: "7", ttl: 24 * time.Hour},       // <TTL_1D>
			{key: "token:permanent", val: "xyz", ttl: noExpiry},            // <TTL_NONE>
			{key: "archive:cold", val: "blob", ttl: 31 * 24 * time.Hour},   // <TTL_30D>
			{key: "session:live", val: "on", ttl: 30 * time.Minute},        // <TTL_OTHER>
		}},
	}, func(t *testing.T, in []rawRedisEntry) RedisSnapshot {
		// Normalize in key order, as collectRedisSnapshot does, so id placeholder
		// numbering is deterministic regardless of case listing order.
		sort.SliceStable(in, func(i, j int) bool { return in[i].key < in[j].key })
		n := goldenbox.NewNormalizer(goldenbox.Rules{Scrubbers: []goldenbox.Scrubber{
			goldenbox.LabelKeys("ID", "id"),
			goldenbox.LabelKeys("UID", "uid"),
			goldenbox.TimeKeys("create_time"),
		}})
		return normalizeRedisEntries(in, n)
	})
}

type redisDiffInput struct{ prev, cur RedisSnapshot }

func TestRedisDiffGolden(t *testing.T) {
	golden.Snapshot(t, goldenDir, []golden.Case[redisDiffInput]{
		// Entry-level multiset diff over normalized (key, value, ttl) triples: a
		// new key added, a dropped key removed, and a value change surfacing as
		// old-triple removed + new-triple added (Redis entries have no PK to pair).
		{Name: "diff_basic", In: redisDiffInput{
			prev: RedisSnapshot{
				{Key: "user:1", Value: "alice", TTL: "<TTL_NONE>"},
				{Key: "user:2", Value: "bob", TTL: "<TTL_NONE>"},
				{Key: "session:1", Value: "active", TTL: "<TTL_SHORT>"},
			},
			cur: RedisSnapshot{
				{Key: "user:1", Value: "alice", TTL: "<TTL_NONE>"},
				{Key: "user:2", Value: "bob-renamed", TTL: "<TTL_NONE>"},
				{Key: "session:2", Value: "active", TTL: "<TTL_SHORT>"},
			},
		}},
	}, func(t *testing.T, in redisDiffInput) RedisDelta {
		return DiffRedis(in.prev, in.cur)
	})
}

// noExpiry is a key with no TTL: redis PTTL returns a negative duration, which
// buckets to <TTL_NONE>.
const noExpiry = -1 * time.Millisecond
