// Package redis is the goldenbox Redis store driver: one testcontainer per
// process, one logical DB (0..15) per case (capping open cases at 16), so a
// parallel case's writes never touch this case's keyspace.
package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/mustard-mh/goldenbox"
)

const defaultName = "redis"

// Config is the Redis store's typed configuration, passed via With.
type Config struct {
	// Name is the golden section key and the key for redis.Of; default "redis".
	// Set it to run several Redis stores in one harness.
	Name string
	// Image is the container image, e.g. "redis:7" — required (no default).
	Image string
}

// Store is a configured Redis store: pass it to goldenbox.New.
type Store interface {
	goldenbox.StoreDriver
	// Of returns this store's slice for case c (panics if c has no such slice).
	Of(c *goldenbox.Case) Handle
}

// With builds a configured Redis store.
func With(cfg Config) Store {
	if cfg.Name == "" {
		cfg.Name = defaultName
	}
	return &redisStore{cfg: cfg}
}

type redisStore struct {
	cfg       Config
	container *tcredis.RedisContainer
	host      string
	port      string
	reused    bool
	// notify: container accepted keyspace-notification config, so cases use an
	// event-count Stamp instead of full keyspace scans.
	notify bool

	mu   sync.Mutex
	free []int // logical DB indexes currently available (0..15)
}

func (s *redisStore) Name() string { return s.cfg.Name }

func (s *redisStore) Start(ctx context.Context, h *goldenbox.Harness) error {
	s.reused = h.ReuseEnabled()
	for i := 0; i < 16; i++ {
		s.free = append(s.free, i)
	}

	var opts []testcontainers.ContainerCustomizer
	if s.reused {
		os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
		opts = append(opts, testcontainers.WithReuseByName(h.Options().ReuseNamePrefix+"-"+s.cfg.Name))
	}
	if s.cfg.Image == "" {
		return fmt.Errorf(`redis: Config.Image is required (e.g. "redis:7")`)
	}
	c, err := tcredis.Run(ctx, s.cfg.Image, opts...)
	if err != nil {
		return fmt.Errorf("start redis container: %w", err)
	}
	s.container = c
	if s.host, err = c.Host(ctx); err != nil {
		return fmt.Errorf("redis host: %w", err)
	}
	port, err := c.MappedPort(ctx, "6379")
	if err != nil {
		return fmt.Errorf("redis port: %w", err)
	}
	s.port = port.Port()

	// Keyspace notifications power the O(1) event-count Stamp. Set every Start —
	// the config does not survive a container restart. Failure only disables the
	// optimization; cases fall back to scan-based stamps.
	admin := goredis.NewClient(&goredis.Options{Addr: net.JoinHostPort(s.host, s.port)})
	s.notify = admin.ConfigSet(ctx, "notify-keyspace-events", "KA").Err() == nil
	_ = admin.Close()
	return nil
}

func (s *redisStore) NewCase(ctx context.Context, caseID int) (goldenbox.StoreCase, error) {
	s.mu.Lock()
	if len(s.free) == 0 {
		s.mu.Unlock()
		return nil, fmt.Errorf("all 16 Redis logical DBs are held by open cases; Close a case first (or lower test parallelism)")
	}
	idx := s.free[len(s.free)-1]
	s.free = s.free[:len(s.free)-1]
	s.mu.Unlock()

	cli := goredis.NewClient(&goredis.Options{Addr: net.JoinHostPort(s.host, s.port), DB: idx})
	// Clear leftover keys from a crashed prior run against reused containers.
	if err := cli.FlushDB(ctx).Err(); err != nil {
		_ = cli.Close()
		s.mu.Lock()
		s.free = append(s.free, idx)
		s.mu.Unlock()
		return nil, fmt.Errorf("flush redis db %d: %w", idx, err)
	}
	c := &redisCase{store: s, idx: idx, cli: cli}
	if s.notify {
		// Subscribe before the target process exists, so no write can precede
		// the subscription.
		c.watch = watchKeyspace(ctx, cli, idx)
	}
	return c, nil
}

func (s *redisStore) Stop(ctx context.Context) error {
	if s.reused || s.container == nil {
		return nil
	}
	return s.container.Terminate(ctx)
}

type redisCase struct {
	store *redisStore
	idx   int
	cli   *goredis.Client
	watch *keyWatcher // nil = notifications unavailable, Stamp scans instead
}

// keyWatcher counts keyspace-notification events for one logical DB as a
// write-state fingerprint: equal counts ⇒ no write delivered in between, which
// is Stamp's contract at O(1) instead of a full keyspace scan per poll.
// Delivery is async but pub/sub latency on a local container is far below the
// recorder's poll interval.
type keyWatcher struct {
	ps    *goredis.PubSub
	count atomic.Int64
	ok    atomic.Bool
}

// watchKeyspace subscribes to db idx's keyspace events, returning nil (scan
// fallback) if the subscription cannot be confirmed. Events dropped during a
// reconnect are lost — acceptable on a local container, where a drop means the
// container died and the test is failing anyway.
func watchKeyspace(ctx context.Context, cli *goredis.Client, idx int) *keyWatcher {
	ps := cli.PSubscribe(ctx, fmt.Sprintf("__keyspace@%d__:*", idx))
	if _, err := ps.Receive(ctx); err != nil {
		_ = ps.Close()
		return nil
	}
	w := &keyWatcher{ps: ps}
	w.ok.Store(true)
	go func() {
		for range ps.Channel() {
			w.count.Add(1)
		}
		w.ok.Store(false) // channel closes only via ps.Close or a fatal error
	}()
	return w
}

// Client returns the case-scoped Redis client.
func (c *redisCase) Client() *goredis.Client { return c.cli }

// Handle is a typed view of a Redis store's per-case slice, returned by Of.
type Handle struct {
	Host, Port string
	DB         int // the case's logical DB index (0..15)
	cli        *goredis.Client
}

// Addr returns "host:port" for a client dialer.
func (h Handle) Addr() string { return net.JoinHostPort(h.Host, h.Port) }

// Client returns the case-scoped Redis client for the case's logical DB.
func (h Handle) Client() *goredis.Client { return h.cli }

// Of returns this store's slice for case c.
func (s *redisStore) Of(c *goldenbox.Case) Handle {
	rc, ok := c.LookupStore(s.cfg.Name).(*redisCase)
	if !ok {
		panic(fmt.Sprintf("redis: store %q is not in this case", s.cfg.Name))
	}
	return Handle{Host: rc.store.host, Port: rc.store.port, DB: rc.idx, cli: rc.cli}
}

func (c *redisCase) Snapshot(ctx context.Context, n *goldenbox.Normalizer) (goldenbox.Snapshot, error) {
	return collectRedisSnapshot(ctx, c.cli, n)
}

// Stamp returns the O(1) keyspace-notification event count, or falls back to a
// sorted scan of this case's logical DB when notifications are unavailable.
// Per-DB either way, so a parallel case's writes never delay this quiescence.
func (c *redisCase) Stamp(ctx context.Context) (string, error) {
	if c.watch != nil && c.watch.ok.Load() {
		return fmt.Sprintf("<events:%d>", c.watch.count.Load()), nil
	}
	var keys []string
	var cursor uint64
	for {
		batch, next, err := c.cli.Scan(ctx, cursor, "*", 100).Result()
		if err != nil {
			return "", fmt.Errorf("scan: %w", err)
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		val, err := redisValue(ctx, c.cli, key)
		if err != nil {
			if err == goredis.Nil {
				continue // expired between SCAN and read
			}
			return "", fmt.Errorf("read %q: %w", key, err)
		}
		fmt.Fprintf(&b, "%s=%v;", key, val)
	}
	return b.String(), nil
}

func (c *redisCase) Reset(ctx context.Context) error {
	if err := c.cli.FlushDB(ctx).Err(); err != nil {
		return fmt.Errorf("flush redis db %d: %w", c.idx, err)
	}
	return nil
}

func (c *redisCase) Close(ctx context.Context) error {
	if c.watch != nil {
		_ = c.watch.ps.Close()
	}
	flushErr := c.cli.FlushDB(ctx).Err()
	closeErr := c.cli.Close()
	c.store.mu.Lock()
	c.store.free = append(c.store.free, c.idx)
	c.store.mu.Unlock()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// Date segments embedded in key names (yyyy-mm-dd or 8-digit yyyymmdd) change
// per run, so normalize them to <DATE>.
var (
	reDateKeyDash = regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)
	reDateKey8    = regexp.MustCompile(`\b\d{8}\b`)
)

func normalizeRedisKey(key string) string {
	key = reDateKeyDash.ReplaceAllString(key, "<DATE>")
	key = reDateKey8.ReplaceAllString(key, "<DATE>")
	return key
}

// collectRedisSnapshot dumps all keys and values, normalizing keys, values and
// TTLs into a snapshot sorted by (key, value, ttl).
func collectRedisSnapshot(ctx context.Context, rdc *goredis.Client, n *goldenbox.Normalizer) (RedisSnapshot, error) {
	var keys []string
	var cursor uint64
	for {
		batch, next, err := rdc.Scan(ctx, cursor, "*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	// Sort before reading: normalization assigns id placeholders to JSON values
	// as it goes, so a non-deterministic SCAN order would swap assignments
	// across runs.
	sort.Strings(keys)
	raw := make([]rawRedisEntry, 0, len(keys))
	for _, key := range keys {
		// Key may expire between SCAN and read (goredis.Nil); it is genuinely
		// gone, so skip rather than fail the snapshot.
		val, err := redisValue(ctx, rdc, key)
		if errors.Is(err, goredis.Nil) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read key %q: %w", key, err)
		}
		ttl, err := rdc.PTTL(ctx, key).Result()
		if errors.Is(err, goredis.Nil) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("ttl key %q: %w", key, err)
		}
		raw = append(raw, rawRedisEntry{key: key, val: val, ttl: ttl})
	}
	return normalizeRedisEntries(raw, n), nil
}

// rawRedisEntry is one fetched key before normalization.
type rawRedisEntry struct {
	key string
	val any
	ttl time.Duration
}

// normalizeRedisEntries is the client-free core of collectRedisSnapshot,
// normalizing keys, values and TTLs then sorting by (key, value, ttl). Callers
// must pass raw sorted by key so id placeholder numbering is deterministic.
func normalizeRedisEntries(raw []rawRedisEntry, n *goldenbox.Normalizer) RedisSnapshot {
	out := make(RedisSnapshot, 0, len(raw))
	for _, e := range raw {
		out = append(out, RedisEntry{
			Key:   normalizeRedisKey(e.key),
			Value: normalizeRedisValue(e.val, n),
			TTL:   bucketTTL(e.ttl),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		if vi, vj := fmt.Sprint(out[i].Value), fmt.Sprint(out[j].Value); vi != vj {
			return vi < vj
		}
		return out[i].TTL < out[j].TTL
	})
	return out
}

// normalizeRedisValue recursively normalizes a value that is itself JSON (time
// and id fields, compactly re-serialized); everything else passes through.
func normalizeRedisValue(val any, n *goldenbox.Normalizer) any {
	s, ok := val.(string)
	if !ok {
		return val
	}
	trimmed := strings.TrimSpace(s)
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		var obj any
		if err := json.Unmarshal([]byte(s), &obj); err == nil {
			return marshalNoEscape(n.Value("", obj))
		}
	}
	return s
}

// marshalNoEscape serializes to compact JSON without HTML escaping, so
// placeholders like <TIME:…> stay literal.
func marshalNoEscape(v any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return fmt.Sprintf("%v", v)
	}
	return strings.TrimRight(buf.String(), "\n")
}

func redisValue(ctx context.Context, rdc *goredis.Client, key string) (any, error) {
	typ, err := rdc.Type(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	switch typ {
	case "string":
		return rdc.Get(ctx, key).Result()
	case "hash":
		return rdc.HGetAll(ctx, key).Result()
	case "set":
		members, err := rdc.SMembers(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		sort.Strings(members)
		return members, nil
	case "list":
		return rdc.LRange(ctx, key, 0, -1).Result()
	case "zset":
		return rdc.ZRangeWithScores(ctx, key, 0, -1).Result()
	default:
		return "<" + typ + ">", nil
	}
}

// bucketTTL normalizes the remaining TTL into stable buckets, absorbing the
// second-level drift between write and snapshot. OTHER is a distinct bucket so
// an unexpected TTL is never silently folded into a neighbor.
func bucketTTL(d time.Duration) string {
	switch {
	case d < 0:
		return PlaceholderTTLNone
	case d <= 5*time.Minute:
		return PlaceholderTTLShort
	case d >= 12*time.Hour && d <= 2*24*time.Hour:
		return PlaceholderTTL1D
	case d > 29*24*time.Hour:
		return PlaceholderTTL30D
	default:
		return PlaceholderTTLOther
	}
}

// RedisEntry is a single normalized Redis key/value/ttl record.
type RedisEntry struct {
	Key   string `yaml:"key"`
	Value any    `yaml:"value"`
	TTL   string `yaml:"ttl"`
}

// RedisSnapshot is an ordered set of RedisEntry.
type RedisSnapshot []RedisEntry

// MarshalYAML renders each entry compactly: a scalar value collapses to one
// flow-style line (see goldenbox.CompactNode), a structured value stays block.
func (s RedisSnapshot) MarshalYAML() (any, error) {
	type plain RedisSnapshot
	return goldenbox.CompactNode(plain(s))
}

// Placeholder TTL tokens written into normalized Redis snapshots.
const (
	PlaceholderTTLNone  = "<TTL_NONE>"
	PlaceholderTTLShort = "<TTL_SHORT>"
	PlaceholderTTL1D    = "<TTL_1D>"
	PlaceholderTTL30D   = "<TTL_30D>"
	PlaceholderTTLOther = "<TTL_OTHER>"
)

// RedisDelta is the prev→cur Redis delta (by key+value+ttl entry).
type RedisDelta struct {
	Added   RedisSnapshot `yaml:"added,omitempty"`
	Removed RedisSnapshot `yaml:"removed,omitempty"`
}

// IsEmpty reports whether the delta contains no change.
func (d RedisDelta) IsEmpty() bool { return len(d.Added) == 0 && len(d.Removed) == 0 }

// DiffRedis computes the prev→cur entry multiset difference (comparing
// key/value/ttl triples).
func DiffRedis(prev, cur RedisSnapshot) RedisDelta {
	delta := RedisDelta{}
	prevCount := map[string]int{}
	for _, e := range prev {
		prevCount[goldenbox.CanonicalJSON(e)]++
	}
	for _, e := range cur {
		k := goldenbox.CanonicalJSON(e)
		if prevCount[k] > 0 {
			prevCount[k]--
			continue
		}
		delta.Added = append(delta.Added, e)
	}
	curCount := map[string]int{}
	for _, e := range cur {
		curCount[goldenbox.CanonicalJSON(e)]++
	}
	for _, e := range prev {
		k := goldenbox.CanonicalJSON(e)
		if curCount[k] > 0 {
			curCount[k]--
			continue
		}
		delta.Removed = append(delta.Removed, e)
	}
	return delta
}

// Diff implements the Snapshot interface for RedisSnapshot (prev may be nil).
func (s RedisSnapshot) Diff(prev goldenbox.Snapshot) (any, bool) {
	var p RedisSnapshot
	if prev != nil {
		p = prev.(RedisSnapshot)
	}
	d := DiffRedis(p, s)
	return d, d.IsEmpty()
}
