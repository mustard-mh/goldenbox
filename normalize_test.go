package goldenbox

// Dogfooding: these are code-driven golden tests through the golden subpackage;
// only the expected OUTPUT lives in ./testdata. Regenerate with -update-golden.

import (
	"testing"
	"time"

	"github.com/mustard-mh/goldenbox/golden"
)

// refNow pins the run clock so wall=now±Nh markers are deterministic.
var refNow = time.Date(2026, 6, 24, 15, 30, 0, 0, time.UTC)

func fixedNormalizer(refNow time.Time, rules Rules) *Normalizer {
	return &Normalizer{
		rules:  rules.withDefaults(),
		refNow: refNow,
		labels: map[idKey]string{},
		next:   map[string]int{},
	}
}

func TestTimeFormatTag(t *testing.T) {
	// The same instant from a UTC vs Asia/Shanghai producer must diverge
	// (wall=now+0h vs +8h); explicit-offset, date-only and far-from-now differ.
	values := []string{
		"2026-06-24 15:30:00",       // naive UTC
		"2026-06-24 23:30:00",       // same instant, +08:00 producer
		"2000-01-01 00:00:00",       // far from now: no wall marker
		"2099-12-31 23:59:59",       // far from now
		"2026-06-24",                // date-only
		"2026-06-24T15:30:00Z",      // explicit offset
		"2026-06-24T23:30:00+08:00", // explicit non-UTC offset
		"2026-06-24 15:30:00.123",   // millisecond precision
	}
	golden.Snapshot(t, "testdata/normalize", []golden.Case[[]string]{
		{Name: "timetag", In: values},
	}, func(t *testing.T, in []string) map[string]string {
		out := map[string]string{}
		for _, s := range in {
			out[s] = timeFormatTag(s, refNow)
		}
		return out
	})
}

var valueRules = Rules{Scrubbers: []Scrubber{
	LabelKeys("ID", "id"),
	LabelKeys("UID", "uid"),
	TimeKeys("create_time"),
}}

func TestNormalizeValue(t *testing.T) {
	golden.Snapshot(t, "testdata/normalize", []golden.Case[any]{
		// uid and owner.uid share <UID_1>; unlisted keys (the timestamp in
		// "message") pass through — no auto-detection.
		{Name: "value_ids_times", In: map[string]any{
			"id":          float64(101),
			"uid":         7, // same real uid as owner.uid
			"owner":       map[string]any{"id": 102, "uid": 7},
			"create_time": "2026-06-24 15:30:00",
			"message":     "job started at 2026-06-24T15:30:00Z, retry later",
			"ratio":       0.5,
			"count":       float64(3),
			"tags":        []any{"a", "b"},
		}},
		// >30 elements collapse to <ARRAY len=N>.
		{Name: "value_array_collapse", In: map[string]any{
			"big":   intSeq(31),
			"small": []any{1, 2, 3},
		}},
	}, func(t *testing.T, in any) any {
		return fixedNormalizer(refNow, valueRules).Value("", in)
	})
}

func TestNormalizeRequest(t *testing.T) {
	golden.Snapshot(t, "testdata/normalize", []golden.Case[any]{
		// Request only collapses integer-valued floats; everything else is verbatim.
		{Name: "request", In: map[string]any{
			"amount":      float64(3),
			"ratio":       1.5,
			"name":        "alice",
			"create_time": "2026-06-24 15:30:00",
			"items":       []any{float64(1), 2.5},
		}},
	}, func(t *testing.T, in any) any {
		return fixedNormalizer(refNow, DefaultRules()).Request(in)
	})
}

func intSeq(n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = i + 1
	}
	return out
}

// These exercise func-valued rules and value shapes a YAML fixture can't express.

func TestScrubberChainOrderAndShortCircuit(t *testing.T) {
	rules := Rules{Scrubbers: []Scrubber{
		func(n *Normalizer, key string, v any) (any, bool) {
			if key == "token" {
				return "<TOKEN>", true
			}
			return nil, false
		},
		LabelKeys("ID", "id"),
	}}
	n := fixedNormalizer(refNow, rules)
	got := n.Value("", map[string]any{
		"token": "abc123",     // first scrubber claims it
		"id":    float64(42),  // falls through to LabelKeys
		"note":  "left as-is", // no scrubber matches
	}).(map[string]any)
	if got["token"] != "<TOKEN>" {
		t.Errorf("token = %v, want <TOKEN>", got["token"])
	}
	if got["id"] != "<ID_1>" {
		t.Errorf("id = %v, want <ID_1>", got["id"])
	}
	if got["note"] != "left as-is" {
		t.Errorf("note = %v, want unchanged", got["note"])
	}
}

func TestLabelIDCorrelatesAcrossValueShapes(t *testing.T) {
	// Keyed on the raw value: a number and its string form correlate, and a UUID
	// labels like any other.
	n := fixedNormalizer(refNow, Rules{Scrubbers: []Scrubber{
		LabelKeys("ID", "id", "ref"),
		LabelKeys("U", "uuid"),
	}})
	got := n.Value("", map[string]any{
		"id":   float64(1000000), // collapsed to int → stable "1000000", not "1e+06"
		"ref":  "1000000",        // same raw value as id → same placeholder
		"uuid": "550e8400-e29b-41d4-a716-446655440000",
	}).(map[string]any)
	if got["id"] != "<ID_1>" || got["ref"] != "<ID_1>" {
		t.Errorf("id=%v ref=%v, want both <ID_1> (number/string correlate)", got["id"], got["ref"])
	}
	if got["uuid"] != "<U_1>" {
		t.Errorf("uuid = %v, want <U_1>", got["uuid"])
	}
}

func TestTimeValuesFormatsCommonTimestamps(t *testing.T) {
	// Recognizes common ISO / SQL datetimes by value, whole or embedded.
	n := fixedNormalizer(refNow, Rules{Scrubbers: []Scrubber{TimeValues()}})
	got := n.Value("", map[string]any{
		"iso":      "2026-06-24T15:30:00Z",
		"sql":      "2026-06-24 15:30:00",
		"embedded": "job started at 2026-06-24T15:30:00Z, retry later",
		"plain":    "not a time",
	}).(map[string]any)
	if got["iso"] != "<TIME:YYYY-MM-DDTHH:mm:ssZ>" {
		t.Errorf("iso = %v", got["iso"])
	}
	if got["sql"] != "<TIME:YYYY-MM-DD HH:mm:ss wall=now+0h>" {
		t.Errorf("sql = %v", got["sql"])
	}
	if got["embedded"] != "job started at <TIME:YYYY-MM-DDTHH:mm:ssZ>, retry later" {
		t.Errorf("embedded = %v", got["embedded"])
	}
	if got["plain"] != "not a time" {
		t.Errorf("plain = %v, want unchanged", got["plain"])
	}
}

func TestFormatKeysCustomFormatter(t *testing.T) {
	// Formatting is extendable: supply your own instead of TimeTag.
	n := fixedNormalizer(refNow, Rules{Scrubbers: []Scrubber{
		FormatKeys(func(string) string { return "<TS>" }, "created_at"),
	}})
	got := n.Value("", map[string]any{"created_at": "2026-06-24T15:30:00Z"}).(map[string]any)
	if got["created_at"] != "<TS>" {
		t.Errorf("created_at = %v, want <TS>", got["created_at"])
	}
}
