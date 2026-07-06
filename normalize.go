package goldenbox

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"time"
)

// Scrubber replaces a scalar value, given the golden's Normalizer and the
// value's key ("" at a document root). It returns (replacement, true) to handle
// the value, or (_, false) to fall through. Scrubbers run on response values
// and store cells alike. See LabelKeys / TimeKeys / TimeValues / FormatKeys.
type Scrubber func(n *Normalizer, key string, value any) (any, bool)

// LabelKeys labels each listed key's value with a stable <CATEGORY_n>
// placeholder (see LabelID) — the same real value correlates everywhere.
func LabelKeys(category string, keys ...string) Scrubber {
	set := keySet(keys)
	return func(n *Normalizer, key string, v any) (any, bool) {
		if set[key] {
			return n.LabelID(category, v), true
		}
		return nil, false
	}
}

// TimeKeys tags each listed key's string value with a <TIME:layout> tag.
func TimeKeys(keys ...string) Scrubber {
	set := keySet(keys)
	return func(n *Normalizer, key string, v any) (any, bool) {
		if s, ok := v.(string); ok && set[key] {
			return n.TimeTag(s), true
		}
		return nil, false
	}
}

// TimeValues tags every ISO-8601 / SQL datetime found inside a string value
// (whole or embedded) — the common formats, without a key list. Opt-in.
func TimeValues() Scrubber {
	return func(n *Normalizer, key string, v any) (any, bool) {
		s, ok := v.(string)
		if !ok {
			return nil, false
		}
		if out := reTimestamp.ReplaceAllStringFunc(s, n.TimeTag); out != s {
			return out, true
		}
		return nil, false
	}
}

// FormatKeys runs a custom formatter over each listed key's string value — the
// extension point when TimeTag's tag is not the shape you want.
func FormatKeys(format func(raw string) string, keys ...string) Scrubber {
	set := keySet(keys)
	return func(n *Normalizer, key string, v any) (any, bool) {
		if s, ok := v.(string); ok && set[key] {
			return format(s), true
		}
		return nil, false
	}
}

func keySet(keys []string) map[string]bool {
	set := make(map[string]bool, len(keys))
	for _, k := range keys {
		set[k] = true
	}
	return set
}

// Rules configure normalization (Options.Normalize). No baked-in names, no
// auto-detection: Scrubbers are the mechanism.
type Rules struct {
	Scrubbers []Scrubber
	// ArrayCollapseThreshold: arrays longer than this collapse to
	// "<ARRAY len=N>". Default 30.
	ArrayCollapseThreshold int
}

// DefaultRules is no scrubbers and the default array threshold.
func DefaultRules() Rules {
	return Rules{ArrayCollapseThreshold: 30}
}

func (r Rules) withDefaults() Rules {
	if r.ArrayCollapseThreshold == 0 {
		r.ArrayCollapseThreshold = 30
	}
	return r
}

// Normalizer is one golden's normalization engine and the driver SDK surface.
// Create one per golden and share it across all of the golden's DB / Redis /
// response normalization, so one real id maps to the same placeholder in each.
type Normalizer struct {
	rules  Rules
	refNow time.Time // run clock (UTC), for wall=now time tags
	labels map[idKey]string
	next   map[string]int
}

type idKey struct {
	cat string
	raw string
}

// NewNormalizer creates a Normalizer with its run clock captured now.
func NewNormalizer(rules Rules) *Normalizer {
	return &Normalizer{
		rules:  rules.withDefaults(),
		refNow: time.Now().UTC(),
		labels: map[idKey]string{},
		next:   map[string]int{},
	}
}

// Rules returns the rule set in effect.
func (n *Normalizer) Rules() Rules { return n.rules }

// LabelID returns the stable per-category placeholder for a raw id value:
// first sight assigns the next number, the same value always returns the same
// placeholder. The value is an opaque key (stringified), so integer and UUID
// ids work identically.
func (n *Normalizer) LabelID(category string, value any) string {
	k := idKey{category, fmt.Sprint(value)}
	if s, ok := n.labels[k]; ok {
		return s
	}
	n.next[category]++
	s := fmt.Sprintf("<%s_%d>", category, n.next[category])
	n.labels[k] = s
	return s
}

// TimeTag converts a time string into a <TIME:layout ...> tag (see timeFormatTag).
func (n *Normalizer) TimeTag(s string) string { return timeFormatTag(s, n.refNow) }

// Value recursively normalizes a JSON-shaped value under key context.
func (n *Normalizer) Value(key string, v any) any {
	switch t := v.(type) {
	case map[string]any:
		// Sorted-key recursion: id numbering is first-sight order, which random
		// map iteration would flake.
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make(map[string]any, len(t))
		for _, k := range keys {
			out[k] = n.Value(k, t[k])
		}
		return out
	case []any:
		if len(t) > n.rules.ArrayCollapseThreshold {
			return fmt.Sprintf("<ARRAY len=%d>", len(t))
		}
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = n.Value(key, val)
		}
		return out
	default:
		return n.scalar(key, t)
	}
}

func (n *Normalizer) scalar(key string, v any) any {
	// Integral float → int: matches YAML/cmp and gives ids a stable string form.
	if f, ok := v.(float64); ok && !math.IsInf(f, 0) && f == math.Trunc(f) {
		v = int(f)
	}
	for _, scrub := range n.rules.Scrubbers {
		if out, ok := scrub(n, key, v); ok {
			return out
		}
	}
	return v
}

// IsVolatile reports whether a scrubber changes (key, value) — i.e. it is a
// per-run value. Drivers use it to keep such cells out of the row sort key. It
// probes on a scratch Normalizer, assigning no real placeholders.
func (n *Normalizer) IsVolatile(key string, value any) bool {
	scratch := &Normalizer{rules: n.rules, refNow: n.refNow, labels: map[idKey]string{}, next: map[string]int{}}
	return fmt.Sprint(scratch.scalar(key, value)) != fmt.Sprint(value)
}

// Request normalizes a request body: only integer-valued floats collapse to
// int; everything else is preserved (request bodies are fixed test inputs).
func (n *Normalizer) Request(body any) any {
	neutral := Normalizer{rules: Rules{ArrayCollapseThreshold: math.MaxInt}}
	return neutral.Value("", body)
}

// Response normalizes the response body and header values through Value.
func (n *Normalizer) Response(resp Response) Response {
	resp.Body = n.Value("", resp.Body)
	if len(resp.Headers) > 0 {
		h := make(map[string]string, len(resp.Headers))
		for k, v := range resp.Headers {
			h[k] = fmt.Sprint(n.Value(k, v))
		}
		resp.Headers = h
	}
	return resp
}

var (
	reDate  = regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)
	reClock = regexp.MustCompile(`\d{2}:\d{2}:\d{2}`)
	reFrac  = regexp.MustCompile(`\.\d+`)
	// reTimestamp matches an ISO-8601 / SQL datetime anywhere in a string.
	reTimestamp = regexp.MustCompile(`\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?`)
	// reExplicitOffset detects a trailing Z or ±hh:mm — the layout then shows the
	// zone, so no wall= marker is added.
	reExplicitOffset = regexp.MustCompile(`(Z|[+-]\d{2}:?\d{2})$`)
)

// naiveLayouts are the offset-less datetime formats parsed (in UTC) for wall=now.
var naiveLayouts = []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05"}

// timeFormatTag turns a time value into a layout tag (e.g. <TIME:YYYY-MM-DD
// HH:mm:ss>), revealing date / clock / precision / timezone. For offset-less
// timestamps near refNow it appends `wall=now±Nh` to expose the producer's zone.
func timeFormatTag(s string, refNow time.Time) string {
	layout := reDate.ReplaceAllString(s, "YYYY-MM-DD")
	layout = reClock.ReplaceAllString(layout, "HH:mm:ss")
	layout = reFrac.ReplaceAllString(layout, ".SSS")
	if z := wallVsNow(s, refNow); z != "" {
		return "<TIME:" + layout + " " + z + ">"
	}
	return "<TIME:" + layout + ">"
}

// wallVsNow emits "wall=now±Nh" for an offset-less timestamp near refNow (its
// UTC-parsed hour offset from refNow reveals the producer's zone). Returns ""
// for explicit-offset, date-only, or far-from-now values. The ±36h window
// admits real offsets (≤14h) while excluding fixed seed timestamps.
func wallVsNow(s string, refNow time.Time) string {
	if reExplicitOffset.MatchString(s) {
		return ""
	}
	var t time.Time
	var err error
	for _, layout := range naiveLayouts {
		if t, err = time.ParseInLocation(layout, s, time.UTC); err == nil {
			break
		}
	}
	if err != nil {
		return ""
	}
	d := t.Sub(refNow)
	if d < -36*time.Hour || d > 36*time.Hour {
		return ""
	}
	return fmt.Sprintf("wall=now%+dh", int(math.Round(d.Hours())))
}

// CanonicalJSON serializes a normalized value into a stable multiset key.
func CanonicalJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}
