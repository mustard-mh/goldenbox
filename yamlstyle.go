package goldenbox

import "gopkg.in/yaml.v3"

// CompactNode encodes v into a yaml.Node with SetCompactStyle applied. Driver
// delta types call it from MarshalYAML through a method-less alias to avoid
// recursing into their own MarshalYAML:
//
//	func (d DBDelta) MarshalYAML() (any, error) {
//	    type plain DBDelta
//	    return goldenbox.CompactNode(plain(d))
//	}
func CompactNode(v any) (*yaml.Node, error) {
	var n yaml.Node
	if err := n.Encode(v); err != nil {
		return nil, err
	}
	SetCompactStyle(&n)
	return &n, nil
}

// SetCompactStyle rewrites n in place (bottom-up, idempotent) so small
// structures render inline: a mapping flows when every value is a scalar or an
// already-flowed collection, and a sequence flows only when all elements are
// scalars. A sequence of maps (a table's rows) therefore stays block — one row
// per line — which also stops flow from propagating up into section containers.
func SetCompactStyle(n *yaml.Node) {
	for _, c := range n.Content {
		SetCompactStyle(c)
	}
	switch n.Kind {
	case yaml.SequenceNode:
		if allScalar(n.Content) {
			n.Style = yaml.FlowStyle
		}
	case yaml.MappingNode:
		// Content is [key0, val0, key1, val1, ...]; only the values decide.
		flow := true
		for i := 1; i < len(n.Content); i += 2 {
			if !flowable(n.Content[i]) {
				flow = false
				break
			}
		}
		if flow {
			n.Style = yaml.FlowStyle
		}
	}
}

// flowable reports whether a mapping value keeps its parent inline: a scalar, or
// a collection already switched to flow style.
func flowable(n *yaml.Node) bool {
	return n.Kind == yaml.ScalarNode || n.Style&yaml.FlowStyle != 0
}

func allScalar(ns []*yaml.Node) bool {
	for _, c := range ns {
		if c.Kind != yaml.ScalarNode {
			return false
		}
	}
	return true
}
