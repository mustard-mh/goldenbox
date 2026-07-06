package goldenbox

import "context"

// StoreDriver manages one backing store for the whole test process.
type StoreDriver interface {
	// Name is the instance's unique label (golden section key and LookupStore
	// key); defaults to the driver kind, set per instance to run several of a
	// kind.
	Name() string
	// Start brings up the backing store (h carries Options and reuse mode).
	Start(ctx context.Context, h *Harness) error
	// NewCase allocates an isolated per-case slice. caseID is unique per
	// harness.
	NewCase(ctx context.Context, caseID int) (StoreCase, error)
	// Stop shuts the backing store down (unless reuse mode keeps it alive).
	Stop(ctx context.Context) error
}

// StoreCase is one case's isolated slice of a store.
type StoreCase interface {
	// Snapshot dumps the slice's full normalized state. n is the golden's
	// shared Normalizer.
	Snapshot(ctx context.Context, n *Normalizer) (Snapshot, error)
	// Stamp returns a CHEAP fingerprint of the slice's write state: equal
	// stamps mean no write landed between them (used for quiescence polling).
	Stamp(ctx context.Context) (string, error)
	// Reset wipes the slice's data without re-provisioning it.
	Reset(ctx context.Context) error
	// Close releases the slice (drop database, free DB index, ...).
	Close(ctx context.Context) error
}

// Snapshot is a store state dump that can diff against a previous dump of the
// same store.
type Snapshot interface {
	// Diff returns the prev→this delta in golden-ready form, and whether it is
	// empty. prev is nil for the first snapshot (delta = everything).
	Diff(prev Snapshot) (delta any, empty bool)
}
