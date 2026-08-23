package octetdb

import (
	"errors"
	"fmt"

	"github.com/yuechen-li-dev/octetdb/internal/core"
)

// ErrorKind identifies an error category suitable for programmatic decisions.
type ErrorKind string

const (
	// ErrorInvalidInput means options or a command were malformed.
	ErrorInvalidInput ErrorKind = "invalid_input"
	// ErrorCapacity means a configured account or batch bound was exceeded.
	ErrorCapacity ErrorKind = "capacity"
	// ErrorStorage means a write, synchronization, or other storage operation failed.
	ErrorStorage ErrorKind = "storage"
	// ErrorCorruption means checksums or structural validation found damaged data.
	ErrorCorruption ErrorKind = "corruption"
	// ErrorIncompatible means the database or behavioral model cannot be read by this version.
	ErrorIncompatible ErrorKind = "incompatible"
	// ErrorClosed means an operation was attempted after Close.
	ErrorClosed ErrorKind = "closed"
	// ErrorPoisoned means an earlier durability failure made further writes unsafe.
	ErrorPoisoned ErrorKind = "poisoned"
)

// Error describes an OctetDB operation failure without making diagnostic text
// part of the API contract.
type Error struct {
	// Kind is the stable category for programmatic handling.
	Kind ErrorKind
	// Op is the public operation that failed.
	Op  string
	err error
}

// Error returns a descriptive diagnostic.
func (e *Error) Error() string { return fmt.Sprintf("octetdb %s: %s: %v", e.Op, e.Kind, e.err) }

// Unwrap returns the underlying diagnostic error.
func (e *Error) Unwrap() error { return e.err }

func wrapError(op string, err error) error {
	if err == nil {
		return nil
	}
	var runtimeErr *core.RuntimeError
	if errors.As(err, &runtimeErr) {
		kind := ErrorStorage
		switch runtimeErr.Kind {
		case core.InvalidCommand:
			kind = ErrorInvalidInput
		case core.CapacityExceeded:
			kind = ErrorCapacity
		case core.EngineClosed:
			kind = ErrorClosed
		case core.EnginePoisoned:
			kind = ErrorPoisoned
		case core.RecoveryCorrupt:
			kind = ErrorCorruption
		case core.RecoveryIncompatible:
			kind = ErrorIncompatible
		}
		return &Error{Kind: kind, Op: op, err: err}
	}
	return &Error{Kind: ErrorStorage, Op: op, err: err}
}
