// Package inventory implements a durable inventory on OctetDB's
// inventory/items catalog topology.
package inventory

import (
	"context"
	"errors"
	"fmt"

	"github.com/yuechen-li-dev/octetdb"
)

const itemTypeIdentity = "participant-a.inventory.Item/v1"

// Item is the durable state for one SKU. Available and Reserved are always
// non-negative.
type Item struct {
	SKU       string `json:"sku"`
	Available int    `json:"available"`
	Reserved  int    `json:"reserved"`
}

// Result describes the durable decision returned by a mutating operation.
// A rejected operation has Applied=false and a stable Code.
type Result struct {
	Item      Item
	Applied   bool
	Code      string
	Duplicate bool
	Sequence  uint64
}

// Store owns the handles for the inventory/items topology.
type Store struct {
	db    *octetdb.Database
	items *octetdb.Dataset
}

// Open creates or reopens a durable inventory rooted at path.
func Open(ctx context.Context, path string) (*Store, error) {
	db, err := octetdb.OpenCatalog(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		return nil, err
	}
	bucket, err := db.Bucket(ctx, "inventory")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	items, err := bucket.Dataset(ctx, "items", octetdb.DatasetOptions{TypeIdentity: itemTypeIdentity})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db, items: items}, nil
}

// Close snapshots and closes the inventory database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// CreateItem creates sku with available units. The command ID is the durable
// idempotency key.
func (s *Store) CreateItem(ctx context.Context, commandID, sku string, available int) (Result, error) {
	if err := validateStoreAndIdentity(s, commandID, sku); err != nil {
		return Result{}, err
	}
	decision, err := s.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.Tx) (any, error) {
		var current Item
		found, err := tx.Get(s.items, sku, &current)
		if err != nil {
			return nil, err
		}
		if found {
			return current, octetdb.RejectWithResult("item_exists", current)
		}
		candidate := Item{SKU: sku, Available: available}
		if available < 0 {
			return candidate, octetdb.RejectWithResult("negative_stock", candidate)
		}
		return candidate, tx.Put(s.items, sku, candidate)
	})
	return inventoryResult(decision, err)
}

// GetItem reads one item by SKU.
func (s *Store) GetItem(ctx context.Context, sku string) (Item, bool, error) {
	if s == nil || s.items == nil {
		return Item{}, false, errors.New("inventory store is closed or uninitialized")
	}
	if sku == "" {
		return Item{}, false, errors.New("sku is required")
	}
	var item Item
	found, err := s.items.Get(ctx, sku, &item)
	return item, found, err
}

// Reserve atomically moves quantity from Available to Reserved. Insufficient
// stock is a durable rejection and cannot make Available negative.
func (s *Store) Reserve(ctx context.Context, commandID, sku string, quantity int) (Result, error) {
	return s.adjust(ctx, commandID, sku, quantity, true)
}

// Release atomically moves quantity from Reserved back to Available.
func (s *Store) Release(ctx context.Context, commandID, sku string, quantity int) (Result, error) {
	return s.adjust(ctx, commandID, sku, quantity, false)
}

func (s *Store) adjust(ctx context.Context, commandID, sku string, quantity int, reserve bool) (Result, error) {
	if err := validateStoreAndIdentity(s, commandID, sku); err != nil {
		return Result{}, err
	}
	decision, err := s.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.Tx) (any, error) {
		var item Item
		found, err := tx.Get(s.items, sku, &item)
		if err != nil {
			return nil, err
		}
		if !found {
			return item, octetdb.RejectWithResult("item_not_found", item)
		}
		if quantity <= 0 {
			return item, octetdb.RejectWithResult("invalid_quantity", item)
		}
		if reserve {
			if item.Available < quantity {
				return item, octetdb.RejectWithResult("insufficient_stock", item)
			}
			item.Available -= quantity
			item.Reserved += quantity
		} else {
			if item.Reserved < quantity {
				return item, octetdb.RejectWithResult("insufficient_reserved", item)
			}
			item.Reserved -= quantity
			item.Available += quantity
		}
		return item, tx.Put(s.items, sku, item)
	})
	return inventoryResult(decision, err)
}

func inventoryResult(decision octetdb.KeyedDecision, err error) (Result, error) {
	if err != nil {
		return Result{}, err
	}
	var item Item
	if err := octetdb.DecodeResult(decision, &item); err != nil {
		return Result{}, err
	}
	return Result{
		Item:      item,
		Applied:   decision.Applied,
		Code:      decision.Code,
		Duplicate: decision.Duplicate,
		Sequence:  decision.Sequence,
	}, nil
}

func validateStoreAndIdentity(s *Store, commandID, sku string) error {
	if s == nil || s.db == nil || s.items == nil {
		return errors.New("inventory store is closed or uninitialized")
	}
	if commandID == "" {
		return errors.New("command ID is required")
	}
	if sku == "" {
		return errors.New("sku is required")
	}
	if len(sku) > 4096 {
		return fmt.Errorf("sku exceeds OctetDB's key limit")
	}
	return nil
}
