package task6

import (
	"context"

	"github.com/yuechen-li-dev/octetdb"
)

const paidStatus = "Paid"

// Item is stored in inventory/items, keyed by SKU.
type Item struct {
	SKU   string `json:"sku"`
	Name  string `json:"name,omitempty"`
	Stock int    `json:"stock"`
}

// Order is stored in commerce/orders, keyed by ID.
type Order struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Total  int    `json:"total"`
}

// WorkloadRequest describes the deterministic mixed workload.
type WorkloadRequest struct {
	LowStockAt        int
	PointOrderID      string
	MutationCommandID string
	RestockSKU        string
	RestockBy         int
}

// MutationResult is the durable result of the final normal mutation.
type MutationResult struct {
	SKU       string `json:"sku"`
	Found     bool   `json:"found"`
	Item      Item   `json:"item"`
	Applied   bool   `json:"applied"`
	Code      string `json:"code,omitempty"`
	Duplicate bool   `json:"-"`
}

// WorkloadResult contains the scan results, point read, and mutation decision.
type WorkloadResult struct {
	LowStock        []Item
	PaidOrders      []Order
	PointOrder      Order
	PointOrderFound bool
	Mutation        MutationResult
}

// Store owns the task 6 commerce/orders and inventory/items dataset handles.
type Store struct {
	db     *octetdb.Database
	orders *octetdb.Dataset
	items  *octetdb.Dataset
}

// Open opens or creates the task 6 topology in path.
func Open(ctx context.Context, path string) (*Store, error) {
	db, err := octetdb.OpenCatalog(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		return nil, err
	}
	store := &Store{db: db}
	if err := store.openDatasets(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) openDatasets(ctx context.Context) error {
	commerce, err := s.db.Bucket(ctx, "commerce")
	if err != nil {
		return err
	}
	orders, err := commerce.Dataset(ctx, "orders", octetdb.DatasetOptions{TypeIdentity: "participant-c.task6.Order/v1"})
	if err != nil {
		return err
	}
	inventory, err := s.db.Bucket(ctx, "inventory")
	if err != nil {
		return err
	}
	items, err := inventory.Dataset(ctx, "items", octetdb.DatasetOptions{TypeIdentity: "participant-c.task6.Item/v1"})
	if err != nil {
		return err
	}
	s.orders = orders
	s.items = items
	return nil
}

// Close closes the underlying OctetDB handle.
func (s *Store) Close() error {
	return s.db.Close()
}

// PutItem upserts an inventory item through OctetDB mutation.
func (s *Store) PutItem(ctx context.Context, commandID string, item Item) (octetdb.KeyedDecision, error) {
	return s.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.Tx) (any, error) {
		if err := tx.Put(s.items, item.SKU, item); err != nil {
			return nil, err
		}
		return item, nil
	})
}

// PutOrder upserts an order through OctetDB mutation.
func (s *Store) PutOrder(ctx context.Context, commandID string, order Order) (octetdb.KeyedDecision, error) {
	return s.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.Tx) (any, error) {
		if err := tx.Put(s.orders, order.ID, order); err != nil {
			return nil, err
		}
		return order, nil
	})
}

// GetItem point-reads one inventory item.
func (s *Store) GetItem(ctx context.Context, sku string) (Item, bool, error) {
	var item Item
	found, err := s.items.Get(ctx, sku, &item)
	return item, found, err
}

// GetOrder point-reads one order.
func (s *Store) GetOrder(ctx context.Context, orderID string) (Order, bool, error) {
	var order Order
	found, err := s.orders.Get(ctx, orderID, &order)
	return order, found, err
}

// RunMixedWorkload scans low-stock items, scans paid orders, point-reads an order,
// and finally performs a normal stock mutation.
func (s *Store) RunMixedWorkload(ctx context.Context, req WorkloadRequest) (WorkloadResult, error) {
	threshold := req.LowStockAt
	result := WorkloadResult{
		LowStock:   make([]Item, 0, 20),
		PaidOrders: make([]Order, 0, 20),
	}
	if err := octetdb.ScanDataset(ctx, s.items, func(_ string, item Item) (octetdb.ScanAction, error) {
		if item.Stock <= threshold {
			result.LowStock = append(result.LowStock, item)
			if len(result.LowStock) == 20 {
				return octetdb.ScanStop, nil
			}
		}
		return octetdb.ScanContinue, nil
	}); err != nil {
		return WorkloadResult{}, err
	}
	if err := octetdb.ScanDataset(ctx, s.orders, func(_ string, order Order) (octetdb.ScanAction, error) {
		if order.Status == paidStatus {
			result.PaidOrders = append(result.PaidOrders, order)
			if len(result.PaidOrders) == 20 {
				return octetdb.ScanStop, nil
			}
		}
		return octetdb.ScanContinue, nil
	}); err != nil {
		return WorkloadResult{}, err
	}
	found, err := s.orders.Get(ctx, req.PointOrderID, &result.PointOrder)
	if err != nil {
		return WorkloadResult{}, err
	}
	result.PointOrderFound = found

	mutation, err := s.restock(ctx, req.MutationCommandID, req.RestockSKU, req.RestockBy)
	if err != nil {
		return WorkloadResult{}, err
	}
	result.Mutation = mutation
	return result, nil
}

func (s *Store) restock(ctx context.Context, commandID, sku string, by int) (MutationResult, error) {
	decision, err := s.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.Tx) (any, error) {
		result := MutationResult{SKU: sku}
		var item Item
		found, err := tx.Get(s.items, sku, &item)
		if err != nil {
			return result, err
		}
		if !found {
			result.Code = "item_not_found"
			return result, octetdb.RejectWithResult(result.Code, result)
		}
		item.Stock += by
		if err := tx.Put(s.items, sku, item); err != nil {
			return result, err
		}
		result.Found = true
		result.Item = item
		result.Applied = true
		return result, nil
	})
	if err != nil {
		return MutationResult{}, err
	}
	var result MutationResult
	if err := octetdb.DecodeResult(decision, &result); err != nil {
		return MutationResult{}, err
	}
	result.Applied = decision.Applied
	result.Code = decision.Code
	result.Duplicate = decision.Duplicate
	return result, nil
}
