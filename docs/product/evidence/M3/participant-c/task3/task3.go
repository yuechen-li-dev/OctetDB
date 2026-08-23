package task3

import (
	"context"
	"errors"
	"fmt"

	"github.com/yuechen-li-dev/octetdb"
)

const (
	orderStatusPlaced = "Placed"
)

// Item is stored in inventory/items, keyed by SKU.
type Item struct {
	SKU   string `json:"sku"`
	Name  string `json:"name,omitempty"`
	Stock int    `json:"stock"`
}

// Order is stored in commerce/orders, keyed by ID.
type Order struct {
	ID       string `json:"id"`
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	Status   string `json:"status"`
}

// PlaceOrderRequest names the order to create or update and the stock to reserve.
type PlaceOrderRequest struct {
	OrderID  string
	SKU      string
	Quantity int
}

// PlaceOrderResult is the durable application result encoded into the OctetDB decision.
type PlaceOrderResult struct {
	Order          Order  `json:"order"`
	Item           Item   `json:"item"`
	Applied        bool   `json:"applied"`
	Code           string `json:"code,omitempty"`
	Duplicate      bool   `json:"-"`
	AvailableStock int    `json:"available_stock"`
}

// Store owns the task 3 commerce/orders and inventory/items dataset handles.
type Store struct {
	db     *octetdb.Database
	orders *octetdb.Dataset
	items  *octetdb.Dataset
}

// Open opens or creates the task 3 topology in path.
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
	inventory, err := s.db.Bucket(ctx, "inventory")
	if err != nil {
		return err
	}
	items, err := inventory.Dataset(ctx, "items", octetdb.DatasetOptions{TypeIdentity: "participant-c.task3.Item/v1"})
	if err != nil {
		return err
	}
	commerce, err := s.db.Bucket(ctx, "commerce")
	if err != nil {
		return err
	}
	orders, err := commerce.Dataset(ctx, "orders", octetdb.DatasetOptions{TypeIdentity: "participant-c.task3.Order/v1"})
	if err != nil {
		return err
	}
	s.items = items
	s.orders = orders
	return nil
}

// Close closes the underlying OctetDB handle.
func (s *Store) Close() error {
	return s.db.Close()
}

// PutItem upserts an inventory item through the real mutation path.
func (s *Store) PutItem(ctx context.Context, commandID string, item Item) (octetdb.KeyedDecision, error) {
	return s.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.Tx) (any, error) {
		if item.SKU == "" {
			return nil, octetdb.Reject("missing_sku")
		}
		if err := tx.Put(s.items, item.SKU, item); err != nil {
			return nil, err
		}
		return item, nil
	})
}

// GetItem reads one inventory record.
func (s *Store) GetItem(ctx context.Context, sku string) (Item, bool, error) {
	var item Item
	found, err := s.items.Get(ctx, sku, &item)
	return item, found, err
}

// GetOrder reads one order record.
func (s *Store) GetOrder(ctx context.Context, orderID string) (Order, bool, error) {
	var order Order
	found, err := s.orders.Get(ctx, orderID, &order)
	return order, found, err
}

// PlaceOrder atomically verifies inventory, reserves stock, and creates or updates an order.
func (s *Store) PlaceOrder(ctx context.Context, commandID string, req PlaceOrderRequest) (PlaceOrderResult, octetdb.KeyedDecision, error) {
	decision, err := s.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.Tx) (any, error) {
		result, err := s.placeOrderInTx(tx, req)
		if err != nil {
			return result, err
		}
		return result, nil
	})
	if err != nil {
		return PlaceOrderResult{}, decision, err
	}
	var result PlaceOrderResult
	if err := octetdb.DecodeResult(decision, &result); err != nil {
		return PlaceOrderResult{}, decision, err
	}
	result.Applied = decision.Applied
	result.Code = decision.Code
	result.Duplicate = decision.Duplicate
	return result, decision, nil
}

func (s *Store) placeOrderInTx(tx *octetdb.Tx, req PlaceOrderRequest) (PlaceOrderResult, error) {
	result := PlaceOrderResult{Order: Order{ID: req.OrderID, SKU: req.SKU, Quantity: req.Quantity, Status: orderStatusPlaced}}
	if req.OrderID == "" {
		result.Code = "missing_order_id"
		return result, octetdb.RejectWithResult(result.Code, result)
	}
	if req.SKU == "" {
		result.Code = "missing_sku"
		return result, octetdb.RejectWithResult(result.Code, result)
	}
	if req.Quantity <= 0 {
		result.Code = "invalid_quantity"
		return result, octetdb.RejectWithResult(result.Code, result)
	}

	var item Item
	found, err := tx.Get(s.items, req.SKU, &item)
	if err != nil {
		return result, err
	}
	if !found {
		result.Code = "item_not_found"
		return result, octetdb.RejectWithResult(result.Code, result)
	}
	result.Item = item
	result.AvailableStock = item.Stock
	if item.Stock < req.Quantity {
		result.Code = "insufficient_stock"
		return result, octetdb.RejectWithResult(result.Code, result)
	}

	var order Order
	found, err = tx.Get(s.orders, req.OrderID, &order)
	if err != nil {
		return result, err
	}
	if found && order.SKU != req.SKU {
		result.Order = order
		result.Code = "order_sku_mismatch"
		return result, octetdb.RejectWithResult(result.Code, result)
	}
	if !found {
		order = Order{ID: req.OrderID, SKU: req.SKU, Status: orderStatusPlaced}
	}
	order.Quantity += req.Quantity
	if order.Status == "" {
		order.Status = orderStatusPlaced
	}
	item.Stock -= req.Quantity
	if err := tx.Put(s.items, item.SKU, item); err != nil {
		return result, err
	}
	if err := tx.Put(s.orders, order.ID, order); err != nil {
		return result, err
	}
	result.Order = order
	result.Item = item
	result.AvailableStock = item.Stock
	result.Applied = true
	return result, nil
}

// RequireApplied converts an application rejection into a normal Go error for callers
// that do not want to inspect the returned decision.
func RequireApplied(result PlaceOrderResult) error {
	if result.Applied {
		return nil
	}
	if result.Code == "" {
		return errors.New("order was not applied")
	}
	return fmt.Errorf("order was not applied: %s", result.Code)
}
