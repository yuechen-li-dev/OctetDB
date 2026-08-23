package main

import (
	"context"
	"fmt"
	"log"
	"os"

	octetdb "github.com/yuechen-li-dev/octetdb"
)

type Item struct {
	SKU   string `json:"sku"`
	Stock int    `json:"stock"`
}

type Order struct {
	ID       string `json:"id"`
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

func run(ctx context.Context, databasePath string) error {
	db, err := octetdb.OpenCatalog(ctx, databasePath, octetdb.DefaultKeyedOptions())
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	shop, err := db.Bucket(ctx, "shop")
	if err != nil {
		return fmt.Errorf("open shop bucket: %w", err)
	}
	inventory, err := shop.Dataset(ctx, "inventory", octetdb.DatasetOptions{TypeIdentity: "shop.Item/v1"})
	if err != nil {
		return fmt.Errorf("open inventory dataset: %w", err)
	}
	orders, err := shop.Dataset(ctx, "orders", octetdb.DatasetOptions{TypeIdentity: "shop.Order/v1"})
	if err != nil {
		return fmt.Errorf("open orders dataset: %w", err)
	}

	_, err = db.Mutate(ctx, octetdb.KeyedCommand{ID: "seed-inventory-v1"}, func(tx *octetdb.Tx) (any, error) {
		initial := []Item{{SKU: "adapter", Stock: 3}, {SKU: "cable", Stock: 8}, {SKU: "widget", Stock: 7}}
		for _, candidate := range initial {
			var existing Item
			found, err := tx.Get(inventory, candidate.SKU, &existing)
			if err != nil {
				return nil, err
			}
			if found {
				continue
			}
			if err := tx.Put(inventory, candidate.SKU, candidate); err != nil {
				return nil, err
			}
		}
		return initial, nil
	})
	if err != nil {
		return fmt.Errorf("seed inventory: %w", err)
	}

	order := Order{ID: "order-42", SKU: "widget", Quantity: 3}
	_, err = db.Mutate(ctx, octetdb.KeyedCommand{ID: "place-order-42"}, func(tx *octetdb.Tx) (any, error) {
		var item Item
		found, err := tx.Get(inventory, order.SKU, &item)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, octetdb.Reject("item_not_found")
		}
		if item.Stock < order.Quantity {
			return item, octetdb.RejectWithResult("insufficient_stock", item)
		}
		item.Stock -= order.Quantity
		if err := tx.Put(inventory, item.SKU, item); err != nil {
			return nil, err
		}
		if err := tx.Put(orders, order.ID, order); err != nil {
			return nil, err
		}
		return order, nil
	})
	if err != nil {
		return fmt.Errorf("place order: %w", err)
	}

	const threshold = 5
	var lowStock []Item
	err = octetdb.ScanDataset(ctx, inventory, func(_ string, item Item) (octetdb.ScanAction, error) {
		if item.Stock <= threshold {
			lowStock = append(lowStock, item)
		}
		return octetdb.ScanContinue, nil
	})
	if err != nil {
		return fmt.Errorf("scan inventory: %w", err)
	}
	fmt.Printf("Low-stock items (stock <= %d):\n", threshold)
	for _, item := range lowStock {
		fmt.Printf("%s: %d\n", item.SKU, item.Stock)
	}
	return nil
}

func main() {
	databasePath := "./data/shop"
	if len(os.Args) > 1 {
		databasePath = os.Args[1]
	}
	if err := run(context.Background(), databasePath); err != nil {
		log.Fatal(err)
	}
}
