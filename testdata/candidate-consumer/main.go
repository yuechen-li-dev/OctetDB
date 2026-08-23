package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"

	"github.com/yuechen-li-dev/octetdb"
)

type Item struct {
	SKU   string `json:"sku"`
	Stock int    `json:"stock"`
}

type Order struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func main() {
	ctx := context.Background()
	path := filepath.Join(".", "data")
	db, items, orders := open(ctx, path)
	decision, err := db.Mutate(ctx, octetdb.KeyedCommand{ID: "place-order-1"}, func(tx *octetdb.Tx) (any, error) {
		item := Item{SKU: "widget", Stock: 3}
		order := Order{ID: "order-1", Status: "placed"}
		if err := tx.Put(items, item.SKU, item); err != nil {
			return nil, err
		}
		if err := tx.Put(orders, order.ID, order); err != nil {
			return nil, err
		}
		return order, nil
	})
	if err != nil || !decision.Applied {
		log.Fatalf("place: decision=%+v err=%v", decision, err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	db, items, orders = open(ctx, path)
	defer db.Close()
	called := false
	retry, err := db.Mutate(ctx, octetdb.KeyedCommand{ID: "place-order-1"}, func(*octetdb.Tx) (any, error) {
		called = true
		return nil, nil
	})
	if err != nil || !retry.Duplicate || called {
		log.Fatalf("retry=%+v called=%v err=%v", retry, called, err)
	}
	var order Order
	if found, err := orders.Get(ctx, "order-1", &order); err != nil || !found {
		log.Fatalf("order found=%v err=%v", found, err)
	}
	var item Item
	if found, err := items.Get(ctx, "widget", &item); err != nil || !found {
		log.Fatalf("item found=%v err=%v", found, err)
	}
	var low []string
	if err := octetdb.ScanDataset(ctx, items, func(key string, value Item) (octetdb.ScanAction, error) {
		if value.Stock <= 5 {
			low = append(low, key)
		}
		return octetdb.ScanContinue, nil
	}); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("candidate-ok duplicate=%v order=%s stock=%d low=%v\n", retry.Duplicate, order.Status, item.Stock, low)
}

func open(ctx context.Context, path string) (*octetdb.Database, *octetdb.Dataset, *octetdb.Dataset) {
	db, err := octetdb.OpenCatalog(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		log.Fatal(err)
	}
	commerce, err := db.Bucket(ctx, "commerce")
	if err != nil {
		log.Fatal(err)
	}
	inventory, err := db.Bucket(ctx, "inventory")
	if err != nil {
		log.Fatal(err)
	}
	orders, err := commerce.Dataset(ctx, "orders", octetdb.DatasetOptions{TypeIdentity: "consumer.Order/v1"})
	if err != nil {
		log.Fatal(err)
	}
	items, err := inventory.Dataset(ctx, "items", octetdb.DatasetOptions{TypeIdentity: "consumer.Item/v1"})
	if err != nil {
		log.Fatal(err)
	}
	return db, items, orders
}
