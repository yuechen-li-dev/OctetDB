package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/yuechen-li-dev/octetdb"
)

type Item struct {
	SKU   string `json:"sku"`
	Stock int    `json:"stock"`
}

func main() {
	ctx := context.Background()
	path, err := os.MkdirTemp("", "octetdb-quickstart-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(path)

	db, items := openInventory(ctx, path)
	command := octetdb.KeyedCommand{ID: "receive-widget-001"}
	decision, err := db.Mutate(ctx, command, func(tx *octetdb.Tx) (any, error) {
		item := Item{SKU: "widget", Stock: 8}
		return item, tx.Put(items, item.SKU, item)
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Reopen the same directory: catalog, records, and command decisions survive.
	db, items = openInventory(ctx, path)
	defer db.Close()
	retry, err := db.Mutate(ctx, command, func(*octetdb.Tx) (any, error) {
		log.Fatal("duplicate command callback ran")
		return nil, nil
	})
	if err != nil {
		log.Fatal(err)
	}
	var item Item
	found, err := items.Get(ctx, "widget", &item)
	if err != nil {
		log.Fatal(err)
	}
	var lowStock []Item
	err = octetdb.ScanDataset(ctx, items, func(_ string, candidate Item) (octetdb.ScanAction, error) {
		if candidate.Stock <= 10 {
			lowStock = append(lowStock, candidate)
		}
		return octetdb.ScanContinue, nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("applied=%v duplicate=%v found=%v stock=%d low_stock=%d\n", decision.Applied, retry.Duplicate, found, item.Stock, len(lowStock))
}

func openInventory(ctx context.Context, path string) (*octetdb.Database, *octetdb.Dataset) {
	db, err := octetdb.OpenCatalog(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		log.Fatal(err)
	}
	bucket, err := db.Bucket(ctx, "inventory")
	if err != nil {
		log.Fatal(err)
	}
	items, err := bucket.Dataset(ctx, "items", octetdb.DatasetOptions{TypeIdentity: "quickstart.Item/v1"})
	if err != nil {
		log.Fatal(err)
	}
	return db, items
}
