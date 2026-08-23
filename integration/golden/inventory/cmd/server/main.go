package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"example.com/octetdb-golden/inventory/internal/httpapi"
	"example.com/octetdb-golden/inventory/internal/service"
	"example.com/octetdb-golden/inventory/internal/store"
)

func main() {
	db, err := store.Open(context.Background(), "./data/inventory")
	if err != nil {
		slog.Error("open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	slog.Info("inventory listening", "address", ":8080")
	if err := http.ListenAndServe(":8080", httpapi.New(service.New(db))); err != nil {
		slog.Error("serve", "error", err)
	}
}
