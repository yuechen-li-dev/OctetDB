package main

import (
	"context"
	"example.com/octetdb-golden/order/internal/httpapi"
	"example.com/octetdb-golden/order/internal/service"
	"example.com/octetdb-golden/order/internal/store"
	"log/slog"
	"net/http"
	"os"
)

func main() {
	db, err := store.Open(context.Background(), "./data/order")
	if err != nil {
		slog.Error("open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := http.ListenAndServe(":8082", httpapi.New(service.New(db))); err != nil {
		slog.Error("serve", "error", err)
	}
}
