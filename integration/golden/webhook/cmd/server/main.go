package main

import (
	"context"
	"example.com/octetdb-golden/webhook/internal/httpapi"
	"example.com/octetdb-golden/webhook/internal/service"
	"example.com/octetdb-golden/webhook/internal/store"
	"log/slog"
	"net/http"
	"os"
)

func main() {
	db, err := store.Open(context.Background(), "./data/webhook")
	if err != nil {
		slog.Error("open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	slog.Info("webhook listening", "address", ":8081")
	if err := http.ListenAndServe(":8081", httpapi.New(service.New(db))); err != nil {
		slog.Error("serve", "error", err)
	}
}
