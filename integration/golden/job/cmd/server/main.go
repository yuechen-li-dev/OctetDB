package main

import (
	"context"
	"example.com/octetdb-golden/job/internal/httpapi"
	"example.com/octetdb-golden/job/internal/service"
	"example.com/octetdb-golden/job/internal/store"
	"log/slog"
	"net/http"
	"os"
)

func main() {
	db, err := store.Open(context.Background(), "./data/job")
	if err != nil {
		slog.Error("open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := http.ListenAndServe(":8083", httpapi.New(service.New(db))); err != nil {
		slog.Error("serve", "error", err)
	}
}
