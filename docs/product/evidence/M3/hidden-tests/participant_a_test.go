package hidden_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/yuechen-li-dev/octetdb"
	"participant-a/inventory"
	"participant-a/jobs"
)

func TestParticipantAConcurrentReservationConservesStock(t *testing.T) {
	ctx := context.Background()
	store, err := inventory.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if result, err := store.CreateItem(ctx, "create", "widget", 10); err != nil || !result.Applied {
		t.Fatalf("create: result=%+v err=%v", result, err)
	}

	results := make(chan inventory.Result, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, id := range []string{"reserve-a", "reserve-b"} {
		wg.Add(1)
		go func(commandID string) {
			defer wg.Done()
			result, err := store.Reserve(ctx, commandID, "widget", 7)
			results <- result
			errs <- err
		}(id)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	applied, rejected := 0, 0
	for result := range results {
		if result.Applied {
			applied++
		} else if result.Code == "insufficient_stock" {
			rejected++
		}
	}
	if applied != 1 || rejected != 1 {
		t.Fatalf("conflicting reservations: applied=%d rejected=%d", applied, rejected)
	}
	item, found, err := store.GetItem(ctx, "widget")
	if err != nil || !found || item.Available != 3 || item.Reserved != 7 {
		t.Fatalf("conservation: item=%+v found=%v err=%v", item, found, err)
	}
}

func TestParticipantAOpenPropagatesCorruption(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	store, err := inventory.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := store.CreateItem(ctx, "create-corrupt", "widget", 1); err != nil || !result.Applied {
		t.Fatalf("create: result=%+v err=%v", result, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	snapshotPath := filepath.Join(path, "snapshot")
	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(snapshotPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = inventory.Open(ctx, path)
	if err == nil {
		t.Fatal("corrupt snapshot unexpectedly opened")
	}
	var productErr *octetdb.Error
	if !errors.As(err, &productErr) || productErr.Kind != octetdb.ErrorCorruption {
		t.Fatalf("open error=%v, want ErrorCorruption", err)
	}
}

func TestParticipantAJobClaimConflictAndRestartOrder(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	queue, err := jobs.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"z", "a", "m", "b"} {
		if result, err := queue.Create(ctx, "create-"+id, id, id); err != nil || !result.Applied {
			t.Fatalf("create %s: result=%+v err=%v", id, result, err)
		}
	}

	results := make(chan jobs.Result, 2)
	var wg sync.WaitGroup
	for i, worker := range []string{"one", "two"} {
		wg.Add(1)
		go func(i int, worker string) {
			defer wg.Done()
			result, err := queue.Claim(ctx, "claim-"+worker, "m", worker)
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			results <- result
		}(i, worker)
	}
	wg.Wait()
	close(results)
	applied := 0
	for result := range results {
		if result.Applied {
			applied++
		}
	}
	if applied != 1 {
		t.Fatalf("applied claims=%d, want 1", applied)
	}
	ready, err := queue.ListReady(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{ready[0].ID, ready[1].ID}; !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("ready order=%v", got)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
	queue, err = jobs.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	ready, err = queue.ListReady(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(ready))
	for i := range ready {
		got[i] = ready[i].ID
	}
	if !reflect.DeepEqual(got, []string{"a", "b", "z"}) {
		t.Fatalf("ready after restart=%v", got)
	}
}
