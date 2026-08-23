package octetdb

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func openGroupCommitTestDB(t *testing.T) (*Database, *Dataset) {
	t.Helper()
	db, err := OpenCatalog(context.Background(), filepath.Join(t.TempDir(), "db"), KeyedOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	bucket, err := db.Bucket(context.Background(), "app")
	if err != nil {
		t.Fatal(err)
	}
	dataset, err := bucket.Dataset(context.Background(), "values", DatasetOptions{TypeIdentity: "group-test/v1"})
	if err != nil {
		t.Fatal(err)
	}
	return db, dataset
}

func errorHasKind(err error, kind ErrorKind) bool {
	var octetErr *Error
	return errors.As(err, &octetErr) && octetErr.Kind == kind
}

func TestGroupCommitSharesSyncAndPreservesOrder(t *testing.T) {
	db, dataset := openGroupCommitTestDB(t)
	blocked := make(chan struct{})
	release := make(chan struct{})
	var syncs atomic.Int32
	db.keyed.beforeSync = func() error {
		if syncs.Add(1) == 1 {
			close(blocked)
			<-release
		}
		return nil
	}

	const commands = 8
	errs := make(chan error, commands)
	start := make(chan struct{})
	for id := 0; id < commands; id++ {
		id := id
		go func() {
			<-start
			_, err := db.Mutate(context.Background(), KeyedCommand{ID: fmt.Sprintf("command-%d", id)}, func(tx *Tx) (any, error) {
				var value int
				_, err := tx.Get(dataset, "counter", &value)
				if err != nil {
					return nil, err
				}
				value++
				return value, tx.Put(dataset, "counter", value)
			})
			errs <- err
		}()
	}
	close(start)
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("first Sync did not start")
	}
	// Let the other callers reach the coordinator while the first Sync is blocked.
	time.Sleep(20 * time.Millisecond)
	close(release)
	for range commands {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}

	var value int
	found, err := dataset.Get(context.Background(), "counter", &value)
	if err != nil || !found || value != commands {
		t.Fatalf("counter=%d found=%v err=%v", value, found, err)
	}
	frames := db.keyed.commitStats.framesSynced.Load()
	syncCalls := db.keyed.commitStats.syncCalls.Load()
	if frames != commands || syncCalls >= frames {
		t.Fatalf("frames=%d syncs=%d; no grouping", frames, syncCalls)
	}
	if db.keyed.commitStats.maxGroup.Load() <= 1 {
		t.Fatalf("max group=%d", db.keyed.commitStats.maxGroup.Load())
	}
}

func TestGroupCommitDurableRejectionOperationalErrorAndPanicIsolation(t *testing.T) {
	db, dataset := openGroupCommitTestDB(t)
	var callbacks atomic.Int32

	decision, err := db.Mutate(context.Background(), KeyedCommand{ID: "reject"}, func(*Tx) (any, error) {
		return "no", RejectWithResult("domain", map[string]int{"value": 7})
	})
	if err != nil || decision.Applied || decision.Code != "domain" {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	replay, err := db.Mutate(context.Background(), KeyedCommand{ID: "reject"}, func(*Tx) (any, error) { callbacks.Add(1); return nil, nil })
	if err != nil || !replay.Duplicate || callbacks.Load() != 0 {
		t.Fatalf("replay=%+v err=%v callbacks=%d", replay, err, callbacks.Load())
	}

	operational := errors.New("temporary")
	_, err = db.Mutate(context.Background(), KeyedCommand{ID: "operational"}, func(*Tx) (any, error) { return nil, operational })
	if !errors.Is(err, operational) {
		t.Fatalf("operational error=%v", err)
	}
	_, err = db.Mutate(context.Background(), KeyedCommand{ID: "operational"}, func(tx *Tx) (any, error) { return nil, tx.Put(dataset, "ok", true) })
	if err != nil {
		t.Fatalf("operational command ID was consumed: %v", err)
	}

	func() {
		defer func() {
			if recover() != "boom" {
				t.Fatalf("panic was not propagated to caller")
			}
		}()
		_, _ = db.Mutate(context.Background(), KeyedCommand{ID: "panic"}, func(*Tx) (any, error) { panic("boom") })
	}()
	_, err = db.Mutate(context.Background(), KeyedCommand{ID: "after-panic"}, func(*Tx) (any, error) { return "healthy", nil })
	if err != nil {
		t.Fatalf("coordinator did not survive callback panic: %v", err)
	}
}

func TestGroupCommitConcurrentDuplicateExecutesCallbackOnce(t *testing.T) {
	db, _ := openGroupCommitTestDB(t)
	const callers = 16
	var callbacks atomic.Int32
	start := make(chan struct{})
	decisions := make(chan KeyedDecision, callers)
	errs := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			decision, err := db.Mutate(context.Background(), KeyedCommand{ID: "same"}, func(*Tx) (any, error) {
				callbacks.Add(1)
				return map[string]int{"answer": 42}, nil
			})
			decisions <- decision
			errs <- err
		}()
	}
	close(start)
	duplicates := 0
	for range callers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		if (<-decisions).Duplicate {
			duplicates++
		}
	}
	if callbacks.Load() != 1 || duplicates != callers-1 {
		t.Fatalf("callbacks=%d duplicates=%d", callbacks.Load(), duplicates)
	}
}

func TestGroupCommitCancellationBoundaries(t *testing.T) {
	db, _ := openGroupCommitTestDB(t)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	var ran atomic.Bool
	_, err := db.Mutate(canceled, KeyedCommand{ID: "before"}, func(*Tx) (any, error) { ran.Store(true); return nil, nil })
	if !errors.Is(err, context.Canceled) || ran.Load() {
		t.Fatalf("err=%v ran=%v", err, ran.Load())
	}

	started := make(chan struct{})
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := db.Mutate(ctx, KeyedCommand{ID: "after"}, func(*Tx) (any, error) { close(started); <-release; return "durable", nil })
		result <- err
	}()
	<-started
	cancel()
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("cancellation after execution boundary replaced durable result: %v", err)
	}
}

func TestGroupCommitSyncFailurePoisonsAllVisibilityUntilReopen(t *testing.T) {
	db, dataset := openGroupCommitTestDB(t)
	db.keyed.beforeSync = func() error { return errors.New("injected Sync failure") }
	_, err := db.Mutate(context.Background(), KeyedCommand{ID: "unknown"}, func(tx *Tx) (any, error) { return nil, tx.Put(dataset, "key", "value") })
	if err == nil {
		t.Fatal("Sync failure was acknowledged")
	}
	var value string
	if _, err := dataset.Get(context.Background(), "key", &value); !errorHasKind(err, ErrorPoisoned) {
		t.Fatalf("read after failure err=%v", err)
	}
	if _, err := db.Mutate(context.Background(), KeyedCommand{ID: "later"}, func(*Tx) (any, error) { return nil, nil }); !errorHasKind(err, ErrorPoisoned) {
		t.Fatalf("mutation after failure err=%v", err)
	}
}

func TestGroupCommitAppendFailureDoesNotAcknowledgeStagedDuplicate(t *testing.T) {
	db, _ := openGroupCommitTestDB(t)
	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	var firstSync sync.Once
	db.keyed.beforeSync = func() error {
		firstSync.Do(func() { close(syncStarted); <-releaseSync })
		return nil
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := db.Mutate(context.Background(), KeyedCommand{ID: "blocker"}, func(*Tx) (any, error) { return nil, nil })
		firstDone <- err
	}()
	<-syncStarted

	db.keyed.beforeAppend = func(record keyedWALRecord) error {
		if record.CommandID == "failure" {
			return errors.New("injected append failure")
		}
		return nil
	}
	results := make(chan error, 3)
	enqueue := func(id string, want int) {
		go func() {
			_, err := db.Mutate(context.Background(), KeyedCommand{ID: id}, func(*Tx) (any, error) { return id, nil })
			results <- err
		}()
		deadline := time.Now().Add(5 * time.Second)
		for {
			db.keyed.commit.mu.Lock()
			queued := len(db.keyed.commit.queue)
			db.keyed.commit.mu.Unlock()
			if queued >= want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s did not queue", id)
			}
			runtime.Gosched()
		}
	}
	enqueue("original", 1)
	enqueue("original", 2)
	enqueue("failure", 3)
	close(releaseSync)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := <-results; !errorHasKind(err, ErrorStorage) {
			t.Fatalf("group member was acknowledged after append failure: %v", err)
		}
	}
}

func TestGroupCommitCloseCompletesAdmittedRequests(t *testing.T) {
	db, _ := openGroupCommitTestDB(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	db.keyed.beforeSync = func() error { once.Do(func() { close(started); <-release }); return nil }
	result := make(chan error, 1)
	go func() {
		_, err := db.Mutate(context.Background(), KeyedCommand{ID: "pending"}, func(*Tx) (any, error) { return nil, nil })
		result <- err
	}()
	<-started
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("admitted mutation failed: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestGroupCommitRestartAfterSyncBeforeResponses(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	db, err := OpenCatalog(ctx, path, KeyedOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := db.Bucket(ctx, "app")
	if err != nil {
		t.Fatal(err)
	}
	dataset, err := bucket.Dataset(ctx, "values", DatasetOptions{TypeIdentity: "response-loss/v1"})
	if err != nil {
		t.Fatal(err)
	}

	firstSync := make(chan struct{})
	releaseFirst := make(chan struct{})
	var syncCount atomic.Int32
	db.keyed.beforeSync = func() error {
		if syncCount.Add(1) == 1 {
			close(firstSync)
			<-releaseFirst
		}
		return nil
	}
	responsesBlocked := make(chan struct{})
	releaseResponses := make(chan struct{})
	db.keyed.afterSync = func() {
		if syncCount.Load() == 2 {
			close(responsesBlocked)
			<-releaseResponses
		}
	}
	blocker := make(chan error, 1)
	go func() {
		_, err := db.Mutate(ctx, KeyedCommand{ID: "blocker"}, func(*Tx) (any, error) { return nil, nil })
		blocker <- err
	}()
	<-firstSync

	results := make(chan error, 3)
	for id := 0; id < 3; id++ {
		id := id
		go func() {
			_, err := db.Mutate(ctx, KeyedCommand{ID: fmt.Sprintf("group-%d", id)}, func(tx *Tx) (any, error) {
				return id, tx.Put(dataset, fmt.Sprintf("key-%d", id), id)
			})
			results <- err
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		db.keyed.commit.mu.Lock()
		queued := len(db.keyed.commit.queue)
		db.keyed.commit.mu.Unlock()
		if queued == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("group did not queue")
		}
		runtime.Gosched()
	}
	close(releaseFirst)
	if err := <-blocker; err != nil {
		t.Fatal(err)
	}
	<-responsesBlocked

	recovered, err := OpenCatalog(ctx, path, KeyedOptions{})
	if err != nil {
		t.Fatalf("open after Sync/response-loss window: %v", err)
	}
	recoveredBucket, _ := recovered.Bucket(ctx, "app")
	recoveredDataset, _ := recoveredBucket.Dataset(ctx, "values", DatasetOptions{TypeIdentity: "response-loss/v1"})
	for id := 0; id < 3; id++ {
		var value int
		found, err := recoveredDataset.Get(ctx, fmt.Sprintf("key-%d", id), &value)
		if err != nil || !found || value != id {
			t.Fatalf("recovered key-%d=%d found=%v err=%v", id, value, found, err)
		}
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	close(releaseResponses)
	for range 3 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
