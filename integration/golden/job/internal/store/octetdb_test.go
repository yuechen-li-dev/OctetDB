package store

import (
	"context"
	"example.com/octetdb-golden/job/internal/service"
	"path/filepath"
	"reflect"
	"testing"
)

func TestJobLifecycleRestartAndRetry(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if d, err := db.Create(ctx, "create", "j1"); err != nil || !d.Applied {
		t.Fatalf("d=%+v err=%v", d, err)
	}
	claim, err := db.Claim(ctx, "claim-a", "j1", "worker-a")
	if err != nil || !claim.Applied || claim.Job.Attempts != 1 {
		t.Fatalf("d=%+v err=%v", claim, err)
	}
	retry, err := db.Claim(ctx, "claim-a", "j1", "worker-a")
	if err != nil || !retry.Duplicate || retry.Job.Owner != "worker-a" {
		t.Fatalf("d=%+v err=%v", retry, err)
	}
	other, err := db.Claim(ctx, "claim-b", "j1", "worker-b")
	if err != nil || other.Applied || other.Code != "not_claimable" {
		t.Fatalf("d=%+v err=%v", other, err)
	}
	if d, err := db.Fail(ctx, "fail-a", "j1", "worker-a", "temporary"); err != nil || !d.Applied {
		t.Fatalf("d=%+v err=%v", d, err)
	}
	if d, err := db.Claim(ctx, "retry-claim", "j1", "worker-b"); err != nil || !d.Applied || d.Job.Attempts != 2 {
		t.Fatalf("d=%+v err=%v", d, err)
	}
	if d, err := db.Complete(ctx, "complete", "j1", "worker-b"); err != nil || !d.Applied {
		t.Fatalf("d=%+v err=%v", d, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	job, err := db.Get(ctx, "j1")
	if err != nil || job.Status != service.Completed || job.Attempts != 2 {
		t.Fatalf("job=%+v err=%v", job, err)
	}
}

func TestListReadyJobsDeterministicTakeLifecycleAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"j30", "j10", "j20", "j40", "j50"} {
		if decision, err := db.Create(ctx, "create-"+id, id); err != nil || !decision.Applied {
			t.Fatalf("create %s decision=%+v err=%v", id, decision, err)
		}
	}
	if decision, err := db.Claim(ctx, "claim-j20", "j20", "worker"); err != nil || !decision.Applied {
		t.Fatalf("claim j20 decision=%+v err=%v", decision, err)
	}
	if decision, err := db.Claim(ctx, "claim-j40", "j40", "worker"); err != nil || !decision.Applied {
		t.Fatalf("claim j40 decision=%+v err=%v", decision, err)
	}
	if decision, err := db.Complete(ctx, "complete-j40", "j40", "worker"); err != nil || !decision.Applied {
		t.Fatalf("complete j40 decision=%+v err=%v", decision, err)
	}
	if decision, err := db.Claim(ctx, "claim-j50", "j50", "worker"); err != nil || !decision.Applied {
		t.Fatalf("claim j50 decision=%+v err=%v", decision, err)
	}
	if decision, err := db.Fail(ctx, "fail-j50", "j50", "worker", "retry"); err != nil || !decision.Applied {
		t.Fatalf("fail j50 decision=%+v err=%v", decision, err)
	}
	ready, err := db.ListReady(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := jobIDs(ready); !reflect.DeepEqual(got, []string{"j10", "j30"}) {
		t.Fatalf("ready before requeue=%v", got)
	}
	if decision, err := db.Requeue(ctx, "requeue-j50", "j50"); err != nil || !decision.Applied || decision.Job.Status != service.Ready {
		t.Fatalf("requeue decision=%+v err=%v", decision, err)
	}
	ready, err = db.ListReady(ctx, 2)
	if err != nil || !reflect.DeepEqual(jobIDs(ready), []string{"j10", "j30"}) {
		t.Fatalf("take ready=%v err=%v", jobIDs(ready), err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ready, err = db.ListReady(ctx, 10)
	if err != nil || !reflect.DeepEqual(jobIDs(ready), []string{"j10", "j30", "j50"}) {
		t.Fatalf("restart ready=%v err=%v", jobIDs(ready), err)
	}
}

func jobIDs(jobs []service.Job) []string {
	ids := make([]string, len(jobs))
	for index, job := range jobs {
		ids[index] = job.ID
	}
	return ids
}
