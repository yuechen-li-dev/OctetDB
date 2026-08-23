package store

import (
	"context"
	"example.com/octetdb-golden/job/internal/service"
	"path/filepath"
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
