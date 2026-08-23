package jobs_test

import (
	"context"
	"reflect"
	"testing"

	"participant-a/jobs"
)

func TestJobLifecycleDiscoveryAndRestart(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	queue, err := jobs.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	// Deliberately create out of key order; scans must still be deterministic.
	for _, id := range []string{"job-04", "job-02", "job-01", "job-03"} {
		result, err := queue.Create(ctx, "create-"+id, id, "payload "+id)
		if err != nil {
			t.Fatal(err)
		}
		if !result.Applied || result.Job.Status != jobs.Ready {
			t.Fatalf("create %s: %+v", id, result)
		}
	}

	claim, err := queue.Claim(ctx, "claim-02", "job-02", "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if !claim.Applied || claim.Job.Status != jobs.Claimed || claim.Job.WorkerID != "worker-a" {
		t.Fatalf("unexpected claim: %+v", claim)
	}

	ready, err := queue.ListReady(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(ready); !reflect.DeepEqual(got, []string{"job-01", "job-03"}) {
		t.Fatalf("first two Ready jobs are not ordered or claimed job leaked: %v", got)
	}

	complete, err := queue.Complete(ctx, "complete-02", "job-02", "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if !complete.Applied || complete.Job.Status != jobs.Completed {
		t.Fatalf("unexpected completion: %+v", complete)
	}

	if _, err := queue.Claim(ctx, "claim-03", "job-03", "worker-b"); err != nil {
		t.Fatal(err)
	}
	failed, err := queue.Fail(ctx, "fail-03", "job-03", "worker-b", "permanent input error")
	if err != nil {
		t.Fatal(err)
	}
	if !failed.Applied || failed.Job.Status != jobs.Failed || failed.Job.Failure != "permanent input error" {
		t.Fatalf("unexpected failure: %+v", failed)
	}

	ready, err = queue.ListReady(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(ready); !reflect.DeepEqual(got, []string{"job-01", "job-04"}) {
		t.Fatalf("terminal/claimed jobs leaked into Ready results: %v", got)
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
	if got := ids(ready); !reflect.DeepEqual(got, []string{"job-01", "job-04"}) {
		t.Fatalf("restart changed Ready discovery: %v", got)
	}
	for id, status := range map[string]jobs.Status{"job-02": jobs.Completed, "job-03": jobs.Failed} {
		job, found, err := queue.Get(ctx, id)
		if err != nil || !found || job.Status != status {
			t.Fatalf("restart state %s: found=%v job=%+v err=%v", id, found, job, err)
		}
	}
}

func TestClaimExcludesImmediatelyAndTransitionsRejectInvalidState(t *testing.T) {
	ctx := context.Background()
	queue, err := jobs.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	if _, err := queue.Create(ctx, "create", "job", "work"); err != nil {
		t.Fatal(err)
	}
	first, err := queue.Claim(ctx, "claim-1", "job", "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := queue.Claim(ctx, "claim-2", "job", "worker-b")
	if err != nil {
		t.Fatal(err)
	}
	if !first.Applied || second.Applied || second.Code != "job_not_ready" || second.Job.WorkerID != "worker-a" {
		t.Fatalf("double claim was not safely rejected: first=%+v second=%+v", first, second)
	}
	ready, err := queue.ListReady(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 0 {
		t.Fatalf("claimed job appeared Ready: %+v", ready)
	}
	wrongWorker, err := queue.Complete(ctx, "complete-wrong", "job", "worker-b")
	if err != nil {
		t.Fatal(err)
	}
	if wrongWorker.Applied || wrongWorker.Code != "wrong_worker" || wrongWorker.Job.Status != jobs.Claimed {
		t.Fatalf("wrong worker completed job: %+v", wrongWorker)
	}
	zero, err := queue.ListReady(ctx, 0)
	if err != nil || len(zero) != 0 {
		t.Fatalf("zero-limit Take result: jobs=%v err=%v", zero, err)
	}
}

func ids(values []jobs.Job) []string {
	result := make([]string, len(values))
	for i := range values {
		result[i] = values[i].ID
	}
	return result
}
