// Package jobs implements durable worker job state and ordered Ready-job
// discovery on OctetDB's workers/jobs catalog topology.
package jobs

import (
	"context"
	"errors"

	"github.com/yuechen-li-dev/octetdb"
)

const jobTypeIdentity = "participant-a.jobs.Job/v1"

// Status is a durable job lifecycle state.
type Status string

const (
	Ready     Status = "Ready"
	Claimed   Status = "Claimed"
	Completed Status = "Completed"
	Failed    Status = "Failed"
)

// Job is one durable job record.
type Job struct {
	ID       string `json:"id"`
	Payload  string `json:"payload,omitempty"`
	Status   Status `json:"status"`
	WorkerID string `json:"worker_id,omitempty"`
	Failure  string `json:"failure,omitempty"`
}

// Result describes the durable decision for one job mutation.
type Result struct {
	Job       Job
	Applied   bool
	Code      string
	Duplicate bool
	Sequence  uint64
}

// Queue owns handles for the workers/jobs topology.
type Queue struct {
	db   *octetdb.Database
	jobs *octetdb.Dataset
}

// Open creates or reopens a durable job queue rooted at path.
func Open(ctx context.Context, path string) (*Queue, error) {
	db, err := octetdb.OpenCatalog(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		return nil, err
	}
	bucket, err := db.Bucket(ctx, "workers")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	dataset, err := bucket.Dataset(ctx, "jobs", octetdb.DatasetOptions{TypeIdentity: jobTypeIdentity})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Queue{db: db, jobs: dataset}, nil
}

// Close snapshots and closes the queue database.
func (q *Queue) Close() error {
	if q == nil || q.db == nil {
		return nil
	}
	return q.db.Close()
}

// Create creates a Ready job.
func (q *Queue) Create(ctx context.Context, commandID, jobID, payload string) (Result, error) {
	if err := validate(q, commandID, jobID); err != nil {
		return Result{}, err
	}
	decision, err := q.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.Tx) (any, error) {
		var current Job
		found, err := tx.Get(q.jobs, jobID, &current)
		if err != nil {
			return nil, err
		}
		if found {
			return current, octetdb.RejectWithResult("job_exists", current)
		}
		job := Job{ID: jobID, Payload: payload, Status: Ready}
		return job, tx.Put(q.jobs, jobID, job)
	})
	return jobResult(decision, err)
}

// Get reads a job by ID.
func (q *Queue) Get(ctx context.Context, jobID string) (Job, bool, error) {
	if q == nil || q.jobs == nil {
		return Job{}, false, errors.New("job queue is closed or uninitialized")
	}
	if jobID == "" {
		return Job{}, false, errors.New("job ID is required")
	}
	var job Job
	found, err := q.jobs.Get(ctx, jobID, &job)
	return job, found, err
}

// Claim changes a Ready job to Claimed by workerID.
func (q *Queue) Claim(ctx context.Context, commandID, jobID, workerID string) (Result, error) {
	if workerID == "" {
		return Result{}, errors.New("worker ID is required")
	}
	return q.transition(ctx, commandID, jobID, func(job Job) (Job, string) {
		if job.Status != Ready {
			return job, "job_not_ready"
		}
		job.Status = Claimed
		job.WorkerID = workerID
		job.Failure = ""
		return job, ""
	})
}

// Complete changes a claimed job to Completed. Only its claiming worker may
// complete it.
func (q *Queue) Complete(ctx context.Context, commandID, jobID, workerID string) (Result, error) {
	if workerID == "" {
		return Result{}, errors.New("worker ID is required")
	}
	return q.transition(ctx, commandID, jobID, func(job Job) (Job, string) {
		if job.Status != Claimed {
			return job, "job_not_claimed"
		}
		if job.WorkerID != workerID {
			return job, "wrong_worker"
		}
		job.Status = Completed
		return job, ""
	})
}

// Fail changes a claimed job to Failed. Only its claiming worker may fail it.
func (q *Queue) Fail(ctx context.Context, commandID, jobID, workerID, reason string) (Result, error) {
	if workerID == "" {
		return Result{}, errors.New("worker ID is required")
	}
	return q.transition(ctx, commandID, jobID, func(job Job) (Job, string) {
		if job.Status != Claimed {
			return job, "job_not_claimed"
		}
		if job.WorkerID != workerID {
			return job, "wrong_worker"
		}
		job.Status = Failed
		job.Failure = reason
		return job, ""
	})
}

// ListReady returns at most n Ready jobs in ascending job-ID order. It scans
// the durable jobs dataset directly and stops the upstream scan as soon as n
// matches have been collected; there is no shadow ready list.
func (q *Queue) ListReady(ctx context.Context, n int) ([]Job, error) {
	if q == nil || q.jobs == nil {
		return nil, errors.New("job queue is closed or uninitialized")
	}
	if n < 0 {
		return nil, errors.New("limit cannot be negative")
	}
	ready := make([]Job, 0, n)
	if n == 0 {
		return ready, nil
	}
	err := octetdb.ScanDataset(ctx, q.jobs, func(_ string, job Job) (octetdb.ScanAction, error) {
		if job.Status != Ready {
			return octetdb.ScanContinue, nil
		}
		ready = append(ready, job)
		if len(ready) == n {
			return octetdb.ScanStop, nil
		}
		return octetdb.ScanContinue, nil
	})
	return ready, err
}

func (q *Queue) transition(ctx context.Context, commandID, jobID string, change func(Job) (Job, string)) (Result, error) {
	if err := validate(q, commandID, jobID); err != nil {
		return Result{}, err
	}
	decision, err := q.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.Tx) (any, error) {
		var job Job
		found, err := tx.Get(q.jobs, jobID, &job)
		if err != nil {
			return nil, err
		}
		if !found {
			return job, octetdb.RejectWithResult("job_not_found", job)
		}
		next, code := change(job)
		if code != "" {
			return job, octetdb.RejectWithResult(code, job)
		}
		return next, tx.Put(q.jobs, jobID, next)
	})
	return jobResult(decision, err)
}

func jobResult(decision octetdb.KeyedDecision, err error) (Result, error) {
	if err != nil {
		return Result{}, err
	}
	var job Job
	if err := octetdb.DecodeResult(decision, &job); err != nil {
		return Result{}, err
	}
	return Result{Job: job, Applied: decision.Applied, Code: decision.Code, Duplicate: decision.Duplicate, Sequence: decision.Sequence}, nil
}

func validate(q *Queue, commandID, jobID string) error {
	if q == nil || q.db == nil || q.jobs == nil {
		return errors.New("job queue is closed or uninitialized")
	}
	if commandID == "" {
		return errors.New("command ID is required")
	}
	if jobID == "" {
		return errors.New("job ID is required")
	}
	return nil
}
