package store

import (
	"context"
	"example.com/octetdb-golden/job/internal/service"
	"github.com/yuechen-li-dev/octetdb"
)

type DB struct {
	db   *octetdb.CatalogDB
	jobs *octetdb.Dataset
}

func Open(ctx context.Context, path string) (*DB, error) {
	db, err := octetdb.OpenCatalog(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		return nil, err
	}
	bucket, err := db.Bucket(ctx, "workers")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	jobs, err := bucket.Dataset(ctx, "jobs", octetdb.DatasetOptions{TypeIdentity: "job.Job/v1"})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &DB{db: db, jobs: jobs}, nil
}
func (s *DB) Close() error { return s.db.Close() }
func (s *DB) Create(ctx context.Context, commandID, id string) (service.Decision, error) {
	return s.mutate(ctx, commandID, id, func(job *service.Job, exists bool) error {
		if exists {
			return octetdb.RejectWithResult("job_exists", *job)
		}
		*job = service.Job{ID: id, Status: service.Ready}
		return nil
	})
}
func (s *DB) Claim(ctx context.Context, commandID, id, owner string) (service.Decision, error) {
	return s.mutate(ctx, commandID, id, func(job *service.Job, exists bool) error {
		if !exists {
			return octetdb.Reject("not_found")
		}
		if job.Status != service.Ready && job.Status != service.Failed {
			return octetdb.RejectWithResult("not_claimable", *job)
		}
		job.Status = service.Claimed
		job.Owner = owner
		job.Attempts++
		job.Failure = ""
		return nil
	})
}
func (s *DB) Complete(ctx context.Context, commandID, id, owner string) (service.Decision, error) {
	return s.mutate(ctx, commandID, id, func(job *service.Job, exists bool) error {
		if !exists {
			return octetdb.Reject("not_found")
		}
		if job.Status != service.Claimed || job.Owner != owner {
			return octetdb.RejectWithResult("not_owner", *job)
		}
		job.Status = service.Completed
		return nil
	})
}
func (s *DB) Fail(ctx context.Context, commandID, id, owner, reason string) (service.Decision, error) {
	return s.mutate(ctx, commandID, id, func(job *service.Job, exists bool) error {
		if !exists {
			return octetdb.Reject("not_found")
		}
		if job.Status != service.Claimed || job.Owner != owner {
			return octetdb.RejectWithResult("not_owner", *job)
		}
		job.Status = service.Failed
		job.Failure = reason
		job.Owner = ""
		return nil
	})
}
func (s *DB) Requeue(ctx context.Context, commandID, id string) (service.Decision, error) {
	return s.mutate(ctx, commandID, id, func(job *service.Job, exists bool) error {
		if !exists {
			return octetdb.Reject("not_found")
		}
		if job.Status != service.Failed {
			return octetdb.RejectWithResult("not_failed", *job)
		}
		job.Status = service.Ready
		job.Owner = ""
		job.Failure = ""
		return nil
	})
}
func (s *DB) mutate(ctx context.Context, commandID, id string, change func(*service.Job, bool) error) (service.Decision, error) {
	decision, err := s.jobs.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.DatasetTx) (any, error) {
		var job service.Job
		exists, err := tx.Get(id, &job)
		if err != nil {
			return nil, err
		}
		if err := change(&job, exists); err != nil {
			return nil, err
		}
		return job, tx.Put(id, job)
	})
	if err != nil {
		return service.Decision{}, err
	}
	var job service.Job
	if len(decision.Result) > 0 {
		if err := octetdb.DecodeResult(decision, &job); err != nil {
			return service.Decision{}, err
		}
	}
	return service.Decision{Applied: decision.Applied, Code: decision.Code, Duplicate: decision.Duplicate, Job: job}, nil
}
func (s *DB) Get(ctx context.Context, id string) (service.Job, error) {
	var job service.Job
	ok, err := s.jobs.Get(ctx, id, &job)
	if err != nil {
		return job, err
	}
	if !ok {
		return job, service.ErrNotFound
	}
	return job, nil
}

// ListReady returns at most limit Ready jobs in deterministic job-ID order.
func (s *DB) ListReady(ctx context.Context, limit int) ([]service.Job, error) {
	if limit <= 0 {
		return []service.Job{}, nil
	}
	jobs := make([]service.Job, 0, limit)
	err := octetdb.ScanDataset(ctx, s.jobs, func(_ string, job service.Job) (octetdb.ScanAction, error) {
		if job.Status != service.Ready {
			return octetdb.ScanContinue, nil
		}
		jobs = append(jobs, job)
		if len(jobs) == limit {
			return octetdb.ScanStop, nil
		}
		return octetdb.ScanContinue, nil
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}
