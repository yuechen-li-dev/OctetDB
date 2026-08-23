package store

import (
	"context"
	"example.com/octetdb-golden/job/internal/service"
	"github.com/yuechen-li-dev/octetdb"
	"net/url"
)

type DB struct{ db *octetdb.KeyedDB }

func Open(ctx context.Context, path string) (*DB, error) {
	db, err := octetdb.OpenKeyed(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		return nil, err
	}
	return &DB{db: db}, nil
}
func (s *DB) Close() error    { return s.db.Close() }
func jobKey(id string) string { return "jobs/" + url.PathEscape(id) }
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
func (s *DB) mutate(ctx context.Context, commandID, id string, change func(*service.Job, bool) error) (service.Decision, error) {
	decision, err := s.db.SubmitKeyed(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.KeyedTx) (any, error) {
		var job service.Job
		exists, err := tx.Get(jobKey(id), &job)
		if err != nil {
			return nil, err
		}
		if err := change(&job, exists); err != nil {
			return nil, err
		}
		return job, tx.Put(jobKey(id), job)
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
	ok, err := s.db.GetKeyed(ctx, jobKey(id), &job)
	if err != nil {
		return job, err
	}
	if !ok {
		return job, service.ErrNotFound
	}
	return job, nil
}
