package baseline

import (
	"context"
	"time"

	"github.com/yuechen-li-dev/database-scheduler/internal/db"
	"github.com/yuechen-li-dev/database-scheduler/internal/workload"
)

type Result struct {
	Err                error
	QueueTime, Service time.Duration
	BatchSize          int
}

type Lane struct{ Store *db.Store }

func (l Lane) Submit(ctx context.Context, op workload.Operation) Result {
	start := time.Now()
	err := l.Store.Execute(ctx, op)
	return Result{Err: err, Service: time.Since(start), BatchSize: 1}
}
