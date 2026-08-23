package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresBackend struct {
	cfg      config
	pool     *pgxpool.Pool
	examined atomic.Uint64
}

func newPostgresBackend(ctx context.Context, cfg config) (*postgresBackend, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.PostgresURL)
	if err != nil {
		return nil, err
	}
	poolCfg.MaxConns = int32(max(4, cfg.Concurrency))
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	return &postgresBackend{cfg: cfg, pool: pool}, nil
}

func (b *postgresBackend) Setup(ctx context.Context) error {
	ddl := `DROP SCHEMA IF EXISTS perfm4 CASCADE; CREATE SCHEMA perfm4;
		CREATE TABLE perfm4.commands (id text PRIMARY KEY, result text NOT NULL);
		CREATE TABLE perfm4.records (
			id bigint PRIMARY KEY, balance bigint NOT NULL DEFAULT 0 CHECK (balance >= 0),
			available bigint NOT NULL DEFAULT 0 CHECK (available >= 0), reserved bigint NOT NULL DEFAULT 0 CHECK (reserved >= 0),
			status text NOT NULL DEFAULT '', attempts integer NOT NULL DEFAULT 0, result text NOT NULL DEFAULT ''
		);
		CREATE INDEX records_status_id ON perfm4.records(status, id);
		CREATE INDEX records_available_id ON perfm4.records(available, id);`
	if _, err := b.pool.Exec(ctx, ddl); err != nil {
		return err
	}
	if b.cfg.Workload == "w4" {
		return nil
	}
	rows := make([][]any, 0, b.cfg.Population)
	for i := 0; i < b.cfg.Population; i++ {
		r := seedRecord(b.cfg.Workload, i)
		if b.cfg.Workload == "w5" {
			r.Status = queryStatus(b.cfg.Selectivity, i)
		}
		rows = append(rows, []any{r.ID, r.Balance, r.Available, r.Reserved, r.Status, r.Attempts, r.Result})
	}
	_, err := b.pool.CopyFrom(ctx, pgx.Identifier{"perfm4", "records"}, []string{"id", "balance", "available", "reserved", "status", "attempts", "result"}, pgx.CopyFromRows(rows))
	return err
}

func (b *postgresBackend) Operation(ctx context.Context, operation int) error {
	switch b.cfg.Workload {
	case "w1":
		return b.transfer(ctx, operation)
	case "w2":
		return b.inventory(ctx, operation)
	case "w3":
		return b.jobs(ctx, operation)
	case "w4":
		return b.webhook(ctx, operation)
	case "w5":
		return b.query(ctx, operation)
	case "w6":
		return b.mixed(ctx, operation)
	default:
		return fmt.Errorf("unknown workload %s", b.cfg.Workload)
	}
}

func (b *postgresBackend) commandTx(ctx context.Context, operation int, work func(pgx.Tx) error) error {
	tx, err := b.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var inserted string
	err = tx.QueryRow(ctx, `INSERT INTO perfm4.commands(id,result) VALUES($1,'ok') ON CONFLICT DO NOTHING RETURNING id`, commandID(operation)).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := work(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (b *postgresBackend) transfer(ctx context.Context, operation int) error {
	source, destination := choosePair(b.cfg, operation)
	return b.commandTx(ctx, operation, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id,balance FROM perfm4.records WHERE id=ANY($1) ORDER BY id FOR UPDATE`, []int64{int64(source), int64(destination)})
		if err != nil {
			return err
		}
		balances := map[int]int64{}
		for rows.Next() {
			var id int
			var balance int64
			if err := rows.Scan(&id, &balance); err != nil {
				rows.Close()
				return err
			}
			balances[id] = balance
		}
		rows.Close()
		if rows.Err() != nil {
			return rows.Err()
		}
		if balances[source] < 1 {
			return nil
		}
		_, err = tx.Exec(ctx, `UPDATE perfm4.records SET balance=CASE WHEN id=$1 THEN balance-1 ELSE balance+1 END WHERE id IN ($1,$2)`, source, destination)
		return err
	})
}

func (b *postgresBackend) inventory(ctx context.Context, operation int) error {
	id := positive(operation) % b.cfg.Population
	return b.commandTx(ctx, operation, func(tx pgx.Tx) error {
		var tag pgconnTag
		var err error
		switch positive(operation) % 3 {
		case 0:
			tag, err = execTag(tx.Exec(ctx, `UPDATE perfm4.records SET available=available-1,reserved=reserved+1 WHERE id=$1 AND available>0`, id))
		case 1:
			tag, err = execTag(tx.Exec(ctx, `UPDATE perfm4.records SET available=available+1,reserved=reserved-1 WHERE id=$1 AND reserved>0`, id))
		case 2:
			tag, err = execTag(tx.Exec(ctx, `UPDATE perfm4.records SET available=available+1 WHERE id=$1`, id))
		}
		_ = tag
		return err
	})
}

func (b *postgresBackend) jobs(ctx context.Context, operation int) error {
	if positive(operation)%5 == 4 {
		rows, err := b.pool.Query(ctx, `SELECT id,status,attempts FROM perfm4.records WHERE status='ready' ORDER BY id LIMIT 10`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			b.examined.Add(1)
		}
		return rows.Err()
	}
	id := positive(operation) % b.cfg.Population
	return b.commandTx(ctx, operation, func(tx pgx.Tx) error {
		var sql string
		switch positive(operation) % 4 {
		case 0:
			sql = `UPDATE perfm4.records SET status='claimed',attempts=attempts+1 WHERE id=$1 AND status IN ('ready','failed')`
		case 1:
			sql = `UPDATE perfm4.records SET status='completed' WHERE id=$1 AND status='claimed'`
		case 2:
			sql = `UPDATE perfm4.records SET status='failed' WHERE id=$1 AND status='claimed'`
		case 3:
			sql = `UPDATE perfm4.records SET status='ready' WHERE id=$1`
		}
		_, err := tx.Exec(ctx, sql, id)
		return err
	})
}

func (b *postgresBackend) webhook(ctx context.Context, operation int) error {
	id := positive(operation) % b.cfg.Population
	expected := fmt.Sprintf("result-%d", id)
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var durable string
	err = tx.QueryRow(ctx, `INSERT INTO perfm4.records(id,status,result) VALUES($1,'processed',$2) ON CONFLICT (id) DO NOTHING RETURNING result`, id, expected).Scan(&durable)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `SELECT result FROM perfm4.records WHERE id=$1`, id).Scan(&durable)
	}
	if err != nil {
		return err
	}
	if durable != expected {
		return fmt.Errorf("webhook durable result mismatch")
	}
	return tx.Commit(ctx)
}

func (b *postgresBackend) query(ctx context.Context, operation int) error {
	switch queryVariant(b.cfg.QueryOp, operation) {
	case 0:
		var r record
		return b.pool.QueryRow(ctx, `SELECT id,balance,available,reserved,status,attempts,result FROM perfm4.records WHERE id=$1`, positive(operation)%b.cfg.Population).Scan(&r.ID, &r.Balance, &r.Available, &r.Reserved, &r.Status, &r.Attempts, &r.Result)
	case 1:
		return b.consume(ctx, `SELECT id FROM perfm4.records WHERE status='ready' ORDER BY id`)
	case 2:
		return b.consume(ctx, `SELECT id FROM perfm4.records WHERE status='ready' ORDER BY id LIMIT 10`)
	case 3:
		return b.consume(ctx, `SELECT id*2 FROM perfm4.records WHERE status='ready' ORDER BY id`)
	default:
		var count int64
		return b.pool.QueryRow(ctx, `SELECT count(*) FROM perfm4.records`).Scan(&count)
	}
}

func (b *postgresBackend) consume(ctx context.Context, sql string) error {
	rows, err := b.pool.Query(ctx, sql)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var value int64
		if err := rows.Scan(&value); err != nil {
			return err
		}
		b.examined.Add(1)
	}
	return rows.Err()
}

func (b *postgresBackend) mixed(ctx context.Context, operation int) error {
	selector := positive(operation) % 100
	writeLimit := 20
	if b.cfg.Mix == "50r40w10q" {
		writeLimit = 40
	}
	readLimit := 90 - writeLimit
	if selector < readLimit {
		var value int64
		return b.pool.QueryRow(ctx, `SELECT available FROM perfm4.records WHERE id=$1`, positive(operation)%b.cfg.Population).Scan(&value)
	}
	if selector < 90 {
		return b.inventory(ctx, operation)
	}
	return b.consume(ctx, `SELECT id FROM perfm4.records WHERE available < 10002 ORDER BY available,id LIMIT 10`)
}

func (b *postgresBackend) Verify(ctx context.Context) (map[string]bool, error) {
	checks := map[string]bool{"records_valid": true, "domain_invariant": true, "idempotency": true}
	var count int
	var invalid int
	var total int64
	if err := b.pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE balance<0 OR available<0 OR reserved<0),coalesce(sum(balance),0) FROM perfm4.records`).Scan(&count, &invalid, &total); err != nil {
		return nil, err
	}
	if b.cfg.Workload != "w4" && count != b.cfg.Population {
		checks["records_valid"] = false
	}
	if invalid != 0 || (b.cfg.Workload == "w1" && total != int64(b.cfg.Population)*100_000) {
		checks["domain_invariant"] = false
	}
	first, err := b.pool.Exec(ctx, `INSERT INTO perfm4.commands(id,result) VALUES('verify-idempotency','ok') ON CONFLICT DO NOTHING`)
	if err != nil {
		return nil, err
	}
	second, err := b.pool.Exec(ctx, `INSERT INTO perfm4.commands(id,result) VALUES('verify-idempotency','bad') ON CONFLICT DO NOTHING`)
	if err != nil {
		return nil, err
	}
	if first.RowsAffected()+second.RowsAffected() > 1 || second.RowsAffected() != 0 {
		checks["idempotency"] = false
	}
	return checks, nil
}

func (b *postgresBackend) StorageBytes() (int64, error) {
	var bytes int64
	err := b.pool.QueryRow(context.Background(), `SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='perfm4' AND c.relkind IN ('r','m')`).Scan(&bytes)
	return bytes, err
}
func (b *postgresBackend) WALBytes() (int64, error) {
	var bytes int64
	err := b.pool.QueryRow(context.Background(), `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')::bigint`).Scan(&bytes)
	return bytes, err
}
func (b *postgresBackend) RecordsExamined() uint64 { return b.examined.Load() }
func (b *postgresBackend) ResetMetrics()           { b.examined.Store(0) }
func (b *postgresBackend) Metadata() map[string]string {
	return map[string]string{"postgres_topology": "out-of-process TCP pool", "specialization_level": "N/A"}
}
func (b *postgresBackend) Close() error {
	if b.pool != nil {
		b.pool.Close()
	}
	return nil
}

// pgconn.CommandTag is kept behind this tiny alias helper so callers can ignore
// the affected-row count while still checking the execution error.
type pgconnTag interface{ RowsAffected() int64 }

func execTag[T pgconnTag](tag T, err error) (pgconnTag, error) { return tag, err }
