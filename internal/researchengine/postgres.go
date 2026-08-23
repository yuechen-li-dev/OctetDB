package engine

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const PostgreSQLSchema = `
CREATE TABLE IF NOT EXISTS m7_accounts (
  id BIGINT PRIMARY KEY,
  balance BIGINT NOT NULL CHECK (balance >= 0),
  status SMALLINT NOT NULL,
  version BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS m7_ledger (
  sequence BIGSERIAL PRIMARY KEY,
  command_id TEXT UNIQUE NOT NULL,
  account_a BIGINT NOT NULL,
  account_b BIGINT,
  amount BIGINT NOT NULL,
  effect SMALLINT NOT NULL
);
CREATE TABLE IF NOT EXISTS m7_pending (
  account_id BIGINT PRIMARY KEY,
  active BOOLEAN NOT NULL,
  target BIGINT NOT NULL,
  amount BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS m7_commands (
  command_id TEXT PRIMARY KEY,
  result JSONB NOT NULL
);`

type PostgreSQLBaseline struct{ Pool *pgxpool.Pool }

func OpenPostgreSQL(ctx context.Context, dsn string) (*PostgreSQLBaseline, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if _, err := pool.Exec(ctx, PostgreSQLSchema); err != nil {
		pool.Close()
		return nil, err
	}
	return &PostgreSQLBaseline{Pool: pool}, nil
}

func (p *PostgreSQLBaseline) Reset(ctx context.Context) error {
	_, err := p.Pool.Exec(ctx, "TRUNCATE m7_commands, m7_pending, m7_ledger, m7_accounts RESTART IDENTITY")
	return err
}

func (p *PostgreSQLBaseline) Execute(ctx context.Context, command Command) (Result, error) {
	if err := ValidateCommand(command); err != nil {
		return Result{}, err
	}
	tx, err := p.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback(ctx)
	var priorBytes []byte
	err = tx.QueryRow(ctx, "SELECT result FROM m7_commands WHERE command_id=$1", command.ID).Scan(&priorBytes)
	if err == nil {
		var prior Result
		if err := json.Unmarshal(priorBytes, &prior); err != nil {
			return Result{}, err
		}
		prior.Duplicate = true
		return prior, tx.Commit(ctx)
	}
	if err != pgx.ErrNoRows {
		return Result{}, err
	}
	ids := []AccountID{command.Account}
	if command.Other != 0 {
		ids = append(ids, command.Other)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	accounts := make(map[AccountID]Account, len(ids))
	for _, id := range ids {
		var a Account
		err := tx.QueryRow(ctx, "SELECT id,balance,status,version FROM m7_accounts WHERE id=$1 FOR UPDATE", id).Scan(&a.ID, &a.Balance, &a.Status, &a.Version)
		if err == nil {
			accounts[id] = a
		} else if err != pgx.ErrNoRows {
			return Result{}, err
		}
	}
	a, aok := accounts[command.Account]
	b, bok := accounts[command.Other]
	var pending pendingTransfer
	err = tx.QueryRow(ctx, "SELECT active,target,amount FROM m7_pending WHERE account_id=$1 FOR UPDATE", command.Account).Scan(&pending.Active, &pending.Target, &pending.Amount)
	if err != nil && err != pgx.ErrNoRows {
		return Result{}, err
	}
	d := decideGo(command, a, aok, b, bok, pending)
	result := Result{CommandID: command.ID, Accepted: d.accepted, ReasonTag: d.reason, EffectTag: d.effect}
	switch d.effect {
	case 1:
		_, err = tx.Exec(ctx, "INSERT INTO m7_accounts(id,balance,status,version) VALUES($1,$2,$3,1)", command.Account, d.balanceA, StatusOpen)
	case 2:
		_, err = tx.Exec(ctx, "UPDATE m7_accounts SET balance=$2,version=version+1 WHERE id=$1 AND version=$3", command.Account, d.balanceA, a.Version)
	case 3:
		_, err = tx.Exec(ctx, "UPDATE m7_accounts SET balance=CASE id WHEN $1::bigint THEN $3::bigint WHEN $2::bigint THEN $4::bigint END,version=version+1 WHERE id IN ($1::bigint,$2::bigint)", command.Account, command.Other, d.balanceA, d.balanceB)
	case 4:
		_, err = tx.Exec(ctx, "UPDATE m7_accounts SET status=$2,version=version+1 WHERE id=$1", command.Account, d.status)
	}
	if err != nil {
		return Result{}, err
	}
	if d.effect != 0 {
		if _, err = tx.Exec(ctx, "INSERT INTO m7_ledger(command_id,account_a,account_b,amount,effect) VALUES($1,$2,NULLIF($3,0),$4,$5)", command.ID, command.Account, command.Other, command.Amount, d.effect); err != nil {
			return Result{}, err
		}
	}
	if isKind(command.Kind, BeginTransfer) && d.accepted {
		pending = pendingTransfer{Active: true, Target: command.Other, Amount: command.Amount}
	}
	if isKind(command.Kind, Confirm) || isKind(command.Kind, Cancel) {
		pending = pendingTransfer{}
	}
	if _, err = tx.Exec(ctx, "INSERT INTO m7_pending(account_id,active,target,amount) VALUES($1,$2,$3,$4) ON CONFLICT(account_id) DO UPDATE SET active=EXCLUDED.active,target=EXCLUDED.target,amount=EXCLUDED.amount", command.Account, pending.Active, pending.Target, pending.Amount); err != nil {
		return Result{}, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return Result{}, err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO m7_commands(command_id,result) VALUES($1,$2)", command.ID, encoded); err != nil {
		return Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, err
	}
	return result, nil
}

func (p *PostgreSQLBaseline) Account(ctx context.Context, id AccountID) (Account, error) {
	var a Account
	err := p.Pool.QueryRow(ctx, "SELECT id,balance,status,version FROM m7_accounts WHERE id=$1", id).Scan(&a.ID, &a.Balance, &a.Status, &a.Version)
	return a, err
}

func (p *PostgreSQLBaseline) Close() { p.Pool.Close() }
