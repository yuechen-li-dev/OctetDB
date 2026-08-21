package db

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yuechen-li-dev/database-scheduler/internal/workload"
)

//go:embed schema.sql
var schemaFS embed.FS

type Store struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = maxConns
	cfg.MaxConnIdleTime = 0
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() { s.Pool.Close() }

func (s *Store) Reset(ctx context.Context) error {
	schema, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return err
	}
	if _, err := s.Pool.Exec(ctx, string(schema)); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "TRUNCATE order_items, orders, inventory, customers CASCADE"); err != nil {
		return err
	}
	for id := int64(1); id <= 100; id++ {
		if _, err := tx.Exec(ctx, "INSERT INTO customers(id,name,email) VALUES($1,$2,$3)", id, fmt.Sprintf("customer-%03d", id), fmt.Sprintf("customer-%03d@example.test", id)); err != nil {
			return err
		}
	}
	for id := int64(1); id <= 20; id++ {
		if _, err := tx.Exec(ctx, "INSERT INTO inventory(product_id,sku,quantity) VALUES($1,$2,100000)", id, fmt.Sprintf("sku-%03d", id)); err != nil {
			return err
		}
	}
	seedTime := time.Unix(1_700_000_000, 0).UTC()
	for id := int64(1); id <= 500; id++ {
		customerID := (id % 100) + 1
		if _, err := tx.Exec(ctx, "INSERT INTO orders(id,customer_id,created_at,status) VALUES($1,$2,$3,'seeded')", id, customerID, seedTime.Add(time.Duration(id)*time.Minute)); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	// Normalize planner statistics and dirty-page state after every reset so a
	// later lane is not penalized merely because it follows a write-heavy run.
	if _, err := s.Pool.Exec(ctx, "VACUUM ANALYZE"); err != nil {
		return fmt.Errorf("vacuum after reset: %w", err)
	}
	if _, err := s.Pool.Exec(ctx, "CHECKPOINT"); err != nil {
		return fmt.Errorf("checkpoint after reset: %w", err)
	}
	return nil
}

func (s *Store) Execute(ctx context.Context, op workload.Operation) error {
	switch op.Kind {
	case workload.PointRead:
		var name string
		return s.Pool.QueryRow(ctx, "SELECT name FROM customers WHERE id=$1", op.CustomerID).Scan(&name)
	case workload.RangeRead:
		rows, err := s.Pool.Query(ctx, "SELECT id FROM orders WHERE customer_id=$1 ORDER BY created_at DESC LIMIT 10", op.CustomerID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				return err
			}
		}
		return rows.Err()
	case workload.OrderWrite:
		tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, "INSERT INTO orders(id,customer_id,created_at,status) VALUES($1,$2,$3,'created')", op.OrderID, op.CustomerID, op.At); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO order_items(order_id,product_id,quantity,unit_price_cents) VALUES($1,$2,1,1000)", op.OrderID, op.ProductID); err != nil {
			return err
		}
		return tx.Commit(ctx)
	case workload.InventoryWrite:
		command, err := s.Pool.Exec(ctx, "UPDATE inventory SET quantity=quantity-1 WHERE product_id=$1 AND quantity>0", op.ProductID)
		if err != nil {
			return err
		}
		if command.RowsAffected() != 1 {
			return fmt.Errorf("inventory exhausted for product %d", op.ProductID)
		}
		return nil
	default:
		return fmt.Errorf("unknown operation kind %d", op.Kind)
	}
}

// ExecuteReadBatch uses pgx's ordinary batch protocol. Only same-class reads
// reach this method; transactions remain individually atomic.
func (s *Store) ExecuteReadBatch(ctx context.Context, ops []workload.Operation) error {
	batch := &pgx.Batch{}
	for _, op := range ops {
		switch op.Kind {
		case workload.PointRead:
			batch.Queue("SELECT name FROM customers WHERE id=$1", op.CustomerID)
		case workload.RangeRead:
			batch.Queue("SELECT id FROM orders WHERE customer_id=$1 ORDER BY created_at DESC LIMIT 10", op.CustomerID)
		default:
			return fmt.Errorf("non-read %s in read batch", op.Kind)
		}
	}
	results := s.Pool.SendBatch(ctx, batch)
	for _, op := range ops {
		rows, err := results.Query()
		if err != nil {
			_ = results.Close()
			return err
		}
		for rows.Next() {
			if op.Kind == workload.PointRead {
				var name string
				if err := rows.Scan(&name); err != nil {
					rows.Close()
					_ = results.Close()
					return err
				}
			} else {
				var id int64
				if err := rows.Scan(&id); err != nil {
					rows.Close()
					_ = results.Close()
					return err
				}
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			_ = results.Close()
			return err
		}
		rows.Close()
	}
	return results.Close()
}

type State struct {
	Orders          int64           `json:"orders"`
	OrderItems      int64           `json:"order_items"`
	InventoryTotal  int64           `json:"inventory_total"`
	CreatedOrderIDs []int64         `json:"created_order_ids"`
	Inventory       map[int64]int64 `json:"inventory"`
}

func (s *Store) Snapshot(ctx context.Context) (State, error) {
	var out State
	if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM orders").Scan(&out.Orders); err != nil {
		return out, err
	}
	if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM order_items").Scan(&out.OrderItems); err != nil {
		return out, err
	}
	if err := s.Pool.QueryRow(ctx, "SELECT coalesce(sum(quantity),0) FROM inventory").Scan(&out.InventoryTotal); err != nil {
		return out, err
	}
	rows, err := s.Pool.Query(ctx, "SELECT id FROM orders WHERE id>=1000000 ORDER BY id")
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return out, err
		}
		out.CreatedOrderIDs = append(out.CreatedOrderIDs, id)
	}
	rows.Close()
	out.Inventory = map[int64]int64{}
	rows, err = s.Pool.Query(ctx, "SELECT product_id,quantity FROM inventory ORDER BY product_id")
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, quantity int64
		if err := rows.Scan(&id, &quantity); err != nil {
			return out, err
		}
		out.Inventory[id] = quantity
	}
	sort.Slice(out.CreatedOrderIDs, func(i, j int) bool { return out.CreatedOrderIDs[i] < out.CreatedOrderIDs[j] })
	return out, rows.Err()
}
