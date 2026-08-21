package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	benchrun "github.com/yuechen-li-dev/database-scheduler/internal/bench"
	"github.com/yuechen-li-dev/database-scheduler/internal/db"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	dsn := flag.String("dsn", "postgres://dbsched:dbsched@localhost:54329/dbsched?sslmode=disable", "PostgreSQL connection string")
	output := flag.String("output", "experiments/M0/runs/latest", "result directory")
	quick := flag.Bool("quick", false, "run a short smoke experiment")
	lanes := flag.String("lanes", "baseline,admission,scheduled", "comma-separated lane order")
	flag.Parse()
	cfg := benchrun.DefaultConfig()
	if *quick {
		cfg.Phases = []benchrun.Phase{
			{Name: "steady", Duration: 2 * time.Second, Rate: 250},
			{Name: "burst", Duration: time.Second, Rate: 2500},
			{Name: "normal_before", Duration: 2 * time.Second, Rate: 250},
			{Name: "overload", Duration: 2 * time.Second, Rate: 5000},
			{Name: "normal_after", Duration: 3 * time.Second, Rate: 250},
		}
	}
	if err := os.MkdirAll(*output, 0o755); err != nil {
		return err
	}
	if err := benchrun.WriteJSON(filepath.Join(*output, "config.json"), cfg); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	store, err := db.Open(ctx, *dsn, cfg.PoolSize)
	if err != nil {
		return fmt.Errorf("open PostgreSQL (start it with docker compose up -d --wait): %w", err)
	}
	defer store.Close()
	correctness, err := benchrun.CheckCorrectness(ctx, cfg, store)
	if err != nil {
		return fmt.Errorf("correctness: %w", err)
	}
	if err := benchrun.WriteJSON(filepath.Join(*output, "correctness.json"), correctness); err != nil {
		return err
	}
	if !correctness.Equal {
		return fmt.Errorf("lane final states differ; see correctness.json")
	}
	for _, lane := range strings.Split(*lanes, ",") {
		lane = strings.TrimSpace(lane)
		result, err := benchrun.Run(ctx, lane, cfg, store)
		if err != nil {
			return fmt.Errorf("%s: %w", lane, err)
		}
		if err := benchrun.WriteJSON(filepath.Join(*output, lane+"-results.json"), result); err != nil {
			return err
		}
		recovery := "n/a"
		if result.Summary.RecoverySeconds != nil {
			recovery = fmt.Sprintf("%.1fs", *result.Summary.RecoverySeconds)
		}
		fmt.Printf("%-10s throughput=%8.1f/s p99=%8.2fms rejected=%d batch=%4.2f recovery=%s\n", lane, result.Summary.ThroughputPerSec, result.Summary.Latency.P99, result.Summary.Rejected, result.Summary.AverageBatchSize, recovery)
	}
	return nil
}
