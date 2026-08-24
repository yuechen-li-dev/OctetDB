package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	bsosim "github.com/yuechen-li-dev/octetdb/experiments/BSOSim/M2"
	"os"
	"time"
)

func main() {
	mode := flag.String("mode", "smoke", "smoke or normal")
	workers := flag.Int("workers", 0, "worker count override")
	bsos := flag.Int("bsos", 0, "BSO count override")
	transfers := flag.Int("transfers", 0, "transfer count override")
	initialBalance := flag.Int64("initial-balance", 0, "initial balance override")
	fault := flag.String("fault-profile", "", "none, fun, mean, or worker-loss")
	workload := flag.String("workload", "", "random, hot-merchant, hot-payer, or affected-set")
	killWorker := flag.Int("kill-worker", -1, "worker to kill (-1 disables)")
	killRound := flag.Int("kill-round", 1, "logical round for worker loss")
	restartBSO := flag.String("restart-bso", "", "BSO identity to restart")
	restartRound := flag.Int("restart-round", 1, "logical round for BSO restart")
	jsonOut := flag.Bool("json", false, "emit JSON")
	flag.Parse()
	configs := []bsosim.Config{}
	base := bsosim.DefaultConfig()
	if *mode == "smoke" {
		base.BSOs, base.Transfers = 10, 40
		base.Faults = bsosim.FaultProfiles["none"]
	} else {
		base.BSOs, base.Transfers = 100, 1000
	}
	if *workers > 0 {
		base.Workers = *workers
	}
	if *bsos > 0 {
		base.BSOs = *bsos
	}
	if *transfers > 0 {
		base.Transfers = *transfers
	}
	if *initialBalance > 0 {
		base.InitialBalance = bsosim.Money(*initialBalance)
	}
	if *fault != "" {
		p, ok := bsosim.FaultProfiles[*fault]
		if !ok {
			fmt.Fprintln(os.Stderr, "unknown fault profile")
			os.Exit(2)
		}
		base.Faults = p
	}
	if *workload != "" {
		base.Workload = *workload
	}
	base.KillWorker, base.KillRound = *killWorker, *killRound
	base.RestartBSO, base.RestartRound = *restartBSO, *restartRound
	if *workers > 0 || *bsos > 0 || *transfers > 0 {
		configs = []bsosim.Config{base}
	} else {
		for _, w := range []int{1, 2, 4, 8} {
			c := base
			c.Workers = w
			configs = append(configs, c)
		}
	}
	start := time.Now()
	results := []bsosim.Result{}
	for _, c := range configs {
		r, err := bsosim.Run(context.Background(), c)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		results = append(results, r)
	}
	if *jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(results)
		return
	}
	fmt.Printf("BSO-SIM-M2 mode=%s runtime=%s\n", *mode, time.Since(start))
	fmt.Println("| Workers | Transfers | Throughput | Speedup | Efficiency | Avg steps/worker | Max steps/worker | Correct |")
	baseline := results[0].Metrics.TransfersPerSecond
	for _, r := range results {
		max := 0
		for _, x := range r.Metrics.WorkerSteps {
			if x > max {
				max = x
			}
		}
		speedup := r.Metrics.TransfersPerSecond / baseline
		fmt.Printf("| %d | %d | %.0f | %.2fx | %.0f%% | %.1f | %d | %t |\n", r.Config.Workers, r.Config.Transfers, r.Metrics.TransfersPerSecond, speedup, speedup/float64(r.Config.Workers)*100, float64(r.Metrics.AgentSteps)/float64(r.Config.Workers), max, r.Correct)
	}
}
