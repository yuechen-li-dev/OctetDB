package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	bsosim "github.com/yuechen-li-dev/octetdb/experiments/BSOSim/M0"
)

type scenario struct {
	Name       string            `json:"name"`
	Comparison bsosim.Comparison `json:"comparison"`
}

type suite struct {
	Mode      string     `json:"mode"`
	Revision  string     `json:"octetdb_revision"`
	Scenarios []scenario `json:"scenarios"`
	TotalMS   int64      `json:"total_ms"`
}

func main() {
	mode := flag.String("mode", "normal", "smoke, normal, or fun-large")
	seed := flag.Int64("seed", 20260823, "deterministic simulation seed")
	bsos := flag.Int("bsos", 0, "override BSO count and run one scenario")
	transfers := flag.Int("transfers", 0, "override transfer count and run one scenario")
	faultName := flag.String("fault-profile", "fun", "none, fun, or mean")
	workload := flag.String("workload", "random", "random, hot-merchant, hot-payer, or institution")
	jsonOutput := flag.Bool("json", false, "emit JSON instead of tables")
	flag.Parse()

	faults, ok := bsosim.FaultProfiles[*faultName]
	if !ok {
		log.Fatalf("unknown fault profile %q", *faultName)
	}
	configs, err := modeConfigs(*mode, *seed, faults)
	if err != nil {
		log.Fatal(err)
	}
	if *bsos != 0 || *transfers != 0 || *workload != "random" {
		config := configs[0].Config
		config.Seed, config.Faults, config.Workload = *seed, faults, *workload
		if *bsos != 0 {
			config.BSOs = *bsos
		}
		if *transfers != 0 {
			config.Transfers = *transfers
		}
		configs = []namedConfig{{Name: config.Workload + "/" + config.Faults.Name, Config: config}}
	}

	started := time.Now()
	output := suite{Mode: *mode, Revision: "512d4ddc7ab025f73bb5a58412f0e06ef88972f7"}
	for _, item := range configs {
		comparison, err := bsosim.RunComparison(context.Background(), item.Config)
		if err != nil {
			log.Fatalf("%s: %v", item.Name, err)
		}
		output.Scenarios = append(output.Scenarios, scenario{Name: item.Name, Comparison: comparison})
	}
	output.TotalMS = time.Since(started).Milliseconds()
	if *jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(output); err != nil {
			log.Fatal(err)
		}
		return
	}
	printTables(output)
}

type namedConfig struct {
	Name   string
	Config bsosim.Config
}

func modeConfigs(mode string, seed int64, faults bsosim.FaultProfile) ([]namedConfig, error) {
	base := bsosim.DefaultConfig()
	base.Seed, base.Faults = seed, faults
	makeConfig := func(name, workload string, bsos, transfers int, profile bsosim.FaultProfile) namedConfig {
		config := base
		config.BSOs, config.Transfers, config.Workload, config.Faults = bsos, transfers, workload, profile
		if workload == "hot-payer" {
			config.InitialBalance = 1_000
		}
		return namedConfig{Name: name, Config: config}
	}
	switch mode {
	case "smoke":
		return []namedConfig{
			makeConfig("scale-10", "random", 10, 50, faults),
			makeConfig("hot-merchant", "hot-merchant", 10, 30, bsosim.FaultProfiles["none"]),
			makeConfig("hot-payer", "hot-payer", 10, 30, bsosim.FaultProfiles["none"]),
		}, nil
	case "normal":
		return []namedConfig{
			makeConfig("scale-10", "random", 10, 300, faults),
			makeConfig("scale-100", "random", 100, 750, faults),
			makeConfig("scale-1000", "random", 1000, 1500, faults),
			makeConfig("hot-merchant", "hot-merchant", 100, 600, bsosim.FaultProfiles["none"]),
			makeConfig("hot-payer", "hot-payer", 100, 1200, bsosim.FaultProfiles["none"]),
			makeConfig("fault-mean", "random", 100, 400, bsosim.FaultProfiles["mean"]),
		}, nil
	case "fun-large":
		return []namedConfig{
			makeConfig("scale-10", "random", 10, 1500, faults),
			makeConfig("scale-100", "random", 100, 4000, faults),
			makeConfig("scale-1000", "random", 1000, 8000, faults),
			makeConfig("hot-merchant", "hot-merchant", 1000, 3000, faults),
			makeConfig("hot-payer", "hot-payer", 100, 3000, faults),
		}, nil
	default:
		return nil, fmt.Errorf("unknown mode %q", mode)
	}
}

func printTables(output suite) {
	fmt.Printf("BSO-SIM-M0 mode=%s seed=%d OctetDB=%s runtime=%s\n", output.Mode, output.Scenarios[0].Comparison.BSO.Config.Seed, output.Revision, time.Duration(output.TotalMS)*time.Millisecond)
	fmt.Println("This is an architectural simulation, not a banking, custody, payment, or cryptocurrency product.")
	fmt.Println()
	fmt.Println("| Scenario | BSOs | Transfers | BSO ops/s | Global ops/s | Messages/transfer | Unresolved | Conservation |")
	fmt.Println("| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
	for _, item := range output.Scenarios {
		bso, global := item.Comparison.BSO, item.Comparison.Global
		fmt.Printf("| %s | %d | %d | %.0f | %.0f | %.2f | %d | %t |\n", item.Name, bso.Config.BSOs, bso.Config.Transfers, bso.Metrics.TransfersPerSecond, global.Metrics.TransfersPerSecond, bso.Metrics.MessagesPerSuccess, bso.Metrics.Unresolved, bso.Conservation)
	}
	fmt.Println()
	fmt.Println("| Fault profile | Success | Retries | Duplicates suppressed | Reconciliations | Lost value | Double apply |")
	fmt.Println("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
	for _, item := range output.Scenarios {
		bso := item.Comparison.BSO
		lost := bso.InitialTotal - bso.FinalTotal
		fmt.Printf("| %s/%s | %d | %d | %d | %d | %d | %d |\n", item.Name, bso.Config.Faults.Name, bso.Metrics.Successful, bso.Metrics.Retries, bso.Metrics.DuplicatesSuppressed, bso.Metrics.ReconciliationActions, lost, bso.Metrics.DoubleDebits+bso.Metrics.DoubleCredits)
	}
	fmt.Println()
	for _, item := range output.Scenarios {
		fmt.Printf("%s digest=%s p50/p95/p99=%d/%d/%d logical rounds durable=%d global-order=%d\n", item.Name, item.Comparison.BSO.CorrectnessDigest, item.Comparison.BSO.Metrics.P50LogicalLatency, item.Comparison.BSO.Metrics.P95LogicalLatency, item.Comparison.BSO.Metrics.P99LogicalLatency, item.Comparison.BSO.Metrics.LocalDurableMutations, item.Comparison.Global.Metrics.GlobalSerializationOps)
	}
}
