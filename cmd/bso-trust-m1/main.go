package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	bsotrustm1 "github.com/yuechen-li-dev/octetdb/experiments/BSOTrust/M1"
)

func main() {
	jsonOut := flag.Bool("json", false, "emit the complete typed experiment result as JSON")
	flag.Parse()
	result, err := bsotrustm1.Run(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(result)
		return
	}
	fmt.Printf("BSO-TRUST-M1 runtime=%dms correct=%t conservation=%t\n", result.ElapsedMilliseconds, result.Correct, result.Conservation)
	fmt.Println("| Role | Provider | Attestations | Share |")
	for _, row := range result.ConcentrationRows {
		fmt.Printf("| %s | %s | %d | %.1f%% |\n", row.Role, row.Provider, row.Attestations, row.Share*100)
	}
	fmt.Println("| Provider removed | Pre-removal success | Post-removal success | Fallbacks | Resilience |")
	for _, row := range result.Outages {
		fmt.Printf("| %s | %d | %d | %d | %.1f%% |\n", row.ProviderRemoved, row.PreRemovalSuccess, row.PostRemovalSuccess, row.Fallbacks, row.Resilience*100)
	}
	fmt.Println("| Payment # | Fresh provider calls | Cached attestations | Provider changes | Settled |")
	for _, row := range result.Recurring {
		fmt.Printf("| %d | %d | %d | %d | %t |\n", row.Payment, row.FreshProviderCalls, row.CachedAttestations, row.ProviderChanges, row.Settled)
	}
	fmt.Println(result.ArchitectureDecision)
	fmt.Println(result.ConcentrationDecision)
	fmt.Println(result.RecurringDecision)
	fmt.Println(result.DataDecision)
	fmt.Println(result.ExperimentDecision)
}
