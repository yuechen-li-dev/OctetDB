package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	bsotrust "github.com/yuechen-li-dev/octetdb/experiments/BSOTrust/M0"
	"os"
	"strings"
	"time"
)

func main() {
	mode := flag.String("mode", "smoke", "smoke or normal")
	jsonOut := flag.Bool("json", false, "emit the complete typed experiment result as JSON")
	migrate := flag.Bool("migrate", false, "kill worker 1 while trust collection is pending")
	flag.Parse()
	if *mode != "smoke" && *mode != "normal" {
		fmt.Fprintln(os.Stderr, "mode must be smoke or normal")
		os.Exit(2)
	}
	config := bsotrust.DefaultConfig()
	if *mode == "normal" {
		config.Faults = bsotrust.FaultProfiles["fun"]
	}
	if *migrate {
		config.KillWorker, config.KillRound = 1, 2
	}
	started := time.Now()
	result, err := bsotrust.Run(context.Background(), config)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(result)
		return
	}

	fmt.Printf("BSO-TRUST-M0 mode=%s runtime=%s correct=%t conservation=%t\n", *mode, time.Since(started), result.Correct, result.Conservation)
	fmt.Println("| Workload | Required roles | Providers consulted | Attestations used | Admitted | Financial mutation |")
	for _, resolution := range result.TrustResolutions {
		roles := make([]string, len(resolution.RequiredRoles))
		for i := range resolution.RequiredRoles {
			roles[i] = string(resolution.RequiredRoles[i])
		}
		mutation := "no"
		if resolution.Admitted {
			mutation = "yes"
		}
		fmt.Printf("| %s | %s | %d | %d | %t | %s |\n", resolution.TransferID, strings.Join(roles, "+"), resolution.ProvidersConsulted, len(resolution.AttestationIDs), resolution.Admitted, mutation)
	}
	fmt.Printf("providers=%d attestations=%d reused=%d fallback=%d intersection_failures=%d migrated=%d settlement_messages=%d durable_mutations=%d\n", result.Metrics.ProvidersConsulted, result.Metrics.AttestationsUsed, result.Metrics.ReusedAttestations, result.Metrics.FallbackProviderUses, result.Metrics.PolicyIntersectionFailures, result.Metrics.AgentsMigrated, result.Metrics.MessagesSent, result.Metrics.LocalDurableMutations)
}
