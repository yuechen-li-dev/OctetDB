package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	bsotrustm2 "github.com/yuechen-li-dev/octetdb/experiments/BSOTrust/M2"
)

func main() {
	jsonOut := flag.Bool("json", false, "emit the complete typed experiment result as JSON")
	flag.Parse()
	r, err := bsotrustm2.Run(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(r)
		return
	}
	fmt.Printf("BSO-TRUST-M2 runtime=%dms correct=%t conservation=%t primary_share=%.1f%% continuity_coverage=%.1f%% failover=%.1f%%\n", r.ElapsedMilliseconds, r.Correct, r.Conservation, r.PrimaryUsageShare*100, r.ContinuityCoverage*100, r.FailoverSuccess*100)
	fmt.Println("| Relationship class | Primary | Verified alternates | Health | Last check | Captive |")
	for _, x := range r.ContinuityRows {
		fmt.Printf("| %s | %s | %s | %s | %d | %t |\n", x.RelationshipClass, x.Primary, x.VerifiedAlternates, x.Health, x.LastCheck, x.Captive)
	}
	fmt.Println(r.ArchitectureDecision)
	fmt.Println(r.FailoverDecision)
	fmt.Println(r.ConcentrationDecision)
	fmt.Println(r.RecurringDecision)
	fmt.Println(r.PrivacyDecision)
	fmt.Println(r.ExperimentDecision)
}
