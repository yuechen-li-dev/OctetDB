package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	bsotrustm3 "github.com/yuechen-li-dev/octetdb/experiments/BSOTrust/M3"
)

func main() {
	jsonOut := flag.Bool("json", false, "emit the complete typed experiment result as JSON")
	flag.Parse()
	r, err := bsotrustm3.Run(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(r)
		return
	}
	fmt.Printf("BSO-TRUST-M3 runtime=%dms correct=%t conservation=%t buggy=%d concurrent=%d/20 consumed=%d unrelated=%d\n", r.ElapsedMilliseconds, r.Correct, r.Conservation, r.BuggyTransfers, r.ConcurrentSucceeded, r.ConcurrentConsumed, r.UnrelatedStateTouched)
	for _, w := range r.Workloads {
		fmt.Printf("| %s | %d | %d | %d | %d | %s |\n", w.Name, w.Attempted, w.Succeeded, w.Rejected, w.FinancialMutations, w.Reason)
	}
	fmt.Println(r.ArchitectureDecision)
	fmt.Println(r.PolicySafetyDecision)
	fmt.Println(r.ProgrammabilityDecision)
	fmt.Println(r.RecoveryDecision)
	fmt.Println(r.StudyDecision)
}
