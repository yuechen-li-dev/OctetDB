package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type record struct {
	ID        int    `json:"id"`
	Balance   int64  `json:"balance,omitempty"`
	Available int64  `json:"available,omitempty"`
	Reserved  int64  `json:"reserved,omitempty"`
	Status    string `json:"status,omitempty"`
	Attempts  int    `json:"attempts,omitempty"`
	Result    string `json:"result,omitempty"`
}
type measurement struct {
	Workload         string `json:"workload"`
	Population       int    `json:"population"`
	LogicalJSONBytes int64  `json:"logical_json_bytes"`
}

func main() {
	cases := []struct {
		workload   string
		population int
	}{{"w1", 1000}, {"w2", 1000}, {"w3", 1000}, {"w5", 1000}, {"w5", 10000}, {"w5", 100000}, {"w6", 1000}}
	result := make([]measurement, 0, len(cases))
	for _, c := range cases {
		var total int64
		for id := 0; id < c.population; id++ {
			encoded, _ := json.Marshal(seed(c.workload, id))
			total += int64(len(encoded))
		}
		result = append(result, measurement{c.workload, c.population, total})
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		panic(err)
	}
	encoded = append(encoded, '\n')
	if len(os.Args) == 2 {
		if err := os.WriteFile(os.Args[1], encoded, 0o644); err != nil {
			panic(err)
		}
		return
	}
	fmt.Print(string(encoded))
}
func seed(workload string, id int) record {
	switch workload {
	case "w1":
		return record{ID: id, Balance: 100000}
	case "w2":
		return record{ID: id, Available: 10000, Reserved: 100}
	case "w3":
		statuses := []string{"ready", "claimed", "completed", "failed"}
		return record{ID: id, Status: statuses[id%4], Attempts: id % 3}
	case "w5":
		status := "other"
		if id%4 == 0 {
			status = "ready"
		}
		return record{ID: id, Status: status, Attempts: id % 3}
	case "w6":
		return record{ID: id, Available: 10000, Reserved: 100, Status: "active"}
	}
	return record{ID: id}
}
