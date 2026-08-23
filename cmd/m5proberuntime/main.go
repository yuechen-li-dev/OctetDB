package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/yuechen-li-dev/octetdb/internal/m5"
)

func main() {
	lane := flag.String("lane", "json", "json or binary")
	data := flag.String("data", "experiments/M5/data", "snapshot directory")
	flag.Parse()
	start := time.Now()
	var rows []m5.Product
	var err error
	switch *lane {
	case "json":
		rows, err = m5.ReadJSON(filepath.Join(*data, "catalog.json"))
	case "binary":
		rows, err = m5.ReadBinary(filepath.Join(*data, "catalog.bin"))
	default:
		panic("unknown lane")
	}
	if err != nil {
		panic(err)
	}
	cache := m5.NewRuntimeCache(rows)
	row, ok := cache.FindByID(m5.FirstProductID + 417)
	if !ok {
		panic("runtime first query failed")
	}
	fmt.Printf("ready_to_first_query_ns=%d id=%d\n", time.Since(start).Nanoseconds(), row.ID)
	if text := os.Getenv("M5_HOLD_MS"); text != "" {
		milliseconds, err := strconv.Atoi(text)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Duration(milliseconds) * time.Millisecond)
	}
}
