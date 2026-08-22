package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/yuechen-li-dev/database-scheduler/internal/m5"
	"github.com/yuechen-li-dev/database-scheduler/internal/m5compiled"
)

func main() {
	start := time.Now()
	row, ok := (m5compiled.IndexedLane{}).FindByID(m5.FirstProductID + 417)
	if !ok {
		panic("compiled E first query failed")
	}
	ready := time.Since(start).Nanoseconds()
	count, digest := m5.DigestProducts((m5compiled.IndexedLane{}).FindByCategory(3))
	priceCount, priceDigest := m5.DigestProducts((m5compiled.IndexedLane{}).ProductsWithPriceBetween(50_000, 80_000))
	projectionCount, projectionDigest := m5.DigestProjections((m5compiled.IndexedLane{}).ActiveProductsInRegion(2))
	fmt.Printf("ready_to_first_query_ns=%d id=%d witness=%d/%x/%d/%x/%d/%x\n", ready, row.ID, count, digest, priceCount, priceDigest, projectionCount, projectionDigest)
	holdIfRequested()
}

func holdIfRequested() {
	if text := os.Getenv("M5_HOLD_MS"); text != "" {
		milliseconds, err := strconv.Atoi(text)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Duration(milliseconds) * time.Millisecond)
	}
}
