package scheduled

import (
	"runtime"

	"github.com/yuechen-li-dev/octetdb/internal/workload"
)

type commandDescriptor struct {
	Kind, Statement, BatchClass, Priority, Conflict, Transaction int
	Name                                                         string
	QueueCapacity, MaxBatch                                      int
}

type statementDescriptor struct {
	Kind                                   int
	Name, SQL, ParameterShape, ResultShape string
}

type executionPlan struct {
	Commands                         [4]commandDescriptor
	Compatibility                    [4][4]bool
	Statements                       [5]statementDescriptor
	QueueCapacity, MaxBatch, Workers int
}

type planLookup interface {
	command(workload.Kind) (commandDescriptor, bool)
	compatible(workload.Kind, workload.Kind) bool
	statementCount() int
}

type staticPlanLookup struct{ plan *executionPlan }

func (p staticPlanLookup) command(kind workload.Kind) (commandDescriptor, bool) {
	if int(kind) >= len(p.plan.Commands) {
		return commandDescriptor{}, false
	}
	return p.plan.Commands[int(kind)], true
}
func (p staticPlanLookup) compatible(left, right workload.Kind) bool {
	if int(left) >= 4 || int(right) >= 4 {
		return false
	}
	return p.plan.Compatibility[int(left)][int(right)]
}
func (p staticPlanLookup) statementCount() int { return len(p.plan.Statements) }

type runtimePlanLookup struct {
	commands      map[workload.Kind]commandDescriptor
	compatibility map[[2]workload.Kind]bool
	statements    []statementDescriptor
}

func (p *runtimePlanLookup) command(kind workload.Kind) (commandDescriptor, bool) {
	d, ok := p.commands[kind]
	return d, ok
}
func (p *runtimePlanLookup) compatible(left, right workload.Kind) bool {
	return p.compatibility[[2]workload.Kind{left, right}]
}
func (p *runtimePlanLookup) statementCount() int { return len(p.statements) }

// buildRuntimePlan is the idiomatic C control: it constructs the same closed
// catalog and relationships with ordinary maps and slices during startup.
func buildRuntimePlan(capacity, maxBatch, workers int) *runtimePlanLookup {
	p := &runtimePlanLookup{
		commands:      make(map[workload.Kind]commandDescriptor, 4),
		compatibility: make(map[[2]workload.Kind]bool, 16),
		statements:    make([]statementDescriptor, 0, 5),
	}
	p.commands[workload.PointRead] = commandDescriptor{0, 0, 0, 0, 0, 0, "point_read", capacity, maxBatch}
	p.commands[workload.RangeRead] = commandDescriptor{1, 1, 1, 0, 1, 0, "range_read", capacity, maxBatch}
	p.commands[workload.OrderWrite] = commandDescriptor{2, 2, 2, 1, 1, 1, "order_write", capacity, 1}
	p.commands[workload.InventoryWrite] = commandDescriptor{3, 4, 2, 1, 2, 2, "inventory_write", capacity, 1}
	for left := workload.PointRead; left <= workload.InventoryWrite; left++ {
		for right := workload.PointRead; right <= workload.InventoryWrite; right++ {
			p.compatibility[[2]workload.Kind{left, right}] = left == right && left <= workload.RangeRead
		}
	}
	p.statements = append(p.statements,
		statementDescriptor{0, "select_customer", "SELECT name FROM customers WHERE id=$1", "CustomerID:Int64", "Name:String/one"},
		statementDescriptor{1, "select_orders", "SELECT id FROM orders WHERE customer_id=$1 ORDER BY created_at DESC LIMIT 10", "CustomerID:Int64", "OrderID:Int64/up-to-10"},
		statementDescriptor{2, "insert_order", "INSERT INTO orders(id,customer_id,created_at,status) VALUES($1,$2,$3,'created')", "OrderID:Int64,CustomerID:Int64,At:Time", "CommandTag/one"},
		statementDescriptor{3, "insert_order_item", "INSERT INTO order_items(order_id,product_id,quantity,unit_price_cents) VALUES($1,$2,1,1000)", "OrderID:Int64,ProductID:Int64", "CommandTag/one"},
		statementDescriptor{4, "update_inventory", "UPDATE inventory SET quantity=quantity-1 WHERE product_id=$1 AND quantity>0", "ProductID:Int64", "CommandTag/one"},
	)
	_ = workers
	return p
}

type InitializationMetrics struct {
	WallTimeUS             float64 `json:"wall_time_us"`
	SchedulerTimeUS        float64 `json:"scheduler_initialization_time_us"`
	MetadataTimeUS         float64 `json:"metadata_construction_time_us"`
	StatementCatalogTimeUS float64 `json:"statement_catalog_setup_time_us"`
	Allocations            uint64  `json:"allocations"`
	AllocatedBytes         uint64  `json:"allocated_bytes"`
	MetadataAllocations    uint64  `json:"metadata_allocations"`
	MetadataAllocatedBytes uint64  `json:"metadata_allocated_bytes"`
	StatementCatalogCount  int     `json:"statement_catalog_count"`
}

func readMem() runtime.MemStats {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats
}
