// Code generated from experiments/M1/static-plan/plan.octest by Oct artifact. DO NOT EDIT.
package scheduled

var staticExecutionPlan = executionPlan{
    Commands: [4]commandDescriptor{
        {Kind: 0, Name: "point_read", Statement: 0, BatchClass: 0, Priority: 0, Conflict: 0, Transaction: 0, QueueCapacity: 128, MaxBatch: 8},
        {Kind: 1, Name: "range_read", Statement: 1, BatchClass: 1, Priority: 0, Conflict: 1, Transaction: 0, QueueCapacity: 128, MaxBatch: 8},
        {Kind: 2, Name: "order_write", Statement: 2, BatchClass: 2, Priority: 1, Conflict: 1, Transaction: 1, QueueCapacity: 128, MaxBatch: 1},
        {Kind: 3, Name: "inventory_write", Statement: 4, BatchClass: 2, Priority: 1, Conflict: 2, Transaction: 2, QueueCapacity: 128, MaxBatch: 1},
    },
    Compatibility: [4][4]bool{
        {true, false, false, false},
        {false, true, false, false},
        {false, false, false, false},
        {false, false, false, false},
    },
    Statements: [5]statementDescriptor{
        {Kind: 0, Name: "select_customer", SQL: "SELECT name FROM customers WHERE id=$1", ParameterShape: "CustomerID:Int64", ResultShape: "Name:String/one"},
        {Kind: 1, Name: "select_orders", SQL: "SELECT id FROM orders WHERE customer_id=$1 ORDER BY created_at DESC LIMIT 10", ParameterShape: "CustomerID:Int64", ResultShape: "OrderID:Int64/up-to-10"},
        {Kind: 2, Name: "insert_order", SQL: "INSERT INTO orders(id,customer_id,created_at,status) VALUES($1,$2,$3,'created')", ParameterShape: "OrderID:Int64,CustomerID:Int64,At:Time", ResultShape: "CommandTag/one"},
        {Kind: 3, Name: "insert_order_item", SQL: "INSERT INTO order_items(order_id,product_id,quantity,unit_price_cents) VALUES($1,$2,1,1000)", ParameterShape: "OrderID:Int64,ProductID:Int64", ResultShape: "CommandTag/one"},
        {Kind: 4, Name: "update_inventory", SQL: "UPDATE inventory SET quantity=quantity-1 WHERE product_id=$1 AND quantity>0", ParameterShape: "ProductID:Int64", ResultShape: "CommandTag/one"},
    },
    QueueCapacity: 128, MaxBatch: 8, Workers: 8,
}
