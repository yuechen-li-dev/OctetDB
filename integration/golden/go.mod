module example.com/octetdb-golden

go 1.23.0

require (
	github.com/go-chi/chi/v5 v5.2.2
	github.com/yuechen-li-dev/octetdb v0.2.0
)

replace github.com/yuechen-li-dev/octetdb => ../..
