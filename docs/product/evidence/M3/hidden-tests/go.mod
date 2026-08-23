module octetdb-m3-hidden-tests

go 1.26.2

require (
	github.com/yuechen-li-dev/octetdb v0.2.0
	github.com/yuechen-li-dev/octetdb-m3-participant-b v0.0.0
	participant-a v0.0.0
	participant-c v0.0.0
)

replace participant-a => ../participant-a

replace participant-c => ../participant-c

replace github.com/yuechen-li-dev/octetdb-m3-participant-b => ../participant-b
