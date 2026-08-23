# Verification record

## Released artifact

- `go mod download -json github.com/yuechen-li-dev/octetdb@v0.2.0`
  resolved commit `76a0659ae9125c8c9b689fe155f2ff30a4b30fc9` and sum
  `h1:AJFkUyS6GbM46L0QvF3UqgiIJA6EDSYooC0TfbZFJOU=`.
- `go test ./...` from the downloaded v0.2.0 module directory passed.

## Participant rerun

```text
participant-a:
ok  participant-a/inventory
ok  participant-a/jobs

participant-b:
ok  github.com/yuechen-li-dev/octetdb-m3-participant-b/lifecycle
ok  github.com/yuechen-li-dev/octetdb-m3-participant-b/webhook

participant-c:
ok  participant-c/task3
ok  participant-c/task6
```

All were run uncached with `go test -count=1 ./...`; `go vet ./...` passed in
each module.

## Hidden rerun

```text
go test -count=1 ./...
ok  octetdb-m3-hidden-tests

go test -race -count=1 ./...
ok  octetdb-m3-hidden-tests
```

The hidden suite includes closed-snapshot corruption and verifies that the
participant adapter propagates `ErrorCorruption`. The coordinator made no edits
to participant application code after submission.
