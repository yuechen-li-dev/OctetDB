package octetdb_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestCandidateExternalModule proves the v0.2 API from an unrelated temporary
// module. The replace directive is candidate wiring only and must be removed by
// the post-tag TestExternalModule proof.
func TestCandidateExternalModule(t *testing.T) {
	repository, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	runCandidate(t, dir, "mod", "init", "example.com/octetdb-v02-consumer")
	runCandidate(t, dir, "mod", "edit", "-require=github.com/yuechen-li-dev/octetdb@v0.2.0")
	runCandidate(t, dir, "mod", "edit", "-replace=github.com/yuechen-li-dev/octetdb="+repository)
	source, err := os.ReadFile(filepath.Join("testdata", "candidate-consumer", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), source, 0o600); err != nil {
		t.Fatal(err)
	}
	output := runCandidate(t, dir, "run", ".")
	if !strings.Contains(output, "candidate-ok duplicate=true order=placed stock=3 low=[widget]") {
		t.Fatalf("unexpected candidate output: %s", output)
	}
	deps := strings.ToLower(runCandidate(t, dir, "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "."))
	for _, forbidden := range []string{"researchengine", "tigerbeetle", "pgx", "postgres", "database-scheduler", "github.com/yuechen-li-dev/oct/"} {
		if strings.Contains(deps, forbidden) {
			t.Fatalf("candidate dependency graph contains %q:\n%s", forbidden, deps)
		}
	}
}

func runCandidate(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("go", args...)
	command.Dir = dir
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(output)
}
