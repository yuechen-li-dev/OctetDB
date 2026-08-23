package octetdb_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestExternalModule runs only when OCTETDB_EXTERNAL_VERSION is set. It proves
// the canonical v0.2 catalog program against a downloadable module version;
// no replace directive or repository path is given to the consumer module.
func TestExternalModule(t *testing.T) {
	version := os.Getenv("OCTETDB_EXTERNAL_VERSION")
	if version == "" {
		t.Skip("set OCTETDB_EXTERNAL_VERSION to a candidate or tagged version")
	}
	dir := t.TempDir()
	runExternal(t, dir, "mod", "init", "example.com/octetdb-consumer")
	runExternal(t, dir, "get", "github.com/yuechen-li-dev/octetdb@"+version)
	source, err := os.ReadFile(filepath.Join("testdata", "candidate-consumer", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), source, 0o600); err != nil {
		t.Fatal(err)
	}
	output := runExternal(t, dir, "run", ".")
	if !strings.Contains(output, "candidate-ok duplicate=true order=placed stock=3 low=[widget]") {
		t.Fatalf("unexpected smoke output: %s", output)
	}
	deps := runExternal(t, dir, "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", ".")
	for _, forbidden := range []string{"pgx", "tigerbeetle", "/internal/bench", "/internal/researchengine", "/internal/model"} {
		if strings.Contains(strings.ToLower(deps), forbidden) {
			t.Fatalf("production dependency graph contains %q:\n%s", forbidden, deps)
		}
	}
	mod, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mod), "replace ") {
		t.Fatalf("external module used replace:\n%s", mod)
	}
}

func runExternal(t *testing.T, dir string, args ...string) string {
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
