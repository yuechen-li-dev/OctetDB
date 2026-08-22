package m5

import (
	"path/filepath"
	"testing"
)

func TestJSONAndBinaryRoundTrip(t *testing.T) {
	rows := GenerateCatalog(257)
	for _, lane := range []struct {
		name  string
		write func(string, []Product) error
		read  func(string) ([]Product, error)
		ext   string
	}{
		{"json", WriteJSON, ReadJSON, ".json"}, {"binary", WriteBinary, ReadBinary, ".bin"},
	} {
		t.Run(lane.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "catalog"+lane.ext)
			if err := lane.write(path, rows); err != nil {
				t.Fatal(err)
			}
			got, err := lane.read(path)
			if err != nil {
				t.Fatal(err)
			}
			if HashRows(got) != HashRows(rows) {
				t.Fatalf("round trip hash mismatch")
			}
		})
	}
}
