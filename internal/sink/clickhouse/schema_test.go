/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// fakeSchemaRows is a minimal driver.Rows over an in-memory (table, name, type)
// list. Embedding driver.Rows satisfies the full interface; only the four
// methods introspectColumns actually calls are implemented.
type fakeSchemaRows struct {
	driver.Rows
	data [][3]string
	idx  int
}

func (r *fakeSchemaRows) Next() bool { return r.idx < len(r.data) }

func (r *fakeSchemaRows) Scan(dest ...any) error {
	row := r.data[r.idx]
	r.idx++
	if len(dest) != len(row) {
		return fmt.Errorf("fakeSchemaRows: expected %d scan targets, got %d", len(row), len(dest))
	}
	for i, d := range dest {
		p, ok := d.(*string)
		if !ok {
			return fmt.Errorf("fakeSchemaRows: scan target %d is not *string", i)
		}
		*p = row[i]
	}
	return nil
}

func (r *fakeSchemaRows) Close() error { return nil }
func (r *fakeSchemaRows) Err() error   { return nil }

// fakeSchemaConn is a driver.Conn whose Query returns a fixed set of
// system.columns rows. Only Query is exercised by the schema-validation tests.
type fakeSchemaConn struct {
	driver.Conn
	rows [][3]string
}

func (c fakeSchemaConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return &fakeSchemaRows{data: c.rows}, nil
}

// fullSchemaRows returns system.columns rows describing a schema that exactly
// matches requiredColumns for both tables.
func fullSchemaRows() [][3]string {
	var rows [][3]string
	for _, table := range []string{tableResourceStates, tableWatchScopes} {
		for _, col := range requiredColumns[table] {
			rows = append(rows, [3]string{table, col.name, col.chType})
		}
	}
	return rows
}

// The one column the drift fixtures mangle, and what they mangle it to.
//
// A single wrong type on a single column is the smallest drift validateSchema
// must still catch, and the one the frozen schema's additive-only policy cannot
// excuse — an extra column is tolerated, a changed one is not. Every test that
// needs an invalid schema uses this same drift rather than inventing its own, so
// "the schema is wrong" means one thing across the package.
const (
	driftedColumn = "sha256"
	driftedType   = "FixedString(64)"
)

// driftedSchemaRows returns system.columns rows matching requiredColumns except
// for driftedColumn on resource_states, whose reported type is wrong.
func driftedSchemaRows() [][3]string {
	rows := fullSchemaRows()
	for i := range rows {
		if rows[i][0] == tableResourceStates && rows[i][1] == driftedColumn {
			rows[i][2] = driftedType
		}
	}
	return rows
}

func TestValidateSchema(t *testing.T) {
	tests := []struct {
		name        string
		rows        [][3]string
		wantErr     bool
		wantInError []string // substrings the mismatch error must name
		notInError  []string // substrings the mismatch error must not name
	}{
		{
			name:    "matching schema returns no error",
			rows:    fullSchemaRows(),
			wantErr: false,
		},
		{
			name: "missing column names the column",
			rows: func() [][3]string {
				var out [][3]string
				for _, r := range fullSchemaRows() {
					if r[0] == tableResourceStates && r[1] == "actors" {
						continue // drop the actors column
					}
					out = append(out, r)
				}
				return out
			}(),
			wantErr:     true,
			wantInError: []string{tableResourceStates, "actors"},
		},
		{
			name:        "type mismatch names the column and both types",
			rows:        driftedSchemaRows(),
			wantErr:     true,
			wantInError: []string{tableResourceStates, driftedColumn, driftedType, "String"},
		},
		{
			// Forward compatibility, per the frozen schema's additive-only
			// policy: a table migrated ahead of the operator carries columns
			// this build has never heard of, and must still validate.
			name: "unknown extra column on resource_states is tolerated",
			rows: append(fullSchemaRows(),
				[3]string{tableResourceStates, "redaction_policy", "Nullable(String)"},
				[3]string{tableResourceStates, "future_flag", "UInt8"},
			),
			wantErr: false,
		},
		{
			name: "unknown extra column on watch_scopes is tolerated",
			rows: append(fullSchemaRows(),
				[3]string{tableWatchScopes, "operator_version", "LowCardinality(String)"},
			),
			wantErr: false,
		},
		{
			// Tolerance must not become blindness: an extra column may not mask
			// a genuinely missing one, and the error names only the real drift.
			name: "extra column does not mask a missing one",
			rows: func() [][3]string {
				var out [][3]string
				for _, r := range fullSchemaRows() {
					if r[0] == tableResourceStates && r[1] == "diff" {
						continue // drop a column the operator does depend on
					}
					out = append(out, r)
				}
				return append(out, [3]string{tableResourceStates, "future_flag", "UInt8"})
			}(),
			wantErr:     true,
			wantInError: []string{tableResourceStates, "diff"},
			notInError:  []string{"future_flag"},
		},
		{
			name: "missing table is reported",
			rows: func() [][3]string {
				var out [][3]string
				for _, r := range fullSchemaRows() {
					if r[0] == tableWatchScopes {
						continue // drop the whole watch_scopes table
					}
					out = append(out, r)
				}
				return out
			}(),
			wantErr:     true,
			wantInError: []string{tableWatchScopes},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSchema(context.Background(), fakeSchemaConn{rows: tt.rows}, "kuberecord")
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got nil")
				}
				var mismatch *schemaMismatchError
				if !errors.As(err, &mismatch) {
					t.Fatalf("expected *schemaMismatchError, got %T: %v", err, err)
				}
				for _, want := range tt.wantInError {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not mention %q", err.Error(), want)
					}
				}
				for _, unwanted := range tt.notInError {
					if strings.Contains(err.Error(), unwanted) {
						t.Errorf("error %q mentions %q, which is not drift the operator may report", err.Error(), unwanted)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}

// TestNoDeletedSentinels enforces the acceptance criterion that the old
// deletion sentinels appear nowhere in the repository's Go sources. The
// needles are assembled from fragments so this test file itself does not
// contain (and would not match) the literals it searches for.
func TestNoDeletedSentinels(t *testing.T) {
	root := repoRoot(t)

	sha256Sentinel := `"` + "DELET" + "ED" + `"`
	dataSentinel := `{"status": ` + `"deleted"}`
	needles := []string{sha256Sentinel, dataSentinel}

	thisFile := "schema_test.go"

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Skip build/output and VCS directories.
			switch d.Name() {
			case "bin", ".git", "testbin":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if filepath.Base(path) == thisFile {
			return nil // the scanner must not flag itself
		}
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		for _, needle := range needles {
			if strings.Contains(string(content), needle) {
				t.Errorf("forbidden deletion sentinel %q found in %s", needle, path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking repo: %v", err)
	}
}

// repoRoot walks up from the current working directory until it finds the
// module's go.mod, so the sentinel scan covers the whole repository regardless
// of which package directory `go test` runs from.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not locate go.mod above %s", dir)
		}
		dir = parent
	}
}
