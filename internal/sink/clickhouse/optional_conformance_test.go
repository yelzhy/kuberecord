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

// The read, scope-log and health halves of the ClickHouse conformance harness:
// everything the optional suites (Task 5.3) need that the Writer half did not.
// The suites themselves are reached from RunWriterSuite, so there is no separate
// Test function here — see writer_conformance_test.go.
//
// What this stand-in can and cannot prove — read this before trusting it:
//
// The write half's intelligence is Go (batching, retry, poison isolation, commit
// settling), so driving it over a fake connection exercises the shipped code. The
// read half's intelligence is SQL, and no fake connection executes SQL. So the
// stand-in below does two things, and the split matters:
//
//  1. It *pins the query text*. Each read is matched against the load-bearing
//     fragments of the statement the reader is supposed to emit — the per-
//     incarnation GROUP BY, the per-incarnation HAVING, the strict ts cutoff, the
//     scope-identity GROUP BY. A statement it does not recognise is a harness
//     failure that names the query, never a silent pass. A reader that quietly
//     regrouped per identity therefore breaks these tests loudly.
//  2. It *evaluates the intended semantics* over the rows the writer really
//     inserted, using the arguments the reader really bound, in the order it
//     bound them. So the whole Go side — which filter values reach which
//     placeholder, how rows are scanned, how a mid-stream failure is propagated —
//     is under test for real.
//
// What is left over is the claim that those SQL fragments mean in ClickHouse what
// this file assumes they mean. That is not testable without ClickHouse, and it is
// not left untested: statereader_integration_test.go and
// scopewriter_integration_test.go run the same queries against a real server under
// `make test-integration`. Neither layer is sufficient alone.

package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/yelzhy/kuberecord/internal/sink"
	"github.com/yelzhy/kuberecord/internal/sink/conformance"
)

// conformanceDatabase is the schema-introspection target the probe validates
// against. Any name would do — the stand-in answers whatever it is asked — but
// Probe reads w.database, so it has to be set to something.
const conformanceDatabase = "kuberecord"

// The positions scopeInsertArgs binds each ScopeEvent field at, and therefore the
// ones scopeEventFromInsertArgs reads them back from. insertScopeEventQuery's
// column list is the source of truth for the order; scopeArgCount is what turns a
// disagreement into a decode error rather than a silently shifted field.
//
// Note that this order is *not* the struct's: the schema interleaves the scope's
// identity columns with the event's own, and api_version sits between api_group
// and kind because that is the column order the frozen schema declares.
const (
	scopeTSArg = iota
	scopeClusterIDArg
	scopeAPIGroupArg
	scopeAPIVersionArg
	scopeKindArg
	scopeNamespaceArg
	scopeActionArg
	scopeRuleRefArg
	scopeArgCount
)

// The fragments of each read statement the stand-in insists on before it will
// answer. They are the clauses that carry the semantics, not merely the ones that
// happen to be present: between them they pin the per-incarnation grouping, the
// tombstone exclusion, the strict epoch cutoff and the scope-identity grouping.
//
// Matching on them rather than on the whole statement is deliberate — whitespace
// and the optional namespace predicate legitimately vary — but each one is a
// clause whose loss would change what the query means.
const (
	fragmentStatesGroupBy = "GROUP BY namespace, name, uid"
	fragmentStatesHaving  = "HAVING argMax(event_type, ts) != 'Deleted'"
	fragmentStatesNarrow  = "AND namespace = ?"
	fragmentEpochSelect   = "SELECT argMax(action, ts)"
	fragmentEpochCutoff   = "AND ts < ?"
	fragmentScopesSelect  = "SELECT api_group, kind, namespace"
	fragmentScopesGroupBy = "GROUP BY api_group, kind, namespace"
	fragmentScopesHaving  = "HAVING argMax(action, ts) = ?"
)

// errConformanceUnreachable is what a probe against an unreachable backend fails
// with. It is a plain error on purpose: the property it serves asserts that
// anything other than a schema mismatch stays unclassified.
var errConformanceUnreachable = errors.New("fake clickhouse: dial tcp: connection refused")

// The read-half state the conformance backend holds. It is declared here rather
// than on conformanceBackend so that file stays about the write half.
type conformanceReadState struct {
	// rows is every resource_states row that reached storage, in insert order —
	// duplicates included, exactly as ReplacingMergeTree would hold them before a
	// merge. The queries below are dedup-safe over it for the same reason the real
	// ones are: the grouping emits one row per (identity, uid) however many
	// physical copies there are.
	rows []sink.Record
	// scopeRows is every watch_scopes row that reached storage, in insert order.
	scopeRows []sink.ScopeEvent
	// readFault breaks the next read part-way through; nil when reads are healthy.
	readFault *conformance.ReadFault
	// probe is the state the next health probe should encounter.
	probe conformance.ProbeOutcome
}

// setReadFault implements Harness.SetReadFault.
func (b *conformanceBackend) setReadFault(fault *conformance.ReadFault) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.read.readFault = fault
}

// setProbeOutcome implements Harness.SetProbeOutcome.
func (b *conformanceBackend) setProbeOutcome(outcome conformance.ProbeOutcome) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.read.probe = outcome
}

// scopeSnapshot implements Harness.ScopeWrites: the transitions the backend has
// durably recorded, in order. The copy is mandatory — the scope worker is still
// appending to it while the suite reads.
func (b *conformanceBackend) scopeSnapshot() []sink.ScopeEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.read.scopeRows)
}

// attemptScopeEvents is one watch_scopes insert attempt.
//
// The suite's write fault is deliberately not consulted here. Harness.SetFault is
// the Writer half's lever, the scope path is a separate hand-off with its own
// batcher and its own retry queue (which is the point of it being separate), and
// no optional property needs a failing scope insert. Applying the record fault to
// these rows would instead make the read properties' scope seeding fail whenever a
// writer property happened to have installed one.
func (b *conformanceBackend) attemptScopeEvents(rows [][]any) error {
	events := make([]sink.ScopeEvent, 0, len(rows))
	for _, args := range rows {
		event, err := scopeEventFromInsertArgs(args)
		if err != nil {
			b.noteDecodeErr(err)
		}
		events = append(events, event)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.read.scopeRows = append(b.read.scopeRows, events...)
	return nil
}

// ping answers the probe's reachability check.
func (b *conformanceBackend) ping(context.Context) error {
	if b.probeOutcome() == conformance.ProbeUnreachable {
		return errConformanceUnreachable
	}
	return nil
}

func (b *conformanceBackend) probeOutcome() conformance.ProbeOutcome {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.read.probe == "" {
		return conformance.ProbeHealthy
	}
	return b.read.probe
}

// query is the stand-in's whole read surface: it recognises each statement the
// backend emits, evaluates it over what was really inserted, and refuses anything
// it does not know.
//
// Refusing is the load-bearing half. A stand-in that answered an unrecognised
// query with its best guess would keep passing after the reader's SQL changed
// meaning, which is precisely the drift the integration tests cannot catch quickly
// and this layer can.
func (b *conformanceBackend) query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "FROM system.columns"):
		return b.introspectSchema()
	case strings.Contains(query, "FROM "+tableResourceStates):
		return b.queryLastKnownStates(query, args)
	case strings.Contains(query, "FROM "+tableWatchScopes):
		return b.queryWatchScopes(query, args)
	}
	return nil, fmt.Errorf("conformance stand-in: unrecognised statement, so nothing here can answer it "+
		"faithfully: %s", collapseSpace(query))
}

// introspectSchema answers the probe's system.columns read: the schema the
// operator writes against, unless the harness was asked to drift it.
func (b *conformanceBackend) introspectSchema() (driver.Rows, error) {
	rows := fullSchemaRows()
	if b.probeOutcome() == conformance.ProbeSchemaMismatch {
		// The package's one shared drift, so "the schema is wrong" means the same
		// thing here as it does to the schema and probe tests.
		rows = driftedSchemaRows()
	}
	return &fakeSchemaRows{data: rows}, nil
}

// queryLastKnownStates evaluates the warm-up read.
func (b *conformanceBackend) queryLastKnownStates(query string, args []any) (driver.Rows, error) {
	if err := requireFragments(query, fragmentStatesGroupBy, fragmentStatesHaving); err != nil {
		return nil, err
	}
	narrowed := strings.Contains(query, fragmentStatesNarrow)
	want := 3
	if narrowed {
		want = 4
	}
	if len(args) != want {
		return nil, fmt.Errorf("conformance stand-in: the warm-up read bound %d arguments, want %d",
			len(args), want)
	}
	apiGroup, kind, clusterID, err := threeStrings(args)
	if err != nil {
		return nil, err
	}
	namespace := ""
	if narrowed {
		ns, ok := args[3].(string)
		if !ok {
			return nil, fmt.Errorf("conformance stand-in: namespace argument is a %T, want string", args[3])
		}
		namespace = ns
	}

	// One row per incarnation, carrying that incarnation's own latest values —
	// argMax(sha256, ts), argMax(api_version, ts), max(ts) — and dropped when its
	// own latest event is a deletion.
	latest := map[string]sink.Record{}
	for _, rec := range b.storedRecords() {
		if rec.APIGroup != apiGroup || rec.Kind != kind || rec.ClusterID != clusterID {
			continue
		}
		if narrowed && rec.Namespace != namespace {
			continue
		}
		key := rec.Namespace + "\x00" + rec.Name + "\x00" + rec.UID
		if prior, seen := latest[key]; seen && !prior.Timestamp.Before(rec.Timestamp) {
			continue
		}
		latest[key] = rec
	}

	keys := make([]string, 0, len(latest))
	for key := range latest {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	out := make([][]any, 0, len(keys))
	for _, key := range keys {
		rec := latest[key]
		if rec.EventType == "Deleted" {
			continue
		}
		out = append(out, []any{rec.Namespace, rec.Name, rec.UID, rec.SHA256, rec.APIVersion, rec.Timestamp})
	}
	return b.rowsFor(out), nil
}

// queryWatchScopes evaluates whichever of the two scope reads it was handed.
//
// Routing on the projection rather than on a grouping or filtering clause is
// deliberate: those clauses are precisely what the pin exists to check, so
// dispatching on one would send a statement that lost it to the *other*
// evaluator, which would then refuse it for the wrong reason and send the next
// reader looking in the wrong place.
func (b *conformanceBackend) queryWatchScopes(query string, args []any) (driver.Rows, error) {
	switch {
	case strings.Contains(query, fragmentScopesSelect):
		return b.queryActiveScopes(query, args)
	case strings.Contains(query, fragmentEpochSelect):
		return b.queryScopeWasActive(query, args)
	}
	return nil, fmt.Errorf("conformance stand-in: a %s read that projects neither %q nor %q, so it is "+
		"neither of the two reads this harness knows how to answer: %s",
		tableWatchScopes, fragmentScopesSelect, fragmentEpochSelect, collapseSpace(query))
}

// queryScopeWasActive evaluates the epoch probe: the last action recorded for
// this exact scope strictly before the cutoff, or the empty string when the range
// holds nothing (which is what an aggregate over an empty range yields for a
// String, and is the same, correct "not active" verdict).
func (b *conformanceBackend) queryScopeWasActive(query string, args []any) (driver.Rows, error) {
	if err := requireFragments(query, fragmentEpochCutoff); err != nil {
		return nil, err
	}
	if len(args) != 5 {
		return nil, fmt.Errorf("conformance stand-in: the epoch probe bound %d arguments, want 5", len(args))
	}
	clusterID, apiGroup, kind, err := threeStrings(args)
	if err != nil {
		return nil, err
	}
	namespace, ok := args[3].(string)
	if !ok {
		return nil, fmt.Errorf("conformance stand-in: namespace argument is a %T, want string", args[3])
	}
	// The cutoff arrives as a query literal rather than a bound instant, on
	// purpose (see scopeWasActiveQuery); it is parsed here the way ClickHouse would
	// parse it against a DateTime64(9, 'UTC') column — as UTC, at full precision.
	cutoffText, ok := args[4].(string)
	if !ok {
		return nil, fmt.Errorf("conformance stand-in: the epoch cutoff is a %T, want a %s string "+
			"(a bound time.Time is formatted at second precision by the driver and would blunt it)",
			args[4], chTimeFormat)
	}
	cutoff, err := time.ParseInLocation(chTimeFormat, cutoffText, time.UTC)
	if err != nil {
		return nil, fmt.Errorf("conformance stand-in: the epoch cutoff %q is not a %s datetime: %w",
			cutoffText, chTimeFormat, err)
	}

	// Namespace is matched exactly here, empty included: this is a scope identity,
	// not a record filter.
	scope := sink.ScopeFilter{ClusterID: clusterID, APIGroup: apiGroup, Kind: kind, Namespace: namespace}
	action := ""
	var at time.Time
	for _, row := range b.scopeSnapshot() {
		if row.Scope != scope || !row.TS.Before(cutoff) {
			continue
		}
		if action == "" || at.Before(row.TS) {
			action, at = string(row.Action), row.TS
		}
	}
	return b.rowsFor([][]any{{action}}), nil
}

// queryActiveScopes evaluates the boot-reconciliation enumeration.
func (b *conformanceBackend) queryActiveScopes(query string, args []any) (driver.Rows, error) {
	if err := requireFragments(query, fragmentScopesGroupBy, fragmentScopesHaving); err != nil {
		return nil, err
	}
	if len(args) != 2 {
		return nil, fmt.Errorf("conformance stand-in: the scope enumeration bound %d arguments, want 2", len(args))
	}
	clusterID, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("conformance stand-in: cluster argument is a %T, want string", args[0])
	}
	wantAction, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("conformance stand-in: the action argument is a %T, want string", args[1])
	}

	// Grouped by the scope's identity columns and deliberately not by api_version,
	// which is provenance: two versions serving one scope are one scope.
	type scopeKey struct{ apiGroup, kind, namespace string }
	latest := map[scopeKey]sink.ScopeEvent{}
	for _, row := range b.scopeSnapshot() {
		if row.Scope.ClusterID != clusterID {
			continue
		}
		key := scopeKey{row.Scope.APIGroup, row.Scope.Kind, row.Scope.Namespace}
		if prior, seen := latest[key]; seen && !prior.TS.Before(row.TS) {
			continue
		}
		latest[key] = row
	}

	keys := make([]scopeKey, 0, len(latest))
	for key := range latest {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, c scopeKey) int {
		return strings.Compare(a.apiGroup+"\x00"+a.kind+"\x00"+a.namespace,
			c.apiGroup+"\x00"+c.kind+"\x00"+c.namespace)
	})

	out := make([][]any, 0, len(keys))
	for _, key := range keys {
		if string(latest[key].Action) != wantAction {
			continue
		}
		out = append(out, []any{key.apiGroup, key.kind, key.namespace})
	}
	return b.rowsFor(out), nil
}

// storedRecords is the durable resource_states set.
func (b *conformanceBackend) storedRecords() []sink.Record {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.read.rows)
}

// rowsFor wraps a result set, applying whatever read fault is installed.
func (b *conformanceBackend) rowsFor(rows [][]any) driver.Rows {
	b.mu.Lock()
	fault := b.read.readFault
	b.mu.Unlock()

	out := &conformanceRows{rows: rows, breakAfter: -1}
	if fault != nil {
		out.breakAfter, out.err = fault.AfterRows, fault.Err
	}
	return out
}

// requireFragments is how the stand-in pins the statement it is answering. A
// missing clause means the reader's SQL no longer means what this file evaluates,
// so the read fails naming both the clause and the statement rather than being
// answered on the old understanding.
func requireFragments(query string, fragments ...string) error {
	for _, fragment := range fragments {
		if !strings.Contains(query, fragment) {
			return fmt.Errorf("conformance stand-in: the statement no longer contains %q, so what this "+
				"harness evaluates is not what the backend asked for: %s", fragment, collapseSpace(query))
		}
	}
	return nil
}

// threeStrings reads the first three arguments, which every read binds as
// strings whatever they mean.
func threeStrings(args []any) (string, string, string, error) {
	var out [3]string
	for i := range out {
		s, ok := args[i].(string)
		if !ok {
			return "", "", "", fmt.Errorf("conformance stand-in: read argument %d is a %T, want string", i, args[i])
		}
		out[i] = s
	}
	return out[0], out[1], out[2], nil
}

// collapseSpace renders a multi-line statement on one line for an error message.
func collapseSpace(query string) string {
	return strings.Join(strings.Fields(query), " ")
}

// conformanceRows is a driver.Rows over an in-memory result set that can be made
// to break part-way through.
//
// The break is modelled the way a dropped connection really behaves, and that
// shape is the whole point: rows already delivered stay delivered, Next simply
// stops yielding, and the failure surfaces through Err — never through Next or
// Scan. A reader that only checks Scan's error therefore sees a short, clean
// result set, which is exactly the mistake the partial-read property exists to
// catch.
type conformanceRows struct {
	driver.Rows
	rows      [][]any
	idx       int
	delivered int
	// breakAfter is how many rows may be delivered before the stream dies; -1 when
	// it is healthy.
	breakAfter int
	err        error
}

func (r *conformanceRows) broken() bool {
	return r.err != nil && r.breakAfter >= 0 && r.delivered >= r.breakAfter
}

func (r *conformanceRows) Next() bool {
	if r.broken() {
		return false
	}
	return r.idx < len(r.rows)
}

func (r *conformanceRows) Scan(dest ...any) error {
	if r.idx >= len(r.rows) {
		return errors.New("conformance stand-in: Scan called past the end of the result set")
	}
	row := r.rows[r.idx]
	r.idx++
	r.delivered++
	if len(dest) != len(row) {
		return fmt.Errorf("conformance stand-in: the read scanned %d columns, but the result carries %d",
			len(dest), len(row))
	}
	for i, d := range dest {
		if err := assignColumn(d, row[i]); err != nil {
			return fmt.Errorf("conformance stand-in: column %d: %w", i, err)
		}
	}
	return nil
}

func (r *conformanceRows) Err() error {
	if r.broken() {
		return r.err
	}
	return nil
}

func (r *conformanceRows) Close() error { return nil }

// assignColumn writes one column into its scan target. Only the two column types
// the sink reads back are supported; anything else is an error rather than a
// zero value, because a silently unscanned column reads as a backend that lost it.
func assignColumn(dest, value any) error {
	switch target := dest.(type) {
	case *string:
		s, ok := value.(string)
		if !ok {
			return fmt.Errorf("scan target is a *string but the column is a %T", value)
		}
		*target = s
	case *time.Time:
		ts, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("scan target is a *time.Time but the column is a %T", value)
		}
		*target = ts
	default:
		return fmt.Errorf("unsupported scan target %T", dest)
	}
	return nil
}

// TestConformanceStandInPinsTheReadStatements is what makes the read half's
// conformance run worth anything.
//
// The stand-in evaluates the *intended* meaning of each read, so on its own it
// would keep answering after the reader's SQL stopped meaning that — a GROUP BY
// regrouped per identity, a cutoff turned from strict to inclusive — and the
// properties above would go on passing against a query nobody was checking. The
// pin is what closes that gap, and a pin that has quietly stopped matching is
// indistinguishable from no pin at all. So both directions are asserted here: the
// statements the reader really emits are recognised, and a statement that lost a
// load-bearing clause is refused rather than answered.
func TestConformanceStandInPinsTheReadStatements(t *testing.T) {
	backend := &conformanceBackend{}
	scope := sink.ScopeFilter{ClusterID: "c1", APIGroup: "apps", Kind: "Deployment", Namespace: "prod"}

	statesQuery, statesArgs := lastKnownStatesQuery(scope)
	epochQuery, epochArgs := scopeWasActiveQuery(scope, time.Now().UTC())
	activeQuery, activeArgs := activeScopesQuery(scope.ClusterID)

	recognised := []struct {
		name  string
		query string
		args  []any
	}{
		{"lastKnownStates", statesQuery, statesArgs},
		{"scopeWasActive", epochQuery, epochArgs},
		{"activeScopes", activeQuery, activeArgs},
	}
	for _, tc := range recognised {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := backend.query(context.Background(), tc.query, tc.args...)
			if err != nil {
				t.Fatalf("the stand-in refused the statement the reader really emits: %v", err)
			}
			if err := rows.Close(); err != nil {
				t.Errorf("closing the result set: %v", err)
			}
		})
	}

	// Each mangling drops exactly one clause that carries meaning, which is what a
	// silent semantic regression would look like.
	mangled := []struct {
		name    string
		query   string
		args    []any
		dropped string
	}{
		{
			name:    "warm-up regrouped per identity",
			query:   strings.Replace(statesQuery, fragmentStatesGroupBy, "GROUP BY namespace, name", 1),
			args:    statesArgs,
			dropped: fragmentStatesGroupBy,
		},
		{
			name:    "warm-up no longer excludes tombstones",
			query:   strings.Replace(statesQuery, fragmentStatesHaving, "HAVING 1", 1),
			args:    statesArgs,
			dropped: fragmentStatesHaving,
		},
		{
			name:    "epoch cutoff turned inclusive",
			query:   strings.Replace(epochQuery, fragmentEpochCutoff, "AND ts <= ?", 1),
			args:    epochArgs,
			dropped: fragmentEpochCutoff,
		},
		{
			name:    "scope enumeration regrouped",
			query:   strings.Replace(activeQuery, fragmentScopesGroupBy, "GROUP BY api_group, kind", 1),
			args:    activeArgs,
			dropped: fragmentScopesGroupBy,
		},
		{
			name:    "an entirely unknown statement",
			query:   "SELECT count() FROM resource_states WHERE cluster_id = ?",
			args:    []any{scope.ClusterID},
			dropped: fragmentStatesGroupBy,
		},
	}
	for _, tc := range mangled {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := backend.query(context.Background(), tc.query, tc.args...)
			if err == nil {
				if closeErr := rows.Close(); closeErr != nil {
					t.Errorf("closing the result set: %v", closeErr)
				}
				t.Fatalf("the stand-in answered a statement that no longer contains %q; it would keep "+
					"certifying the read half against SQL that means something else", tc.dropped)
			}
			if !strings.Contains(err.Error(), tc.dropped) {
				t.Errorf("the refusal does not name the clause that went missing (%q): %v", tc.dropped, err)
			}
		})
	}
}

// scopeEventFromInsertArgs is the inverse of scopeInsertArgs, and exists for the
// same reason recordFromInsertArgs does: the suite reasons in sink.ScopeEvents and
// a driver.Conn only ever sees positional args.
//
// It decodes all eight columns rather than the few a property compares, because
// the epoch properties check a recorded transition field by field against the one
// that was handed over — a column this dropped would read as a backend that
// mangled it. A type or arity mismatch is an error, never a zero value: that means
// scopeInsertArgs has changed under it.
func scopeEventFromInsertArgs(args []any) (sink.ScopeEvent, error) {
	if len(args) != scopeArgCount {
		return sink.ScopeEvent{}, fmt.Errorf("watch_scopes insert carried %d args, want %d",
			len(args), scopeArgCount)
	}

	ts, ok := args[scopeTSArg].(time.Time)
	if !ok {
		return sink.ScopeEvent{}, fmt.Errorf("watch_scopes insert arg %d is a %T, want time.Time "+
			"(a formatted string would be reinterpreted in time.Local by the driver)",
			scopeTSArg, args[scopeTSArg])
	}

	// bad remembers the first wrong-typed column so each string column is one line.
	var bad error
	str := func(i int) string {
		s, ok := args[i].(string)
		if !ok && bad == nil {
			bad = fmt.Errorf("watch_scopes insert arg %d is a %T, want string", i, args[i])
		}
		return s
	}

	return sink.ScopeEvent{
		Action: sink.ScopeAction(str(scopeActionArg)),
		Scope: sink.ScopeFilter{
			ClusterID: str(scopeClusterIDArg),
			APIGroup:  str(scopeAPIGroupArg),
			Kind:      str(scopeKindArg),
			Namespace: str(scopeNamespaceArg),
		},
		APIVersion: str(scopeAPIVersionArg),
		RuleRef:    str(scopeRuleRefArg),
		TS:         ts,
	}, bad
}
