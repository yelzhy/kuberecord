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

// This file is where the ClickHouse backend is measured against the sink contract
// rather than against its own history. It asserts nothing itself: every assertion
// lives in internal/sink/conformance (Tasks 5.1 and 5.3), so the properties
// ClickHouse passes here are the same ones the next backend will have to pass,
// worded once (D11).
//
// Which assertion belongs where — read this before adding one:
//
// From the suite (contract obligations; never re-assert them in this package):
//
//   - ExactlyOnceCommit/Success — every job settles once, as a success, and its
//     record is really at the backend.
//   - ExactlyOnceCommit/PermanentFailure — a backend that refuses every attempt
//     still settles every job once, as a failure, and stores nothing.
//   - ExactlyOnceCommit/ContextCancelledMidFlight — cancelling with a write
//     genuinely in flight settles each job once, never twice.
//   - ExactlyOnceCommit/Drain — jobs still queued at shutdown are flushed and
//     settled by the drain, once each.
//   - NoLostJobs — one refused record among accepted ones settles false and its
//     blameless batch-mates settle true; nothing settles both ways or neither.
//   - DrainOrdering — shutdown flushes what is in flight and only then closes the
//     connection; no write attempt follows the close, and nothing is stranded.
//   - EnqueueBounded — Invariant 1: a saturated hand-off costs the caller a
//     bounded wait and an error, and honours the caller's own deadline; a job
//     Enqueue refused is never committed.
//   - AtLeastOnceIdempotency — a lost acknowledgement and an upstream requeue both
//     re-write byte-identical rows, so one identity stays one logical record.
//   - ConcurrentEnqueueStorm — many producers handing off at once still settle
//     every job exactly once (its value is under -race).
//
// CHWriter also implements all three optional halves, so RunWriterSuite discovers
// them by type assertion and runs their properties too (see
// optional_conformance_test.go for the stand-in that backs them):
//
//   - StateReader/PerIncarnationResults — the warm-up read answers per (identity,
//     UID), so an incarnation whose death went unrecorded is still visible.
//   - StateReader/TombstonedIncarnationsExcluded — an incarnation whose own latest
//     event is a deletion is gone, and closing one out does not close its identity.
//   - StateReader/PartialReadIsAnError — a read that dies mid-stream is an error,
//     never a short success.
//   - StateReaderScopeEpoch/ScopeWasActiveHonoursAsOf — the epoch probe is strict
//     about the cutoff and exact about the scope, empty namespace included.
//   - StateReaderScopeEpoch/ActiveScopesEnumeratesOpenScopes — the boot-
//     reconciliation enumeration returns the scopes left open and only those.
//   - ScopeEventWriter/EpochTransitionsRecordedExactlyOnce — one row per accepted
//     transition, fields intact, order preserved.
//   - ScopeEventWriter/RejectionIsSurfaced — a transition the sink will not take
//     is refused to the caller and leaves no trace.
//   - Prober/{HealthyBackendPasses,SchemaMismatchIsClassified,
//     OtherFailuresReadAsUnreachable} — the probe's classification, which is the
//     whole of what the manager reads.
//
// Backend-specific, and therefore in writer_test.go, scopewriter_test.go,
// instance_test.go and schema_test.go (the suite is deliberately silent about all
// of it, since none of it is an obligation of the contract):
//
//   - Client-side batching bounds: how many Send calls a job count produces, that
//     a lone job flushes on batchMaxWait, and that a partial batch is held until
//     the drain and then flushed as one batch.
//   - Poison-row isolation: a single bad row is blamed alone instead of dooming
//     its batch — a quality of this implementation, not a contract obligation.
//   - Isolation-phase bounding (Fix 3): the phase budget caps a hung backend, and
//     is sized never to truncate a slow-but-alive one. The suite cannot reach that
//     settling path, so its exactly-once guard is asserted there too.
//   - Metrics accounting: the per-outcome writes_total series. The suite observes
//     commit callbacks, never metrics.
//   - Timezone binding: insertArgs binds an instant, not a formatted string, and
//     is deterministic and zone-independent; the epoch cutoff is the opposite, and
//     for a reason.
//   - The read queries' SQL shape (which columns are grouped, which are filtered)
//     and the exact system.columns contract validateSchema enforces: the suite
//     asks what the reader must *answer*, never how it asks.
//   - Scope-path mechanics: that rows target watch_scopes and not the record
//     path's table, the dedicated batcher, and the retry queue that carries an
//     epoch across an outage.
//   - The Checkpoint cadence, which the sink contract does not speak about.
package clickhouse

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/sink"
	"github.com/yelzhy/kuberecord/internal/sink/conformance"
)

// The positions insertArgs binds each Record field at, and therefore the ones
// recordFromInsertArgs reads them back from. insertResourceStateQuery's column
// list is the single source of truth for the order; this block is the only other
// place in the package that has to agree with it, and insertArgCount is what makes
// a disagreement a decode error rather than a silently shifted field.
const (
	tsArg = iota
	clusterIDArg
	eventTypeArg
	apiGroupArg
	apiVersionArg
	kindArg
	namespaceArg
	nameArg
	uidArg
	resourceVersionArg
	labelsArg
	actorsArg
	dataArg
	diffArg
	sha256Arg
	insertArgCount
)

// The tuning every conformance harness builds its CHWriter with.
//
// It is production-shaped rather than minimal — four workers, several rows to a
// batch — so the properties exercise the real batching and commit paths, and fast
// enough that the whole suite settles in seconds.
//
// conformanceMaxRetryBackoff is the one value chosen for a specific property:
// backoff/v4 stops once elapsed+next would exceed MaxElapsedTime and its first
// interval is 250–750ms, so one second buys exactly one whole-batch retry. That is
// what makes AtLeastOnceIdempotency exercise the batch-level re-write a lost
// acknowledgement really produces in production, while the permanently-failing and
// poison properties still drive the per-row isolation re-insert.
const (
	conformanceQueueCapacity   = 256
	conformanceWorkers         = 4
	conformanceBatchMaxRows    = 8
	conformanceBatchMaxWait    = 20 * time.Millisecond
	conformanceInsertTimeout   = 2 * time.Second
	conformanceMaxRetryBackoff = 1 * time.Second
	conformanceDrainTimeout    = 10 * time.Second
	conformanceEnqueueTimeout  = 2 * time.Second
	// conformanceSettleWithin must exceed the writer's whole retry budget, not one
	// attempt: the permanently-failing property waits for every job to settle
	// against a backend that refuses each of them.
	conformanceSettleWithin = 15 * time.Second
	// conformanceScopeRetryBackoff shortens the scope path's own retry window from
	// its production 30s. Nothing faults a scope insert here, so this only bounds
	// how long a property would wait before failing rather than hanging.
	conformanceScopeRetryBackoff = 500 * time.Millisecond
)

// TestWriterConformance runs the backend-agnostic Writer suite against CHWriter.
//
// The harness constructor is called once per property (the suite shuts the Writer
// down in several of them, and a Writer is not restartable), so each property gets
// a fresh connection, a fresh event log and a fresh metrics registry and cannot
// inherit another's state.
func TestWriterConformance(t *testing.T) {
	conformance.RunWriterSuite(t, newConformanceHarness)
}

// newConformanceHarness wires a CHWriter over the package's own fakeConn up as a
// conformance.Harness: no production code is involved beyond NewCHWriter, which is
// the point — the suite passes or fails on the shipped writer's own behaviour.
func newConformanceHarness(t *testing.T) conformance.Harness {
	t.Helper()

	backend := &conformanceBackend{}
	conn := recordingConn{
		// One handler for every send, told which statement it is answering:
		// resource_states rows (a batch attempt or a one-row poison-isolation
		// attempt alike) and watch_scopes rows both reach the backend here, and
		// fifteen versus eight positional args is not something to guess at.
		fakeConn: &fakeConn{batchErr: backend.attempt},
		backend:  backend,
	}

	w := NewCHWriter(conn, conformanceQueueCapacity, conformanceWorkers, conformanceBatchMaxRows,
		conformanceInsertTimeout, conformanceMaxRetryBackoff, conformanceDrainTimeout,
		conformanceBatchMaxWait, conformanceEnqueueTimeout,
		pipeline.NewPipelineMetrics(prometheus.NewRegistry()).ForSink(testSinkID))
	// The two fields NewCHWriter does not take, and that the optional halves need:
	// Probe validates against a named database, and the scope path's retry window
	// is production-sized (30s) where the suite needs a transition to settle inside
	// one property.
	w.database = conformanceDatabase
	w.scopeMaxRetryBackoff = conformanceScopeRetryBackoff

	// A decode failure is not a contract violation, so it is reported here rather
	// than through the suite: it would otherwise surface as a property comparing
	// records this backend never really stored, and the reader would go looking for
	// the bug in the writer.
	t.Cleanup(func() {
		if err := backend.firstDecodeErr(); err != nil {
			t.Errorf("decoding a resource_states insert back to a sink.Record failed: %v; "+
				"recordFromInsertArgs has drifted out of step with insertArgs, so what the suite "+
				"compared is not what the backend stored", err)
		}
	})

	return conformance.Harness{
		Writer:         w,
		Events:         backend.snapshot,
		SetFault:       backend.setFault,
		LogicalKey:     resourceStatesKey,
		Dedup:          conformance.DedupMergeCollapse,
		QueueCapacity:  conformanceQueueCapacity,
		EnqueueTimeout: conformanceEnqueueTimeout,
		SettleWithin:   conformanceSettleWithin,

		// The optional halves. CHWriter implements all three, so the suite
		// discovers them by type assertion and runs their properties too; see
		// optional_conformance_test.go for what backs each lever.
		ScopeWrites:     backend.scopeSnapshot,
		SetReadFault:    backend.setReadFault,
		SetProbeOutcome: backend.setProbeOutcome,
	}
}

// conformanceBackend is the observable ClickHouse stand-in the suite drives. It
// owns the three things the Harness needs and CHWriter cannot provide: the fault
// consulted on every write attempt, the ordered log of what the backend saw, and
// the translation between the two representations — the suite reasons in
// sink.Records, a driver.Conn only ever sees positional args.
type conformanceBackend struct {
	mu     sync.Mutex
	events []conformance.Event
	fault  conformance.FaultFunc
	// decodeErr is the first args-to-Record decode failure. It is remembered rather
	// than returned because a Send runs on a worker goroutine with no test to fail;
	// the harness asserts it in a t.Cleanup instead.
	decodeErr error
	// read is the storage the optional halves answer from, and the levers that
	// break them. It lives in optional_conformance_test.go, guarded by the same mu
	// as everything else here: the read half is called from the suite's goroutine
	// while the write half is still appending from the writer's.
	read conformanceReadState
}

// setFault implements Harness.SetFault. The suite only ever calls it before Start,
// but the workers read it afterwards, so it is guarded like every other field here.
func (b *conformanceBackend) setFault(f conformance.FaultFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.fault = f
}

// attempt is one durable-write attempt on either insert path, routed by the
// statement the batch was prepared with. Guessing from the row shape would work
// today and break the first time a column is added to either table.
func (b *conformanceBackend) attempt(ctx context.Context, query string, rows [][]any) error {
	switch {
	case strings.Contains(query, "INSERT INTO "+tableResourceStates):
		return b.attemptRecords(ctx, rows)
	case strings.Contains(query, "INSERT INTO "+tableWatchScopes):
		return b.attemptScopeEvents(rows)
	}
	err := fmt.Errorf("insert targets neither %s nor %s, so its rows cannot be decoded: %s",
		tableResourceStates, tableWatchScopes, collapseSpace(query))
	b.noteDecodeErr(err)
	return err
}

// attemptRecords is one resource_states insert attempt: the rows are decoded back
// to Records, the installed fault decides the outcome, and the attempt is logged
// with whatever it returned. It is logged *after* the fault returns so the log
// reads in completion order — which is what makes "a write that ran after the
// connection closed" visible to the drain-ordering property at all.
func (b *conformanceBackend) attemptRecords(ctx context.Context, rows [][]any) error {
	records := make([]sink.Record, 0, len(rows))
	for _, args := range rows {
		rec, err := recordFromInsertArgs(args)
		if err != nil {
			b.noteDecodeErr(err)
		}
		records = append(records, rec)
	}

	b.mu.Lock()
	fault := b.fault
	b.mu.Unlock()

	var err error
	if fault != nil {
		err = fault(ctx, records)
	}

	b.record(conformance.Event{Kind: conformance.EventWrite, Records: records, Err: err})
	return err
}

// record appends to the observation log and, for an attempt whose rows reached
// storage, to the set the read half answers from. Event.Durable draws that line
// rather than this backend, so a lost acknowledgement leaves its rows readable
// here exactly as it would leave them in a real table.
func (b *conformanceBackend) record(ev conformance.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, ev)
	if ev.Durable() {
		b.read.rows = append(b.read.rows, ev.Records...)
	}
}

// snapshot implements Harness.Events. The copy is mandatory: the suite reads the
// log while workers are still appending to it.
func (b *conformanceBackend) snapshot() []conformance.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.events)
}

func (b *conformanceBackend) noteDecodeErr(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.decodeErr == nil {
		b.decodeErr = err
	}
}

func (b *conformanceBackend) firstDecodeErr() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.decodeErr
}

// recordingConn is a fakeConn whose Close lands in the same ordered log as its
// writes, which is what lets the drain-ordering property compare the two, and
// which answers the reads and pings the optional halves make.
//
// It wraps rather than extends the shared fake: PrepareBatch and fakeBatch are
// inherited untouched, so the suite's attempts reach the backend through exactly
// the encoder the rest of this package's tests drive. Query and Ping are declared
// here rather than on fakeConn because no other test in the package needs a
// connection that reads back what it wrote, and the embedded driver.Conn is nil —
// so a method that is not shadowed here panics rather than lying, which is the
// right failure for a call this stand-in has not thought about.
type recordingConn struct {
	*fakeConn
	backend *conformanceBackend
}

func (c recordingConn) Close() error {
	err := c.fakeConn.Close()
	c.backend.record(conformance.Event{Kind: conformance.EventClose, Err: err})
	return err
}

func (c recordingConn) Ping(ctx context.Context) error { return c.backend.ping(ctx) }

func (c recordingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return c.backend.query(ctx, query, args...)
}

// resourceStatesKey implements Harness.LogicalKey: resource_states' ORDER BY tuple
// (deploy/clickhouse/schema/001_resource_states.sql), which is precisely what
// ReplacingMergeTree collapses a duplicate on.
//
// It is emphatically not the object identity key of Invariant 7: ts is part of it,
// so two events for one object are two logical records, while a re-insert of one
// event is the same record — the distinction the at-least-once write path lives on.
func resourceStatesKey(rec sink.Record) string {
	return strings.Join([]string{
		rec.ClusterID, rec.APIGroup, rec.Kind, rec.Namespace, rec.Name,
		rec.Timestamp.UTC().Format(time.RFC3339Nano),
	}, "|")
}

// recordFromInsertArgs is the inverse of insertArgs, and exists because fakeConn
// only ever sees []any while the suite reasons in sink.Records.
//
// It decodes all fifteen columns rather than the handful a property happens to
// compare: the idempotency property checks a stored record field by field against
// the record it repeats, so a column this dropped would read as a backend that
// mangled it. A type or arity mismatch is returned as an error — never absorbed as
// a zero value — because that means insertArgs has changed under it.
func recordFromInsertArgs(args []any) (sink.Record, error) {
	if len(args) != insertArgCount {
		return sink.Record{}, fmt.Errorf("insert carried %d args, want %d", len(args), insertArgCount)
	}

	// bad remembers the first wrong-typed column so every string column is read
	// with one line each rather than four.
	var bad error
	str := func(i int) string {
		s, ok := args[i].(string)
		if !ok && bad == nil {
			bad = fmt.Errorf("insert arg %d is a %T, want string", i, args[i])
		}
		return s
	}

	ts, ok := args[tsArg].(time.Time)
	if !ok {
		return sink.Record{}, fmt.Errorf("insert arg %d is a %T, want time.Time "+
			"(a formatted string would be reinterpreted in time.Local by the driver)", tsArg, args[tsArg])
	}
	labels, ok := args[labelsArg].(map[string]string)
	if !ok {
		return sink.Record{}, fmt.Errorf("insert arg %d is a %T, want map[string]string", labelsArg, args[labelsArg])
	}
	actors, ok := args[actorsArg].([]string)
	if !ok {
		return sink.Record{}, fmt.Errorf("insert arg %d is a %T, want []string", actorsArg, args[actorsArg])
	}

	return sink.Record{
		Timestamp:       ts,
		ClusterID:       str(clusterIDArg),
		EventType:       str(eventTypeArg),
		APIGroup:        str(apiGroupArg),
		APIVersion:      str(apiVersionArg),
		Kind:            str(kindArg),
		Namespace:       str(namespaceArg),
		Name:            str(nameArg),
		UID:             str(uidArg),
		ResourceVersion: str(resourceVersionArg),
		Labels:          maps.Clone(labels),
		Actors:          slices.Clone(actors),
		Data:            str(dataArg),
		Diff:            str(diffArg),
		SHA256:          str(sha256Arg),
	}, bad
}
