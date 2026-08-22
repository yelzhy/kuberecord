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

// The ClickHouse-specific half of this backend's write-path tests. The properties
// every sink.Writer must uphold — commit-exactly-once on all four settling paths,
// no lost jobs, drain before close, a bounded Enqueue, and a replay that collapses
// to one logical record — are asserted by the shared suite instead, from
// writer_conformance_test.go, whose header comment is the inventory of which
// assertion belongs where.
//
// Task 0.7's lettered batching ACs are still traceable: (a)–(d) live below, while
// (d)'s "flushes before conn.Close" half became the suite's DrainOrdering, (e) the
// concurrent-Enqueue storm became ConcurrentEnqueueStorm, and (f) cancel-mid-batch
// became ExactlyOnceCommit/ContextCancelledMidFlight. They are gone from this file
// because they are contract obligations, not ClickHouse behaviour.

package clickhouse

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/sink"
)

// testSinkName is the sink every writer built by these tests reports its
// write-path metrics under (Task 1.8 made those series per-sink). The value is
// irrelevant to what the tests assert — writesTotalValue matches on the outcome
// label — but a named constant keeps it consistent across the package's test
// files.
const testSinkName = "test-sink"

// testSinkID is that sink's identity, which is what ForSink takes since Task 4.1
// (and what its label value renders as: "ClickHouseSink/test-sink"). These tests
// build ClickHouse writers, so the kind is not in doubt.
var testSinkID = sink.ID{Kind: sink.DefaultSinkKind, Name: testSinkName}

// erroringConn is a driver.Conn whose batch and single-row writes always fail,
// used to prove the failure-outcome accounting. Embedding the interface
// satisfies the full method set; only PrepareBatch, Exec, and Close are ever
// exercised here. PrepareBatch failing forces the batch path to exhaust retries
// and fall through to individual isolation, whose Exec then also fails.
type erroringConn struct {
	driver.Conn
}

func (erroringConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, errors.New("clickhouse unavailable")
}

func (erroringConn) Exec(context.Context, string, ...any) error {
	return errors.New("clickhouse unavailable")
}

func (erroringConn) Close() error { return nil }

// fakeConn is a controllable driver.Conn for the batching tests. Send outcomes are
// injected via sendErr / rowErr (nil hook = success), and it records Send/Exec
// counts plus a monotonic sequence so tests can assert ordering (e.g. the drain
// flush's Send happening before Close).
//
// Both the batch attempt and the per-row isolation attempt reach the backend the
// same way — a prepared batch — because that is the only encoder the writer uses
// (see insertArgs). rowErr therefore exists purely to keep each test's intent
// legible: it is consulted for a one-row send, sendErr for a multi-row one. Inserts
// must never arrive through Exec, and execCount is what proves it.
type fakeConn struct {
	driver.Conn

	sendCount atomic.Int64
	execCount atomic.Int64

	seq      atomic.Int64 // monotonic tick incremented on Send and Close
	lastSend atomic.Int64 // seq of the most recent Send
	closeSeq atomic.Int64 // seq at which Close ran

	// sendErr, if set, decides the outcome of a multi-row Send given its context
	// and the appended rows. rowErr does the same for a one-row Send, given that
	// row's arguments.
	sendErr func(ctx context.Context, rows [][]any) error
	rowErr  func(ctx context.Context, args []any) error

	// batchErr, if set, decides the outcome of *every* Send and is additionally
	// told which statement the batch was prepared with. It takes precedence over
	// sendErr and rowErr.
	//
	// It exists for the conformance harness, which drives both insert paths
	// through a single backend stand-in: resource_states rows and watch_scopes
	// rows are fifteen and eight positional args with no self-description, so
	// without the statement the stand-in would have to guess which it was decoding
	// — and a wrong guess is a mis-decoded row that reads as a mangled record.
	batchErr func(ctx context.Context, query string, rows [][]any) error

	// queryMu guards batchQueries, which records the statement each PrepareBatch
	// was called with. The scope-event tests use it to prove their rows target
	// watch_scopes rather than the record path's resource_states.
	queryMu      sync.Mutex
	batchQueries []string
}

func (c *fakeConn) PrepareBatch(ctx context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.queryMu.Lock()
	c.batchQueries = append(c.batchQueries, query)
	c.queryMu.Unlock()
	return &fakeBatch{conn: c, ctx: ctx, query: query}, nil
}

// preparedQueries returns the statements PrepareBatch has been called with.
func (c *fakeConn) preparedQueries() []string {
	c.queryMu.Lock()
	defer c.queryMu.Unlock()
	return slices.Clone(c.batchQueries)
}

// Exec is only reachable through the DDL path (autoCreateSchema); the insert paths
// never use it. The counter lets a test assert that.
func (c *fakeConn) Exec(context.Context, string, ...any) error {
	c.execCount.Add(1)
	return nil
}

func (c *fakeConn) Close() error {
	c.closeSeq.Store(c.seq.Add(1))
	return nil
}

// fakeBatch is the driver.Batch returned by fakeConn.PrepareBatch. It buffers
// appended rows so a sendErr hook can inspect them, and captures the context so
// a test can make Send block until the context is cancelled (the "cancel
// mid-batch" scenario).
type fakeBatch struct {
	driver.Batch

	conn *fakeConn
	ctx  context.Context
	// query is the statement this batch was prepared with, carried so batchErr can
	// tell resource_states rows from watch_scopes ones.
	query string
	rows  [][]any
}

func (b *fakeBatch) Append(v ...any) error {
	b.rows = append(b.rows, v)
	return nil
}

func (b *fakeBatch) Send() error {
	b.conn.sendCount.Add(1)
	b.conn.lastSend.Store(b.conn.seq.Add(1))
	if b.conn.batchErr != nil {
		return b.conn.batchErr(b.ctx, b.query, b.rows)
	}
	if len(b.rows) == 1 && b.conn.rowErr != nil {
		return b.conn.rowErr(b.ctx, b.rows[0])
	}
	if b.conn.sendErr != nil {
		return b.conn.sendErr(b.ctx, b.rows)
	}
	return nil
}

func (b *fakeBatch) Abort() error { return nil }
func (b *fakeBatch) Close() error { return nil }

// commitLog records every commit callback invocation, keyed by record name, so
// tests can assert both the outcome (true/false counts) and — crucially for the
// exactly-once contract — that no job's callback fired more than once.
type commitLog struct {
	mu     sync.Mutex
	byName map[string][]bool
}

func newCommitLog() *commitLog { return &commitLog{byName: map[string][]bool{}} }

func (c *commitLog) record(name string, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byName[name] = append(c.byName[name], ok)
}

// counts returns the total number of commits, and how many were true / false.
func (c *commitLog) counts() (total, trues, falses int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, oks := range c.byName {
		for _, ok := range oks {
			total++
			if ok {
				trues++
			} else {
				falses++
			}
		}
	}
	return total, trues, falses
}

// maxPerName returns the highest number of commits any single job received; a
// value >1 means some job's callback fired more than once (contract violation).
func (c *commitLog) maxPerName() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	maxN := 0
	for _, oks := range c.byName {
		if len(oks) > maxN {
			maxN = len(oks)
		}
	}
	return maxN
}

// enqueueNamed submits a single record whose Name is name, routing its commit
// outcome into log.
func enqueueNamed(t *testing.T, w *CHWriter, ctx context.Context, log *commitLog, name string) {
	t.Helper()
	if err := w.Enqueue(ctx, sink.Job{
		Record: sink.Record{Name: name},
		Commit: func(ok bool) { log.record(name, ok) },
	}); err != nil {
		t.Fatalf("Enqueue(%s): %v", name, err)
	}
}

// waitForCommits blocks until log has recorded at least n commits or the
// timeout elapses, failing the test on timeout.
func waitForCommits(t *testing.T, log *commitLog, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if total, _, _ := log.counts(); total >= n {
			return
		}
		select {
		case <-deadline:
			total, _, _ := log.counts()
			t.Fatalf("timed out waiting for %d commits; got %d", n, total)
		case <-tick.C:
		}
	}
}

// TestBatchFlushBoundsSendCalls covers AC (a): 100 jobs with batchMaxRows=10
// produce at most ⌈100/10⌉ + workers Send calls — the full batches plus, at
// worst, one trailing partial batch per worker flushed during drain.
func TestBatchFlushBoundsSendCalls(t *testing.T) {
	const jobs, batchMaxRows, workers = 100, 10, 4
	conn := &fakeConn{} // healthy: nil hooks

	reg := prometheus.NewRegistry()
	m := pipeline.NewPipelineMetrics(reg).ForSink(testSinkID)
	// Large batchMaxWait so only row-count and drain drive flushes, never the timer.
	w := NewCHWriter(conn, jobs, workers, batchMaxRows, 5*time.Millisecond, time.Second, time.Second, 30*time.Second, time.Second, m)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	log := newCommitLog()
	for i := range jobs {
		enqueueNamed(t, w, ctx, log, "r"+strconv.Itoa(i))
	}

	// Stop accepting, letting the drain flush every partial batch.
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	// Every job settling is the precondition for the flush bound below, not a claim
	// about the commit contract: exactly-once is the suite's property, and
	// re-asserting it here would be the duplication Task 5.2 removes.
	if total, trues, _ := log.counts(); total != jobs || trues != jobs {
		t.Fatalf("commits: total=%d trues=%d, want %d/%d", total, trues, jobs, jobs)
	}
	bound := int64((jobs+batchMaxRows-1)/batchMaxRows + workers)
	if got := conn.sendCount.Load(); got > bound {
		t.Fatalf("Send calls = %d, want <= %d", got, bound)
	}
}

// TestPoisonRowIsolation covers AC (b): a single poison row in a 10-row batch,
// whose batch Send always fails, yields exactly one commit(false) (the poison
// row) and nine commit(true) after individual isolation.
func TestPoisonRowIsolation(t *testing.T) {
	const batchMaxRows = 10
	conn := &fakeConn{
		// The batch never succeeds, forcing the isolation path.
		sendErr: func(context.Context, [][]any) error { return errors.New("batch rejected") },
		// Only the poison row fails its individual attempt.
		rowErr: func(_ context.Context, args []any) error {
			if len(args) > nameArg && args[nameArg] == "poison" {
				return errors.New("bad row")
			}
			return nil
		},
	}

	reg := prometheus.NewRegistry()
	m := pipeline.NewPipelineMetrics(reg).ForSink(testSinkID)
	// One worker so all ten jobs land in one batch; tiny retry cap so the batch
	// exhausts quickly; large batchMaxWait so row-count (not the timer) flushes.
	w := NewCHWriter(conn, 100, 1, batchMaxRows, 5*time.Millisecond, 20*time.Millisecond, time.Second, 30*time.Second, time.Second, m)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	log := newCommitLog()
	for i := range batchMaxRows - 1 {
		enqueueNamed(t, w, ctx, log, "ok"+strconv.Itoa(i))
	}
	enqueueNamed(t, w, ctx, log, "poison")

	waitForCommits(t, log, batchMaxRows, 10*time.Second)

	total, trues, falses := log.counts()
	if total != batchMaxRows || trues != 9 || falses != 1 {
		t.Fatalf("commits: total=%d trues=%d falses=%d, want 10/9/1", total, trues, falses)
	}
	if v := writesTotalValue(t, reg, "failed"); v != 1 {
		t.Fatalf("writes_total{failed} = %v, want 1", v)
	}
	if v := writesTotalValue(t, reg, "success"); v != 9 {
		t.Fatalf("writes_total{success} = %v, want 9", v)
	}
	// The isolation attempts went through a prepared batch, not Exec. That is not a
	// stylistic preference: Exec interpolates arguments into SQL text, which reduces
	// a DateTime64(9) to second precision, so a re-inserted row would differ from the
	// batch attempt it follows and ReplacingMergeTree could never collapse it.
	if n := conn.execCount.Load(); n != 0 {
		t.Errorf("isolation issued %d Exec calls, want 0: inserts must use the batch encoder", n)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
}

// TestLoneJobFlushesOnWait covers AC (c): a single job flushes after
// batchMaxWait elapses even though no further traffic ever arrives to fill the
// batch — and it does not flush instantly, proving the timer (not the drain)
// drove it.
func TestLoneJobFlushesOnWait(t *testing.T) {
	const batchMaxWait = 80 * time.Millisecond
	conn := &fakeConn{} // healthy

	reg := prometheus.NewRegistry()
	m := pipeline.NewPipelineMetrics(reg).ForSink(testSinkID)
	// batchMaxRows well above 1 so only the wait timer can flush the lone job.
	w := NewCHWriter(conn, 100, 1, 100, 5*time.Millisecond, time.Second, time.Second, batchMaxWait, time.Second, m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	log := newCommitLog()
	start := time.Now()
	enqueueNamed(t, w, ctx, log, "lonely")

	waitForCommits(t, log, 1, 5*time.Second)
	elapsed := time.Since(start)

	if elapsed < batchMaxWait/2 {
		t.Fatalf("lone job settled in %s, too fast to have waited for batchMaxWait (%s)", elapsed, batchMaxWait)
	}
	if total, trues, _ := log.counts(); total != 1 || trues != 1 {
		t.Fatalf("commits: total=%d trues=%d, want 1/1", total, trues)
	}
	if got := conn.sendCount.Load(); got != 1 {
		t.Fatalf("Send calls = %d, want 1", got)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
}

// TestShutdownFlushesPartialBatch is the batching half of AC (d): a half-full
// batch is held — no Send at all — until shutdown, and the drain then flushes it
// as one batch rather than row by row.
//
// What this test used to also assert is now the suite's: that the drain settles
// those jobs (ExactlyOnceCommit/Drain) and that the flush precedes the
// connection's closure (DrainOrdering). What is left is the claim the contract is
// silent about — how many Sends this writer's batching produces.
func TestShutdownFlushesPartialBatch(t *testing.T) {
	conn := &fakeConn{} // healthy

	reg := prometheus.NewRegistry()
	m := pipeline.NewPipelineMetrics(reg).ForSink(testSinkID)
	// One worker, batchMaxRows=10, large batchMaxWait: the 5 jobs never reach a
	// row-count or timer flush, so only the drain can flush them.
	w := NewCHWriter(conn, 100, 1, 10, 5*time.Millisecond, time.Second, time.Second, 30*time.Second, time.Second, m)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	log := newCommitLog()
	const partial = 5
	for i := range partial {
		enqueueNamed(t, w, ctx, log, "p"+strconv.Itoa(i))
	}

	// Nothing has flushed yet (batch is half full, timer far off).
	if got := conn.sendCount.Load(); got != 0 {
		t.Fatalf("Send calls before shutdown = %d, want 0", got)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	if got := conn.sendCount.Load(); got != 1 {
		t.Fatalf("Send calls = %d, want 1 (the held partial batch, drained as a single batch)", got)
	}
}

// TestWritesTotalFailedIncrements is the metrics half of the permanently-failing
// path: a job whose write can never succeed (a permanently-erroring conn) accounts
// for exactly one writes_total{outcome="failed"} and no success.
//
// That such a job settles false exactly once is the suite's property
// (ExactlyOnceCommit/PermanentFailure); the suite observes commit callbacks and is
// blind to metrics, so the per-outcome accounting stays here. The commit outcome is
// still read below, because it is what proves the counter has settled.
func TestWritesTotalFailedIncrements(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := pipeline.NewPipelineMetrics(reg).ForSink(testSinkID)

	// Tiny per-attempt timeout and retry cap so the job exhausts retries and
	// settles quickly; small batchMaxWait so the lone job flushes on the timer.
	w := NewCHWriter(erroringConn{}, 10, 1, 10, 5*time.Millisecond, 20*time.Millisecond, time.Second, 20*time.Millisecond, time.Second, m)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	committed := make(chan bool, 1)
	if err := w.Enqueue(ctx, sink.Job{
		Record: sink.Record{},
		Commit: func(ok bool) { committed <- ok },
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case ok := <-committed:
		if ok {
			t.Fatalf("expected the write to be reported as failed, got ok=true")
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for the write to settle")
	}

	// The failed counter is incremented before commit fires, so it is already
	// settled by the time we read it above.
	if v := writesTotalValue(t, reg, "failed"); v != 1 {
		t.Fatalf("writes_total{outcome=\"failed\"} = %v, want 1", v)
	}
	if v := writesTotalValue(t, reg, "success"); v != 0 {
		t.Fatalf("writes_total{outcome=\"success\"} = %v, want 0", v)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
}

// TestInsertArgsTimestampFrozen supports Fix 2 (Task 0.9): it proves that
// rendering the same sink.Record to insert args twice produces byte-identical
// positional args, including the timestamp. Record.Timestamp is stamped once at
// reconcile time and never re-stamped on a retry / re-Exec, which is exactly the
// property ReplacingMergeTree relies on — a re-inserted row is byte-identical
// and therefore collapses on merge.
func TestInsertArgsTimestampFrozen(t *testing.T) {
	rec := sink.Record{
		Timestamp:       time.Date(2026, 7, 23, 10, 30, 45, 123456789, time.UTC),
		ClusterID:       "c1",
		EventType:       "Modified",
		APIGroup:        "apps",
		APIVersion:      "v1",
		Kind:            "Deployment",
		Namespace:       "default",
		Name:            "frozen",
		UID:             "uid-frozen",
		ResourceVersion: "42",
		Labels:          map[string]string{"app": "demo"},
		Actors:          []string{"kubectl"},
		Data:            "",
		Diff:            `[{"op":"replace","path":"/x","value":1}]`,
		SHA256:          "deadbeef",
	}

	first := insertArgs(rec)
	second := insertArgs(rec)

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("insertArgs is not deterministic:\n first  = %v\n second = %v", first, second)
	}

	// The timestamp is arg 0, bound as the instant itself. Assert it round-trips to
	// the exact frozen value both times (a re-stamp would change it between calls),
	// and that it is an instant rather than a rendered string — a string would be
	// reinterpreted in time.Local by the driver's column binder.
	wantTS := rec.Timestamp.UTC()
	gotFirst, ok := first[0].(time.Time)
	if !ok {
		t.Fatalf("timestamp arg is a %T, want a time.Time: a formatted string is parsed in time.Local by the driver", first[0])
	}
	gotSecond, _ := second[0].(time.Time)
	if !gotFirst.Equal(wantTS) || !gotSecond.Equal(wantTS) {
		t.Fatalf("timestamp arg = (%v, %v), want %v both times", first[0], second[0], wantTS)
	}
	if loc := gotFirst.Location(); loc != time.UTC {
		t.Errorf("timestamp arg location = %v, want UTC", loc)
	}
}

// TestInsertArgsIsZoneIndependent is the regression guard for the timezone bug: the
// bound arguments must not depend on the operator process's local zone.
//
// The pinned driver parses a bare datetime string and then reinterprets those
// wall-clock digits in time.Local, so while insertArgs rendered a string, an
// operator running in (say) CEST wrote every ts two hours earlier than the event
// actually happened — silently, in the column the whole audit trail is ordered by.
// Pinning time.Local to a half-hour-offset zone would expose any rendering that
// still consults it (and catches an hour-granular "fix" too).
func TestInsertArgsIsZoneIndependent(t *testing.T) {
	rec := sink.Record{
		Timestamp: time.Date(2026, 7, 23, 12, 0, 0, 987654321, time.UTC),
		ClusterID: "c1",
		Name:      "zoned",
	}

	inUTC := insertArgs(rec)

	restore := pinLocalZone(t, 5*time.Hour+30*time.Minute)
	shifted := insertArgs(rec)
	restore()

	if !reflect.DeepEqual(inUTC, shifted) {
		t.Fatalf("insertArgs depends on the local zone:\n UTC host     = %v\n non-UTC host = %v",
			inUTC, shifted)
	}
	if got := shifted[0].(time.Time); !got.Equal(rec.Timestamp) {
		t.Errorf("timestamp arg on a non-UTC host = %v, want the record's instant %v", got, rec.Timestamp)
	}
}

// pinLocalZone makes the process look like it runs at the given UTC offset, for as
// long as the returned function has not been called (it is also registered as a
// cleanup). It mutates time.Local, which is process-wide — safe here only because no
// test in this package runs in parallel, and it is the one honest way to reproduce a
// non-UTC host: time.Local is exactly what the driver consults.
func pinLocalZone(t *testing.T, offset time.Duration) (restore func()) {
	t.Helper()
	previous := time.Local
	time.Local = time.FixedZone("kuberecord-test", int(offset/time.Second))
	var once sync.Once
	restore = func() { once.Do(func() { time.Local = previous }) }
	t.Cleanup(restore)
	return restore
}

// TestIsolationPhaseBoundedOnHungBackend is the Fix 3 (M3) guard against a
// wedged worker (Task 0.9). The backend is "hung": each row's Exec blocks until
// its own context is cancelled (it never succeeds or errors on its own), and the
// batch Send always fails so the whole batch falls through to per-row isolation.
// With maxIsolationPhase set well below len(batch)×insertTimeout, the isolation
// phase must return at roughly the phase budget — NOT insertTimeout×batchMaxRows,
// which is the unbounded behavior this fix removes. Every job must still commit
// exactly once, with outcome false. Run under -race.
func TestIsolationPhaseBoundedOnHungBackend(t *testing.T) {
	const batchMaxRows = 10
	const insertTimeout = 2 * time.Second
	const maxIsolationPhase = 500 * time.Millisecond

	conn := &fakeConn{
		// Batch always fails → forces the isolation path.
		sendErr: func(context.Context, [][]any) error { return errors.New("batch rejected") },
		// Hung backend: block until the row context is cancelled, then surface it.
		rowErr: func(ctx context.Context, _ []any) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	reg := prometheus.NewRegistry()
	m := pipeline.NewPipelineMetrics(reg).ForSink(testSinkID)
	// One worker so all rows land in one batch; tiny retry cap so the batch phase
	// exhausts fast and the isolation phase dominates; large batchMaxWait so only
	// the row-count flush fires.
	w := NewCHWriter(conn, 100, 1, batchMaxRows, insertTimeout, 20*time.Millisecond, time.Second, 30*time.Second, time.Second, m)
	w.maxIsolationPhase = maxIsolationPhase // same-package override; not a NewCHWriter param.

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	log := newCommitLog()
	start := time.Now()
	for i := range batchMaxRows {
		enqueueNamed(t, w, ctx, log, "h"+strconv.Itoa(i))
	}

	waitForCommits(t, log, batchMaxRows, 10*time.Second)
	elapsed := time.Since(start)

	// The unbounded behavior would be insertTimeout×batchMaxRows = 20s. A bound
	// far below that (and far above the phase budget to tolerate CI jitter)
	// proves the phase cap fired rather than every hung row burning insertTimeout.
	const unbounded = insertTimeout * batchMaxRows
	if elapsed >= unbounded/4 {
		t.Fatalf("isolation phase took %s; expected roughly the phase budget (%s), not near %s",
			elapsed, maxIsolationPhase, unbounded)
	}

	total, trues, falses := log.counts()
	if total != batchMaxRows || trues != 0 || falses != batchMaxRows {
		t.Fatalf("commits: total=%d trues=%d falses=%d, want %d/0/%d", total, trues, falses, batchMaxRows, batchMaxRows)
	}
	if n := log.maxPerName(); n != 1 {
		t.Fatalf("a job committed %d times, want exactly 1", n)
	}
	if v := writesTotalValue(t, reg, "failed"); v != batchMaxRows {
		t.Fatalf("writes_total{failed} = %v, want %d", v, batchMaxRows)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
}

// TestIsolationPhaseDoesNotTruncateSlowBackend is the Fix 3 non-starvation guard
// (Task 0.9). Each isolation Exec succeeds after a delay comfortably under
// insertTimeout but non-trivial, so the cumulative loop time is a meaningful
// fraction of the phase budget; the batch Send always fails to force isolation.
// Every row must settle commit(true) — none cancelled by the phase bound — which
// proves the bound is sized never to convert mere slowness into a manufactured
// failure. That non-truncation property is what distinguishes a correct Fix 3
// from a naive whole-loop timeout. Run under -race.
func TestIsolationPhaseDoesNotTruncateSlowBackend(t *testing.T) {
	const batchMaxRows = 20
	const insertTimeout = 1 * time.Second
	const rowDelay = 50 * time.Millisecond // comfortably under insertTimeout
	const maxIsolationPhase = 4 * time.Second

	conn := &fakeConn{
		sendErr: func(context.Context, [][]any) error { return errors.New("batch rejected") },
		// Slow but alive: each row returns success after rowDelay.
		rowErr: func(context.Context, []any) error {
			time.Sleep(rowDelay)
			return nil
		},
	}

	reg := prometheus.NewRegistry()
	m := pipeline.NewPipelineMetrics(reg).ForSink(testSinkID)
	w := NewCHWriter(conn, 100, 1, batchMaxRows, insertTimeout, 20*time.Millisecond, time.Second, 30*time.Second, time.Second, m)
	w.maxIsolationPhase = maxIsolationPhase

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	log := newCommitLog()
	for i := range batchMaxRows {
		enqueueNamed(t, w, ctx, log, "s"+strconv.Itoa(i))
	}

	waitForCommits(t, log, batchMaxRows, 30*time.Second)

	total, trues, falses := log.counts()
	if total != batchMaxRows || trues != batchMaxRows || falses != 0 {
		t.Fatalf("commits: total=%d trues=%d falses=%d, want %d/%d/0", total, trues, falses, batchMaxRows, batchMaxRows)
	}
	if n := log.maxPerName(); n != 1 {
		t.Fatalf("a job committed %d times, want exactly 1", n)
	}
	if v := writesTotalValue(t, reg, "success"); v != batchMaxRows {
		t.Fatalf("writes_total{success} = %v, want %d", v, batchMaxRows)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
}

// writesTotalValue gathers reg and returns the value of
// kuberecord_writes_total{outcome=outcome}, or fails the test if absent.
func writesTotalValue(t *testing.T, reg prometheus.Gatherer, outcome string) float64 {
	t.Helper()
	const metric, label = "kuberecord_writes_total", "outcome"
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != metric {
			continue
		}
		for _, mtc := range mf.GetMetric() {
			for _, lp := range mtc.GetLabel() {
				if lp.GetName() == label && lp.GetValue() == outcome {
					return mtc.GetCounter().GetValue()
				}
			}
		}
	}
	t.Fatalf("counter %s{%s=%q} not found", metric, label, outcome)
	return 0
}

// TestLastKnownStatesQueryScoping proves the warm-up query is GVK-scoped by
// default and namespace-scoped only when ScopeFilter.Namespace is set — the
// behavior-preserving extraction of restoreAndWarm's original inline query — and
// that it answers per *incarnation* rather than per identity (Task 1.12).
func TestLastKnownStatesQueryScoping(t *testing.T) {
	t.Run("grouping is per incarnation, not per identity", func(t *testing.T) {
		q, _ := lastKnownStatesQuery(sink.ScopeFilter{
			ClusterID: "c1", APIGroup: "apps", Kind: "Deployment",
		})
		// Per-UID grouping is what makes an unrecorded death detectable at all: a
		// per-identity argMax(uid, ts) would simply return the successor's UID once
		// its first row landed, and the prior incarnation would vanish from the
		// answer with nothing amiss.
		if !strings.Contains(q, "GROUP BY namespace, name, uid") {
			t.Errorf("expected a per-incarnation GROUP BY, got query:\n%s", q)
		}
		// The HAVING keeps its "most recent event decides" shape, now scoped to one
		// incarnation: a UID whose own latest event is Deleted is closed out.
		if !strings.Contains(q, "HAVING argMax(event_type, ts) != 'Deleted'") {
			t.Errorf("expected the per-incarnation HAVING clause, got query:\n%s", q)
		}
		// uid is a grouping column now, so it is selected directly rather than
		// aggregated; api_version and ts are what make a close-out derivable from
		// history alone.
		for _, column := range []string{
			"argMax(sha256, ts)", "argMax(api_version, ts)", "max(ts)",
		} {
			if !strings.Contains(q, column) {
				t.Errorf("expected %s in the projection, got query:\n%s", column, q)
			}
		}
		if strings.Contains(q, "argMax(uid, ts)") {
			t.Errorf("uid is a grouping column and must not be aggregated, got query:\n%s", q)
		}
	})

	t.Run("no namespace matches every namespace", func(t *testing.T) {
		q, args := lastKnownStatesQuery(sink.ScopeFilter{
			ClusterID: "c1", APIGroup: "apps", Kind: "Deployment",
		})
		if strings.Contains(q, "namespace = ?") {
			t.Errorf("expected no namespace predicate, got query:\n%s", q)
		}
		if len(args) != 3 {
			t.Fatalf("expected 3 args (api_group, kind, cluster_id), got %d: %v", len(args), args)
		}
		if args[0] != "apps" || args[1] != "Deployment" || args[2] != "c1" {
			t.Errorf("args = %v, want [apps Deployment c1]", args)
		}
	})

	t.Run("namespace narrows the scope", func(t *testing.T) {
		q, args := lastKnownStatesQuery(sink.ScopeFilter{
			ClusterID: "c1", APIGroup: "apps", Kind: "Deployment", Namespace: "team-a",
		})
		if !strings.Contains(q, "namespace = ?") {
			t.Errorf("expected a namespace predicate, got query:\n%s", q)
		}
		if len(args) != 4 || args[3] != "team-a" {
			t.Fatalf("expected 4 args ending in the namespace, got %v", args)
		}
	})
}

// Compile-time proof that CHWriter still satisfies the pipeline's optional
// Checkpoint-policy half of the sink contract (Task 2.2). It is asserted from a
// test file rather than from writer.go because the production package must not
// import internal/pipeline (see the Metrics interface for the same rule); losing
// the method would otherwise be invisible — the pipeline's type assertion would
// simply stop matching and every sink would silently stop checkpointing.
var _ pipeline.CheckpointPolicy = (*CHWriter)(nil)

// TestCheckpointEveryResolution pins how a sink's Checkpoint cadence reaches the
// writer the pipeline consults.
//
// The zero case is the whole point: every other writer knob treats 0 as "unset,
// use the default", but here 0 is a *meaningful* value — the sink owner's off
// switch — so it must survive Open unchanged. Only a negative value (which no
// CRD-validated spec can produce) falls back to the shipped default.
func TestCheckpointEveryResolution(t *testing.T) {
	t.Run("a directly constructed writer ships the default cadence", func(t *testing.T) {
		w := NewCHWriter(nil, 1, 1, 1, time.Second, 0, time.Second, time.Second, time.Second, probeMetrics())
		if got := w.CheckpointEvery(); got != DefaultCheckpointEvery {
			t.Errorf("CheckpointEvery() = %d, want the shipped default %d", got, DefaultCheckpointEvery)
		}
	})

	tests := []struct {
		name string
		cfg  int
		want int
	}{
		{name: "an explicit cadence is honoured", cfg: 7, want: 7},
		{name: "zero disables checkpointing and is never clamped", cfg: 0, want: 0},
		{name: "a negative value falls back to the default", cfg: -3, want: DefaultCheckpointEvery},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := Open(Config{
				Addr:            "127.0.0.1:9000",
				Database:        "kuberecord",
				Username:        "default",
				DialTimeout:     time.Second,
				ReadTimeout:     time.Second,
				CheckpointEvery: tt.cfg,
			}, probeMetrics())
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() {
				if err := w.conn.Close(); err != nil {
					t.Errorf("closing the connection: %v", err)
				}
			})
			if got := w.CheckpointEvery(); got != tt.want {
				t.Errorf("CheckpointEvery() = %d, want %d", got, tt.want)
			}
		})
	}
}
