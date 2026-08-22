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

package conformance

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// The fixed field values every record the suite enqueues shares. They are
// constants, and suiteEpoch is a fixed instant rather than time.Now(), because
// the idempotency property replays a record and compares what the backend stored
// against what it stored the first time: anything derived from the wall clock
// would differ between the two rounds and would turn a correct backend into a
// failure.
const (
	suiteClusterID  = "conformance-cluster"
	suiteAPIGroup   = "apps"
	suiteAPIVersion = "v1"
	suiteKind       = "Deployment"
	suiteNamespace  = "conformance"
	suiteActor      = "conformance-suite"
)

// suiteEpoch is the base timestamp for generated records. See the block above.
var suiteEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// errFault is the permanent, non-retriable-looking failure the suite injects when
// it wants a write to fail for good.
var errFault = errors.New("conformance: injected write failure")

// shutdownGrace is how long past SettleWithin the suite waits for Start to
// return after cancellation before calling the shutdown unbounded.
const shutdownGrace = 10 * time.Second

// testRecord builds record i: distinct in every field that identifies it, stable
// across calls, and byte-identical for the same i so a replay can be compared
// field by field.
func testRecord(i int) sink.Record {
	name := fmt.Sprintf("obj-%05d", i)
	return sink.Record{
		Timestamp:       suiteEpoch.Add(time.Duration(i) * time.Millisecond),
		ClusterID:       suiteClusterID,
		EventType:       "Added",
		APIGroup:        suiteAPIGroup,
		APIVersion:      suiteAPIVersion,
		Kind:            suiteKind,
		Namespace:       suiteNamespace,
		Name:            name,
		UID:             "uid-" + name,
		ResourceVersion: strconv.Itoa(1000 + i),
		Labels:          map[string]string{"app": name},
		Actors:          []string{suiteActor},
		Data:            `{"metadata":{"name":"` + name + `"}}`,
		SHA256:          fmt.Sprintf("%064x", i),
	}
}

// testRecords builds n records, indexed by job number.
func testRecords(n int) []sink.Record {
	out := make([]sink.Record, n)
	for i := range n {
		out[i] = testRecord(i)
	}
	return out
}

// recordsEqual compares two records field by field.
//
// It is not reflect.DeepEqual: a backend that round-trips a record through its
// physical form legitimately turns a nil Labels map into an empty one (nothing
// distinguishes the two once written), and legitimately returns a timestamp whose
// monotonic reading or location differs while naming the same instant. Failing a
// backend for either would be the suite asserting its own in-memory
// representation rather than the contract.
func recordsEqual(a, b sink.Record) bool {
	return a.Timestamp.Equal(b.Timestamp) &&
		a.ClusterID == b.ClusterID &&
		a.EventType == b.EventType &&
		a.APIGroup == b.APIGroup &&
		a.APIVersion == b.APIVersion &&
		a.Kind == b.Kind &&
		a.Namespace == b.Namespace &&
		a.Name == b.Name &&
		a.UID == b.UID &&
		a.ResourceVersion == b.ResourceVersion &&
		a.Data == b.Data &&
		a.Diff == b.Diff &&
		a.SHA256 == b.SHA256 &&
		maps.Equal(a.Labels, b.Labels) &&
		slices.Equal(a.Actors, b.Actors)
}

// describe renders a record compactly for a failure message: enough to identify
// which record is at fault without printing its payload.
func describe(rec sink.Record) string {
	return fmt.Sprintf("%s/%s@%s (%s)", rec.Namespace, rec.Name,
		rec.Timestamp.UTC().Format(time.RFC3339Nano), rec.EventType)
}

// commitCounter is the instrument the exactly-once properties are built on: one
// atomic counter per job, incremented by that job's own Commit callback.
//
// Counting per job (rather than totalling commits) is what makes the property
// falsifiable in both directions at once — a stranded job and a double-settled
// job both leave the total looking plausible while min or max gives them away —
// and the counters are atomic because the callbacks fire on the Writer's worker
// goroutines, several at a time, which is exactly where a double settle is most
// likely to come from.
type commitCounter struct {
	counts []atomic.Int64
	trues  []atomic.Int64
	total  atomic.Int64
}

func newCommitCounter(n int) *commitCounter {
	return &commitCounter{counts: make([]atomic.Int64, n), trues: make([]atomic.Int64, n)}
}

// commitFor returns the Commit callback for job i.
func (c *commitCounter) commitFor(i int) func(bool) {
	return func(ok bool) {
		c.counts[i].Add(1)
		if ok {
			c.trues[i].Add(1)
		}
		c.total.Add(1)
	}
}

// settled is the total number of commit callbacks fired, across all jobs.
func (c *commitCounter) settled() int { return int(c.total.Load()) }

// countOf is how many times job i settled: 0 means stranded, >1 means the
// exactly-once contract is broken.
func (c *commitCounter) countOf(i int) int64 { return c.counts[i].Load() }

// ok reports whether job i settled as durably written.
func (c *commitCounter) ok(i int) bool { return c.trues[i].Load() > 0 }

// minMax returns the smallest and largest per-job commit count over the first n
// jobs. Both ends matter and neither is visible in a total: min<1 is a stranded
// job, max>1 a double settle, and one of each cancel out.
func (c *commitCounter) minMax(n int) (int64, int64) {
	if n <= 0 {
		return 0, 0
	}
	minN, maxN := c.counts[0].Load(), c.counts[0].Load()
	for i := 1; i < n; i++ {
		got := c.counts[i].Load()
		minN = min(minN, got)
		maxN = max(maxN, got)
	}
	return minN, maxN
}

// offenders lists up to limit of the first n jobs whose commit count is not want,
// so a failure names the jobs instead of only the aggregate.
func (c *commitCounter) offenders(n int, want int64, limit int) string {
	var b strings.Builder
	shown := 0
	for i := 0; i < n && shown < limit; i++ {
		if got := c.counts[i].Load(); got != want {
			if shown > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "job %d settled %d times", i, got)
			shown++
		}
	}
	if shown == 0 {
		return "none"
	}
	return b.String()
}

// assertExactlyOnce is the AC's "max == 1 and min == 1", over the first n jobs
// (the rest, where there are any, are jobs Enqueue refused). when names the
// scenario so a failure says which path broke.
func assertExactlyOnce(t conformanceT, c *commitCounter, n int, when string) {
	t.Helper()
	minN, maxN := c.minMax(n)
	if minN != 1 || maxN != 1 {
		t.Errorf("%s: per-job commit count min=%d max=%d, want exactly 1 and 1 "+
			"(min<1 strands a job, max>1 settles one twice and corrupts the caller's dedup cache); %s",
			when, minN, maxN, c.offenders(n, 1, 5))
	}
}

// assertNeverSettled asserts that jobs Enqueue rejected were never committed. A
// Writer that both refuses a job and settles it has settled it twice from the
// caller's point of view: the caller already treated the Enqueue error as the
// job's outcome.
func assertNeverSettled(t conformanceT, c *commitCounter, indexes []int, when string) {
	t.Helper()
	for _, i := range indexes {
		if n := c.countOf(i); n != 0 {
			t.Errorf("%s: job %d was rejected by Enqueue but its commit callback fired %d times, want 0",
				when, i, n)
		}
	}
}

// waitFor polls cond until it holds or timeout elapses. Polling (rather than a
// channel the properties would have to thread through every callback) keeps the
// commit path to a single atomic add, which is what makes it safe to assert on
// under -race.
func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

// waitSettled waits for want commit callbacks in total, failing with the shortfall
// when they do not arrive. A shortfall is the "no lost jobs" violation: some job
// never settled either way.
func waitSettled(t conformanceT, c *commitCounter, want int, timeout time.Duration, when string) {
	t.Helper()
	if waitFor(func() bool { return c.settled() >= want }, timeout) {
		return
	}
	t.Errorf("%s: %d of %d jobs settled within %s; the rest were never committed either way",
		when, c.settled(), want, timeout)
}

// durableIndex is the durable set, grouped by the backend's own dedup key: the
// physical rows that survived, arranged the way the backend itself would fold
// them.
type durableIndex struct {
	byKey map[string][]sink.Record
	total int
}

// indexDurable folds an event log into the durable set. Attempts that failed
// contribute nothing — except ErrLostAck, whose whole point is that they do.
func indexDurable(h Harness, events []Event) durableIndex {
	idx := durableIndex{byKey: map[string][]sink.Record{}}
	for _, ev := range events {
		if !ev.Durable() {
			continue
		}
		for _, rec := range ev.Records {
			key := h.LogicalKey(rec)
			idx.byKey[key] = append(idx.byKey[key], rec)
			idx.total++
		}
	}
	return idx
}

// contains reports whether rec is durably present, as itself and not merely as
// something sharing its key.
func (d durableIndex) contains(h Harness, rec sink.Record) bool {
	for _, got := range d.byKey[h.LogicalKey(rec)] {
		if recordsEqual(got, rec) {
			return true
		}
	}
	return false
}

// assertCommittedTrueAreDurable checks the half of the commit contract that no
// counter can see: a job settled true must actually be in the backend.
//
// This is the failure that silently corrupts an audit trail. commit(true) is what
// licenses the pipeline to advance its version-gated hashCache for that object,
// so a true that was never written means every later change to that object is
// deduplicated away against a state the sink does not hold.
func assertCommittedTrueAreDurable(t conformanceT, h Harness, c *commitCounter, enq []sink.Record) {
	t.Helper()
	idx := indexDurable(h, h.Events())
	for i, rec := range enq {
		if c.ok(i) && !idx.contains(h, rec) {
			t.Errorf("job %d %s settled true but is not durable at the backend: "+
				"commit(true) is the caller's licence to advance its dedup cache past this record",
				i, describe(rec))
			return
		}
	}
}

// runner owns one Writer's lifecycle for the duration of one property: it starts
// Start on its own goroutine, hands out the context the Writer is running under,
// and makes cancellation and the wait for Start to return idempotent so a
// property can shut down explicitly (most do — the shutdown is what they are
// asserting) and still `defer` the same call as a safety net.
type runner struct {
	t         conformanceT
	h         Harness
	ctx       context.Context
	cancelFn  context.CancelFunc
	done      chan error
	cancelled bool
	waited    bool
}

// startWriter runs h.Writer.Start on its own goroutine.
func startWriter(t conformanceT, h Harness) *runner {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &runner{t: t, h: h, ctx: ctx, cancelFn: cancel, done: make(chan error, 1)}
	go func() { r.done <- h.Writer.Start(ctx) }()
	return r
}

// cancel signals shutdown without waiting for it.
func (r *runner) cancel() {
	if r.cancelled {
		return
	}
	r.cancelled = true
	r.cancelFn()
}

// wait blocks until Start returns, failing if it does not — an unbounded
// shutdown strands whatever the Writer still holds and would hang the suite.
func (r *runner) wait() {
	r.t.Helper()
	if r.waited {
		return
	}
	r.waited = true
	bound := r.h.SettleWithin + shutdownGrace
	select {
	case err := <-r.done:
		if err != nil {
			r.t.Errorf("Start returned %v; a clean shutdown must return nil", err)
		}
	case <-time.After(bound):
		r.t.Errorf("Start did not return within %s of cancellation: shutdown is not bounded, "+
			"so queued work is stranded rather than drained", bound)
	}
}

// stop is cancel followed by wait: the ordinary end of a property.
func (r *runner) stop() {
	r.t.Helper()
	r.cancel()
	r.wait()
}

// mustEnqueue submits job i, failing the property if the hand-off is refused.
// An Enqueue error here is a harness-tuning problem (too small a queue, too short
// a timeout for the backend it is driving), not a contract violation, so the
// message says so.
func mustEnqueue(t conformanceT, ctx context.Context, w sink.Writer, c *commitCounter, i int, rec sink.Record) {
	t.Helper()
	if err := w.Enqueue(ctx, sink.Job{Record: rec, Commit: c.commitFor(i)}); err != nil {
		t.Fatalf("Enqueue(job %d, %s): %v; the suite needs every job accepted here — "+
			"raise Harness.QueueCapacity or Harness.EnqueueTimeout if the backend cannot keep up",
			i, describe(rec), err)
	}
}

// enqueueWithin calls Enqueue on its own goroutine and gives up waiting after
// bound, reporting whether it returned at all.
//
// The goroutine is what keeps the bounded-Enqueue property falsifiable: a Writer
// that ignores its own timeout would otherwise hang the suite forever, and a
// suite that hangs proves nothing — it has to *fail*, and say why.
func enqueueWithin(ctx context.Context, w sink.Writer, job sink.Job, bound time.Duration) (bool, error) {
	res := make(chan error, 1)
	go func() { res <- w.Enqueue(ctx, job) }()
	select {
	case err := <-res:
		return true, err
	case <-time.After(bound):
		return false, nil
	}
}

// seedHistory makes records into history the backend really holds, by writing
// them through its own Writer and waiting for every one to settle true.
//
// The read properties could have been given a hook that planted rows behind the
// Writer's back, and that would have been faster. It would also have tested the
// wrong thing: what a warm-up reads back is what the write path actually wrote,
// so a backend whose reader and writer disagree about a field — a timestamp
// rounded on the way in, a UID stored under a different column — must fail the
// read properties, and it only can if the same path produced the history.
//
// A record that settles false here is a harness problem, not a contract failure:
// the property below would be asserting against history that was never written,
// so it stops immediately and says so.
func seedHistory(t conformanceT, r *runner, h Harness, records []sink.Record) {
	t.Helper()
	const when = "seeding history"

	c := newCommitCounter(len(records))
	for i, rec := range records {
		mustEnqueue(t, r.ctx, h.Writer, c, i, rec)
	}
	waitSettled(t, c, len(records), h.SettleWithin, when)
	for i, rec := range records {
		if !c.ok(i) {
			t.Fatalf("%s: record %d %s settled false against a backend with no fault installed; "+
				"the read properties would be reading history that was never written", when, i, describe(rec))
		}
	}
}

// seedScopeHistory does the same for the scope log: it hands the transitions to
// the backend's own ScopeEventWriter and waits for them to be recorded.
//
// It takes the writer as an argument rather than re-asserting it from the harness
// so that a caller cannot reach this with a backend that has no scope log — the
// suite decides that once, in runOptionalSuite, and not again per property.
func seedScopeHistory(t conformanceT, r *runner, h Harness, w sink.ScopeEventWriter, events []sink.ScopeEvent) {
	t.Helper()
	const when = "seeding scope history"

	for i, event := range events {
		if err := w.EnqueueScopeEvent(r.ctx, event); err != nil {
			t.Fatalf("%s: EnqueueScopeEvent(%d, %s) was refused: %v; the epoch properties need every "+
				"transition accepted here", when, i, describeScope(event), err)
		}
	}
	waitScopeWrites(t, h, len(events), h.SettleWithin, when)
}

// waitScopeWrites waits until the backend has recorded want scope transitions,
// failing with the shortfall when it does not. A shortfall means an accepted
// transition never landed, which is the audit hole the scope log exists to close.
func waitScopeWrites(t conformanceT, h Harness, want int, timeout time.Duration, when string) {
	t.Helper()
	if waitFor(func() bool { return len(h.ScopeWrites()) >= want }, timeout) {
		return
	}
	t.Errorf("%s: the backend recorded %d of %d scope transitions within %s; an accepted transition that "+
		"never lands is an epoch nothing can re-derive", when, len(h.ScopeWrites()), want, timeout)
}

// describeScope renders a transition compactly for a failure message.
func describeScope(event sink.ScopeEvent) string {
	return fmt.Sprintf("%s %s/%s/%s ns=%q@%s (rule %q)", event.Action, event.Scope.ClusterID,
		event.Scope.APIGroup, event.Scope.Kind, event.Scope.Namespace,
		event.TS.UTC().Format(time.RFC3339Nano), event.RuleRef)
}

// scopeEventsEqual compares two transitions field by field, on the same footing
// as recordsEqual: an instant is compared as an instant, so a backend that
// round-trips it through its physical form is not failed for the location or the
// monotonic reading it came back with.
func scopeEventsEqual(a, b sink.ScopeEvent) bool {
	return a.Action == b.Action &&
		a.Scope == b.Scope &&
		a.APIVersion == b.APIVersion &&
		a.RuleRef == b.RuleRef &&
		a.TS.Equal(b.TS)
}

// countCloses returns how many EventClose entries the log holds and the index of
// the first, or -1 when there is none.
func countCloses(events []Event) (int, int) {
	n, first := 0, -1
	for i, ev := range events {
		if ev.Kind != EventClose {
			continue
		}
		n++
		if first < 0 {
			first = i
		}
	}
	return n, first
}
