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
	"sync"
	"sync/atomic"
	"time"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// The subtest names, as constants because the non-vacuity tests address
// individual properties by name and a typo there would silently prove nothing.
const (
	propExactlyOnceSuccess   = "ExactlyOnceCommit/Success"
	propExactlyOnceFailure   = "ExactlyOnceCommit/PermanentFailure"
	propExactlyOnceCancelled = "ExactlyOnceCommit/ContextCancelledMidFlight"
	propExactlyOnceDrain     = "ExactlyOnceCommit/Drain"
	propNoLostJobs           = "NoLostJobs"
	propDrainOrdering        = "DrainOrdering"
	propEnqueueBounded       = "EnqueueBounded"
	propIdempotency          = "AtLeastOnceIdempotency"
	propStorm                = "ConcurrentEnqueueStorm"
)

// suiteJobs is the job count for the properties that do not need a specific one:
// comfortably more than any plausible batch size, so several batches and several
// workers are involved, and small enough that a failure message can name the
// offending jobs.
const suiteJobs = 20

// stormProducers and stormPerProducer size the concurrent enqueue storm. The
// product deliberately exceeds any reasonable QueueCapacity so that producers
// genuinely contend for room while workers drain.
const (
	stormProducers   = 10
	stormPerProducer = 50
)

// writerProperties is the mandatory half of the sink contract, as executable
// obligations. Every backend must pass all of them; the list is the definition of
// "a Writer", and a new obligation belongs here rather than in any one backend's
// tests.
func writerProperties() []property {
	return []property{
		{name: propExactlyOnceSuccess, run: exactlyOnceOnSuccess},
		{name: propExactlyOnceFailure, run: exactlyOnceOnPermanentFailure},
		{name: propExactlyOnceCancelled, run: exactlyOnceOnCancellation},
		{name: propExactlyOnceDrain, run: exactlyOnceOnDrain},
		{name: propNoLostJobs, run: noLostJobs},
		{name: propDrainOrdering, run: drainOrdering},
		{name: propEnqueueBounded, run: enqueueBounded},
		{name: propIdempotency, run: atLeastOnceIdempotency},
		{name: propStorm, run: concurrentEnqueueStorm},
	}
}

// propertyByName finds a property in any of the tables — mandatory or optional.
// It exists for the non-vacuity tests, which run one property at a time against a
// Writer built to violate it.
func propertyByName(name string) (property, bool) {
	for _, p := range allProperties() {
		if p.name == name {
			return p, true
		}
	}
	return property{}, false
}

// exactlyOnceOnSuccess: against a healthy backend every job settles exactly once,
// as a success, and the record it settled is really there.
func exactlyOnceOnSuccess(t conformanceT, h Harness) {
	t.Helper()
	const when = "healthy backend"

	r := startWriter(t, h)
	defer r.stop()

	enq := testRecords(suiteJobs)
	c := newCommitCounter(suiteJobs)
	for i, rec := range enq {
		mustEnqueue(t, r.ctx, h.Writer, c, i, rec)
	}
	waitSettled(t, c, suiteJobs, h.SettleWithin, when)
	r.stop()

	assertExactlyOnce(t, c, suiteJobs, when)
	for i := range suiteJobs {
		if !c.ok(i) {
			t.Errorf("%s: job %d settled as failed against a backend that accepted every write", when, i)
			break
		}
	}
	assertCommittedTrueAreDurable(t, h, c, enq)
}

// exactlyOnceOnPermanentFailure: when no attempt can ever succeed, every job
// still settles — once, as a failure. Abandoning a job silently, or retrying it
// forever, both leave the caller unable to requeue it.
func exactlyOnceOnPermanentFailure(t conformanceT, h Harness) {
	t.Helper()
	const when = "permanently failing backend"

	h.SetFault(func(context.Context, []sink.Record) error { return errFault })

	r := startWriter(t, h)
	defer r.stop()

	enq := testRecords(suiteJobs)
	c := newCommitCounter(suiteJobs)
	for i, rec := range enq {
		mustEnqueue(t, r.ctx, h.Writer, c, i, rec)
	}
	waitSettled(t, c, suiteJobs, h.SettleWithin, when)
	r.stop()

	assertExactlyOnce(t, c, suiteJobs, when)
	for i := range suiteJobs {
		if c.ok(i) {
			t.Errorf("%s: job %d settled true, but every write attempt was refused: "+
				"a false success is indistinguishable from a durable write to the caller", when, i)
			break
		}
	}
	if idx := indexDurable(h, h.Events()); idx.total != 0 {
		t.Errorf("%s: %d records are durable at a backend that refused every attempt; the harness is mis-reporting durability",
			when, idx.total)
	}
}

// exactlyOnceOnCancellation: cancelling while a write is genuinely in flight —
// not merely queued — settles every job exactly once. This is the path where a
// double settle is most likely: the interrupted attempt and the shutdown drain
// can each believe they own the job.
func exactlyOnceOnCancellation(t conformanceT, h Harness) {
	t.Helper()
	const when = "cancellation mid-flight"

	inFlight := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	h.SetFault(func(ctx context.Context, _ []sink.Record) error {
		once.Do(func() { close(inFlight) })
		select {
		case <-release:
		case <-ctx.Done():
		}
		return errFault
	})

	r := startWriter(t, h)
	defer r.stop()

	enq := testRecords(suiteJobs)
	c := newCommitCounter(suiteJobs)
	for i, rec := range enq {
		mustEnqueue(t, r.ctx, h.Writer, c, i, rec)
	}

	awaitInFlight(t, inFlight, h.SettleWithin, when)
	r.cancel()
	// Let the blocked attempt (and every attempt the drain makes after it) fail
	// promptly, so what is measured is the settling, not the drain deadline.
	close(release)
	r.wait()

	waitSettled(t, c, suiteJobs, h.SettleWithin, when)
	assertExactlyOnce(t, c, suiteJobs, when)
	assertCommittedTrueAreDurable(t, h, c, enq)
}

// exactlyOnceOnDrain: jobs cancelled while still queued are flushed by the
// shutdown, once each, and their records land. Distinct from the success case,
// which lets the Writer settle in its own time first: here the drain is the only
// thing that can settle them.
func exactlyOnceOnDrain(t conformanceT, h Harness) {
	t.Helper()
	const when = "shutdown drain"

	r := startWriter(t, h)
	defer r.stop()

	enq := testRecords(suiteJobs)
	c := newCommitCounter(suiteJobs)
	for i, rec := range enq {
		mustEnqueue(t, r.ctx, h.Writer, c, i, rec)
	}
	r.stop()

	if got := c.settled(); got != suiteJobs {
		t.Errorf("%s: %d commit callbacks fired for %d jobs by the time Start returned, want exactly one each "+
			"(fewer means the shutdown stranded a job, more means it settled one twice)", when, got, suiteJobs)
	}
	assertExactlyOnce(t, c, suiteJobs, when)
	for i := range suiteJobs {
		if !c.ok(i) {
			t.Errorf("%s: job %d settled as failed against a healthy backend: the drain must flush queued work, not abandon it",
				when, i)
			break
		}
	}
	assertCommittedTrueAreDurable(t, h, c, enq)
}

// noLostJobs: with one record the backend refuses and the rest it accepts, every
// job still settles exactly one way. The refused record must settle false — no
// backend may report a write it could not perform — and no job may settle true
// without its record being durable.
//
// Whether the refused record takes its batch-mates down with it is deliberately
// not asserted: isolating a poison row is a quality of implementation (ClickHouse
// does it), not an obligation of the contract.
func noLostJobs(t conformanceT, h Harness) {
	t.Helper()
	const when = "mixed outcomes"

	enq := testRecords(suiteJobs)
	poison := suiteJobs - 1
	poisonName := enq[poison].Name
	h.SetFault(func(_ context.Context, records []sink.Record) error {
		for _, rec := range records {
			if rec.Name == poisonName {
				return errFault
			}
		}
		return nil
	})

	r := startWriter(t, h)
	defer r.stop()

	c := newCommitCounter(suiteJobs)
	for i, rec := range enq {
		mustEnqueue(t, r.ctx, h.Writer, c, i, rec)
	}
	waitSettled(t, c, suiteJobs, h.SettleWithin, when)
	r.stop()

	assertExactlyOnce(t, c, suiteJobs, when)
	if c.ok(poison) {
		t.Errorf("%s: the poison job %d settled true, but every attempt carrying it was refused", when, poison)
	}
	idx := indexDurable(h, h.Events())
	if idx.contains(h, enq[poison]) {
		t.Errorf("%s: the poison record %s is durable at a backend that refused it; the harness is mis-reporting durability",
			when, describe(enq[poison]))
	}
	assertCommittedTrueAreDurable(t, h, c, enq)
}

// drainOrdering: shutdown flushes what is in flight and only then closes the
// backend, and nothing is left behind.
//
// The fault holds the first attempt open so there is unambiguously work in flight
// when cancellation arrives; it is released immediately afterwards so the drain
// can complete. Both halves matter: a Writer that closed its connection first
// would lose the flush, and one that never flushed would lose the jobs.
func drainOrdering(t conformanceT, h Harness) {
	t.Helper()
	const when = "drain ordering"

	inFlight := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	h.SetFault(func(ctx context.Context, _ []sink.Record) error {
		once.Do(func() { close(inFlight) })
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	r := startWriter(t, h)
	defer r.stop()

	enq := testRecords(suiteJobs)
	c := newCommitCounter(suiteJobs)
	for i, rec := range enq {
		mustEnqueue(t, r.ctx, h.Writer, c, i, rec)
	}

	awaitInFlight(t, inFlight, h.SettleWithin, when)
	r.cancel()
	close(release)
	r.wait()

	assertExactlyOnce(t, c, suiteJobs, when)
	for i := range suiteJobs {
		if !c.ok(i) {
			t.Errorf("%s: job %d settled false; a drain against a healthy backend must flush, not fail", when, i)
			break
		}
	}

	events := h.Events()
	closes, firstClose := countCloses(events)
	switch {
	case closes == 0:
		t.Errorf("%s: the backend was never closed; shutdown must release the connection it owns "+
			"(a harness with nothing to close must still record one EventClose)", when)
		return
	case closes > 1:
		t.Errorf("%s: the backend was closed %d times, want exactly 1", when, closes)
	}
	if err := events[firstClose].Err; err != nil {
		t.Errorf("%s: closing the backend failed: %v", when, err)
	}
	for i, ev := range events {
		if ev.Kind == EventWrite && i > firstClose {
			t.Errorf("%s: a write attempt (event %d) ran after the backend was closed (event %d): "+
				"shutdown must drain before it closes, never the other way round", when, i, firstClose)
			break
		}
	}

	idx := indexDurable(h, events)
	for i, rec := range enq {
		if !idx.contains(h, rec) {
			t.Errorf("%s: job %d %s was committed but never reached the backend: the drain stranded it",
				when, i, describe(rec))
			break
		}
	}
}

// enqueueBounded is Invariant 1: the hand-off is bounded, so a stalled backend
// costs the caller a bounded wait and an error, never an unbounded block on the
// hot path.
//
// The queue is saturated by not starting the Writer at all. Every alternative
// (blocking the backend and out-running its workers) depends on how much work the
// workers happen to have absorbed, which is unknowable from outside and would
// make the property flaky rather than false. The Writer is started at the end so
// the accepted jobs are shown to settle normally — the rejected ones must not.
func enqueueBounded(t conformanceT, h Harness) {
	t.Helper()
	const when = "saturated queue"

	capacity := h.QueueCapacity
	overflow, deadlined := capacity, capacity+1
	c := newCommitCounter(capacity + 2)

	// Fill it. Nothing is draining, so all of these must be accepted immediately.
	// A refusal here is not the contract failing: it means Harness.QueueCapacity
	// overstates the queue, and the message has to say so or the next reader will
	// go looking for the bug in the Writer.
	fillStart := time.Now()
	for i := range capacity {
		job := sink.Job{Record: testRecord(i), Commit: c.commitFor(i)}
		if err := h.Writer.Enqueue(context.Background(), job); err != nil {
			t.Fatalf("%s: Enqueue refused job %d of the %d Harness.QueueCapacity declares: %v; "+
				"declare the number of jobs the Writer accepts before it must wait for room",
				when, i, capacity, err)
		}
	}
	if elapsed := time.Since(fillStart); elapsed > h.EnqueueTimeout {
		t.Errorf("%s: filling the queue to its declared capacity (%d jobs) took %s, longer than one EnqueueTimeout (%s): "+
			"Harness.QueueCapacity looks larger than the queue really is", when, capacity, elapsed, h.EnqueueTimeout)
	}

	// One more than it can hold: the caller must get an error back, within the
	// declared timeout and a generous allowance for a loaded machine.
	slack := max(h.EnqueueTimeout, time.Second)
	start := time.Now()
	returned, err := enqueueWithin(context.Background(), h.Writer,
		sink.Job{Record: testRecord(overflow), Commit: c.commitFor(overflow)}, h.EnqueueTimeout+slack)
	elapsed := time.Since(start)
	switch {
	case !returned:
		t.Errorf("%s: Enqueue on a full queue had not returned after %s (its timeout is %s): "+
			"an unbounded hand-off blocks the caller's hot path indefinitely", when, h.EnqueueTimeout+slack, h.EnqueueTimeout)
	case err == nil:
		t.Errorf("%s: Enqueue accepted a job beyond the declared capacity of %d; either the queue is larger than "+
			"Harness.QueueCapacity says, or the job was dropped silently", when, capacity)
	default:
		t.Logf("%s: Enqueue refused the overflow job after %s: %v", when, elapsed, err)
	}

	// And it honours a caller's own deadline rather than always burning its full
	// timeout: the caller's backoff, not the Writer's, decides how long to wait.
	callerTimeout := h.EnqueueTimeout / 4
	ctx, cancel := context.WithTimeout(context.Background(), callerTimeout)
	defer cancel()
	start = time.Now()
	returned, err = enqueueWithin(ctx, h.Writer,
		sink.Job{Record: testRecord(deadlined), Commit: c.commitFor(deadlined)}, h.EnqueueTimeout+slack)
	elapsed = time.Since(start)
	switch {
	case !returned:
		t.Errorf("%s: Enqueue with a %s caller deadline had not returned after %s", when, callerTimeout, h.EnqueueTimeout+slack)
	case err == nil:
		t.Errorf("%s: Enqueue accepted a job on a full queue despite the caller's %s deadline", when, callerTimeout)
	case elapsed >= h.EnqueueTimeout:
		t.Errorf("%s: Enqueue took %s to honour a %s caller deadline — it waited out its own %s timeout instead",
			when, elapsed, callerTimeout, h.EnqueueTimeout)
	}

	// The Writer never ran, so the accepted jobs are still queued: start it, drain
	// it, and confirm the accepted ones settle and the refused ones stay refused.
	r := startWriter(t, h)
	defer r.stop()
	waitSettled(t, c, capacity, h.SettleWithin, when)
	r.stop()

	assertExactlyOnce(t, c, capacity, when)
	assertNeverSettled(t, c, []int{overflow, deadlined}, when)
}

// atLeastOnceIdempotency: replaying identical work leaves one logical record per
// identity.
//
// Both ways a replay happens are exercised. A lost acknowledgement makes the
// Writer re-write records that are already durable without ever knowing it; an
// upstream requeue hands it the same records again from scratch. Neither may
// produce a second logical record, which is only possible if what the backend
// stores the second time is byte-identical to the first — a re-stamped timestamp
// or a re-rendered payload is a second row that nothing will ever collapse.
func atLeastOnceIdempotency(t conformanceT, h Harness) {
	t.Helper()
	const when = "replayed batch"
	const rounds = 2

	// The first attempt persists and then reports failure: the Writer's own retry
	// is what produces the duplicate, exactly as a timeout after a successful
	// server-side write would.
	var attempts atomic.Int64
	h.SetFault(func(context.Context, []sink.Record) error {
		if attempts.Add(1) == 1 {
			return ErrLostAck
		}
		return nil
	})

	r := startWriter(t, h)
	defer r.stop()

	enq := testRecords(suiteJobs)
	c := newCommitCounter(rounds * suiteJobs)
	for round := range rounds {
		for i, rec := range enq {
			mustEnqueue(t, r.ctx, h.Writer, c, round*suiteJobs+i, rec)
		}
		waitSettled(t, c, (round+1)*suiteJobs, h.SettleWithin, when)
	}
	r.stop()

	assertExactlyOnce(t, c, rounds*suiteJobs, when)
	for i := range rounds * suiteJobs {
		if !c.ok(i) {
			t.Errorf("%s: job %d settled false; only the first attempt was faulted, and it was faulted as a lost "+
				"acknowledgement, so every job must end durably written", when, i)
			break
		}
	}

	idx := indexDurable(h, h.Events())
	if len(idx.byKey) != suiteJobs {
		t.Errorf("%s: the backend holds %d logical records for %d identities written %d times each; "+
			"a replay must collapse, not accumulate", when, len(idx.byKey), suiteJobs, rounds)
	}
	if idx.total <= suiteJobs {
		t.Logf("%s: the backend absorbed the replay without a physical duplicate (%d rows for %d identities)",
			when, idx.total, suiteJobs)
	}

	for i, rec := range enq {
		stored := idx.byKey[h.LogicalKey(rec)]
		if len(stored) == 0 {
			t.Errorf("%s: nothing durable under the key of job %d %s", when, i, describe(rec))
			break
		}
		if bad, ok := firstMismatch(stored, rec); ok {
			t.Errorf("%s: job %d stored %s, want %s — a re-write that differs from the record it repeats is a "+
				"second logical record the backend can never collapse", when, i, describe(bad), describe(rec))
			break
		}
		if h.Dedup != DedupMergeCollapse && len(stored) != 1 {
			t.Errorf("%s: %d physical copies under one key with Dedup=%s, want exactly 1",
				when, len(stored), h.Dedup)
			break
		}
	}
}

// concurrentEnqueueStorm: many producers handing off at once still settle every
// job exactly once. Its real value is under -race, where it exercises the
// Writer's queue, batching and commit paths against each other; without the
// detector it is only a throughput check.
func concurrentEnqueueStorm(t conformanceT, h Harness) {
	t.Helper()
	const when = "concurrent enqueue storm"
	const jobs = stormProducers * stormPerProducer

	r := startWriter(t, h)
	defer r.stop()

	enq := testRecords(jobs)
	c := newCommitCounter(jobs)
	var wg sync.WaitGroup
	for p := range stormProducers {
		wg.Go(func() {
			for i := range stormPerProducer {
				n := p*stormPerProducer + i
				mustEnqueue(t, r.ctx, h.Writer, c, n, enq[n])
			}
		})
	}
	wg.Wait()

	r.stop()

	if got := c.settled(); got != jobs {
		t.Errorf("%s: %d commit callbacks fired for %d jobs, want exactly one each", when, got, jobs)
	}
	assertExactlyOnce(t, c, jobs, when)
	assertCommittedTrueAreDurable(t, h, c, enq)
}

// awaitInFlight waits for the fault hook to report that a write attempt has begun.
//
// It tolerates the attempt never starting rather than deadlocking on it: a Writer
// batching on a long timer may legitimately make its first attempt only during the
// drain. The properties that call this stay valid either way — the ordering and
// exactly-once assertions then cover the drain flush instead of an interrupted
// one — so the timeout is logged, not failed.
func awaitInFlight(t conformanceT, inFlight <-chan struct{}, timeout time.Duration, when string) {
	t.Helper()
	select {
	case <-inFlight:
	case <-time.After(timeout):
		t.Logf("%s: no write attempt had begun after %s; the assertions below now cover the drain flush "+
			"rather than an interrupted attempt", when, timeout)
	}
}

// firstMismatch returns the first stored record that is not the record it should
// repeat.
func firstMismatch(stored []sink.Record, want sink.Record) (sink.Record, bool) {
	for _, got := range stored {
		if !recordsEqual(got, want) {
			return got, true
		}
	}
	return sink.Record{}, false
}
