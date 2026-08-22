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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// fakeWriter is a minimal but genuinely compliant sink.Writer over an in-memory
// backend, with switches that break one contract obligation each.
//
// It carries the whole weight of this package's own credibility. Run against the
// compliant configuration, it proves the suite can be passed at all — without
// that, a suite that failed everything would look identical to a rigorous one.
// Run against each broken configuration, it proves the corresponding property
// actually objects. It is also the shortest worked example of what a Writer has
// to do, which is what a new backend will read first.
//
// Its shape mirrors the one implementation that exists: a bounded hand-off queue,
// a small pool of batching workers, and a shutdown that stops accepting, waits for
// in-flight senders, drains, and only then closes the backend.
type fakeWriter struct {
	opts fakeOpts

	jobs           chan sink.Job
	workers        int
	batchMax       int
	batchWait      time.Duration
	enqueueTimeout time.Duration
	retries        int
	retryPause     time.Duration

	// mu guards closing; inflight tracks Enqueue calls that passed the closing
	// check and may therefore still send, so close(jobs) can never race a send.
	mu       sync.Mutex
	closing  bool
	inflight sync.WaitGroup
	// hardStop is closed at the very start of shutdown, before anything waits on
	// anything: it releases a blocked Enqueue (so the drain cannot deadlock behind
	// one) and, for the drop-on-drain fixture, tells the workers to abandon
	// whatever they are holding.
	hardStop chan struct{}

	faultMu sync.Mutex
	fault   FaultFunc

	evMu   sync.Mutex
	events []Event
	// durable is everything that reached storage, in write order. It is what the
	// read half answers from, so this fake's reader and writer cannot disagree
	// about what was written — the same relationship a real backend has between
	// its table and its queries.
	durable []sink.Record

	// scopeEvents is the scope-log hand-off, deliberately its own queue drained by
	// its own single worker: a scope transition must not wait behind a backlog of
	// object rows, and one worker is what keeps a scope's Started and Stopped from
	// inverting.
	scopeEvents chan sink.ScopeEvent
	scopeMu     sync.Mutex
	scopeRows   []sink.ScopeEvent

	readMu    sync.Mutex
	readFault *ReadFault

	probeMu sync.Mutex
	probe   ProbeOutcome

	// stamp makes each stored record unique for the non-idempotent fixture.
	stamp atomic.Int64
}

// fakeOpts selects which obligation this writer violates. The zero value is the
// compliant writer.
type fakeOpts struct {
	// doubleCommit settles every job twice — the corruption D11 exists to catch.
	doubleCommit bool
	// dropOnDrain abandons queued and buffered work when shutdown begins, closing
	// the backend without flushing and without settling those jobs.
	dropOnDrain bool
	// lyingCommit reports every job as durably written, whatever the backend said.
	lyingCommit bool
	// unboundedEnqueue ignores the enqueue timeout and blocks on a full queue
	// until shutdown.
	unboundedEnqueue bool
	// nonIdempotent re-stamps each record as it is stored, so a re-written record
	// is a second logical record rather than a collapsible duplicate.
	nonIdempotent bool

	// The switches below break one obligation each of the optional halves.

	// collapseIncarnations folds an identity's incarnations into one last-known
	// state, destroying the only evidence that a prior was left unclosed.
	collapseIncarnations bool
	// keepTombstones reports incarnations whose own latest event is a deletion, so
	// a warm-up resurrects objects that are gone.
	keepTombstones bool
	// shortRead returns the rows a broken read managed to deliver, with a nil
	// error, so a truncated scan reads as a complete one.
	shortRead bool
	// ignoreAsOf answers the epoch probe from the whole scope log, letting the
	// caller's own epoch decide a question that is only about previous ones.
	ignoreAsOf bool
	// keepStoppedScopesActive enumerates every scope ever started, so boot
	// reconciliation closes epochs that are already closed.
	keepStoppedScopesActive bool
	// coalesceScopeEvents records only the first transition it sees per scope, so
	// a Stopped that followed a Started is lost.
	coalesceScopeEvents bool
	// swallowScopeRejection tells the caller a refused transition was accepted, so
	// the caller drops it from its retry queue and the epoch is lost.
	swallowScopeRejection bool
	// unclassifiedSchemaError reports a schema mismatch as a bare error, so the
	// manager can only label it Unreachable.
	unclassifiedSchemaError bool
	// schemaErrorForEverything classifies an unreachable backend as a schema
	// failure, sending an operator to migrate a schema that is already correct.
	schemaErrorForEverything bool
	// probeAlwaysFails refuses even a healthy backend, so the sink never goes
	// Ready.
	probeAlwaysFails bool
}

// The tuning the fixtures run with: small enough that several batches and both
// workers are exercised by suiteJobs, fast enough that a whole property settles
// in well under a second against a healthy backend.
const (
	fakeQueueCapacity  = 64
	fakeWorkers        = 2
	fakeBatchMax       = 8
	fakeBatchWait      = 20 * time.Millisecond
	fakeEnqueueTimeout = time.Second
	fakeRetries        = 2
	fakeRetryPause     = 5 * time.Millisecond
	fakeSettleWithin   = 3 * time.Second
	// fakeScopeQueueCapacity is generous on purpose: the scope path's obligations
	// are about what is recorded, never about back-pressure, so a hand-off that
	// could fill would only add flakiness.
	fakeScopeQueueCapacity = 256
)

var errFakeQueueFull = errors.New("fake: write queue still full")
var errFakeShuttingDown = errors.New("fake: shutting down, refusing new write")
var errFakeSchemaDrift = errors.New("fake: stored objects do not carry the fields the operator writes")
var errFakeUnreachable = errors.New("fake: backend did not answer")

func newFakeWriter(opts fakeOpts) *fakeWriter {
	return &fakeWriter{
		opts:           opts,
		jobs:           make(chan sink.Job, fakeQueueCapacity),
		workers:        fakeWorkers,
		batchMax:       fakeBatchMax,
		batchWait:      fakeBatchWait,
		enqueueTimeout: fakeEnqueueTimeout,
		retries:        fakeRetries,
		retryPause:     fakeRetryPause,
		hardStop:       make(chan struct{}),
		scopeEvents:    make(chan sink.ScopeEvent, fakeScopeQueueCapacity),
	}
}

// newFakeHarness wires a fakeWriter up as a Harness, optional halves included.
func newFakeHarness(opts fakeOpts) Harness {
	w := newFakeWriter(opts)
	h := fakeHarnessOver(w, w)
	h.ScopeWrites = w.scopeSnapshot
	h.SetReadFault = w.setReadFault
	h.SetProbeOutcome = w.setProbeOutcome
	return h
}

// newWriterOnlyHarness is the same backend seen through a facade that implements
// none of the optional halves, with every optional lever left nil — the shape of
// D12's archive tier, and what the skip path has to cope with.
func newWriterOnlyHarness(w *fakeWriter) Harness {
	return fakeHarnessOver(w, writerOnly{w})
}

// fakeHarnessOver is the mandatory half of a harness over w, with the Writer the
// suite sees supplied separately: the two harnesses above differ only in how much
// of the backend they let the suite discover.
func fakeHarnessOver(w *fakeWriter, facade sink.Writer) Harness {
	return Harness{
		Writer:         facade,
		Events:         w.snapshot,
		SetFault:       w.setFault,
		LogicalKey:     fakeLogicalKey,
		Dedup:          DedupMergeCollapse,
		QueueCapacity:  fakeQueueCapacity,
		EnqueueTimeout: fakeEnqueueTimeout,
		SettleWithin:   fakeSettleWithin,
	}
}

// writerOnly exposes a fakeWriter as nothing but a sink.Writer.
//
// The methods are forwarded explicitly rather than by embedding, because
// embedding would promote the optional halves' methods too and the type would
// satisfy exactly the interfaces it exists to not satisfy.
type writerOnly struct{ w *fakeWriter }

func (o writerOnly) Start(ctx context.Context) error               { return o.w.Start(ctx) }
func (o writerOnly) Enqueue(ctx context.Context, j sink.Job) error { return o.w.Enqueue(ctx, j) }

var _ sink.Writer = writerOnly{}

// fakeLogicalKey is this backend's dedup key, modelled on the one real backend's:
// the identity plus the instant, so two events for the same object are two
// records and a re-write of one event is the same record.
func fakeLogicalKey(rec sink.Record) string {
	return strings.Join([]string{
		rec.ClusterID, rec.APIGroup, rec.Kind, rec.Namespace, rec.Name,
		rec.Timestamp.UTC().Format(time.RFC3339Nano),
	}, "|")
}

func (w *fakeWriter) setFault(f FaultFunc) {
	w.faultMu.Lock()
	defer w.faultMu.Unlock()
	w.fault = f
}

// snapshot returns a copy of the observation log; the suite reads it while the
// workers are still appending to it.
func (w *fakeWriter) snapshot() []Event {
	w.evMu.Lock()
	defer w.evMu.Unlock()
	out := make([]Event, len(w.events))
	copy(out, w.events)
	return out
}

// record appends to the observation log and, for an attempt whose records
// reached storage, to the durable set the read half answers from. Event.Durable
// draws that line rather than the fake, so a lost acknowledgement stores its
// records here exactly as it would at a real backend.
func (w *fakeWriter) record(ev Event) {
	w.evMu.Lock()
	defer w.evMu.Unlock()
	w.events = append(w.events, ev)
	if ev.Durable() {
		w.durable = append(w.durable, ev.Records...)
	}
}

// Enqueue is the bounded hand-off: it never blocks past its timeout, the caller's
// deadline, or shutdown — except for the fixture built to do exactly that.
func (w *fakeWriter) Enqueue(ctx context.Context, job sink.Job) error {
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return errFakeShuttingDown
	}
	w.inflight.Add(1)
	w.mu.Unlock()
	defer w.inflight.Done()

	// A nil channel never fires, which is how the unbounded fixture ignores its
	// own timeout without also ignoring shutdown (that would deadlock the drain
	// rather than fail the property).
	var full <-chan time.Time
	if !w.opts.unboundedEnqueue {
		timer := time.NewTimer(w.enqueueTimeout)
		defer timer.Stop()
		full = timer.C
	}

	select {
	case w.jobs <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-full:
		return errFakeQueueFull
	case <-w.hardStop:
		return errFakeShuttingDown
	}
}

// Start runs the workers until ctx is cancelled, then shuts down in the order the
// contract requires: stop accepting, wait for senders, close the queue, let the
// workers drain it, and only then close the backend.
func (w *fakeWriter) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	for range w.workers {
		wg.Go(func() { w.worker(ctx) })
	}
	// One scope worker, never more: two would let a scope's Started and Stopped
	// reach storage in either order, and an inverted epoch is worse than a missing
	// one because it reads as true.
	wg.Go(w.scopeWorker)

	<-ctx.Done()

	w.mu.Lock()
	w.closing = true
	w.mu.Unlock()
	close(w.hardStop)
	w.inflight.Wait()
	close(w.jobs)
	close(w.scopeEvents)
	wg.Wait()

	w.record(Event{Kind: EventClose})
	return nil
}

// EnqueueScopeEvent is the scope-log hand-off, guarded exactly like Enqueue: the
// closing check and the inflight registration together are what let Start close
// the channel without ever racing a send.
func (w *fakeWriter) EnqueueScopeEvent(ctx context.Context, event sink.ScopeEvent) error {
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		if w.opts.swallowScopeRejection {
			// The fixture: drop it and tell the caller it landed.
			return nil
		}
		return errFakeShuttingDown
	}
	w.inflight.Add(1)
	w.mu.Unlock()
	defer w.inflight.Done()

	timer := time.NewTimer(w.enqueueTimeout)
	defer timer.Stop()

	select {
	case w.scopeEvents <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errFakeQueueFull
	case <-w.hardStop:
		return errFakeShuttingDown
	}
}

// scopeWorker drains the scope queue in order and stores what it drains,
// including whatever is still queued when the channel closes — a transition
// happens once and cannot be re-derived, so the drain must not abandon one.
func (w *fakeWriter) scopeWorker() {
	for event := range w.scopeEvents {
		w.storeScopeEvent(event)
	}
}

func (w *fakeWriter) storeScopeEvent(event sink.ScopeEvent) {
	w.scopeMu.Lock()
	defer w.scopeMu.Unlock()
	if w.opts.coalesceScopeEvents {
		for _, stored := range w.scopeRows {
			if stored.Scope == event.Scope {
				return
			}
		}
	}
	w.scopeRows = append(w.scopeRows, event)
}

// scopeSnapshot implements Harness.ScopeWrites.
func (w *fakeWriter) scopeSnapshot() []sink.ScopeEvent {
	w.scopeMu.Lock()
	defer w.scopeMu.Unlock()
	return slices.Clone(w.scopeRows)
}

func (w *fakeWriter) setReadFault(f *ReadFault) {
	w.readMu.Lock()
	defer w.readMu.Unlock()
	w.readFault = f
}

func (w *fakeWriter) currentReadFault() *ReadFault {
	w.readMu.Lock()
	defer w.readMu.Unlock()
	return w.readFault
}

func (w *fakeWriter) setProbeOutcome(outcome ProbeOutcome) {
	w.probeMu.Lock()
	defer w.probeMu.Unlock()
	w.probe = outcome
}

func (w *fakeWriter) currentProbeOutcome() ProbeOutcome {
	w.probeMu.Lock()
	defer w.probeMu.Unlock()
	if w.probe == "" {
		return ProbeHealthy
	}
	return w.probe
}

// LastKnownStates answers from what the write path actually stored, per
// incarnation, excluding those whose own latest event is a deletion.
//
// A broken read is reported as an error and nothing else: the contract's demand
// is that a truncated scan is never mistaken for a complete one, and the surest
// way to honour it is to return no rows at all alongside the failure.
func (w *fakeWriter) LastKnownStates(_ context.Context, filter sink.ScopeFilter) ([]sink.KnownState, error) {
	states := w.lastKnownStates(filter)
	fault := w.currentReadFault()
	if fault == nil {
		return states, nil
	}
	if w.opts.shortRead {
		// The fixture: hand back the rows the read managed to deliver, as a success.
		return states[:min(fault.AfterRows, len(states))], nil
	}
	return nil, fault.Err
}

func (w *fakeWriter) lastKnownStates(filter sink.ScopeFilter) []sink.KnownState {
	w.evMu.Lock()
	stored := slices.Clone(w.durable)
	w.evMu.Unlock()

	latest := map[string]sink.Record{}
	for _, rec := range stored {
		if rec.ClusterID != filter.ClusterID || rec.APIGroup != filter.APIGroup || rec.Kind != filter.Kind {
			continue
		}
		// The *record query* reading of Namespace: empty matches every namespace.
		if filter.Namespace != "" && rec.Namespace != filter.Namespace {
			continue
		}
		key := rec.Namespace + "/" + rec.Name
		if !w.opts.collapseIncarnations {
			key += "#" + rec.UID
		}
		if prior, seen := latest[key]; seen && !prior.Timestamp.Before(rec.Timestamp) {
			continue
		}
		latest[key] = rec
	}

	states := make([]sink.KnownState, 0, len(latest))
	for _, rec := range latest {
		if rec.EventType == eventDeleted && !w.opts.keepTombstones {
			continue
		}
		states = append(states, sink.KnownState{
			Namespace: rec.Namespace, Name: rec.Name, UID: rec.UID,
			SHA256: rec.SHA256, APIVersion: rec.APIVersion, TS: rec.Timestamp,
		})
	}
	slices.SortFunc(states, func(a, b sink.KnownState) int {
		return strings.Compare(a.Namespace+"/"+a.Name+"#"+a.UID, b.Namespace+"/"+b.Name+"#"+b.UID)
	})
	return states
}

// ScopeWasActive reports whether this scope's most recent action strictly before
// asOf is Started. The scope is matched exactly, empty namespace included: it is
// a scope identity here, not a record filter.
func (w *fakeWriter) ScopeWasActive(_ context.Context, filter sink.ScopeFilter, asOf time.Time) (bool, error) {
	if asOf.IsZero() {
		asOf = time.Now().UTC()
	}
	var latest *sink.ScopeEvent
	for _, row := range w.scopeSnapshot() {
		if row.Scope != filter {
			continue
		}
		if !row.TS.Before(asOf) && !w.opts.ignoreAsOf {
			continue
		}
		if latest == nil || latest.TS.Before(row.TS) {
			latest = &row
		}
	}
	return latest != nil && latest.Action == sink.ScopeActionStarted, nil
}

// ActiveScopes enumerates the scopes whose most recent action is Started.
func (w *fakeWriter) ActiveScopes(_ context.Context, clusterID string) ([]sink.ScopeFilter, error) {
	latest := map[sink.ScopeFilter]sink.ScopeEvent{}
	everStarted := map[sink.ScopeFilter]bool{}
	for _, row := range w.scopeSnapshot() {
		if row.Scope.ClusterID != clusterID {
			continue
		}
		if row.Action == sink.ScopeActionStarted {
			everStarted[row.Scope] = true
		}
		if prior, seen := latest[row.Scope]; seen && !prior.TS.Before(row.TS) {
			continue
		}
		latest[row.Scope] = row
	}

	var open []sink.ScopeFilter
	for scope, row := range latest {
		if row.Action == sink.ScopeActionStarted || (w.opts.keepStoppedScopesActive && everStarted[scope]) {
			open = append(open, scope)
		}
	}
	return open, nil
}

// Probe classifies the outcome the harness arranged. The wrapping is the whole
// of the contract: a mismatch must satisfy errors.Is(err, sink.ErrSchemaInvalid),
// and nothing else may.
func (w *fakeWriter) Probe(context.Context) error {
	switch w.currentProbeOutcome() {
	case ProbeSchemaMismatch:
		if w.opts.unclassifiedSchemaError {
			return errFakeSchemaDrift
		}
		return fmt.Errorf("%w: %w", sink.ErrSchemaInvalid, errFakeSchemaDrift)
	case ProbeUnreachable:
		if w.opts.schemaErrorForEverything {
			return fmt.Errorf("%w: %w", sink.ErrSchemaInvalid, errFakeUnreachable)
		}
		return errFakeUnreachable
	default:
		if w.opts.probeAlwaysFails {
			return errFakeUnreachable
		}
		return nil
	}
}

// The fake is the package's worked example of a backend, so it claims every half
// of the contract explicitly rather than by accident.
var (
	_ sink.Writer           = (*fakeWriter)(nil)
	_ sink.StateReader      = (*fakeWriter)(nil)
	_ sink.ScopeEventWriter = (*fakeWriter)(nil)
	_ sink.Prober           = (*fakeWriter)(nil)
)

// worker accumulates jobs into a batch and flushes it on size, on the batch timer,
// or on the final (closed) receive — the drain flush the contract turns on.
func (w *fakeWriter) worker(ctx context.Context) {
	batch := make([]sink.Job, 0, w.batchMax)
	var timer *time.Timer
	var timerC <-chan time.Time
	disarm := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
			timerC = nil
		}
	}
	defer disarm()

	// Non-nil only for the fixture that abandons work at shutdown; nil otherwise,
	// so the compliant writer's select cannot take this branch at all.
	var abandon <-chan struct{}
	if w.opts.dropOnDrain {
		abandon = w.hardStop
	}

	for {
		// The fixture has to abandon *deterministically*. Left to the select alone,
		// a shutdown with jobs still buffered is a coin flip between the abandon
		// case and the receive case every iteration, so under load this writer can
		// drain everything and behave exactly like a compliant one — and the drain
		// properties' non-vacuity proof then reports, at random, that they assert
		// nothing. An intermittent proof of rigour is worse than none, because the
		// failure it produces is the one people learn to re-run away.
		//
		// The case below stays as well: it is what unblocks a worker that is idle
		// on an empty queue when shutdown begins, which this check cannot see.
		if abandon != nil {
			select {
			case <-abandon:
				return
			default:
			}
		}
		select {
		case <-abandon:
			return
		case job, ok := <-w.jobs:
			if !ok {
				if len(batch) > 0 {
					w.flush(ctx, batch)
				}
				return
			}
			batch = append(batch, job)
			if len(batch) == 1 {
				timer = time.NewTimer(w.batchWait)
				timerC = timer.C
			}
			if len(batch) >= w.batchMax {
				w.flush(ctx, batch)
				batch = batch[:0]
				disarm()
			}
		case <-timerC:
			timer = nil
			timerC = nil
			w.flush(ctx, batch)
			batch = batch[:0]
		}
	}
}

// flush attempts one batch, retries it a bounded number of times, and settles
// every job in it exactly once — the whole contract, in one place, which is what
// makes settling it twice a one-line fixture rather than a redesign.
func (w *fakeWriter) flush(ctx context.Context, batch []sink.Job) {
	records := make([]sink.Record, len(batch))
	for i, job := range batch {
		records[i] = w.stored(job.Record)
	}

	var err error
	for attempt := range w.retries + 1 {
		if attempt > 0 {
			time.Sleep(w.retryPause)
		}
		if err = w.attempt(ctx, records); err == nil {
			break
		}
	}

	ok := err == nil
	if w.opts.lyingCommit {
		ok = true
	}
	for _, job := range batch {
		job.Commit(ok)
		if w.opts.doubleCommit {
			job.Commit(ok)
		}
	}
}

// attempt consults the fault and logs what happened. ErrLostAck is recorded as
// the error it is; Event.Durable is what decides that its records still landed.
func (w *fakeWriter) attempt(ctx context.Context, records []sink.Record) error {
	w.faultMu.Lock()
	f := w.fault
	w.faultMu.Unlock()

	var err error
	if f != nil {
		err = f(ctx, records)
	}
	w.record(Event{Kind: EventWrite, Records: slices.Clone(records), Err: err})
	return err
}

// stored renders a record as this backend keeps it: unchanged, unless the
// non-idempotent fixture is re-stamping it so no two writes of the same record
// can ever collapse.
func (w *fakeWriter) stored(rec sink.Record) sink.Record {
	if w.opts.nonIdempotent {
		rec.Timestamp = rec.Timestamp.Add(time.Duration(w.stamp.Add(1)) * time.Nanosecond)
	}
	return rec
}
