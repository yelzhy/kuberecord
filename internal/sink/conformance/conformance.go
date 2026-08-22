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

// Package conformance is the executable form of the sink contract: the properties
// every backend must uphold, written against internal/sink alone so that a backend
// passes or fails them on its own merits.
//
// sink.Writer is the mandatory half and every property in writer_suite.go applies
// to every backend. sink.StateReader, sink.ScopeEventWriter and sink.Prober are
// optional and duck-typed by the SinkManager, so the suite duck-types them too:
// it runs each half's properties when the Writer satisfies the interface and says
// out loud, naming the interface, when it does not (see optional.go).
//
// It exists because those properties were, until now, provable only for
// ClickHouse. With one backend that is merely redundant; with three it is
// dangerous. Each new backend would re-derive "commit fires exactly once" from
// the prose in sink.go and each would get some subtlety wrong — most likely on
// the paths nobody writes a test for first (a retry that settles a job twice, a
// drain that strands one). A double commit corrupts the pipeline's version-gated
// hashCache silently: the audit trail simply stops recording an object's changes,
// with nothing in the logs to say so. That failure mode is why this package is a
// gate (D11) rather than a convenience.
//
// A backend adopts it from its own test package:
//
//	func TestWriterConformance(t *testing.T) {
//	    conformance.RunWriterSuite(t, func(t *testing.T) conformance.Harness {
//	        return conformance.Harness{Writer: ..., Events: ..., ...}
//	    })
//	}
//
// The suite owns the Writer's lifecycle — it starts it, cancels it, and waits for
// Start to return — because half the properties are statements about shutdown.
// The Harness owns everything backend-specific: how the backend is observed, how
// a write is made to fail, and how the backend deduplicates a re-written record.
//
// It deliberately does not import internal/pipeline (which imports internal/sink,
// so the dependency can only run one way) and names no backend.
package conformance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// minEnqueueTimeout is the smallest Harness.EnqueueTimeout the suite will accept.
//
// The bounded-Enqueue property measures elapsed time against fractions of that
// timeout, so a harness declaring a very small one would be asserting against
// scheduler noise on a loaded CI machine and would flake rather than prove
// anything. A backend whose production timeout is smaller should still build its
// harness with a comfortable one: the property under test is "Enqueue respects
// whatever bound it was given", not the specific value.
const minEnqueueTimeout = 100 * time.Millisecond

// defaultSettleWithin is the fallback for Harness.SettleWithin.
const defaultSettleWithin = 10 * time.Second

// ErrLostAck is the fault a FaultFunc returns to simulate the one hazard a
// Writer cannot avoid and must therefore survive: the records reached durable
// storage and the acknowledgement of that fact did not.
//
// A harness receiving it must make the attempt's records durable *and* report the
// attempt to its Writer as failed, which is precisely what a timeout or a reset
// connection after a successful server-side write looks like from the client. The
// Writer's own retry then re-writes records that are already there, and the
// at-least-once idempotency property is what checks that the re-write is
// byte-identical, so the backend can still collapse it to one logical record.
//
// It is a sentinel rather than a Harness flag because the suite, not the backend,
// decides when the ack is lost, and because a harness that quietly ignored it
// would be visibly wrong: its durable set would come up short.
var ErrLostAck = errors.New("conformance: acknowledgement lost after the records were made durable")

// FaultFunc decides the outcome of one durable-write attempt. It is the suite's
// only lever on backend behaviour, and it is deliberately narrow: the suite
// cannot know what a write *is* for a given backend (an INSERT, a PUT, an
// append), only that one happened and what it carried.
//
// The harness calls it on the Writer's own goroutine, once per attempt, with that
// attempt's records and the context the attempt is running under, and honours the
// result:
//
//   - nil — the attempt succeeds and its records are durable;
//   - ErrLostAck — the records are durable and the attempt is reported failed;
//   - any other error — the attempt fails and nothing becomes durable.
//
// Blocking inside it is legitimate and is how the suite arranges for a write to
// be genuinely in flight when shutdown begins; the suite always releases such a
// block itself, so a harness must not add a timeout of its own.
type FaultFunc func(ctx context.Context, records []sink.Record) error

// EventKind distinguishes the two things a backend does that the suite can
// observe from outside.
type EventKind string

const (
	// EventWrite is one durable-write attempt, successful or not.
	EventWrite EventKind = "Write"
	// EventClose is the teardown of the backend connection (or its equivalent —
	// the client's Close, the last object's finalisation). Exactly one is expected
	// per Writer lifetime, and every EventWrite must precede it: that ordering is
	// the whole of the drain-ordering property.
	EventClose EventKind = "Close"
)

// Event is one entry in the harness's ordered observation log. The log, rather
// than a "what is durable now?" snapshot, is the primitive the suite asks for
// because two of the properties are about *order* — a write that lands after the
// connection closed, or a job stranded because it never landed at all, are both
// invisible to a snapshot taken at the end.
type Event struct {
	// Kind is EventWrite or EventClose.
	Kind EventKind
	// Records are the records this write attempt carried, as the backend stored
	// (or would have stored) them — not as they were enqueued. A backend that
	// transforms a record on the way in must report the transformed form here, or
	// the idempotency property is checking the wrong bytes. Empty on EventClose.
	Records []sink.Record
	// Err is the outcome the backend reported for this attempt: nil for a
	// success, otherwise whatever the FaultFunc returned. On EventClose it is the
	// teardown error, which must be nil for a clean shutdown.
	Err error
}

// Durable reports whether this attempt's records reached durable storage.
//
// It is the suite that draws the line, not the harness, because ErrLostAck is
// precisely the case where "the attempt failed" and "the records are durable" are
// both true — the distinction the whole at-least-once design turns on.
func (e Event) Durable() bool {
	return e.Kind == EventWrite && (e.Err == nil || errors.Is(e.Err, ErrLostAck))
}

// DedupMode is how a backend reacts to being handed the same record twice.
//
// The suite needs it because "one logical record per identity" is a single
// promise kept three structurally different ways, and asserting the wrong one
// would either pass a broken backend or fail a correct one. It is declared by the
// backend rather than inferred, since no observation of a durable set can tell
// "the second write was rejected" apart from "the second write overwrote the
// first".
type DedupMode string

const (
	// DedupMergeCollapse means duplicates are stored and folded together later, on
	// the backend's own schedule — ClickHouse's ReplacingMergeTree, keyed on its
	// ORDER BY tuple. The collapse is only lossless if the duplicates are
	// byte-identical, so that is what the suite checks; a physical duplicate is
	// expected and is not a failure.
	DedupMergeCollapse DedupMode = "MergeCollapse"
	// DedupUniqueConstraint means the backend refuses the duplicate outright (a
	// primary key, a conditional put). At most one physical copy per key may
	// survive.
	DedupUniqueConstraint DedupMode = "UniqueConstraint"
	// DedupObjectOverwrite means the second write replaces the first in place (an
	// object store keyed by a deterministic object name). Exactly one physical
	// copy per key survives, and it must equal the record that was written.
	DedupObjectOverwrite DedupMode = "ObjectOverwrite"
)

// Harness is everything the suite needs from a backend that the sink.Writer
// interface itself cannot express: how to watch the backend, how to break it, and
// how it deduplicates.
//
// It is a struct of fields rather than an interface so the optional halves of the
// contract (StateReader, ScopeEventWriter, Prober) could be added as further
// fields without invalidating a backend harness that predates them — the same
// reason the sink contract splits its optional halves into separate interfaces
// instead of growing Writer. Those fields now exist, and a backend implementing
// none of the optional halves still leaves every one of them nil.
//
// A fresh Harness is built per property, so no property can inherit another's
// backend state, and the Writer it carries must not have been started: the suite
// starts it, because the drain properties are assertions about what Start does on
// its way out.
type Harness struct {
	// Writer is the live, *unstarted* Writer under test.
	Writer sink.Writer

	// Events returns an ordered snapshot of everything observed at the backend so
	// far. It is called concurrently with the Writer's own goroutines, so it must
	// return a copy under whatever lock guards the log — returning the live slice
	// is a data race the suite will find under -race, in the harness rather than
	// in the backend.
	Events func() []Event

	// SetFault installs the fault the harness consults on every subsequent write
	// attempt; nil clears it. The suite only ever calls it before Start, so a
	// harness needs no more synchronisation than a plain field assignment, and the
	// fault it installs is what the whole run sees.
	SetFault func(FaultFunc)

	// LogicalKey renders the key this backend deduplicates on: the ClickHouse
	// ORDER BY tuple, an S3 object key, a unique constraint's columns. Two records
	// with the same key are one logical record; two with different keys are two,
	// however similar they look.
	//
	// This is emphatically *not* the object identity key of Invariant 7, and must
	// not be confused with it: identity is version-agnostic and spans an object's
	// whole history, whereas this key distinguishes one recorded event from the
	// next (ClickHouse's includes the timestamp). It lives in each backend's test
	// harness because it describes that backend's physical storage, which nothing
	// outside the backend is entitled to know.
	LogicalKey func(sink.Record) string

	// Dedup declares which of the three shapes above this backend implements.
	Dedup DedupMode

	// QueueCapacity is how many jobs the Writer accepts before Enqueue must start
	// waiting for room, measured with the Writer *not started* so nothing is
	// draining. It is the only way the suite can saturate the hand-off
	// deterministically; every alternative depends on how fast the workers happen
	// to drain and would flake instead of proving anything.
	//
	// Declare the true buffer size. Understating it makes the bounded-Enqueue
	// property fail (the overflow job is accepted); overstating it makes the
	// property fail earlier, on a job that should have been accepted.
	QueueCapacity int

	// EnqueueTimeout is the bound this Writer was configured to place on a
	// blocked Enqueue. Must be at least minEnqueueTimeout so the property is
	// measuring the Writer rather than the scheduler.
	EnqueueTimeout time.Duration

	// SettleWithin is how long the suite waits for every enqueued job to settle
	// against a responsive backend, including a backend that is failing every
	// attempt — so it must exceed the Writer's whole retry budget, not just one
	// attempt. Zero means defaultSettleWithin.
	SettleWithin time.Duration

	// The fields below serve the *optional* halves of the sink contract, and each
	// is required only when this backend's Writer implements the half it belongs
	// to (see optional.go). A backend that implements none of them — the
	// Writer-only archive tier of D12 — leaves all three nil and is skipped
	// loudly rather than silently certified.

	// ScopeWrites returns, in order, the watch-scope transitions the backend has
	// durably recorded so far. Required when the Writer implements
	// sink.ScopeEventWriter.
	//
	// It is an ordered log rather than a set because the epoch design turns on
	// order: a Started that overtook its own Stopped inverts the epoch, and a set
	// cannot see that. Like Events it is read while the backend's own goroutines
	// are still appending, so it must return a copy.
	ScopeWrites func() []sink.ScopeEvent

	// SetReadFault breaks the backend's next read part-way through; nil clears it.
	// Required when the Writer implements sink.StateReader.
	//
	// It is the only lever that can produce the failure the read contract singles
	// out — a stream that delivered some rows and then died — and no observation
	// of a healthy backend can produce it. The suite installs it, reads, and
	// clears it again.
	SetReadFault func(*ReadFault)

	// SetProbeOutcome arranges what this backend's next Probe will encounter.
	// Required when the Writer implements sink.Prober.
	//
	// The outcome is declared rather than injected as an error, because "your
	// schema is not the one the operator writes" is a backend-specific state (a
	// drifted column type, a missing table, an object-format version) that the
	// suite cannot construct and must not have to know about. What the suite does
	// know is how each state must be *classified*.
	SetProbeOutcome func(ProbeOutcome)
}

// withDefaults fills in the fields a harness may leave zero.
func (h Harness) withDefaults() Harness {
	if h.SettleWithin <= 0 {
		h.SettleWithin = defaultSettleWithin
	}
	return h
}

// validate fails the property immediately, naming the field, when a harness is
// incomplete. It is a Fatalf rather than a skip on purpose: a half-filled harness
// means the backend is not under test, and Phase 5 exists to prevent exactly that
// from reading as a pass.
func (h Harness) validate(t conformanceT) {
	t.Helper()
	switch {
	case h.Writer == nil:
		t.Fatalf("conformance: Harness.Writer is nil; the suite needs a live, unstarted Writer")
	case h.Events == nil:
		t.Fatalf("conformance: Harness.Events is nil; the suite cannot observe what the backend received")
	case h.SetFault == nil:
		t.Fatalf("conformance: Harness.SetFault is nil; the suite cannot force a write failure")
	case h.LogicalKey == nil:
		t.Fatalf("conformance: Harness.LogicalKey is nil; the suite cannot tell one logical record from two")
	}
	switch h.Dedup {
	case DedupMergeCollapse, DedupUniqueConstraint, DedupObjectOverwrite:
	default:
		t.Fatalf("conformance: Harness.Dedup is %q; want one of %q, %q, %q",
			h.Dedup, DedupMergeCollapse, DedupUniqueConstraint, DedupObjectOverwrite)
	}
	if h.QueueCapacity <= 0 {
		t.Fatalf("conformance: Harness.QueueCapacity is %d; declare how many jobs Enqueue accepts before it blocks",
			h.QueueCapacity)
	}
	if h.EnqueueTimeout < minEnqueueTimeout {
		t.Fatalf("conformance: Harness.EnqueueTimeout is %s; want at least %s so the bound is measurable",
			h.EnqueueTimeout, minEnqueueTimeout)
	}
}

// conformanceT is the slice of *testing.T the properties use.
//
// The indirection has one purpose, and it is the one Phase 5 insists on: it lets
// the suite be run against a recorder instead of a real test, so this package can
// prove that each property *fails* when handed a Writer that violates it (see
// nonvacuity_test.go). A suite that asserts nothing passes everything, and
// without this seam there would be no way to tell the two apart from the inside.
//
// *testing.T satisfies it as-is; Fatalf must abandon the property (runtime.Goexit)
// in any other implementation, exactly as testing does.
type conformanceT interface {
	Helper()
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// property is one named contract obligation and the code that checks it. The
// tables (writerProperties and the optional halves' own) are addressable by name
// so the non-vacuity tests can run a single property against a deliberately
// broken Writer and assert it objects.
type property struct {
	name string
	// needs, when non-nil, checks the harness levers this particular property
	// requires beyond the mandatory ones — the optional halves each need a lever
	// the Writer contract has no use for. It is per property rather than per
	// suite so a harness is only ever failed for a field the property it is
	// running actually reaches for.
	needs func(t conformanceT, h Harness)
	run   func(t conformanceT, h Harness)
}

// runProperty is the single entry point both RunWriterSuite and the non-vacuity
// tests go through, so a property is validated and defaulted identically whether
// a real backend or a broken fixture is on the other end.
func runProperty(t conformanceT, p property, h Harness) {
	t.Helper()
	h = h.withDefaults()
	h.validate(t)
	if p.needs != nil {
		p.needs(t, h)
	}
	p.run(t, h)
}

// RunWriterSuite asserts every mandatory sink.Writer property against the backend
// newWriter builds, one separately named subtest per property, and then runs the
// optional halves of the contract the backend turned out to implement.
//
// The optional halves are reached from here rather than left to each backend to
// remember, because "we never wired that one up" and "we do not implement that
// one" are indistinguishable from the outside — and the first is how a backend
// quietly ships an unchecked StateReader. Capability detection mirrors what
// SinkManager itself does (a type assertion on the Writer), so the suite runs
// exactly the halves the runtime will use, and logs the ones it does not.
//
// newWriter is called once per property with that subtest's *testing.T — never
// once for the whole suite — because several properties end by shutting the
// Writer down, and a Writer is not restartable. Registering the backend's teardown
// on the *testing.T it is handed is the harness's business; the suite's own
// shutdown is not a substitute for it.
func RunWriterSuite(t *testing.T, newWriter func(t *testing.T) Harness) {
	t.Helper()
	if newWriter == nil {
		t.Fatalf("conformance: RunWriterSuite needs a non-nil harness constructor")
	}
	for _, p := range writerProperties() {
		t.Run(p.name, func(t *testing.T) {
			runProperty(t, p, newWriter(t))
		})
	}
	RunStateReaderSuite(t, newWriter)
	RunScopeEventWriterSuite(t, newWriter)
	RunProberSuite(t, newWriter)
}
