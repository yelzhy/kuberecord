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

// Where the "one event per transition, never one per rule" rule is enforced —
// read this before adding an assertion here.
//
// The *decision* that a transition happened at all is not the sink's. A scope
// gains its Started row on the edge where it acquires its first interested rule
// and its Stopped row where it loses its last, and the component that computes
// those edges — collapsing however many rules contributed into a single edge per
// (sink, scope) — is watch.ScopeEpochRecorder (ScopeStarted / ScopeStopped), whose
// TestScopeEpochRecorderTransitionSemantics is where "never per rule" is proven.
// sink.ScopeEventWriter is downstream of that decision: it is handed transitions
// and never sees a rule.
//
// So the obligation this file can and does enforce is the sink-boundary half of
// the same guarantee, and it is exactly what the recorder's correctness rests on:
// each accepted transition is recorded once — never coalesced away, never
// amplified — with its own action, scope, version, rule reference and instant
// intact, and in the order it was handed over. A backend that folded two
// transitions of one scope into one would make a correct recorder look wrong; a
// backend that duplicated one would invent an epoch edge that never happened.

package conformance

import (
	"context"
	"time"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// The subtest names, as constants because the non-vacuity tests address
// individual properties by name.
const (
	propScopeEventsRecordedOnce = "EpochTransitionsRecordedExactlyOnce"
	propScopeRejectionSurfaced  = "RejectionIsSurfaced"
)

// scopeEventWriterProperties is the scope-log half of the contract as executable
// obligations.
func scopeEventWriterProperties() []property {
	return []property{
		{name: propScopeEventsRecordedOnce, needs: requireScopeWrites, run: epochTransitionsRecordedExactlyOnce},
		{name: propScopeRejectionSurfaced, needs: requireScopeWrites, run: rejectionIsSurfaced},
	}
}

// epochTransitionsRecordedExactlyOnce: what the caller handed over is what the
// backend recorded — one row per transition, each carrying its own fields, in
// order.
//
// The two transitions of one scope deliberately name *different* rules. A backend
// that keyed anything on the rule reference — deduplicating on it, or splitting
// one transition into one row per rule it has seen for that scope — fails here,
// which is the sink-boundary reading of "never per rule" (see the file header).
// The second scope is the all-namespaces one, so a backend that treated an empty
// namespace as a wildcard would collapse the two scopes into one epoch and lose
// half the rows.
func epochTransitionsRecordedExactlyOnce(t conformanceT, h Harness) {
	t.Helper()
	const when = "scope epoch log"

	watched, wide := scopeIn(suiteNamespace), scopeIn("")
	handed := []sink.ScopeEvent{
		{
			Action: sink.ScopeActionStarted, Scope: watched, APIVersion: suiteAPIVersion,
			RuleRef: scopeIdentityRuleV1, TS: readAt(1),
		},
		{
			Action: sink.ScopeActionStarted, Scope: wide, APIVersion: suiteAPIVersion,
			RuleRef: "clusterstreamrule//platform-baseline", TS: readAt(1),
		},
		{
			// The same scope as the first, closed by a different rule: the last
			// interest to go was not the first to arrive.
			Action: sink.ScopeActionStopped, Scope: watched, APIVersion: suiteAPIVersion,
			RuleRef: "streamrule/conformance/rule-two", TS: readAt(2),
		},
	}

	r := startWriter(t, h)
	defer r.stop()
	seedScopeHistory(t, r, h, scopeWriter(t, h), handed)

	// The drain must not re-emit what it already wrote, so the count is taken again
	// after shutdown rather than only before it.
	r.stop()
	recorded := h.ScopeWrites()

	if len(recorded) != len(handed) {
		t.Errorf("%s: the backend recorded %d transitions for %d handed over: %v; a transition happens once "+
			"and cannot be re-derived, so losing one is an audit hole and inventing one is a fabricated epoch",
			when, len(recorded), len(handed), describeScopes(recorded))
		return
	}
	for i, want := range handed {
		if !scopeEventsEqual(recorded[i], want) {
			t.Errorf("%s: transition %d was recorded as %s, want %s — every field is part of the epoch "+
				"record, and the instant especially: it must be the one the transition was observed at, "+
				"never the one the write happened to land at",
				when, i, describeScope(recorded[i]), describeScope(want))
		}
	}

	// Order within one scope is the property the epoch design cannot survive
	// losing: a Started that overtook its own Stopped inverts it.
	started, stopped := indexOfTransition(recorded, watched, sink.ScopeActionStarted),
		indexOfTransition(recorded, watched, sink.ScopeActionStopped)
	if started < 0 || stopped < 0 {
		t.Errorf("%s: the watched scope's Started/Stopped pair is not both present (indexes %d and %d): %v",
			when, started, stopped, describeScopes(recorded))
		return
	}
	if started > stopped {
		t.Errorf("%s: the watched scope's Stopped row (position %d) was recorded before its Started row "+
			"(position %d); a scope that stops before it starts reads as an epoch that never opened",
			when, stopped, started)
	}
}

// rejectionIsSurfaced: a transition the backend will not take is refused out
// loud, and refusing it means not recording it.
//
// A scope epoch is unlike a resource_states row in the one way that matters here:
// the object behind a dropped row will be observed again and re-recorded, whereas
// a transition happens once. So the caller — watch.ScopeEpochRecorder, which keeps
// its own retry queue precisely for this — has to be told, and a backend that
// returned nil while dropping the event would leave the recorder believing an
// epoch is on disk that is not.
//
// Shutdown is the lever, because it is the one refusal every backend must be able
// to produce (sink.ScopeEventWriter names it explicitly) and the only one the
// suite can arrange deterministically: a full queue depends on how fast the
// backend happens to drain, and a cancelled context races the hand-off's own
// select against a queue that has room.
func rejectionIsSurfaced(t conformanceT, h Harness) {
	t.Helper()
	const when = "scope event after shutdown"

	events := scopeWriter(t, h)
	accepted := sink.ScopeEvent{
		Action: sink.ScopeActionStarted, Scope: scopeIn(suiteNamespace), APIVersion: suiteAPIVersion,
		RuleRef: scopeIdentityRuleV1, TS: readAt(1),
	}

	r := startWriter(t, h)
	defer r.stop()
	seedScopeHistory(t, r, h, events, []sink.ScopeEvent{accepted})
	r.stop()

	before := len(h.ScopeWrites())
	refused := sink.ScopeEvent{
		Action: sink.ScopeActionStopped, Scope: scopeIn(suiteNamespace), APIVersion: suiteAPIVersion,
		RuleRef: scopeIdentityRuleV1, TS: readAt(2),
	}
	// A live context on purpose: the refusal must come from the backend's own
	// shut-down state, not from a context the caller had already given up on.
	err := events.EnqueueScopeEvent(context.Background(), refused)
	if err == nil {
		t.Errorf("%s: EnqueueScopeEvent returned nil after the backend had shut down; the caller takes that "+
			"as \"recorded\" and drops the transition from its retry queue, and the epoch is gone for good", when)
	} else {
		t.Logf("%s: EnqueueScopeEvent refused it: %v", when, err)
	}

	// A refusal that recorded the event anyway is the mirror-image bug: the caller
	// retries it elsewhere and the epoch is recorded twice.
	if after := waitScopeWriteCount(h, before+1, settleGraceForRejection); after > before && err != nil {
		t.Errorf("%s: the transition was refused but %d of them reached the backend anyway; a refused event "+
			"the caller will retry must leave no trace", when, after-before)
	}
}

// settleGraceForRejection is how long the property waits to see whether a refused
// transition shows up after all. It is short on purpose: nothing is expected to
// arrive, so this is time spent proving a negative, and the backend has already
// been fully shut down by the time it starts.
const settleGraceForRejection = 250 * time.Millisecond

// waitScopeWriteCount returns the recorded count, having waited up to timeout for
// it to reach want. It reports whatever it saw rather than failing, so the caller
// decides what the count means.
func waitScopeWriteCount(h Harness, want int, timeout time.Duration) int {
	waitFor(func() bool { return len(h.ScopeWrites()) >= want }, timeout)
	return len(h.ScopeWrites())
}

// indexOfTransition finds the position of one scope's transition in the log, or
// -1.
func indexOfTransition(recorded []sink.ScopeEvent, scope sink.ScopeFilter, action sink.ScopeAction) int {
	for i, ev := range recorded {
		if ev.Scope == scope && ev.Action == action {
			return i
		}
	}
	return -1
}

// describeScopes renders a whole log for a failure message.
func describeScopes(events []sink.ScopeEvent) []string {
	out := make([]string, len(events))
	for i, ev := range events {
		out[i] = describeScope(ev)
	}
	return out
}
