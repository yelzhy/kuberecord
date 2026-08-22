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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// The subtest names, as constants because the non-vacuity tests address
// individual properties by name and a typo there would silently prove nothing.
const (
	propPerIncarnation  = "PerIncarnationResults"
	propTombstoned      = "TombstonedIncarnationsExcluded"
	propPartialRead     = "PartialReadIsAnError"
	propScopeAsOf       = "ScopeWasActiveHonoursAsOf"
	propActiveScopes    = "ActiveScopesEnumeratesOpenScopes"
	scopeIdentityRuleV1 = "streamrule/conformance/rule-one"
)

// errPartialRead is the failure the suite makes a read break with. It is only
// ever produced through Harness.SetReadFault, so a backend that reports it has
// reported the fault it was given rather than one of its own.
var errPartialRead = errors.New("conformance: injected mid-stream read failure")

// stateReaderProperties are the read half's object-history obligations: the ones
// answerable by any StateReader, with no scope log required.
func stateReaderProperties() []property {
	return []property{
		{name: propPerIncarnation, run: perIncarnationResults},
		{name: propTombstoned, run: tombstonedIncarnationsExcluded},
		{name: propPartialRead, needs: requireReadFault, run: partialReadIsAnError},
	}
}

// scopeEpochReadProperties are the read half's scope-epoch obligations, which the
// suite can only ask of a backend that also records scope epochs (see
// scopeEpochReadSuite).
func scopeEpochReadProperties() []property {
	return []property{
		{name: propScopeAsOf, needs: requireScopeWrites, run: scopeWasActiveHonoursAsOf},
		{name: propActiveScopes, needs: requireScopeWrites, run: activeScopesEnumeratesOpenScopes},
	}
}

// observation is one already-happened event the read properties plant, spelled in
// the terms the reader answers in — an incarnation, an instant, and what happened
// to it — rather than as a fully-populated Record.
//
// It exists because these properties turn entirely on the (namespace, name, uid,
// ts, event_type) tuple, and building each Record inline would bury that tuple in
// fields none of them look at.
type observation struct {
	namespace  string
	name       string
	uid        string
	apiVersion string
	eventType  string
	at         time.Time
}

// record renders the observation as the Record the write path would have carried.
//
// The digest is derived from the tuple rather than chosen, so two observations of
// one incarnation differ in it exactly when they differ in substance — which is
// what makes "the reader returned the *latest* sha256 for this incarnation" a
// falsifiable claim rather than a coincidence.
func (e observation) record() sink.Record {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		e.namespace, e.name, e.uid, e.eventType, e.at.UTC().Format(time.RFC3339Nano),
	}, "|")))
	rec := sink.Record{
		Timestamp:       e.at,
		ClusterID:       suiteClusterID,
		EventType:       e.eventType,
		APIGroup:        suiteAPIGroup,
		APIVersion:      e.apiVersion,
		Kind:            suiteKind,
		Namespace:       e.namespace,
		Name:            e.name,
		UID:             e.uid,
		ResourceVersion: strconv.FormatInt(e.at.UnixMilli(), 10),
		Labels:          map[string]string{"app": e.name},
		Actors:          []string{suiteActor},
		Data:            `{"metadata":{"name":"` + e.name + `","uid":"` + e.uid + `"}}`,
		SHA256:          hex.EncodeToString(sum[:]),
	}
	// A deletion has no live object left to inspect, so it carries no actors and
	// no payload (see sink.Record.Actors). Seeding one that did would be seeding
	// history the write path never produces.
	if e.eventType == eventDeleted {
		rec.Actors = nil
		rec.Data = ""
	}
	return rec
}

// knownState is what the reader must report for this observation, when this
// observation is the incarnation's latest.
func (e observation) knownState() sink.KnownState {
	rec := e.record()
	return sink.KnownState{
		Namespace: rec.Namespace, Name: rec.Name, UID: rec.UID,
		SHA256: rec.SHA256, APIVersion: rec.APIVersion, TS: rec.Timestamp,
	}
}

// The event types the read properties plant. Only Deleted is load-bearing (it is
// what closes an incarnation out); the rest are there so the history reads like
// history.
const (
	eventAdded    = "Added"
	eventModified = "Modified"
	eventDeleted  = "Deleted"
)

// readAt returns the instant of the nth step of a property's planted history.
// Seconds apart, so a backend that stores its timestamps at coarser than
// nanosecond precision still orders them the way the property intends.
func readAt(n int) time.Time { return suiteEpoch.Add(time.Duration(n) * time.Second) }

// readFilter is the scope every read property queries: the suite's own GVK, with
// the *record query* reading of Namespace (empty matches every namespace).
func readFilter() sink.ScopeFilter {
	return sink.ScopeFilter{ClusterID: suiteClusterID, APIGroup: suiteAPIGroup, Kind: suiteKind}
}

// seedObservations plants a history through the backend's own write path.
func seedObservations(t conformanceT, r *runner, h Harness, observations []observation) {
	t.Helper()
	records := make([]sink.Record, len(observations))
	for i, o := range observations {
		records[i] = o.record()
	}
	seedHistory(t, r, h, records)
}

// perIncarnationResults: the reader answers per (identity, UID), not per identity.
//
// An identity whose death went unrecorded — a delete-and-recreate the operator was
// down across — has two live-looking incarnations in history, and the multiplicity
// is the *only* evidence the older one was never closed out. A reader that folds
// them to one row destroys that evidence, and the warm-up then seeds the dedup
// cache for the survivor while the prior stays open in the audit trail for ever:
// an object recorded as still existing that has not existed for months, with
// nothing anywhere to say so.
func perIncarnationResults(t conformanceT, h Harness) {
	t.Helper()
	const when = "per-incarnation read"
	const twoLived, oneLived = "dep-two-lives", "dep-one-life"

	// dep-two-lives died and came back while nobody was recording: the prior
	// (uid-old, on an older served version) has no Deleted row, and the successor
	// has since been modified.
	prior := observation{suiteNamespace, twoLived, "uid-old", "v1beta1", eventAdded, readAt(1)}
	successorLatest := observation{suiteNamespace, twoLived, "uid-new", suiteAPIVersion, eventModified, readAt(4)}
	ordinary := observation{suiteNamespace, oneLived, "uid-plain", suiteAPIVersion, eventAdded, readAt(2)}

	r := startWriter(t, h)
	defer r.stop()
	seedObservations(t, r, h, []observation{
		prior,
		ordinary,
		{suiteNamespace, twoLived, "uid-new", suiteAPIVersion, eventAdded, readAt(3)},
		successorLatest,
	})

	states := mustReadStates(t, r.ctx, h, when)
	assertKnownStates(t, when, states, []sink.KnownState{
		prior.knownState(), successorLatest.knownState(), ordinary.knownState(),
	})

	// Spelled out separately from the set comparison because it is the claim the
	// property is named for, and a bare set diff would not say which half of it
	// broke.
	if got := countFor(states, twoLived); got != 2 {
		t.Errorf("%s: %s has %d incarnation(s) in the result, want 2 — an identity whose prior death was "+
			"never recorded must surface as two, since that multiplicity is the only evidence the prior "+
			"was left open", when, twoLived, got)
	}
	if got := countFor(states, oneLived); got != 1 {
		t.Errorf("%s: the ordinary identity %s has %d incarnation(s) in the result, want exactly 1",
			when, oneLived, got)
	}
}

// tombstonedIncarnationsExcluded: an incarnation whose own most recent event is a
// deletion is closed and must not come back, and closing one out must not take
// its identity's other incarnations with it.
//
// The history is arranged so a per-*identity* reading fails loudly: the prior's
// close-out lands *after* the successor's first row, which is exactly what a
// late-observed delete looks like. A reader that asks "is this identity's latest
// event a deletion?" then drops a live object from the warm-up, and every
// subsequent change to it is re-emitted as if the object were new.
func tombstonedIncarnationsExcluded(t conformanceT, h Harness) {
	t.Helper()
	const when = "tombstone read"
	const recreated, changed, gone = "dep-recreated", "dep-changed", "dep-gone"

	successor := observation{suiteNamespace, recreated, "uid-second", suiteAPIVersion, eventAdded, readAt(3)}
	survivorLatest := observation{suiteNamespace, changed, "uid-changed", suiteAPIVersion, eventModified, readAt(2)}

	r := startWriter(t, h)
	defer r.stop()
	seedObservations(t, r, h, []observation{
		{suiteNamespace, recreated, "uid-first", suiteAPIVersion, eventAdded, readAt(1)},
		successor,
		// The close-out for the *first* incarnation, observed late — after its
		// successor was already recorded.
		{suiteNamespace, recreated, "uid-first", suiteAPIVersion, eventDeleted, readAt(4)},
		{suiteNamespace, changed, "uid-changed", suiteAPIVersion, eventAdded, readAt(1)},
		survivorLatest,
		{suiteNamespace, gone, "uid-gone", suiteAPIVersion, eventAdded, readAt(1)},
		{suiteNamespace, gone, "uid-gone", suiteAPIVersion, eventDeleted, readAt(5)},
	})

	states := mustReadStates(t, r.ctx, h, when)
	assertKnownStates(t, when, states, []sink.KnownState{successor.knownState(), survivorLatest.knownState()})

	for _, st := range states {
		if st.UID == "uid-first" {
			t.Errorf("%s: the tombstoned incarnation %s/%s (%s) is still reported; its own most recent "+
				"event is a deletion, so warming a cache from it would resurrect an object that is gone",
				when, st.Namespace, st.Name, st.UID)
		}
	}
	if countFor(states, gone) != 0 {
		t.Errorf("%s: %s is still reported although its only incarnation was deleted", when, gone)
	}
	if countFor(states, recreated) != 1 {
		t.Errorf("%s: %s has %d incarnation(s) in the result, want exactly 1 (the successor); closing out "+
			"one incarnation must not close out the identity", when, recreated, countFor(states, recreated))
	}
}

// partialReadIsAnError: a read that dies mid-stream is reported as a failure, not
// as a short-but-successful result.
//
// This is the quietest way a sink can corrupt an audit trail. The warm-up's whole
// premise is that what it read is *all* there was, so a truncated scan that came
// back with a nil error seeds the dedup cache for the objects it happened to see
// and treats every other live object as new — and then the zombie garbage
// collector, reading the same short list, writes Deleted rows for objects that
// were never deleted. Nothing about either outcome looks like a failure.
func partialReadIsAnError(t conformanceT, h Harness) {
	t.Helper()
	const when = "partial read"
	const rows = 4
	const deliver = 2

	r := startWriter(t, h)
	defer r.stop()

	seeded := make([]observation, rows)
	want := make([]sink.KnownState, rows)
	for i := range rows {
		seeded[i] = observation{
			suiteNamespace, fmt.Sprintf("dep-%02d", i), fmt.Sprintf("uid-%02d", i),
			suiteAPIVersion, eventAdded, readAt(i + 1),
		}
		want[i] = seeded[i].knownState()
	}
	seedObservations(t, r, h, seeded)

	h.SetReadFault(&ReadFault{AfterRows: deliver, Err: errPartialRead})
	states, err := reader(t, h).LastKnownStates(r.ctx, readFilter())
	switch {
	case err == nil:
		t.Errorf("%s: LastKnownStates returned %d of %d states and a nil error after the read broke "+
			"%d rows in; a short success tells the warm-up that the objects it never saw do not exist, "+
			"and the zombie sweep then writes Deleted rows for objects nobody deleted",
			when, len(states), rows, deliver)
	case !errors.Is(err, errPartialRead):
		// Not a failure: a backend may legitimately wrap or replace the driver's
		// error. What matters is that it reported one at all.
		t.Logf("%s: the broken read was reported as %v rather than as the injected fault; the obligation "+
			"is that it is reported, not how", when, err)
	}

	// Clearing the fault must restore a complete read. Without this the property
	// would also pass against a reader that simply always failed, which is not
	// what it is asserting.
	h.SetReadFault(nil)
	states = mustReadStates(t, r.ctx, h, when+" (fault cleared)")
	assertKnownStates(t, when+" (fault cleared)", states, want)
}

// scopeWasActiveHonoursAsOf: the epoch probe answers strictly about rows *before*
// the instant it is given, and about the scope it is given and no other.
//
// The cutoff is what makes zombie garbage collection safe. Scope rows are written
// asynchronously, so the current epoch's own Started row may land at any moment;
// a probe that counted it would report "a previous epoch left this scope open" for
// a scope that was in fact opened seconds ago by this very process, and the sweep
// would then attribute every object it cannot see in the live cache to a deletion
// that never happened.
//
// The documented "a zero asOf means as of now" convenience is deliberately not
// asserted: pinning it would tie the property to the wall clock of whatever
// machine runs it, and the obligation that matters — an explicit cutoff is obeyed
// exactly — is fully covered below.
func scopeWasActiveHonoursAsOf(t conformanceT, h Harness) {
	t.Helper()
	const when = "scope epoch probe"

	watched := scopeIn(suiteNamespace)
	// The all-namespaces scope: a different scope with an independent epoch, not a
	// wildcard over the one above (see sink.ScopeFilter's identity reading).
	wide := scopeIn("")

	r := startWriter(t, h)
	defer r.stop()
	seedScopeHistory(t, r, h, scopeWriter(t, h), []sink.ScopeEvent{
		scopeTransition(sink.ScopeActionStarted, watched, readAt(2)),
		scopeTransition(sink.ScopeActionStopped, watched, readAt(4)),
	})

	cases := []struct {
		what  string
		scope sink.ScopeFilter
		asOf  time.Time
		want  bool
		why   string
	}{
		{
			what: "at the instant of its own Started row", scope: watched, asOf: readAt(2), want: false,
			why: "the cutoff is strict, and this is the row the caller's own epoch is trying not to see",
		},
		{
			what: "after Started and before Stopped", scope: watched, asOf: readAt(3), want: true,
			why: "a previous epoch of this scope was genuinely left open",
		},
		{
			what: "at the instant of the Stopped row", scope: watched, asOf: readAt(4), want: true,
			why: "only Started is strictly earlier, so as of this instant the scope was still open",
		},
		{
			what: "after Stopped", scope: watched, asOf: readAt(5), want: false,
			why: "the trail already says we stopped watching here, and the objects' states are dated to that row",
		},
		{
			what: "for the all-namespaces scope, which has no history", scope: wide, asOf: readAt(5), want: false,
			why: "an empty namespace is a scope of its own, not a wildcard: one scope's history must never answer for another's",
		},
	}
	for _, tc := range cases {
		got, err := reader(t, h).ScopeWasActive(r.ctx, tc.scope, tc.asOf)
		if err != nil {
			t.Errorf("%s %s: ScopeWasActive returned %v; a scope with no matching history is the ordinary "+
				"brand-new case and must read as false, not as an error", when, tc.what, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s %s: ScopeWasActive(ns=%q, asOf=%s) = %t, want %t — %s",
				when, tc.what, tc.scope.Namespace, tc.asOf.UTC().Format(time.RFC3339Nano), got, tc.want, tc.why)
		}
	}
}

// activeScopesEnumeratesOpenScopes: the boot-reconciliation enumeration returns
// every scope whose most recent recorded action is Started, and only those.
//
// Boot reconciliation needs the enumeration rather than a per-scope probe because
// the scopes it has to close are precisely the ones nothing in the desired state
// mentions any more — a rule deleted while the operator was down leaves an open
// scope with no candidate to probe for. A reader that also returns closed scopes
// makes the operator write a second Stopped row over an epoch that is already
// closed; one that omits an open scope leaves it open for ever.
func activeScopesEnumeratesOpenScopes(t conformanceT, h Harness) {
	t.Helper()
	const when = "active scope enumeration"

	open, closed, reopened, wide := scopeIn("alpha"), scopeIn("beta"), scopeIn("gamma"), scopeIn("")

	r := startWriter(t, h)
	defer r.stop()
	seedScopeHistory(t, r, h, scopeWriter(t, h), []sink.ScopeEvent{
		scopeTransition(sink.ScopeActionStarted, open, readAt(1)),
		scopeTransition(sink.ScopeActionStarted, closed, readAt(1)),
		scopeTransition(sink.ScopeActionStarted, reopened, readAt(1)),
		scopeTransition(sink.ScopeActionStarted, wide, readAt(1)),
		scopeTransition(sink.ScopeActionStopped, reopened, readAt(2)),
		scopeTransition(sink.ScopeActionStopped, closed, readAt(3)),
		scopeTransition(sink.ScopeActionStarted, reopened, readAt(4)),
	})

	got, err := reader(t, h).ActiveScopes(r.ctx, suiteClusterID)
	if err != nil {
		t.Fatalf("%s: ActiveScopes returned %v", when, err)
	}
	want := []sink.ScopeFilter{open, reopened, wide}
	sortScopes(got)
	sortScopes(want)
	if !slices.Equal(got, want) {
		t.Errorf("%s: ActiveScopes returned %v, want %v — a scope is open iff its most recent recorded "+
			"action is Started (%q was stopped; %q was stopped and started again)",
			when, got, want, closed.Namespace, reopened.Namespace)
	}
	for _, scope := range got {
		if scope.ClusterID != suiteClusterID {
			t.Errorf("%s: ActiveScopes returned %v with cluster %q, want %q; each returned filter must be "+
				"directly usable as a sink.ScopeEvent.Scope", when, scope, scope.ClusterID, suiteClusterID)
			break
		}
	}
}

// reader is the backend's read half. runOptionalSuite has already established
// that the assertion holds, so a failure here is the suite's own bug.
func reader(t conformanceT, h Harness) sink.StateReader {
	t.Helper()
	r, ok := h.Writer.(sink.StateReader)
	if !ok {
		t.Fatalf("conformance: a %s property ran against a Writer that does not implement it", capStateReader)
	}
	return r
}

// scopeWriter is the backend's scope-log half, on the same footing as reader.
func scopeWriter(t conformanceT, h Harness) sink.ScopeEventWriter {
	t.Helper()
	w, ok := h.Writer.(sink.ScopeEventWriter)
	if !ok {
		t.Fatalf("conformance: a %s property ran against a Writer that does not implement it", capScopeEventWriter)
	}
	return w
}

// mustReadStates performs a complete read, failing the property if it errors.
func mustReadStates(t conformanceT, ctx context.Context, h Harness, when string) []sink.KnownState {
	t.Helper()
	states, err := reader(t, h).LastKnownStates(ctx, readFilter())
	if err != nil {
		t.Fatalf("%s: LastKnownStates returned %v against a backend with no fault installed", when, err)
	}
	return states
}

// assertKnownStates compares the reader's answer to the expected one as a set,
// keyed on the incarnation. Order is not part of the contract — the warm-up
// indexes what it gets — so comparing sequences would fail a correct backend for
// the order its storage happens to scan in.
func assertKnownStates(t conformanceT, when string, got, want []sink.KnownState) {
	t.Helper()
	byUID := make(map[string]sink.KnownState, len(got))
	for _, st := range got {
		key := incarnationKey(st.Namespace, st.Name, st.UID)
		if prior, dup := byUID[key]; dup {
			t.Errorf("%s: %s was reported twice (%+v and %+v); one incarnation is one last-known state",
				when, key, prior, st)
		}
		byUID[key] = st
	}
	for _, w := range want {
		key := incarnationKey(w.Namespace, w.Name, w.UID)
		st, ok := byUID[key]
		if !ok {
			t.Errorf("%s: %s is missing from the result; its most recent event is not a deletion, so it "+
				"is still live history and the warm-up needs it", when, key)
			continue
		}
		delete(byUID, key)
		if st.SHA256 != w.SHA256 || st.APIVersion != w.APIVersion || !st.TS.Equal(w.TS) {
			t.Errorf("%s: %s came back as sha=%s api=%s ts=%s, want sha=%s api=%s ts=%s — each field must "+
				"be this incarnation's own latest, since a close-out is dated and versioned from the "+
				"incarnation it closes", when, key, st.SHA256, st.APIVersion,
				st.TS.UTC().Format(time.RFC3339Nano), w.SHA256, w.APIVersion, w.TS.UTC().Format(time.RFC3339Nano))
		}
	}
	for key, st := range byUID {
		t.Errorf("%s: %s was reported but is not live history: %+v", when, key, st)
	}
}

// countFor is how many incarnations the result holds for one identity in the
// suite's own namespace, which is the only namespace the read properties plant
// history in.
func countFor(states []sink.KnownState, name string) int {
	n := 0
	for _, st := range states {
		if st.Namespace == suiteNamespace && st.Name == name {
			n++
		}
	}
	return n
}

// incarnationKey renders one (identity, UID) pair for a failure message. It is
// emphatically not Invariant 7's identity key — that one is version-agnostic and
// spans an object's whole history, whereas this names a single incarnation.
func incarnationKey(namespace, name, uid string) string {
	return namespace + "/" + name + "#" + uid
}

// scopeIn builds the suite's GVK scope in one namespace. An empty namespace is
// the all-namespaces scope itself, matched exactly.
func scopeIn(namespace string) sink.ScopeFilter {
	return sink.ScopeFilter{
		ClusterID: suiteClusterID, APIGroup: suiteAPIGroup, Kind: suiteKind, Namespace: namespace,
	}
}

// scopeTransition builds one epoch edge for a scope at an instant.
func scopeTransition(action sink.ScopeAction, scope sink.ScopeFilter, at time.Time) sink.ScopeEvent {
	return sink.ScopeEvent{
		Action: action, Scope: scope, APIVersion: suiteAPIVersion, RuleRef: scopeIdentityRuleV1, TS: at,
	}
}

// sortScopes puts scopes in a deterministic order so two sets can be compared.
func sortScopes(scopes []sink.ScopeFilter) {
	slices.SortFunc(scopes, func(a, b sink.ScopeFilter) int {
		return strings.Compare(a.ClusterID+"|"+a.APIGroup+"|"+a.Kind+"|"+a.Namespace,
			b.ClusterID+"|"+b.APIGroup+"|"+b.Kind+"|"+b.Namespace)
	})
}
