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

// This file is the capability-detection half of the suite: which optional
// contracts a backend turned out to implement, how each is skipped when it does
// not, and the harness levers each half needs.
//
// The skip is the delicate part. A backend that omits StateReader is a legitimate
// design (D12's S3 archive tier is exactly that), so the suite must not fail it —
// but a skip that says nothing is indistinguishable from a pass, and the whole
// point of D11 is that a badge nobody can interpret is worse than no badge. So
// every skip names the interface, lists the properties that are consequently
// unchecked, and states what the runtime turns off for such a sink. Two failure
// modes read identically from outside and both are caught by that message: a
// backend that genuinely cannot read its own history, and a backend whose reader
// exists but whose method set drifted out of the interface.

package conformance

import (
	"fmt"
	"strings"
	"testing"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// ReadFault breaks the backend's next read part-way through: the read delivers
// AfterRows rows and then reports Err.
//
// It exists because the read contract's sharpest obligation — "a partial read
// must be reported as an error, never as a short-but-successful result" — has no
// natural occurrence a test can wait for. A caller that mistakes a truncated scan
// for a complete one warms its dedup cache from half the history and then
// suppresses every change to the objects it never learned about, which is the
// silent-audit-gap failure this package exists to prevent.
//
// AfterRows of zero breaks the read before its first row; that is still a failed
// read, but it is not the case the property is about, so the suite always asks
// for at least one row to be delivered first.
type ReadFault struct {
	// AfterRows is how many rows the read delivers before it breaks.
	AfterRows int
	// Err is the failure the broken read must report.
	Err error
}

// ProbeOutcome is the state the suite asks a backend to arrange for its next
// health probe. See Harness.SetProbeOutcome for why it is declared rather than
// injected as an error value.
type ProbeOutcome string

const (
	// ProbeHealthy is a reachable backend carrying the schema the operator writes
	// against.
	ProbeHealthy ProbeOutcome = "Healthy"
	// ProbeSchemaMismatch is a backend that answers but whose schema is not the
	// one the operator writes. It is the outcome that must be classifiable, since
	// it will not fix itself with time and needs a human.
	ProbeSchemaMismatch ProbeOutcome = "SchemaMismatch"
	// ProbeUnreachable is a backend that does not answer at all: refused, timed
	// out, or rejected the credentials.
	ProbeUnreachable ProbeOutcome = "Unreachable"
)

// The optional contracts, spelled as a Go reader meets them, so a skip line can
// be pasted into a grep and land on the interface it names.
const (
	capStateReader      = "sink.StateReader"
	capScopeEventWriter = "sink.ScopeEventWriter"
	capProber           = "sink.Prober"
	// capScopeEpochReads is the read half's scope-epoch questions, which need a
	// scope log to have been written before they can be asked. A backend could in
	// principle answer them from history planted some other way, but none does:
	// the two interfaces are two halves of one story, and a StateReader without a
	// ScopeEventWriter has nothing to read.
	capScopeEpochReads = capStateReader + " together with " + capScopeEventWriter
)

// optionalSuite is one optional half of the contract as the suite runs it: how to
// detect it, what to say when it is absent, and what to assert when it is there.
type optionalSuite struct {
	// group is the subtest the half's properties are nested under, so a skip is a
	// single line in the output rather than one per property.
	group string
	// capability is the interface, named exactly as it is declared.
	capability string
	// consequence is what the operator loses by this backend not implementing it —
	// the half of a skip message that tells a reader whether to care.
	consequence string
	// implements reports whether this backend's Writer satisfies the half. It is
	// a type assertion, deliberately the same one SinkManager.newLiveSink makes.
	implements func(sink.Writer) bool
	properties func() []property
}

// stateReaderSuite is the read half's object-history properties: everything that
// can be asked of a StateReader without a scope log existing first.
func stateReaderSuite() optionalSuite {
	return optionalSuite{
		group:      "StateReader",
		capability: capStateReader,
		consequence: "A Writer-only sink runs with cache warm-up, zombie garbage-collection and boot " +
			"reconciliation of scope epochs disabled, and tags every record as a permanent Snapshot (D12). " +
			"That degradation is legitimate, but it must be a declared capability limit rather than an " +
			"unnoticed one, so it is reported here instead of passing quietly.",
		implements: func(w sink.Writer) bool { _, ok := w.(sink.StateReader); return ok },
		properties: stateReaderProperties,
	}
}

// scopeEpochReadSuite is the read half's other two questions — was this scope
// open in a previous epoch, and which scopes did a previous process leave open —
// which can only be asked of a backend that also records scope epochs.
func scopeEpochReadSuite() optionalSuite {
	return optionalSuite{
		group:      "StateReaderScopeEpoch",
		capability: capScopeEpochReads,
		consequence: "Without both halves there is no scope log to read back, so the epoch questions the " +
			"warm/GC coordinator asks of history cannot be answered and boot reconciliation is disabled " +
			"for this backend.",
		implements: func(w sink.Writer) bool {
			_, reads := w.(sink.StateReader)
			_, writes := w.(sink.ScopeEventWriter)
			return reads && writes
		},
		properties: scopeEpochReadProperties,
	}
}

func scopeEventWriterSuite() optionalSuite {
	return optionalSuite{
		group:      "ScopeEventWriter",
		capability: capScopeEventWriter,
		consequence: "A sink that cannot record scope epochs simply never receives them, and the operator's " +
			"audit trail loses the \"we stopped watching\" versus \"it was deleted\" distinction for that " +
			"sink alone.",
		implements: func(w sink.Writer) bool { _, ok := w.(sink.ScopeEventWriter); return ok },
		properties: scopeEventWriterProperties,
	}
}

func proberSuite() optionalSuite {
	return optionalSuite{
		group:      "Prober",
		capability: capProber,
		consequence: "A backend that cannot be probed gets no probe loop at all: its CR carries no " +
			"SchemaValid verdict, and an unreachable or drifted backend is discovered only when a write " +
			"fails.",
		implements: func(w sink.Writer) bool { _, ok := w.(sink.Prober); return ok },
		properties: proberProperties,
	}
}

// optionalSuites is every optional half, in the order RunWriterSuite runs them.
func optionalSuites() []optionalSuite {
	return []optionalSuite{stateReaderSuite(), scopeEpochReadSuite(), scopeEventWriterSuite(), proberSuite()}
}

// RunStateReaderSuite asserts the sink.StateReader contract against the backend
// newWriter builds, skipping loudly when it implements no read half.
//
// It runs two groups, because the read half asks two kinds of question and the
// second kind needs a scope log to exist: the object-history properties need only
// a StateReader, while ScopeWasActive and ActiveScopes also need the backend to
// implement sink.ScopeEventWriter, which is how the suite plants the epochs it
// then reads back.
func RunStateReaderSuite(t *testing.T, newWriter func(t *testing.T) Harness) {
	t.Helper()
	runOptionalSuite(t, newWriter, stateReaderSuite())
	runOptionalSuite(t, newWriter, scopeEpochReadSuite())
}

// RunScopeEventWriterSuite asserts the sink.ScopeEventWriter contract against the
// backend newWriter builds, skipping loudly when it records no scope epochs.
func RunScopeEventWriterSuite(t *testing.T, newWriter func(t *testing.T) Harness) {
	t.Helper()
	runOptionalSuite(t, newWriter, scopeEventWriterSuite())
}

// RunProberSuite asserts the sink.Prober contract against the backend newWriter
// builds, skipping loudly when it answers no health probe.
func RunProberSuite(t *testing.T, newWriter func(t *testing.T) Harness) {
	t.Helper()
	runOptionalSuite(t, newWriter, proberSuite())
}

// runOptionalSuite is the shared shape of all three: detect, or say why not.
//
// Detection costs one throwaway harness, built on the group's own *testing.T and
// never started. That is deliberate rather than reusing a property's harness: the
// answer decides whether any property runs at all, and asking it once keeps the
// skip to one line instead of repeating it per property.
func runOptionalSuite(t *testing.T, newWriter func(t *testing.T) Harness, s optionalSuite) {
	t.Helper()
	if newWriter == nil {
		t.Fatalf("conformance: running the %s suite needs a non-nil harness constructor", s.capability)
	}
	t.Run(s.group, func(t *testing.T) {
		probe := newWriter(t)
		if probe.Writer == nil {
			t.Fatalf("conformance: Harness.Writer is nil; the suite cannot tell which optional halves this backend implements")
		}
		if !s.implements(probe.Writer) {
			t.Logf("%s", missingCapabilityMessage(s))
			t.Skipf("conformance: this backend does not implement %s", s.capability)
		}
		for _, p := range s.properties() {
			t.Run(p.name, func(t *testing.T) {
				runProperty(t, p, newWriter(t))
			})
		}
	})
}

// missingCapabilityMessage is the whole of what a skip has to say: which contract
// is absent, which obligations therefore went unchecked, and what the runtime
// turns off as a result.
//
// It is a function rather than an inline Logf so the non-vacuity tests can assert
// on its content directly — a skip message that stopped naming the capability
// would put the suite straight back into the silence this file exists to prevent,
// and no test that merely observes a skipped subtest could tell.
func missingCapabilityMessage(s optionalSuite) string {
	names := make([]string, 0, len(s.properties()))
	for _, p := range s.properties() {
		names = append(names, s.group+"/"+p.name)
	}
	return fmt.Sprintf(
		"conformance: this backend's Writer does not implement %s, so the following properties were NOT "+
			"checked and this suite certifies nothing about them: %s. %s",
		s.capability, strings.Join(names, ", "), s.consequence)
}

// The harness levers the optional halves need, as per-property validators. They
// are Fatalf for the same reason the mandatory validate is: a harness missing the
// lever a property reaches for means that property is not testing the backend,
// and Phase 5 exists to stop that reading as a pass.
//
// They are checked per property rather than per suite so a backend is only ever
// failed for a field the property actually uses.
func requireReadFault(t conformanceT, h Harness) {
	t.Helper()
	if h.SetReadFault == nil {
		t.Fatalf("conformance: Harness.SetReadFault is nil but this backend implements %s; "+
			"the suite cannot break a read part-way through, and the partial-read obligation is the one "+
			"that silently truncates a warm-up", capStateReader)
	}
}

func requireScopeWrites(t conformanceT, h Harness) {
	t.Helper()
	if h.ScopeWrites == nil {
		t.Fatalf("conformance: Harness.ScopeWrites is nil but this backend implements %s; "+
			"the suite cannot see which scope transitions the backend recorded, so it cannot tell one "+
			"recorded epoch from two or from none", capScopeEventWriter)
	}
}

func requireProbeOutcome(t conformanceT, h Harness) {
	t.Helper()
	if h.SetProbeOutcome == nil {
		t.Fatalf("conformance: Harness.SetProbeOutcome is nil but this backend implements %s; "+
			"the suite cannot arrange an unhealthy backend, and a probe that is only ever asked about a "+
			"healthy one classifies nothing", capProber)
	}
}

// allProperties is every obligation this package asserts, mandatory and optional
// alike. The non-vacuity tests walk it to prove each one can fail.
func allProperties() []property {
	all := writerProperties()
	for _, s := range optionalSuites() {
		all = append(all, s.properties()...)
	}
	return all
}
