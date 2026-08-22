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
	"errors"
	"testing"

	"github.com/yelzhy/kuberecord/internal/sink"
)

// The subtest names, as constants because the non-vacuity tests address
// individual properties by name.
const (
	propProbeHealthy     = "HealthyBackendPasses"
	propProbeSchema      = "SchemaMismatchIsClassified"
	propProbeUnreachable = "OtherFailuresReadAsUnreachable"
)

// proberProperties is the health half of the contract as executable obligations.
//
// All three are about one thing: the *classification* a failure carries, not the
// failure itself. The SinkManager turns a probe error into one of two conditions
// on the sink's CR, and they mean opposite things to whoever is on call —
// SchemaInvalid needs a human to run a migration and will never clear on its own,
// Unreachable usually clears by itself and needs nobody. A backend that reports a
// schema drift as unreachable therefore sends an operator to wait for a recovery
// that cannot happen, and one that reports an outage as a schema failure pages
// somebody to migrate a schema that is already correct.
func proberProperties() []property {
	return []property{
		{name: propProbeHealthy, needs: requireProbeOutcome, run: healthyBackendPasses},
		{name: propProbeSchema, needs: requireProbeOutcome, run: schemaMismatchIsClassified},
		{name: propProbeUnreachable, needs: requireProbeOutcome, run: otherFailuresReadAsUnreachable},
	}
}

// healthyBackendPasses: a reachable backend with the expected schema probes
// clean. Without this the other two would also be satisfied by a Prober that
// simply always failed.
func healthyBackendPasses(t conformanceT, h Harness) {
	t.Helper()
	const when = "healthy probe"

	h.SetProbeOutcome(ProbeHealthy)
	r := startWriter(t, h)
	defer r.stop()

	if err := prober(t, h).Probe(r.ctx); err != nil {
		t.Errorf("%s: Probe returned %v against a reachable backend carrying the schema the operator "+
			"writes against; a sink that can never report itself healthy never becomes Ready", when, err)
	}
}

// schemaMismatchIsClassified: a backend whose schema is not the one the operator
// writes against fails the probe with an error that satisfies errors.Is(err,
// sink.ErrSchemaInvalid).
//
// The wrapping is the whole interface between a backend's private notion of
// "wrong shape" and the manager's public verdict. The manager knows nothing about
// system.columns, object-format versions or constraint names, so a mismatch that
// is not wrapped is indistinguishable from an outage and is reported as one.
func schemaMismatchIsClassified(t conformanceT, h Harness) {
	t.Helper()
	const when = "schema-mismatch probe"

	h.SetProbeOutcome(ProbeSchemaMismatch)
	r := startWriter(t, h)
	defer r.stop()

	err := prober(t, h).Probe(r.ctx)
	switch {
	case err == nil:
		t.Errorf("%s: Probe succeeded against a backend whose schema does not match the one the operator "+
			"writes; the sink would go Ready and then write rows that silently mismatch the frozen schema", when)
	case !errors.Is(err, sink.ErrSchemaInvalid):
		t.Errorf("%s: Probe failed with %v, which does not satisfy errors.Is(err, sink.ErrSchemaInvalid); "+
			"the manager can only classify it as Unreachable, so the operator waits for a recovery that "+
			"cannot happen without a migration nobody has been told to run", when, err)
	default:
		t.Logf("%s: Probe classified it: %v", when, err)
	}
}

// otherFailuresReadAsUnreachable: every failure that is not a schema mismatch
// reads as unreachable — including a probe whose own introspection never
// completed, which says nothing at all about the schema.
func otherFailuresReadAsUnreachable(t conformanceT, h Harness) {
	t.Helper()
	const when = "unreachable probe"

	h.SetProbeOutcome(ProbeUnreachable)
	r := startWriter(t, h)
	defer r.stop()

	err := prober(t, h).Probe(r.ctx)
	switch {
	case err == nil:
		t.Errorf("%s: Probe succeeded against a backend that does not answer; an unreachable sink would be "+
			"reported Ready and its writes would fail with nothing on the CR to explain why", when)
	case errors.Is(err, sink.ErrSchemaInvalid):
		t.Errorf("%s: Probe reported an unreachable backend as a schema failure (%v); SchemaInvalid tells "+
			"an operator to migrate, and there is nothing here to migrate", when, err)
	default:
		t.Logf("%s: Probe reported it as reachability rather than schema: %v", when, err)
	}
}

// prober is the backend's health half. runOptionalSuite has already established
// that the assertion holds, so a failure here is the suite's own bug.
func prober(t conformanceT, h Harness) sink.Prober {
	t.Helper()
	p, ok := h.Writer.(sink.Prober)
	if !ok {
		t.Fatalf("conformance: a %s property ran against a Writer that does not implement it", capProber)
	}
	return p
}

// Compile-time assurance that the exported optional runners keep the signature
// RunWriterSuite hands them, so a backend adopting one directly cannot be broken
// by a change here going unnoticed.
var (
	_ func(*testing.T, func(*testing.T) Harness) = RunStateReaderSuite
	_ func(*testing.T, func(*testing.T) Harness) = RunScopeEventWriterSuite
	_ func(*testing.T, func(*testing.T) Harness) = RunProberSuite
)
