# Developing kuberecord

This project is scaffolded with [Kubebuilder](https://book.kubebuilder.io/) and
uses its standard Makefile targets. `make help` lists every one; this page covers
the ones you will actually reach for and what each test suite is *for*.

## Prerequisites

- Go v1.25+ (see `go.mod`)
- Docker (or another `CONTAINER_TOOL`) for building images and for the suites
  that need a real backend
- [kind] for the e2e, chaos and quickstart suites

Every other tool — `kustomize`, `controller-gen`, `setup-envtest`, `helm`,
`kubeconform`, `promtool`, `golangci-lint` — is bootstrapped into `bin/` by the
Makefile itself. There is nothing to install by hand.

## The targets

```sh
make build            # go build the manager binary
make run              # run the controller locally against your current kubeconfig context
                      #   (OPERATOR_NAMESPACE=<ns> selects where it reads sink credentials Secrets)
make test             # run the unit/envtest suite (make setup-envtest fetches the binaries)
make test-integration # run the ClickHouse integration tests against a throwaway container (needs Docker)
make test-e2e         # run the end-to-end acceptance suite on a Kind cluster (needs Docker + Kind)
make test-chaos       # run the failure-mode (chaos) suite on a Kind cluster (needs Docker + Kind)
make bench-load       # run the synthetic-churn load harness on a named scale profile
                      #   PROFILE=small|medium|massive (see test/loadgen/profiles/);
                      #   PPROF_DIR=<dir> also writes heap/alloc profiles there
make quickstart       # stand the whole thing up on Kind and query the rows (see examples/quickstart/)
make verify-packaging # lint the Helm chart and validate both install paths' manifests
make verify-observability # validate the dashboards and alert rules
make build-installer  # regenerate the committed dist/install.yaml
make lint             # run golangci-lint (see .golangci.yml for the enabled linters)
make lint-fix         # run golangci-lint with --fix
make fmt vet          # gofmt + go vet
```

## Unit and envtest suites — `make test`

`make test` runs both the pure-Go unit tests (e.g. `internal/pipeline/`,
`internal/plan/`, `cmd/main_test.go`) and the Ginkgo/envtest-based CRD validation
suite in `api/v1alpha1/`, which spins up a real (test-only) API server via
`envtest` — no live cluster is required for it, but the envtest binaries must be
present locally (`make setup-envtest`).

The pipeline's own suite deliberately needs neither an API server nor a database:
it drives the workqueue through in-package fakes for the watch cache and the
sink.

Anything touching goroutines, mutexes or channels carries a `-race` test, and
long-lived goroutines carry a `goleak`-style shutdown test.

### The sink conformance suite

`internal/sink/conformance` holds the properties every `sink.Writer` must uphold
— commit-exactly-once on all four settling paths, no lost jobs, drain before
close, a bounded `Enqueue`, and a replay that collapses to one logical record —
written against the contract in `internal/sink` and against no backend in
particular. A new backend adopts it from its own test package by calling
`conformance.RunWriterSuite` with a `Harness` describing how to observe it, how
to make one of its writes fail, and how it deduplicates a record written twice.
The suite is tested in both directions: it passes a compliant in-memory writer
and is proven to *fail*, property by property, against writers built to violate
each obligation.

ClickHouse is its first adopter: `internal/sink/clickhouse/writer_conformance_test.go`
holds the harness (and nothing else — every assertion is the suite's), and its
header comment is the inventory of which claims come from the suite and which are
backend-specific, so a new assertion has one obvious home. What stays in
`writer_test.go` is what the contract is silent about: batching bounds, poison-row
isolation, the isolation-phase budget, metrics accounting, and timestamp binding.

#### The optional halves

`sink.StateReader`, `sink.ScopeEventWriter` and `sink.Prober` are optional and
duck-typed by the `SinkManager`, so the suite duck-types them too: `RunWriterSuite`
type-asserts the `Harness`'s `Writer` and runs `RunStateReaderSuite`,
`RunScopeEventWriterSuite` and `RunProberSuite` for whichever halves it satisfies.
A backend that omits one is **skipped, never failed** — a `Writer`-only archive
tier is a legitimate design (D12) — but the skip names the interface, lists the
properties that consequently certify nothing, and states what the runtime turns
off, because a silent skip is indistinguishable from a pass.

Three `Harness` fields serve those halves and are required only when the
corresponding interface is implemented: `ScopeWrites` (the ordered log of recorded
transitions), `SetReadFault` (break a read part-way through — the only way to
produce the partial read the contract forbids reporting as a short success), and
`SetProbeOutcome` (arrange a healthy, schema-drifted or unreachable backend, since
"your schema is wrong" is a state only the backend can construct).

Two things the suite deliberately does *not* claim. It cannot assert "one `Started`
per first-interest transition, never one per rule": that collapsing is
`watch.ScopeEpochRecorder`'s, proven by
`TestScopeEpochRecorderTransitionSemantics`, and a `ScopeEventWriter` never sees a
rule. What it asserts instead is the sink-boundary half the recorder depends on —
each accepted transition recorded exactly once, fields intact, order preserved.
And for ClickHouse the read half's intelligence is SQL, which no fake connection
executes: `optional_conformance_test.go` therefore *pins the query text* (a
statement missing a load-bearing clause is refused, never answered on the old
understanding) and evaluates the intended semantics over the rows the writer
really inserted, while `make test-integration` is what proves those clauses mean
in ClickHouse what the stand-in assumes.

## Integration tests — `make test-integration`

Two suites run against a real, dockerized ClickHouse (build tag `integration`),
in a container the target creates and always tears down:

- `internal/sink` exercises the writer and reader paths against a real server;
- `test/queries` executes **every query kuberecord publishes** — the SQL in
  [`docs/QUERIES.md`](QUERIES.md) and in every shipped Grafana dashboard —
  against tables built from the shipped DDL alone. That is how "these queries
  only touch frozen-schema columns" stays a tested claim rather than a review
  convention.

## End-to-end tests — `make test-e2e`

The acceptance suite. It creates a Kind cluster, builds and side-loads the
manager image, deploys a single-node ClickHouse
(`test/e2e/manifests/clickhouse.yaml`) and the operator, and then drives real
custom resources while **asserting by querying ClickHouse directly** — not by
reading the operator's own status.

| Scenario | What it proves |
|---|---|
| Lifecycle | create / scale / delete a Deployment yields `Added` (full payload, hash, actors), `Modified` (a diff of the change, no payload) and exactly one `Deleted` (no payload at all), plus a `watch_scopes` `Started` row |
| Dynamic churn | deleting a rule writes a `Stopped` row and **zero** `Deleted` rows for objects that are still alive; re-creating it reopens the scope without re-announcing them |
| RBAC | a rule for an ungranted kind reports `RBACGranted=False` while every other rule keeps streaming; applying the preset heals it within one resync, with no restart |
| Restart | an object deleted while the operator is down yields exactly one `Deleted`; one deleted and re-created yields the reincarnation close-out |
| Cluster scope | a `ClusterStreamRule` streams `v1/Node`; a namespaced `StreamRule` naming it reports `ResourceResolved=False` |
| Events | a rule naming `v1/Event` persists the Event stream past its TTL, with no false `Deleted` rows |
| Redaction | a configured path is scrubbed before hashing, and neither the payload, the diff nor the hash can reveal it |

It needs Docker and [kind] and nothing else — the suite installs everything it
depends on and tears the cluster down afterwards. Budget under 15 minutes; the
RBAC scenario alone waits out the rule reconciler's two-minute resync on purpose,
because self-healing without a restart is the property being tested. Override the
Go test timeout with `E2E_TIMEOUT` and the cluster name with `KIND_CLUSTER`.

The suite is install-path agnostic: `E2E_INSTALL=kustomize|helm|installer` chooses
*how* the operator gets onto the cluster and changes nothing else, because all
three paths produce the same object names.

```sh
make test-e2e-helm       # the happy path against `helm install deploy/charts/kuberecord`
make test-e2e-installer  # the happy path against `kubectl apply -f dist/install.yaml`
```

Both focus the lifecycle scenario (that is the packaging claim being tested, and
it keeps each smoke to one scenario); `E2E_FOCUS=` runs the whole suite against
the chosen path. Each gets its own Kind cluster, because `helm install` refuses
to adopt objects another install path already owns.

## Chaos tests — `make test-chaos`

The failure-mode suite. It stands the same real operator up on its own Kind
cluster, but against a ClickHouse it **stops and starts** — and it kills the
operator itself. Every mechanism kuberecord is built around (version gating,
delete claims, scope epochs, batch poison isolation, the bounded hand-off queue)
exists for conditions the e2e suite never creates; this is where those conditions
are created on purpose, and each scenario asserts through direct ClickHouse
queries **and** the operator's own `/metrics` endpoint.

| Scenario | What it proves |
|---|---|
| ClickHouse down at boot | rules still go active (only `Ready=False/SinkNotReady`), the scope watches in Snapshot mode (`kuberecord_safe_mode=1`), and when the backend appears each pre-existing object lands **once**, as `Snapshot` and never as `Added`; the scope then leaves Snapshot mode and the next change is a `Modified` with a diff |
| Mid-stream outage beyond the retry budget | writes fail terminally (`kuberecord_writes_total{outcome="failed"}` rises) and are re-driven rather than abandoned; on recovery every object converges on exactly one latest row whose `sha256` equals a live recompute of the object |
| Queue saturation | with the backend stopped and load three times the queue's capacity, `kuberecord_enqueue_timeouts_total` rises and no `enqueue_block_seconds` observation exceeds the configured 2s timeout; the operator never restarts, and recovery drains the queue |
| Poison row | one record made individually un-insertable fails its batch, its blameless batch-mates still land, and the poison key keeps retrying visibly (counted and logged) instead of being dropped |
| Kill -9 + offline delete | after a `SIGKILL` with writes in flight: exactly one `Deleted` for the offline deletion, the reincarnation closed out exactly once, and `watch_scopes` left consistent — a rule deleted during the outage is closed with a `Stopped` row and **zero** `Deleted` rows, and no scope stays open once its rule is gone |

Every scenario additionally asserts the standing invariant that no object's
deletion was recorded more than once.

The fixture differs from the e2e one in two ways that the scenarios require: its
ClickHouse is backed by a PersistentVolumeClaim, so rows survive an outage, and
its sink runs a single writer with a 50-job queue, so batching and backpressure
are reachable deterministically. Budget an hour — three scenarios have to outlast
the ClickHouse writer's 60-second per-batch retry budget before the failure they
test is observable at all. Override the Go test timeout with `CHAOS_TIMEOUT`, the
cluster name with `CHAOS_KIND_CLUSTER`, and keep the cluster up for inspection
with `CHAOS_KEEP_CLUSTER=true`.

## Load harness — `make bench-load`

Synthetic churn against a dockerized ClickHouse plus an in-process envtest
apiserver. `PROFILE` names one of the shipped scale profiles — `small`, `medium`,
`massive` — and the profile file *is* the whole load definition: objects, rate,
payload, duration, delete ratio, kinds, and the pass criteria the run judges
itself against. That is what makes a published envelope reproducible from its
name alone. See [`docs/PERFORMANCE.md`](PERFORMANCE.md).

## Packaging and observability checks

```sh
make verify-packaging      # helm lint + kubeconform over the chart and dist/install.yaml
make verify-observability  # JSON-schema checks plus `promtool check rules`
make build-installer       # regenerate dist/install.yaml — CI fails if the committed one is stale
```

Chart-vs-kustomize parity is asserted object by object in `test/chart`, which
runs under `make test`.

## Releasing — `make release-dry-run`

A release is a tag, and the tag-triggered workflow is a thin caller of make
targets you can run yourself:

```sh
make release-dry-run                              # the whole release, nothing published
make release-dry-run RELEASE_VERSION=v0.2.0-rc.1  # rehearse a candidate
make release-notes RELEASE_VERSION=v0.2.0         # the gate, on its own
```

The dry run writes `dist/release/` (git-ignored): the notes extracted from
`CHANGELOG.md`, `install.yaml`, the packaged chart, and `checksums.txt` — and it
builds the image for every supported platform without a registry to push to.
`test/release` covers the extractor and the wiring under `make test`.
[`docs/RELEASING.md`](RELEASING.md) is the versioning policy and the checklist for
cutting one.

## Documentation checks

`test/docs` runs under `make test` and guards the things prose cannot guard
itself against: that the quickstart's files all exist and agree on the password
they share, that every relative link in every published page resolves, and that
none of the environment-variable configuration Phase 1 removed has crept back
into an instruction. Those settings went away with no compatibility shim, so a
document still telling a reader to set one is telling them to do something that
cannot work — a worse failure than a stale sentence, because the reader has no
way to tell it is stale. [`CHANGELOG.md`](../CHANGELOG.md) is exempt, and has its
own test requiring it to keep naming them: its migration table is how an upgrader
finds out what each became.

## CI

| Workflow | What it runs |
|---|---|
| `test.yml` | `make test` |
| `lint.yml` | `make lint-config` and `make lint` |
| `test-e2e.yml` | `make test-e2e` |
| `install-paths.yml` | `make verify-packaging`, the chart tests, a `dist/install.yaml` staleness check, and the Helm and installer smokes on Kind |
| `observability.yml` | `make verify-observability` |
| `quickstart.yml` | `make quickstart` with `QUICKSTART_BUDGET_SECONDS=600` — the ten-minute claim, tested |
| `release.yml` | On a `vX.Y.Z` tag: the version and changelog gate, the multi-arch image, then the artifacts and the GitHub Release. On `workflow_dispatch`: the same sequence with nothing pushed or published |

[kind]: https://kind.sigs.k8s.io/
