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

package clickhouse

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/sink"
)

// probeMetrics is a throwaway per-sink metrics view: Probe records nothing itself,
// but NewCHWriter's contract is that its metrics are never nil, and a test that
// passed nil would be asserting against a writer production never builds.
func probeMetrics() *pipeline.SinkMetrics {
	return pipeline.NewPipelineMetrics(prometheus.NewRegistry()).ForSink(testSinkID)
}

// baseConfig is the reference configuration the fingerprint tests mutate one
// field of at a time.
func baseConfig() Config {
	return Config{
		Addr:                 "clickhouse.kuberecord-system.svc:9000",
		Database:             "kuberecord",
		Username:             "kuberecord",
		Password:             "s3cret",
		DialTimeout:          5 * time.Second,
		ReadTimeout:          10 * time.Second,
		AutoCreateSchema:     false,
		BatchMaxRows:         1000,
		BatchMaxWait:         time.Second,
		WriteQueueSize:       5000,
		WriteWorkers:         4,
		EnqueueTimeout:       2 * time.Second,
		ShutdownDrainTimeout: 15 * time.Second,
		CheckpointEvery:      DefaultCheckpointEvery,
	}
}

// TestConfigFingerprint pins the recycle discriminator the SinkManager diffs on:
// an unchanged configuration must fingerprint identically (so a re-reconciled sink
// is never needlessly recycled), and every field — the password above all — must
// change it.
func TestConfigFingerprint(t *testing.T) {
	base := baseConfig()

	if base.Fingerprint() != baseConfig().Fingerprint() {
		t.Error("two identical configurations fingerprint differently; every reconcile would recycle the sink")
	}
	// The digest must not leak the credential: it is compared *and logged*.
	if strings.Contains(base.Fingerprint(), base.Password) {
		t.Error("the fingerprint contains the password in clear")
	}

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"addr", func(c *Config) { c.Addr = "other:9000" }},
		{"database", func(c *Config) { c.Database = "other" }},
		{"username", func(c *Config) { c.Username = "other" }},
		{"password", func(c *Config) { c.Password = "rotated" }},
		{"dial timeout", func(c *Config) { c.DialTimeout = 6 * time.Second }},
		{"read timeout", func(c *Config) { c.ReadTimeout = 11 * time.Second }},
		{"auto-create schema", func(c *Config) { c.AutoCreateSchema = true }},
		{"batch max rows", func(c *Config) { c.BatchMaxRows = 500 }},
		{"batch max wait", func(c *Config) { c.BatchMaxWait = 2 * time.Second }},
		{"queue size", func(c *Config) { c.WriteQueueSize = 10000 }},
		{"workers", func(c *Config) { c.WriteWorkers = 8 }},
		{"enqueue timeout", func(c *Config) { c.EnqueueTimeout = 3 * time.Second }},
		{"drain timeout", func(c *Config) { c.ShutdownDrainTimeout = 20 * time.Second }},
		// Re-tuning the Checkpoint cadence must recycle the instance too: the
		// cadence is read off the running writer (CheckpointEvery), so a
		// fingerprint that ignored it would leave the old cadence in effect until
		// the next restart.
		{"checkpoint cadence", func(c *Config) { c.CheckpointEvery = 10 }},
		{"checkpointing disabled", func(c *Config) { c.CheckpointEvery = 0 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			changed := baseConfig()
			tc.mutate(&changed)
			if changed.Fingerprint() == base.Fingerprint() {
				t.Errorf("changing the %s left the fingerprint unchanged; the sink would keep running the old settings", tc.name)
			}
		})
	}

	// Neighbouring fields must not be re-splittable into another configuration:
	// "a"+"bc" and "ab"+"c" are different sinks and must fingerprint differently.
	left, right := baseConfig(), baseConfig()
	left.Addr, left.Database = "a", "bc"
	right.Addr, right.Database = "ab", "c"
	if left.Fingerprint() == right.Fingerprint() {
		t.Error("two configurations differing only in where a field boundary falls fingerprint identically")
	}
}

// fakeProbeConn is a driver.Conn whose Ping and system.columns Query outcomes are
// both injectable, which is everything Probe consults.
type fakeProbeConn struct {
	driver.Conn
	pingErr  error
	queryErr error
	rows     [][3]string
}

func (c fakeProbeConn) Ping(context.Context) error { return c.pingErr }

func (c fakeProbeConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return &fakeSchemaRows{data: c.rows}, nil
}

func (c fakeProbeConn) Close() error { return nil }

// TestProbeClassifiesOutcomes covers the whole probe contract: a healthy backend
// passes, an unreachable one fails on the Ping (before its schema is judged), a
// drifted schema is wrapped so the manager can label it SchemaInvalid, and a
// transient introspection failure is *not* so wrapped — it is reported as
// unreachable, because the query never completed.
func TestProbeClassifiesOutcomes(t *testing.T) {
	pingErr := errors.New("connection refused")
	queryErr := errors.New("read: i/o timeout")

	tests := []struct {
		name           string
		conn           fakeProbeConn
		wantErr        bool
		wantSchemaKind bool     // errors.Is(err, sink.ErrSchemaInvalid)
		wantInMessage  []string // substrings the error must name
	}{
		{
			name: "a reachable backend with the expected schema passes",
			conn: fakeProbeConn{rows: fullSchemaRows()},
		},
		{
			name:          "an unreachable backend fails on the ping",
			conn:          fakeProbeConn{pingErr: pingErr, rows: fullSchemaRows()},
			wantErr:       true,
			wantInMessage: []string{"ping", "connection refused"},
		},
		{
			name:           "a drifted schema is classified as a schema failure",
			conn:           fakeProbeConn{rows: driftedSchemaRows()},
			wantErr:        true,
			wantSchemaKind: true,
			wantInMessage:  []string{driftedColumn, driftedType},
		},
		{
			name:          "a failed introspection is not a schema verdict",
			conn:          fakeProbeConn{queryErr: queryErr},
			wantErr:       true,
			wantInMessage: []string{"i/o timeout"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := NewCHWriter(tc.conn, 1, 1, 1, time.Second, time.Second, time.Second, time.Second, time.Second, probeMetrics())
			w.database = "kuberecord"

			err := w.Probe(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("Probe() error = %v, want error: %t", err, tc.wantErr)
			}
			if err == nil {
				return
			}
			if got := errors.Is(err, sink.ErrSchemaInvalid); got != tc.wantSchemaKind {
				t.Errorf("errors.Is(err, sink.ErrSchemaInvalid) = %t, want %t (err = %v)", got, tc.wantSchemaKind, err)
			}
			for _, want := range tc.wantInMessage {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Probe() error %q does not mention %q", err, want)
				}
			}
		})
	}
}

// TestProbeIsRefusedWhileShuttingDown proves the probe registers on the shared
// connection like every other reader: once the writer is closing, it declines
// rather than racing Start's connection closure.
func TestProbeIsRefusedWhileShuttingDown(t *testing.T) {
	w := NewCHWriter(fakeProbeConn{rows: fullSchemaRows()}, 1, 1, 1,
		time.Second, time.Second, time.Second, time.Second, time.Second, probeMetrics())
	w.database = "kuberecord"

	w.mu.Lock()
	w.closing = true
	w.mu.Unlock()

	if err := w.Probe(context.Background()); err == nil {
		t.Error("Probe succeeded while the writer was shutting down")
	}
}

// TestProbeAgainstAnUnreachableAddress is the acceptance criteria's "unreachable
// addr" case against the real driver rather than a fake: a sink pointed at a
// closed port must report a failure the SinkManager classifies as Unreachable, not
// hang and not panic.
//
// The port is obtained by opening a listener and closing it immediately, so the
// address is genuinely unused rather than merely assumed to be.
func TestProbeAgainstAnUnreachableAddress(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a port: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("closing the reserved listener: %v", err)
	}

	w, err := Open(Config{
		Addr:        addr,
		Database:    "kuberecord",
		Username:    "default",
		DialTimeout: time.Second,
		ReadTimeout: time.Second,
	}, probeMetrics())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := w.conn.Close(); err != nil {
			t.Errorf("closing the connection: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = w.Probe(ctx)
	if err == nil {
		t.Fatal("Probe succeeded against a closed port")
	}
	if errors.Is(err, sink.ErrSchemaInvalid) {
		t.Errorf("an unreachable backend was classified as a schema failure: %v", err)
	}
}
