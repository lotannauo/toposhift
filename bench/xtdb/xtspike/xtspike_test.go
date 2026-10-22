package xtspike

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
	"github.com/lotannauo/toposhift/bench/xtdb/xtreport"
)

func (r *rig) readLoadRecord(dir string) xtreport.Load {
	r.t.Helper()
	var l xtreport.Load
	if err := xtreport.ReadFile(filepath.Join(dir, "load.json"), &l); err != nil {
		r.t.Fatal(err)
	}
	return l
}

func TestNoCommandAndAnUnknownCommandAreRefused(t *testing.T) {
	r := newRig(t)
	if err := r.app.Run(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Errorf("no command: %v", err)
	}
	if err := r.app.Run(context.Background(), []string{"timed"}); err == nil || !strings.Contains(err.Error(), `no command "timed"`) {
		t.Errorf("an unknown command: %v", err)
	}
	// The write label every later timing carries names the read-back.
	if !strings.Contains(WriteLabel, "read-back of the meta document") {
		t.Error(WriteLabel)
	}
}

func TestLoadThenReadThenSizesOfAPlan(t *testing.T) {
	for name, spec := range map[string]runner.Spec{"no retention": tinySpec(), "a retention": retainingSpec()} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			plan, sum := planFile(t, spec)
			p, err := runner.LoadPlan(plan)
			if err != nil {
				t.Fatal(err)
			}
			out := filepath.Join(t.TempDir(), "rec")
			input := []string{"-plan", plan, "-expect-plan", sum}

			// load
			args := func(cmd string, extra ...string) []string {
				return append(append([]string{cmd}, r.common(out)...), append(input, extra...)...)
			}
			if err := r.run(args("load", "-registered", "-label", "tiny")...); err != nil {
				t.Fatalf("%v\n%s\n%s", err, r.out, r.errOut)
			}
			noDuration(t, "load", r.out.String()+r.errOut.String())
			l := r.readLoadRecord(out)
			if !l.Settled || !l.Env.Registered || l.Env.Container == nil || l.Prefix != "" || l.Input.Kind != "plan" || l.Input.PlanSHA256 != sum || l.Input.Label != "tiny" {
				t.Fatalf("%+v", l)
			}
			if l.Input.Records != p.Stream.Records || l.Input.LastSeq != p.Stream.LastSeq || l.Input.StreamDigest != p.Stream.Digest || l.Input.Retentions != len(p.Stream.Retentions) {
				t.Errorf("input %+v, plan stream %+v", l.Input, p.Stream)
			}
			// The writer record is what the model counts for the same stream.
			m := xtmodel.New()
			if _, err := runner.Drive(context.Background(), p.Spec, countingSink{m}); err != nil {
				t.Fatal(err)
			}
			want := writerOf(m.Counts(), m.Counts().Batches+uint64(len(p.Stream.Retentions)))
			if !reflect.DeepEqual(l.Writer, want) || l.Writer.Records != p.Stream.Records || l.Writer.Retentions != uint64(len(p.Stream.Retentions)) {
				t.Errorf("writer %+v, want %+v", l.Writer, want)
			}
			if l.Writer.Documents == 0 || l.Writer.Statements < l.Writer.Documents || len(l.Writer.Tables) != xtmodel.NumTables {
				t.Errorf("writer %+v", l.Writer)
			}

			// A second load into the node that holds the stream is refused, and writes nothing.
			if err := r.run(args("load")...); err == nil || !strings.Contains(err.Error(), "already exists") {
				t.Errorf("a second load into the same output: %v", err)
			}
			if err := r.run(append([]string{"load"}, append(r.common(filepath.Join(t.TempDir(), "other")), input...)...)...); err == nil || !strings.Contains(err.Error(), "already holds a stream") {
				t.Errorf("a second load into the same node: %v", err)
			}

			// read
			if err := r.run(args("read", "-registered", "-load", filepath.Join(out, "load.json"))...); err != nil {
				t.Fatalf("%v\n%s", err, r.out)
			}
			noDuration(t, "read", r.out.String())
			var rd xtreport.Read
			if err := xtreport.ReadFile(filepath.Join(out, "read.json"), &rd); err != nil {
				t.Fatal(err)
			}
			if !rd.G0.OK || rd.G0.Queries != len(p.Queries) || rd.G0.Matched != rd.G0.Queries || rd.G0.AnswersDigest == "" || !rd.Settled || len(rd.G0.Mismatches) != 0 {
				t.Fatalf("%+v", rd.G0)
			}
			if rd.Reads.ClientFold != 0 || rd.Reads.Native < uint64(len(p.Queries)) {
				t.Errorf("reads %+v", rd.Reads)
			}
			wantSum, err := fileSHA(filepath.Join(out, "load.json"))
			if err != nil || rd.LoadSHA256 != wantSum {
				t.Errorf("load sha %s, want %s (%v)", rd.LoadSHA256, wantSum, err)
			}
			families := map[string]uint64{}
			for _, c := range rd.Capability {
				families[c.Family] = c.Rows
			}
			if len(families) != 4 || families["retention"] != uint64(len(p.Stream.Retentions)) || families["sub-microsecond boundary"] != 0 {
				t.Errorf("capability rows %+v", rd.Capability)
			}
			if !strings.Contains(r.out.String(), "G0 total: ") || !strings.Contains(r.out.String(), "capability row: read work") {
				t.Errorf("%s", r.out)
			}

			// sizes
			data := filepath.Join(t.TempDir(), "data")
			for dir, n := range map[string]int{"log/a": 1000, "log/b": 24, "storage/l00/x": 4000, "storage/l01/y": 1} {
				if err := os.MkdirAll(filepath.Dir(filepath.Join(data, dir)), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(data, dir), make([]byte, n), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			r.dirOfContainer(data)
			if err := r.run(args("sizes", "-registered", "-load", filepath.Join(out, "load.json"), "-data", data)...); err != nil {
				t.Fatalf("%v\n%s", err, r.out)
			}
			noDuration(t, "sizes", r.out.String())
			var sz xtreport.Sizes
			if err := xtreport.ReadFile(filepath.Join(out, "sizes.json"), &sz); err != nil {
				t.Fatal(err)
			}
			if !sz.Settled || sz.Bytes != 5025 || sz.RecordsHeld != p.Stream.Records || len(sz.Dirs) != 2 || sz.Dirs[0].Path != "log" || sz.Dirs[0].Bytes != 1024 || sz.Dirs[1].Files != 2 || sz.ListingDigest == "" {
				t.Fatalf("%+v", sz)
			}
			if want := 5025 / float64(p.Stream.Records); sz.BytesPerRecord != want {
				t.Errorf("bytes per record %v, want %v", sz.BytesPerRecord, want)
			}
		})
	}
}

// dirOfContainer makes the fake docker say the container keeps its files in dir.
func (r *rig) dirOfContainer(dir string) { r.dir = dir }

// countingSink gives a model a stream the way the engine is given one: retentions count as
// transactions of their own on the node, not on the model.
type countingSink struct{ m *xtmodel.Model }

func (c countingSink) Write(b []engine.Record) error { return c.m.Write(b) }
func (c countingSink) Retain(h time.Time) error      { return c.m.Retain(h) }
