package xtreport_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/xtdb/xtreport"
)

// TestNoRecordHoldsATime: a record of a run holds counts, bytes and digests. A field of a
// time or duration type, anywhere in any record, fails this test, and so does a field whose
// name says it is a time.
func TestNoRecordHoldsATime(t *testing.T) {
	timeT, durT := reflect.TypeFor[time.Time](), reflect.TypeFor[time.Duration]()
	seen := map[reflect.Type]bool{}
	var walk func(path string, typ reflect.Type)
	walk = func(path string, typ reflect.Type) {
		if typ == timeT || typ == durT {
			t.Errorf("%s is a %s", path, typ)
			return
		}
		switch typ.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			walk(path, typ.Elem())
		case reflect.Map:
			walk(path, typ.Key())
			walk(path, typ.Elem())
		case reflect.Struct:
			if seen[typ] {
				return
			}
			seen[typ] = true
			for i := range typ.NumField() {
				f := typ.Field(i)
				name := strings.ToLower(f.Name)
				for _, bad := range []string{"duration", "elapsed", "latency", "timestamp"} {
					if strings.Contains(name, bad) {
						t.Errorf("%s.%s is named as a time", path, f.Name)
					}
				}
				walk(path+"."+f.Name, f.Type)
			}
		}
	}
	for _, v := range []any{xtreport.Load{}, xtreport.Read{}, xtreport.Sizes{}} {
		walk(reflect.TypeOf(v).Name(), reflect.TypeOf(v))
	}
	if len(seen) < 8 {
		t.Errorf("the walk saw %d types", len(seen))
	}
}

func TestARecordIsWrittenOnceAndReadBack(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out", "deeper")
	want := xtreport.Load{
		Version: xtreport.Version, Kind: "load", Prefix: "p", Settled: true,
		Env:    xtreport.Env{Commit: "abc", Overrides: []string{"TOPOSHIFT_XTDB_CONFIG"}},
		Writer: xtreport.Writer{Batches: 3, Tables: []xtreport.Table{{Name: "seg_f", Inserts: 4}}},
	}
	if err := xtreport.Write(dir, "load.json", want); err != nil {
		t.Fatal(err)
	}
	if err := xtreport.Write(dir, "load.json", want); !errors.Is(err, os.ErrExist) {
		t.Fatalf("a second write: %v", err)
	}
	var got xtreport.Load
	if err := xtreport.ReadFile(filepath.Join(dir, "load.json"), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%+v, want %+v", got, want)
	}
	if info, err := os.Stat(filepath.Join(dir, "load.json")); err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("%v, %v", info, err)
	}
}

func TestARecordOfAnotherShapeIsRefused(t *testing.T) {
	dir := t.TempDir()
	put := func(name, text string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	var l xtreport.Load
	for name, text := range map[string]string{
		"an unknown field": `{"Version":1,"Kind":"load","Surprise":1}`,
		"another version":  `{"Version":2,"Kind":"load"}`,
		"no version":       `{"Kind":"load"}`,
		"not json":         `{`,
	} {
		if err := xtreport.ReadFile(put(name+".json", text), &l); !errors.Is(err, xtreport.ErrFormat) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := xtreport.ReadFile(filepath.Join(dir, "missing.json"), &l); err == nil {
		t.Error("a missing record was read")
	}
}
