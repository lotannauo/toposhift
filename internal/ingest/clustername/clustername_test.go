package clustername_test

import (
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/ingest/clustername"
)

// in builds an input; the fields not named are zero.
type in = clustername.Input

func TestDecide(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		in   in
		// record, off say what the decision does; msg and warn are substrings the
		// message and the warning must contain. A decision that does nothing has no
		// message; one with no warn has no warning.
		record, off bool
		msg, warn   string
	}{
		// A store without the key.
		{
			name:   "absent, no records: record the configured name",
			in:     in{Configured: "prod", ConfiguredSet: true},
			record: true,
		},
		{
			name:   "absent, no records: record that none is set",
			in:     in{},
			record: true,
		},
		{
			name:   "absent, no records, a matching flag: still just recorded",
			in:     in{Configured: "prod", ConfiguredSet: true, Accept: "prod", AcceptGiven: true},
			record: true,
		},
		{
			name: "absent, records: ingest off, the recorded name unknown",
			in:   in{LastSeq: 7, Configured: "prod", ConfiguredSet: true},
			off:  true, msg: `started with cluster name unknown and is now configured with "prod"; start once with --accept-new-cluster-name=prod`,
		},
		{
			name: "absent, records, none configured: ingest off, accepted by an empty flag",
			in:   in{LastSeq: 7},
			off:  true, msg: "started with cluster name unknown and is now configured with (none set); start once with --accept-new-cluster-name= to",
		},
		{
			name:   "absent, records, accepted by the configured name",
			in:     in{LastSeq: 7, Configured: "prod", ConfiguredSet: true, Accept: "prod", AcceptGiven: true},
			record: true, msg: "accepted the change of cluster name from unknown",
		},
		{
			name:   "absent, records, none configured, accepted by an empty flag",
			in:     in{LastSeq: 7, Accept: "", AcceptGiven: true},
			record: true, msg: "accepted the change of cluster name from unknown",
		},
		{
			name: "absent, records, a flag naming another value",
			in:   in{LastSeq: 7, Configured: "prod", ConfiguredSet: true, Accept: "dev", AcceptGiven: true},
			off:  true, msg: "ingest is off", warn: "--accept-new-cluster-name=dev is ignored",
		},

		// The key is there and equal.
		{
			name: "present and equal: nothing to do",
			in:   in{Present: true, Recorded: "prod", RecordedSet: true, LastSeq: 9, Configured: "prod", ConfiguredSet: true},
		},
		{
			name: "present, none set, none configured: nothing to do",
			in:   in{Present: true, LastSeq: 9},
		},
		{
			name: "present and equal, a flag naming it: nothing to do, no warning",
			in:   in{Present: true, Recorded: "prod", RecordedSet: true, LastSeq: 9, Configured: "prod", ConfiguredSet: true, Accept: "prod", AcceptGiven: true},
		},
		{
			name: "present and equal, an empty flag: it names something else, so a warning",
			in:   in{Present: true, Recorded: "prod", RecordedSet: true, LastSeq: 9, Configured: "prod", ConfiguredSet: true, AcceptGiven: true},
			warn: "--accept-new-cluster-name= is ignored",
		},
		{
			name: "present and equal, no records: nothing to do",
			in:   in{Present: true, Recorded: "prod", RecordedSet: true, Configured: "prod", ConfiguredSet: true},
		},

		// A changed value.
		{
			name: "present and different: ingest off, names both and the flag",
			in:   in{Present: true, Recorded: "prod", RecordedSet: true, LastSeq: 9, Configured: "staging", ConfiguredSet: true},
			off:  true, msg: `ingest is off: this store was started with cluster name "prod" and is now configured with "staging"; start once with --accept-new-cluster-name=staging to accept the change`,
		},
		{
			name: "present and different, no records: still off",
			in:   in{Present: true, Recorded: "prod", RecordedSet: true, Configured: "staging", ConfiguredSet: true},
			off:  true, msg: "ingest is off",
		},
		{
			name: "a name set, now none configured: off, the flag is empty",
			in:   in{Present: true, Recorded: "prod", RecordedSet: true, LastSeq: 9},
			off:  true, msg: `cluster name "prod" and is now configured with (none set); start once with --accept-new-cluster-name= to accept`,
		},
		{
			name: "none set, now a name configured: off",
			in:   in{Present: true, LastSeq: 9, Configured: "prod", ConfiguredSet: true},
			off:  true, msg: `cluster name (none set) and is now configured with "prod"; start once with --accept-new-cluster-name=prod to accept`,
		},
		{
			name: "names differ by case only: a change",
			in:   in{Present: true, Recorded: "Prod", RecordedSet: true, LastSeq: 9, Configured: "prod", ConfiguredSet: true},
			off:  true, msg: "ingest is off",
		},
		{
			name:   "accepted by the configured name",
			in:     in{Present: true, Recorded: "prod", RecordedSet: true, LastSeq: 9, Configured: "staging", ConfiguredSet: true, Accept: "staging", AcceptGiven: true},
			record: true, msg: `from "prod" to "staging"`,
		},
		{
			name:   "accepted: a name set to none, by an empty flag",
			in:     in{Present: true, Recorded: "prod", RecordedSet: true, LastSeq: 9, Accept: "", AcceptGiven: true},
			record: true, msg: `from "prod" to (none set)`,
		},
		{
			name:   "accepted: none set to a name",
			in:     in{Present: true, LastSeq: 9, Configured: "prod", ConfiguredSet: true, Accept: "prod", AcceptGiven: true},
			record: true, msg: `from (none set) to "prod"`,
		},
		{
			name: "an empty flag does not accept a configured name",
			in:   in{Present: true, Recorded: "prod", RecordedSet: true, LastSeq: 9, Configured: "staging", ConfiguredSet: true, AcceptGiven: true},
			off:  true, msg: "ingest is off", warn: "--accept-new-cluster-name= is ignored",
		},
		{
			name: "a flag naming the configured name does not accept none",
			in:   in{Present: true, Recorded: "prod", RecordedSet: true, LastSeq: 9, Accept: "staging", AcceptGiven: true},
			off:  true, msg: "ingest is off", warn: "--accept-new-cluster-name=staging is ignored",
		},
		{
			name: "a flag not given accepts nothing, whatever its value",
			in:   in{Present: true, Recorded: "prod", RecordedSet: true, LastSeq: 9, Configured: "staging", ConfiguredSet: true, Accept: "staging"},
			off:  true, msg: "ingest is off",
		},
		{
			name: "a stale flag in a unit file: it names the old accepted value",
			in:   in{Present: true, Recorded: "staging", RecordedSet: true, LastSeq: 9, Configured: "qa", ConfiguredSet: true, Accept: "staging", AcceptGiven: true},
			off:  true, msg: `configured with "qa"`, warn: "--accept-new-cluster-name=staging is ignored",
		},
		{
			name: "a stale empty flag in a unit file",
			in:   in{Present: true, Recorded: "staging", RecordedSet: true, LastSeq: 9, Configured: "qa", ConfiguredSet: true, AcceptGiven: true},
			off:  true, warn: "--accept-new-cluster-name= is ignored",
		},
		{
			name: "the flag is matched exactly, not by prefix or case",
			in:   in{Present: true, Recorded: "prod", RecordedSet: true, LastSeq: 9, Configured: "staging", ConfiguredSet: true, Accept: "Staging", AcceptGiven: true},
			off:  true, warn: "is ignored",
		},

		// A return to an earlier name is a change like any other.
		{
			name: "returning to an earlier name needs accepting",
			in:   in{Present: true, Recorded: "staging", RecordedSet: true, LastSeq: 20, Configured: "prod", ConfiguredSet: true},
			off:  true, msg: `cluster name "staging" and is now configured with "prod"`,
		},
		{
			name:   "returning to an earlier name, accepted",
			in:     in{Present: true, Recorded: "staging", RecordedSet: true, LastSeq: 20, Configured: "prod", ConfiguredSet: true, Accept: "prod", AcceptGiven: true},
			record: true,
		},
		{
			name: "returning to an earlier name, with the flag that accepted the last change",
			in:   in{Present: true, Recorded: "staging", RecordedSet: true, LastSeq: 20, Configured: "prod", ConfiguredSet: true, Accept: "staging", AcceptGiven: true},
			off:  true, warn: "is ignored",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := clustername.Decide(tc.in)
			if d.Record != tc.record {
				t.Errorf("Record = %v, want %v", d.Record, tc.record)
			}
			if d.IngestOff != tc.off {
				t.Errorf("IngestOff = %v, want %v", d.IngestOff, tc.off)
			}
			if !tc.record && !tc.off && d.Message != "" {
				t.Errorf("Message = %q, want none when nothing is to be done", d.Message)
			}
			if tc.off && d.Message == "" {
				t.Errorf("no Message, though ingest is off")
			}
			if !strings.Contains(d.Message, tc.msg) {
				t.Errorf("Message = %q, want it to contain %q", d.Message, tc.msg)
			}
			if tc.warn == "" && d.Warning != "" {
				t.Errorf("Warning = %q, want none", d.Warning)
			}
			if !strings.Contains(d.Warning, tc.warn) {
				t.Errorf("Warning = %q, want it to contain %q", d.Warning, tc.warn)
			}
			// Ingest is never both off and recording: a change that is not accepted
			// writes nothing, and one that is accepted starts ingest.
			if d.Record && d.IngestOff {
				t.Errorf("the decision records and keeps ingest off: %+v", d)
			}
		})
	}
}

// Over every combination of the inputs that matter, a store whose recorded value
// is not the configured one starts only with a flag that names the configured
// value, and a flag that does not name it never starts ingest or records.
func TestDecideAcceptsOnlyTheConfiguredValue(t *testing.T) {
	t.Parallel()
	names := []string{"", "prod", "staging"}
	for _, present := range []bool{false, true} {
		for _, lastSeq := range []uint64{0, 5} {
			for _, rec := range names {
				for _, recSet := range []bool{false, true} {
					for _, cfg := range names {
						for _, cfgSet := range []bool{false, true} {
							for _, given := range []bool{false, true} {
								for _, accept := range names {
									// An empty name is not a name: skip the inputs a caller cannot make.
									if (recSet && rec == "") || (!recSet && rec != "") || (cfgSet && cfg == "") || (!cfgSet && cfg != "") {
										continue
									}
									if !present && (rec != "" || recSet) {
										continue
									}
									if !given && accept != "" {
										continue
									}
									i := in{Recorded: rec, RecordedSet: recSet, Present: present, LastSeq: lastSeq, Configured: cfg, ConfiguredSet: cfgSet, Accept: accept, AcceptGiven: given}
									d := clustername.Decide(i)
									same := present && rec == cfg && recSet == cfgSet
									fresh := !present && lastSeq == 0
									named := given && accept == cfg
									switch {
									case same:
										if d.Record || d.IngestOff {
											t.Errorf("%+v: an unchanged name does something: %+v", i, d)
										}
									case fresh:
										if !d.Record || d.IngestOff || d.Message != "" {
											t.Errorf("%+v: a store with no records does not just record: %+v", i, d)
										}
									case named:
										if !d.Record || d.IngestOff || d.Message == "" {
											t.Errorf("%+v: an accepted change is not recorded and logged: %+v", i, d)
										}
									default:
										if d.Record || !d.IngestOff || d.Message == "" {
											t.Errorf("%+v: a change that is not accepted does not stop ingest: %+v", i, d)
										}
									}
									if given && !named && d.Warning == "" {
										t.Errorf("%+v: a flag naming another value gave no warning", i)
									}
									if (!given || named) && d.Warning != "" {
										t.Errorf("%+v: a warning with no stray flag: %q", i, d.Warning)
									}
								}
							}
						}
					}
				}
			}
		}
	}
}
