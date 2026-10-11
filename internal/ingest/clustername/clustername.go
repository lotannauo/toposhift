// Package clustername decides, at boot, whether ingest may start given the
// cluster name the store recorded and the one the process is configured with.
//
// A store records the cluster name it was started with, or that none was set. A
// change of name splits the history: records under the old name stay, and their
// references lapse by their TTL. So when the configured name differs from the
// recorded one, ingest stays off until the operator has said so, by starting once
// with --accept-new-cluster-name=<the configured name>. Accepting that no name is
// set takes an empty value, --accept-new-cluster-name=. A flag that names any other
// value is ignored with a warning, so a flag left in a unit file cannot accept a
// later change. Accepting only acknowledges the split and records the new name;
// returning to an earlier name is a change like any other.
//
// A store with records and no recorded name (it was written before the name was
// recorded, or restored without it) is treated as a changed name, with the recorded
// name unknown. A store with no records records the configured name at its start.
//
// The package is pure: it reads and writes nothing. The caller reads the recorded
// name from the store, calls [Decide], writes the name when [Decision.Record] says
// to, before ingest starts, and keeps the receiver off with [Decision.Message] when
// [Decision.IngestOff] says to.
package clustername

import "fmt"

// FlagName is the name of the flag that accepts a change of cluster name.
const FlagName = "accept-new-cluster-name"

// Input is what the decision is made from.
type Input struct {
	// Recorded and RecordedSet are the name the store recorded and whether it
	// recorded one (false when it recorded that none was set). They mean nothing
	// when Present is false.
	Recorded    string
	RecordedSet bool
	// Present says the store has the key.
	Present bool
	// LastSeq is the store's last sequence number: 0 for a store with no records.
	LastSeq uint64
	// Configured and ConfiguredSet are the configured name and whether one is
	// configured. Configured is empty when ConfiguredSet is false.
	Configured    string
	ConfiguredSet bool
	// Accept is the value of the flag, and AcceptGiven says the flag was given: an
	// empty value that was given accepts that no name is set.
	Accept      string
	AcceptGiven bool
}

// Decision is what the caller does.
type Decision struct {
	// Record says to write the configured value to the store before ingest starts.
	Record bool
	// IngestOff says ingest must not start.
	IngestOff bool
	// Message is for the boot log and, while IngestOff, the receiver's 503. It is
	// also set when a change was accepted, for the log alone. It is empty
	// otherwise.
	Message string
	// Warning is for the boot log: the flag was given and names a value other than
	// the configured one, so it was ignored.
	Warning string
}

// Decide applies the rule. The flag accepts a change only when its value equals
// the configured name, or is empty when none is configured.
func Decide(in Input) Decision {
	var d Decision
	configured := ""
	if in.ConfiguredSet {
		configured = in.Configured
	}
	accepted := in.AcceptGiven && in.Accept == configured
	if in.AcceptGiven && !accepted {
		d.Warning = fmt.Sprintf("--%s=%s is ignored: it does not name the configured cluster name %s, so it accepts nothing", FlagName, in.Accept, show(in.Configured, in.ConfiguredSet))
	}

	switch {
	case !in.Present && in.LastSeq == 0:
		// A store with no records has nothing under another name.
		d.Record = true
		return d
	case in.Present && in.RecordedSet == in.ConfiguredSet && (!in.ConfiguredSet || in.Recorded == in.Configured):
		return d
	}

	// The name changed, or the store has records and never recorded a name.
	was := "unknown"
	if in.Present {
		was = show(in.Recorded, in.RecordedSet)
	}
	if accepted {
		d.Record = true
		d.Message = fmt.Sprintf("accepted the change of cluster name from %s to %s: records under the earlier name stay, and their references lapse by their TTL", was, show(in.Configured, in.ConfiguredSet))
		return d
	}
	d.IngestOff = true
	d.Message = fmt.Sprintf("ingest is off: this store was started with cluster name %s and is now configured with %s; start once with --%s=%s to accept the change",
		was, show(in.Configured, in.ConfiguredSet), FlagName, configured)
	return d
}

// show is a name as the messages give it.
func show(name string, set bool) string {
	if !set {
		return "(none set)"
	}
	return fmt.Sprintf("%q", name)
}
