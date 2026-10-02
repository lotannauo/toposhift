package oracle

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// Durable is the oracle with a file under a directory: every batch and every
// retention it accepts is appended to it, and opening the directory again
// replays them. It exists so the reopen check has an engine that is correct by
// construction and persists, which the in-memory oracle cannot be, and so a
// harness test has something to break. It survives Close and reopening, not a
// crash: nothing is synced.
type Durable struct {
	*Oracle

	mu   sync.Mutex // keeps the file in the order the oracle applied things
	file *os.File
	enc  *json.Encoder
}

var _ engine.Engine = (*Durable)(nil)

// logName is the file Durable keeps in its directory.
const logName = "oracle.log"

// OpenDurable opens the oracle kept under dir, which is empty (a new, empty
// oracle) or holds what an earlier Durable left (that oracle, exactly).
func OpenDurable(dir string) (*Durable, error) {
	path := filepath.Join(dir, logName)
	d := &Durable{Oracle: New()}
	if err := d.replay(path); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	d.file, d.enc = f, json.NewEncoder(f)
	return d, nil
}

// entry is one line of the log: a batch that was accepted, or a retention.
type entry struct {
	Write  []wireRecord `json:"write,omitempty"`
	Retain *time.Time   `json:"retain,omitempty"`
}

// wireRecord is engine.Record as the log keeps it: fingerprints as text, with
// the empty string for none.
type wireRecord struct {
	Layer       catalog.Layer        `json:"layer"`
	SubjectKind engine.SubjectKind   `json:"subjectKind"`
	A           string               `json:"a"`
	B           string               `json:"b"`
	Relation    catalog.RelationType `json:"relation"`
	Producer    lifecycle.Producer   `json:"producer"`
	EventTime   time.Time            `json:"eventTime"`
	Seq         uint64               `json:"seq"`
	Op          lifecycle.Kind       `json:"op"`
	TTL         time.Duration        `json:"ttl"`
	Through     time.Time            `json:"through"`
	Payload     []byte               `json:"payload"`
}

func toWire(r engine.Record) wireRecord {
	text := func(f identity.Fingerprint) string { return f.String() } // "" for the zero fingerprint
	return wireRecord{
		Layer: r.Layer, SubjectKind: r.Subject.Kind, A: text(r.Subject.A), B: text(r.Subject.B), Relation: r.Subject.Relation,
		Producer: r.Producer, EventTime: r.EventTime, Seq: r.Seq, Op: r.Kind, TTL: r.TTL, Through: r.Through, Payload: r.Payload,
	}
}

func fromWire(w wireRecord) (engine.Record, error) {
	parse := func(s string) (identity.Fingerprint, error) {
		if s == "" {
			return identity.Fingerprint{}, nil
		}
		return identity.ParseFingerprint(s)
	}
	a, err := parse(w.A)
	if err != nil {
		return engine.Record{}, err
	}
	b, err := parse(w.B)
	if err != nil {
		return engine.Record{}, err
	}
	return engine.Record{
		Layer: w.Layer, Subject: engine.Subject{Kind: w.SubjectKind, A: a, B: b, Relation: w.Relation},
		Producer: w.Producer, EventTime: w.EventTime, Seq: w.Seq, Kind: w.Op, TTL: w.TTL, Through: w.Through, Payload: w.Payload,
	}, nil
}

// replay applies what an earlier Durable left in the file at path, if any.
func (d *Durable) replay(path string) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	dec := json.NewDecoder(f)
	for n := 1; ; n++ {
		var e entry
		if err := dec.Decode(&e); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return fmt.Errorf("oracle: replaying %s, entry %d: %w", path, n, err)
		}
		switch {
		case e.Retain != nil:
			if err := d.Oracle.Retain(*e.Retain); err != nil {
				return err
			}
		default:
			recs := make([]engine.Record, len(e.Write))
			for i, w := range e.Write {
				if recs[i], err = fromWire(w); err != nil {
					return fmt.Errorf("oracle: replaying %s, entry %d: %w", path, n, err)
				}
			}
			if err := d.Oracle.Write(recs); err != nil {
				return fmt.Errorf("oracle: replaying %s, entry %d: %w", path, n, err)
			}
		}
	}
}

// Write implements [engine.Engine]. A batch the oracle refuses is not logged.
func (d *Durable) Write(batch []engine.Record) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.Oracle.Write(batch); err != nil {
		return err
	}
	e := entry{Write: make([]wireRecord, len(batch))}
	for i, r := range batch {
		e.Write[i] = toWire(r)
	}
	return d.enc.Encode(e)
}

// Retain implements [engine.Engine].
func (d *Durable) Retain(horizon time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.Oracle.Retain(horizon); err != nil {
		return err
	}
	return d.enc.Encode(entry{Retain: &horizon})
}

// Size implements [engine.Engine]: the bytes in the log.
func (d *Durable) Size() (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	info, err := d.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Close implements [engine.Engine].
func (d *Durable) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.file.Close()
}
