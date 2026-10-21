package vmengine

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Metrics is the server's own counters, as /metrics reports them, by the name with its
// labels as printed (vm_rows{type="storage/small"}).
type Metrics map[string]float64

func parseMetrics(text string) (Metrics, error) {
	m := Metrics{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			return nil, fmt.Errorf("vmengine: a line of /metrics with no value: %q", line)
		}
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if err != nil {
			return nil, fmt.Errorf("vmengine: a line of /metrics with a value that is not a number: %q", line)
		}
		m[strings.TrimSpace(line[:i])] = v
	}
	return m, nil
}

// Metrics reads /metrics.
func (c *Client) Metrics(ctx context.Context) (Metrics, error) {
	b, err := c.ok(ctx, http.MethodGet, "/metrics", nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return parseMetrics(string(b))
}

// sum adds the values of the named metric with each of the type labels.
func (m Metrics) sum(name string, types ...string) float64 {
	var t float64
	for _, ty := range types {
		t += m[name+`{type="`+ty+`"}`]
	}
	return t
}

// Counters are the numbers of the server that decide whether everything written arrived:
// what it received, what it stored, what it refused and how many series it made.
type Counters struct {
	// Received are the rows of the remote-write protocol the server parsed, and Added the
	// rows it added to storage: a sample or a marker is one row.
	Received, Added uint64
	// Ignored are the rows it dropped, for any reason (a timestamp out of retention, a
	// label too long, too many labels).
	Ignored uint64
	// NewSeries are the series it created.
	NewSeries uint64
	// Stored are the rows held in storage parts (in memory, small and big): after
	// everything has been flushed it is every row that was added.
	Stored uint64
	// PendingRows are rows not yet in a part.
	PendingRows uint64
	// Parts are the parts in memory, small and big; Merging the merges running, the
	// forced merge among them; Merges the merges done; DataBytes the bytes of the parts.
	InMemoryParts, SmallParts, BigParts uint64
	Merging, Merges, DataBytes          uint64
}

// Counters reads the server's counters.
func (c *Client) Counters(ctx context.Context) (Counters, error) {
	m, err := c.Metrics(ctx)
	if err != nil {
		return Counters{}, err
	}
	for _, name := range []string{`vm_rows_inserted_total{type="promremotewrite"}`, "vm_rows_added_to_storage_total", "vm_new_timeseries_created_total", `vm_rows{type="storage/small"}`, "vm_active_force_merges"} {
		if _, ok := m[name]; !ok {
			return Counters{}, fmt.Errorf("%w: %s", ErrNoMetric, name)
		}
	}
	st := []string{"storage/inmemory", "storage/small", "storage/big"}
	var ignored float64
	for k, v := range m {
		if strings.HasPrefix(k, "vm_rows_ignored_total{") {
			ignored += v
		}
	}
	return Counters{
		Received:      uint64(m[`vm_rows_inserted_total{type="promremotewrite"}`]),
		Added:         uint64(m["vm_rows_added_to_storage_total"]),
		Ignored:       uint64(ignored),
		NewSeries:     uint64(m["vm_new_timeseries_created_total"]),
		Stored:        uint64(m.sum("vm_rows", st...)),
		PendingRows:   uint64(m[`vm_pending_rows{type="storage"}`]),
		InMemoryParts: uint64(m.sum("vm_parts", "storage/inmemory")),
		SmallParts:    uint64(m.sum("vm_parts", "storage/small")),
		BigParts:      uint64(m.sum("vm_parts", "storage/big")),
		Merging:       uint64(m.sum("vm_active_merges", "storage/inmemory", "storage/small", "storage/big", "indexdb/inmemory", "indexdb/file") + m["vm_active_force_merges"]),
		Merges:        uint64(m.sum("vm_merges_total", "storage/inmemory", "storage/small", "storage/big", "indexdb/inmemory", "indexdb/file")),
		DataBytes:     uint64(m.sum("vm_data_size_bytes", st...)),
	}, nil
}

// Settle waits until the server is idle: no row waits to be put in a part, none is in
// memory, no merge runs, and the parts, the merges done and the bytes held have not
// changed over stable consecutive reads, interval apart. It returns the counters of the
// last read. The server flushes the parts in memory by itself within a few seconds, and
// merges the parts it has in the background, so only the absence of change over some
// reads says nothing more is going to happen.
func (c *Client) Settle(ctx context.Context, interval time.Duration, stable int) (Counters, error) {
	var prev Counters
	same := 0
	for {
		cur, err := c.Counters(ctx)
		if err != nil {
			return Counters{}, err
		}
		idle := cur.PendingRows == 0 && cur.InMemoryParts == 0 && cur.Merging == 0
		if idle && same > 0 && cur.SmallParts == prev.SmallParts && cur.BigParts == prev.BigParts && cur.Merges == prev.Merges && cur.DataBytes == prev.DataBytes {
			same++
		} else if idle {
			same = 1
		} else {
			same = 0
		}
		prev = cur
		if same >= stable {
			return cur, nil
		}
		select {
		case <-ctx.Done():
			return Counters{}, ctx.Err()
		case <-time.After(interval):
		}
	}
}

// Compact brings the data to its merged state: flush, wait until the parts in memory are
// in parts of their own, force a merge (which covers only the parts there are then), and
// wait until the server is idle again. It returns the counters then. The size of the
// result is a measure only on the runner class it is made on (the order of the merges is
// not fixed).
func (c *Client) Compact(ctx context.Context, interval time.Duration, stable int) (Counters, error) {
	if err := c.Flush(ctx); err != nil {
		return Counters{}, err
	}
	if _, err := c.Settle(ctx, interval, stable); err != nil {
		return Counters{}, err
	}
	if err := c.ForceMerge(ctx); err != nil {
		return Counters{}, err
	}
	return c.Settle(ctx, interval, stable)
}
