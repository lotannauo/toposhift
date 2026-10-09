package pebblekv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// Meta keys hold what a layout needs to survive a reopening. Each layout
// encodes the key for its own comparer; the names and the value forms are
// shared.
const (
	MetaFormat  = "format"
	MetaLastSeq = "lastSeq"
	// MetaHorizon is the name of the spike's single retention horizon, a time with
	// no sequence number. A store that keeps a horizon per layer uses
	// [HorizonMetaName] and [EncodeLayerHorizon].
	MetaHorizon = "horizon"
)

// HorizonMetaName is the meta key name of a layer's retention horizon: "horizon/"
// and the layer's number, "horizon/1" for the first layer.
func HorizonMetaName(l catalog.Layer) string {
	return MetaHorizon + "/" + strconv.Itoa(int(l))
}

// GetMeta returns the value at a meta key, or nil if there is none.
func (k *KV) GetMeta(key []byte) ([]byte, error) {
	v, closer, err := k.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	return slices.Clone(v), nil
}

// EncodeSeq is the stored form of a sequence number.
func EncodeSeq(seq uint64) []byte { return binary.BigEndian.AppendUint64(nil, seq) }

// DecodeSeq reads a sequence number written by [EncodeSeq]; nil is zero, the
// token of an empty store.
func DecodeSeq(b []byte) (uint64, error) {
	if b == nil {
		return 0, nil
	}
	if len(b) != 8 {
		return 0, fmt.Errorf("sequence number is %d bytes, want 8: %w", len(b), ErrValue)
	}
	return binary.BigEndian.Uint64(b), nil
}

// EncodeHorizon is the stored form of a retention horizon that has no sequence
// number: the instant exactly, whatever it is, in time.Time's own binary form.
// It is the spike's form, kept for reading what the spike wrote; a store writes
// [EncodeLayerHorizon].
func EncodeHorizon(h time.Time) ([]byte, error) { return h.MarshalBinary() }

// DecodeHorizon reads a horizon written by [EncodeHorizon]; nil is the zero
// time, the horizon of a store that has never retained.
func DecodeHorizon(b []byte) (time.Time, error) {
	var h time.Time
	if b == nil {
		return h, nil
	}
	if err := h.UnmarshalBinary(b); err != nil {
		return time.Time{}, fmt.Errorf("horizon: %w: %w", err, ErrValue)
	}
	return h, nil
}

// seqBytes is the width of the sequence number at the end of a layer horizon.
const seqBytes = 8

// EncodeLayerHorizon is the stored form of a layer's retention horizon: the
// instant exactly, in time.Time's binary form (its length varies by one byte
// with the zone), followed by the sequence number as 8 bytes, big endian. The
// store contract's horizon carries both, because a read pinned below the
// horizon's sequence number is refused as well as one before its time.
func EncodeLayerHorizon(h time.Time, seq uint64) ([]byte, error) {
	b, err := h.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("horizon: %w: %w", err, ErrValue)
	}
	return binary.BigEndian.AppendUint64(b, seq), nil
}

// DecodeLayerHorizon reads a horizon written by [EncodeLayerHorizon]; nil is the
// zero time and sequence number zero, the horizon of a layer that has never
// retained.
func DecodeLayerHorizon(b []byte) (time.Time, uint64, error) {
	if b == nil {
		return time.Time{}, 0, nil
	}
	if len(b) < seqBytes {
		return time.Time{}, 0, fmt.Errorf("horizon is %d bytes, shorter than its sequence number: %w", len(b), ErrValue)
	}
	cut := len(b) - seqBytes
	var h time.Time
	if err := h.UnmarshalBinary(b[:cut]); err != nil {
		return time.Time{}, 0, fmt.Errorf("horizon: %w: %w", err, ErrValue)
	}
	return h, binary.BigEndian.Uint64(b[cut:]), nil
}

// CheckFormat makes sure a database holds the format a layout expects, writing
// it if the database is new, and refuses one written by another layout or
// another version of this one. key is the layout's meta key for [MetaFormat].
func (k *KV) CheckFormat(key, want []byte) error {
	got, err := k.GetMeta(key)
	if err != nil {
		return err
	}
	switch {
	case got == nil:
		return k.Set(key, want, k.wo)
	case !slices.Equal(got, want):
		return fmt.Errorf("pebblekv: the database holds format %q, this is %q", got, want)
	}
	return nil
}
