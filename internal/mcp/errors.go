package mcp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// Sentinel errors of the source itself. A tool never returns them: it returns
// an [*Error] that carries the sentence for one of them.
var (
	// ErrShuttingDown is returned for a call the source no longer takes, or ends,
	// because it is being drained.
	ErrShuttingDown = errors.New("the server is shutting down")
	// ErrBusy is returned for a call that waited too long for a free slot.
	ErrBusy = errors.New("the server is busy")
	// ErrNoRangeReads is returned for a range tool on a store without range
	// reads.
	ErrNoRangeReads = errors.New("range answers are not available on this store")
)

// Error is the failure of a tool. Message is a plain sentence for the client;
// Cause is the error it was made from, if any, for errors.Is and errors.As.
type Error struct {
	Message string
	Cause   error
}

func (e *Error) Error() string { return e.Message }

func (e *Error) Unwrap() error { return e.Cause }

// invalid is the refusal of an argument. The message names the argument in
// words; the cause is [store.ErrInvalid].
func invalid(format string, args ...any) error {
	return &Error{Message: fmt.Sprintf(format, args...), Cause: store.ErrInvalid}
}

// quote shows a value a client sent, short enough to put in a sentence.
func quote(v string) string {
	const max = 80
	if len(v) > max {
		cut := max
		for cut > 0 && !utf8.RuneStart(v[cut]) {
			cut--
		}
		v = v[:cut] + "..."
	}
	return fmt.Sprintf("%q", v)
}

// layerError says in which layer a store read failed, so that a refusal for the
// retention horizon can name the layer. The query packages' errors do not carry
// it.
type layerError struct {
	layer catalog.Layer
	err   error
}

func (e *layerError) Error() string { return fmt.Sprintf("layer %s: %v", e.layer, e.err) }

func (e *layerError) Unwrap() error { return e.err }

func inLayer(l catalog.Layer, err error) error {
	if err == nil {
		return nil
	}
	return &layerError{layer: l, err: err}
}

// fail turns an error into the [*Error] a tool returns. An error that already is
// one passes through unchanged.
func (s *Source) fail(err error) error {
	var te *Error
	if errors.As(err, &te) {
		return te
	}
	msg, internal := s.sentence(err)
	if internal && s.logf != nil {
		s.logf("mcp: internal error: %v", err)
	}
	return &Error{Message: msg, Cause: err}
}

// sentence chooses the sentence for an error, in this order:
//
//  1. the source is busy, or the store has no range reads;
//  2. the context was cancelled or its deadline passed (or the source is
//     draining, which is the cause of a cancellation it made);
//  3. the store is closed, or the source withdrew it;
//  4. a read before the retention horizon, which is checked before the
//     [store.ErrInvalid] it wraps;
//  5. a retained layer ([interval.ErrRetained]);
//  6. an invalid argument, or an identity error;
//  7. anything else, an internal error.
func (s *Source) sentence(err error) (msg string, internal bool) {
	switch {
	case errors.Is(err, ErrBusy):
		return "The server is busy; try again shortly.", false
	case errors.Is(err, ErrNoRangeReads):
		return "Range answers are not available on this store until it has an interval read.", false
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		if s.isDraining() {
			return "The server is shutting down.", false
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Sprintf("The call took longer than %s and was stopped.", durationWords(s.callTimeout)), false
		}
		return "The call was cancelled.", false
	case errors.Is(err, ErrShuttingDown), errors.Is(err, store.ErrClosed):
		return "The server is shutting down.", false
	case errors.Is(err, store.ErrBeforeHorizon):
		return s.horizonSentence(err), false
	case errors.Is(err, interval.ErrRetained):
		return s.retainedSentence(), false
	case errors.Is(err, store.ErrInvalid), isIdentityError(err):
		return "The request was refused: " + strings.TrimSuffix(err.Error(), ": "+store.ErrInvalid.Error()) + ".", false
	}
	return "Internal error: " + err.Error(), true
}

func isIdentityError(err error) bool {
	var ae *identity.AttrError
	return errors.As(err, &ae) || errors.Is(err, identity.ErrFingerprint)
}

// durationWords writes a timeout as "30 s", or in the unit it is whole in.
func durationWords(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return fmt.Sprintf("%d h", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%d min", d/time.Minute)
	case d%time.Second == 0:
		return fmt.Sprintf("%d s", d/time.Second)
	}
	return d.String()
}

// retainedLayers lists the layers with a retention horizon, ascending.
func (s *Source) retainedLayers() []catalog.Layer {
	var out []catalog.Layer
	for l := catalog.L0; l <= catalog.L3; l++ {
		if h := s.st.LayerHorizon(l); !h.IsZero() {
			out = append(out, l)
		}
	}
	return out
}

// horizonSentence says that a read asked for history the store no longer keeps.
// The layer comes from the read that failed when the caller knew it, and
// otherwise every layer with a horizon is named.
func (s *Source) horizonSentence(err error) string {
	var le *layerError
	if errors.As(err, &le) {
		return horizonOf(le.layer, s.st.LayerHorizon(le.layer))
	}
	var parts []string
	for _, l := range s.retainedLayers() {
		parts = append(parts, horizonOf(l, s.st.LayerHorizon(l)))
	}
	if len(parts) == 0 {
		return "The store no longer keeps the history asked for."
	}
	return strings.Join(parts, " ")
}

func horizonOf(l catalog.Layer, h store.Horizon) string {
	return fmt.Sprintf("The store no longer keeps history of layer %s before %s or below token %d.", l, fmtTime(h.Time), h.Seq)
}

// retainedSentence says that a range answer needs a layer that has been
// retained. The error of the query packages does not name the layer, so every
// layer with a horizon is named: a range read of any of them fails.
func (s *Source) retainedSentence() string {
	var parts []string
	for _, l := range s.retainedLayers() {
		parts = append(parts, fmt.Sprintf("Range answers for layer %s are unavailable since retention began at %s.", l, fmtTime(s.st.LayerHorizon(l).Time)))
	}
	if len(parts) == 0 {
		return "Range answers are unavailable for a layer that has been retained."
	}
	slices.Sort(parts)
	return strings.Join(parts, " ")
}

func fmtTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
