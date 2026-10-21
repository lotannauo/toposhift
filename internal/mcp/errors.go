package mcp

import (
	"context"
	"errors"
	"fmt"
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
	// ErrNoRangeReads is returned for a range tool on a store that [RangeReadsFor]
	// does not allow.
	ErrNoRangeReads = errors.New("range answers are not available on this store")
	// ErrNoListing is returned by find_entities on a store whose source found no
	// listing read (see [Lister]).
	ErrNoListing = errors.New("listing is not available on this store")
	// ErrFutureToken is returned for a subgraph question whose asof is above the
	// store's LastSeq: that surface refuses it and never clamps it.
	ErrFutureToken = errors.New("the token is ahead of the store")
	// ErrAnswerTooLarge is returned for a subgraph answer over its size cap: it is
	// refused whole, never cut.
	ErrAnswerTooLarge = errors.New("the answer is over the size cap")
)

// Error is the failure of a tool. Message is a plain sentence for the client;
// Cause is the error it was made from, if any, for errors.Is and errors.As.
// Cause may carry ids the Message withholds; print Message, never Cause.
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
// one passes through with its sentence unchanged, unless the sentence prints an
// endpoint id the request did not name.
//
// No sentence prints such an id. The sentence of an internal error also prints no
// raw hex key, and the log line prints no endpoint id and no raw hex key at all:
// a log can be shipped off the machine, which no request authorized. The cause
// keeps the error as it was.
func (s *Source) fail(err error, req *request) error {
	var te *Error
	if errors.As(err, &te) {
		if msg := redactEndpoints(te.Message, req.named); msg != te.Message {
			return &Error{Message: msg, Cause: te.Cause}
		}
		return te
	}
	return &Error{Message: s.safeSentence(err, req.named, ""), Cause: err}
}

// safeSentence is the sentence for an error that is not already a tool's: it
// prints no endpoint id that named does not hold, and, for an internal error, no
// raw hex key either. An internal error is logged, through redactLog, under the
// label what ("" or " reading names", for example). Every place that turns an
// error into text for a client or a log goes through here.
func (s *Source) safeSentence(err error, named map[string]bool, what string) string {
	msg, internal := s.sentence(err)
	msg = redactEndpoints(msg, named)
	if internal {
		msg = redactHexRuns(msg)
		if s.logf != nil {
			s.logf("mcp: internal error%s: %s", what, redactLog(err.Error()))
		}
	}
	return msg
}

// sentence chooses the sentence for an error, in this order:
//
//  1. the source is busy, the store is not one [RangeReadsFor] allows, or it has
//     no listing read;
//  2. the context was cancelled or its deadline passed (or the source is
//     draining, which is the cause of a cancellation it made);
//  3. the store is closed, or the source withdrew it;
//  4. a read before the retention horizon, which is checked before the
//     [store.ErrInvalid] it wraps;
//  5. a description recorded before the retention horizon ([interval.ErrRetained],
//     with its seed in an [interval.RetainedError]);
//  6. a subgraph question's token ahead of the store, and a subgraph answer over
//     its size cap;
//  7. an invalid argument, or an identity error;
//  8. an answer the last check refused for an endpoint id, which has a sentence
//     of its own, and anything else, an internal error.
func (s *Source) sentence(err error) (msg string, internal bool) {
	var ee *egressError
	switch {
	case errors.Is(err, ErrBusy):
		return "The server is busy; try again shortly.", false
	case errors.Is(err, ErrNoRangeReads):
		return "Range answers are not available on this store.", false
	case errors.Is(err, ErrNoListing):
		return "find_entities needs a listing read, which this store does not offer.", false
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
		return retainedSentence(err), false
	case errors.Is(err, ErrFutureToken):
		return "The token is ahead of the store; it is never clamped here.", false
	case errors.Is(err, ErrAnswerTooLarge):
		return "The answer is over the size cap; it is never cut. Narrow the window or the scope.", false
	case errors.Is(err, store.ErrInvalid), isIdentityError(err):
		return "The request was refused: " + strings.TrimSuffix(err.Error(), ": "+store.ErrInvalid.Error()) + ".", false
	case errors.As(err, &ee):
		return egressSentence, true
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
		return horizonOf(le.layer, s.st.LayerHorizon(le.layer)) + horizonNote
	}
	var parts []string
	for _, l := range s.retainedLayers() {
		parts = append(parts, horizonOf(l, s.st.LayerHorizon(l)))
	}
	if len(parts) == 0 {
		return "The store no longer keeps the history asked for."
	}
	return strings.Join(parts, " ") + horizonNote
}

// horizonNote ends every horizon sentence that names a horizon: a window that
// starts at or just after one is not always answerable.
const horizonNote = " A window that starts at or just after a horizon can still be refused for a description recorded before it."

func horizonOf(l catalog.Layer, h store.Horizon) string {
	return fmt.Sprintf("The store no longer keeps history of layer %s before %s or below token %d.", l, fmtTime(h.Time), h.Seq)
}

// retainedSentence says which description the store no longer keeps and, when the
// seed has a TTL, from when a window at the same token does not need it. It names an id, so it goes
// through the same redaction as every other sentence (fail, safeSentence).
func retainedSentence(err error) string {
	var re *interval.RetainedError
	if !errors.As(err, &re) {
		return "A description this answer needs was recorded with an event time before the retention horizon and is no longer kept."
	}
	what := re.Subject.A.String()
	if re.Subject.Kind == store.SubjectEdge {
		what = fmt.Sprintf("the %s edge from %s to %s", re.Subject.Relation, re.Subject.A, re.Subject.B)
	}
	msg := fmt.Sprintf("The description of %s at %s by %s was recorded with an event time before the retention horizon and is no longer kept.", what, fmtTime(re.At), re.Producer)
	if d, ok := re.Deadline(); ok {
		return msg + " A window at this token that starts at or after " + fmtTime(d) + " does not need it."
	}
	return msg + " It cannot be described until its producer asserts it again."
}

func fmtTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
