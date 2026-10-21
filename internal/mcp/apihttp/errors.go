package apihttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/mcp"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// errorBody is what every refusal that is not plain text writes.
type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// writeError writes a refusal with a sentence of the package's own.
func writeError(w http.ResponseWriter, status int, code, message string) {
	b, err := json.Marshal(errorBody{errorDetail{Code: code, Message: message}})
	if err != nil {
		http.Error(w, http.StatusText(status), status)
		return
	}
	if status == http.StatusServiceUnavailable && code == "busy" {
		w.Header().Set("Retry-After", "5")
	}
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// writeFailure writes the failure of a call. Every error a source returns is an
// [*mcp.Error]: its Message is the sentence to print, and its Cause, which may
// hold ids the sentence withholds, is read for the status and never printed.
func writeFailure(w http.ResponseWriter, err error) {
	status, code := classify(err)
	writeError(w, status, code, sentenceOf(err))
}

// writeSourceError writes the failure to get a source: no store is attached yet,
// which says why in its sentence, or the server is drained.
func writeSourceError(w http.ResponseWriter, err error) {
	if errors.Is(err, mcp.ErrShuttingDown) {
		writeError(w, http.StatusServiceUnavailable, "shutting_down", sentenceOf(err))
		return
	}
	writeError(w, http.StatusServiceUnavailable, "unavailable", sentenceOf(err))
}

// sentenceOf is the sentence of an error, and never its cause.
func sentenceOf(err error) string {
	var me *mcp.Error
	if errors.As(err, &me) {
		return me.Message
	}
	return "Internal error."
}

// classify chooses the status and the code of a failure from its cause, in the
// order the tools choose their sentences.
//
// A call cancelled while its request is live was ended by the drain, which is the
// only thing that cancels it; a deadline that passes in the same instant as a
// drain is told as the deadline, where the sentence says the server is stopping.
func classify(err error) (status int, code string) {
	var identityErr *identity.AttrError
	switch {
	case errors.Is(err, mcp.ErrBusy):
		return http.StatusServiceUnavailable, "busy"
	case errors.Is(err, mcp.ErrNoRangeReads):
		return http.StatusNotImplemented, "no_range_reads"
	case errors.Is(err, context.Canceled):
		return http.StatusServiceUnavailable, "shutting_down"
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, "deadline"
	case errors.Is(err, mcp.ErrShuttingDown), errors.Is(err, store.ErrClosed):
		return http.StatusServiceUnavailable, "shutting_down"
	case errors.Is(err, store.ErrBeforeHorizon), errors.Is(err, interval.ErrRetained):
		return http.StatusGone, "before_horizon"
	case errors.Is(err, mcp.ErrFutureToken):
		return http.StatusConflict, "future_token"
	case errors.Is(err, mcp.ErrAnswerTooLarge):
		return http.StatusUnprocessableEntity, "answer_too_large"
	case errors.Is(err, store.ErrInvalid), errors.As(err, &identityErr), errors.Is(err, identity.ErrFingerprint):
		return http.StatusBadRequest, "invalid_argument"
	}
	return http.StatusInternalServerError, "internal"
}
