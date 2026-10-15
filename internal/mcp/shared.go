package mcp

import (
	"context"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
)

// UnattributedAt is a placement the server refuses to guess: over its spans the
// entity is placed on none of its candidates. Prefer "unattributed" to
// "guessed".
type UnattributedAt struct {
	Entity  Ref    `json:"entity"`
	Reason  string `json:"reason"`
	Subject string `json:"subject"`
	Basis   string `json:"basis"`
	Rule    string `json:"rule"`
	// Relation and TargetType are set for a walk's placement claims.
	Relation   string `json:"relation,omitempty"`
	TargetType string `json:"target_type,omitempty"`
	Spans      []Span `json:"spans"`
	Candidates []Ref  `json:"candidates"`
	Note       string `json:"note"`
}

func refs(fps []identity.Fingerprint) []Ref {
	out := make([]Ref, len(fps))
	for i, fp := range fps {
		out[i] = ref(fp)
	}
	return out
}

// finish completes an answer: it fills the names, shortens the lists that
// overflow the hard cap, and closes the notes with the sentence that says how to
// read the same world again. notes must point at out's own notes.
func (s *Source) finish(ctx context.Context, out any, notes *[]string, token uint64, from, to time.Time, lists ...namedTail) error {
	note, err := s.fillNames(ctx, out, token, from, to)
	if err != nil {
		return err
	}
	if note != "" {
		*notes = append(*notes, note)
	}
	cut, err := fitAnswer(out, maxAnswerBytes, lists...)
	if err != nil {
		return err
	}
	*notes = append(*notes, cut...)
	*notes = append(*notes, passAsOf(token))
	return nil
}
