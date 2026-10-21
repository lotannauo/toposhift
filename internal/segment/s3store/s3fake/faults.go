package s3fake

// FaultKind is a way for the server to misbehave.
type FaultKind int

const (
	// Fail500 answers 500 InternalError and does nothing else: no object is
	// stored or removed.
	Fail500 FaultKind = iota + 1
	// SlowDown answers 503 SlowDown and does nothing else.
	SlowDown
	// Drop closes the connection without an answer and does nothing else.
	Drop
	// DropAfterStore does what the request asked (stores the object, or removes
	// it), then closes the connection without an answer: a response lost on the
	// way. The client cannot tell it from Drop, and a retry of a PutObject gets
	// 412.
	DropAfterStore
	// TruncateBody, on GetObject, announces the object's length, sends half of
	// it and closes the connection.
	TruncateBody
	// CorruptBody, on GetObject, answers with the full length and one byte
	// changed.
	CorruptBody
	// Stall holds the request. On PutObject the server takes the first bytes of
	// the body and then stops reading; on GetObject it sends half the body and
	// stops. Either way [Server.Stalled] yields, and the request is held until
	// the client gives up or the server is closed. Nothing is stored.
	Stall
	// NoDigest, on HeadObject and GetObject, leaves the sha256 user metadata out
	// of the answer, as for an object that never had it.
	NoDigest
)

func (k FaultKind) String() string {
	switch k {
	case Fail500:
		return "Fail500"
	case SlowDown:
		return "SlowDown"
	case Drop:
		return "Drop"
	case DropAfterStore:
		return "DropAfterStore"
	case TruncateBody:
		return "TruncateBody"
	case CorruptBody:
		return "CorruptBody"
	case Stall:
		return "Stall"
	case NoDigest:
		return "NoDigest"
	}
	return "none"
}

// Fault is a rule for misbehaving. A request matches it when its operation is
// Op (empty matches every operation) and its key is Key (empty matches every
// key); the rule applies to the matching requests after the first After of
// them, Times times (zero means once, a negative number means always). A
// request is checked against the rules in the order they were injected, and the
// first one that applies wins. Faults apply only to requests that passed the
// signature check.
type Fault struct {
	Op    string
	Key   string
	After int
	Times int
	Kind  FaultKind
}

type fault struct {
	Fault
	seen, applied int
}

// Inject adds a rule.
func (s *Server) Inject(f Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = append(s.faults, &fault{Fault: f})
}

// ClearFaults removes every rule.
func (s *Server) ClearFaults() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = nil
}

// takeFault returns the rule that applies to a request, if any, and counts it.
func (s *Server) takeFault(op, key string) *fault {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.faults {
		if (f.Op != "" && f.Op != op) || (f.Key != "" && f.Key != key) {
			continue
		}
		f.seen++
		if f.seen <= f.After {
			continue
		}
		limit := f.Times
		if limit == 0 {
			limit = 1
		}
		if limit > 0 && f.applied >= limit {
			continue
		}
		f.applied++
		return f
	}
	return nil
}
