package interval

// ReadingEveryEndpoint returns a reader like r that reads the endpoints of
// every edge in the history before To, with or without an interval in the
// window, so tests can show the skip changes no answer.
func ReadingEveryEndpoint(r *Reader) *Reader {
	c := *r
	c.everyEndpoint = true
	return &c
}
