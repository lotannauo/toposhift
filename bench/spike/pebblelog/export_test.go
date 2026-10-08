package pebblelog

// CheckRememberedState is the first way the engine's map of what the writer
// remembers is not what a read of the whole database finds, or nil; and
// Complete says whether the map claims to hold every prefix. They are for the
// test of a whole stream, which cannot be in this package (it needs the runner).
func CheckRememberedState(e *Engine) error { return rememberedStateMismatch(e) }

// Complete says whether the engine's map claims to hold every prefix.
func Complete(e *Engine) bool { return e.complete }

// Remembered is how many prefixes the engine's map holds.
func Remembered(e *Engine) int { return len(e.states) }
