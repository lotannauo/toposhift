package segment

import (
	"fmt"
	"strings"
)

// Limits on names.
const (
	maxNameLen    = 512
	maxElementLen = 128
)

// ValidName reports whether name is a valid segment name, with the reason as an
// error wrapping [ErrInvalidName].
//
// A name is 1 to 512 bytes: one or more elements separated by single slashes,
// with no leading or trailing slash. Each element is 1 to 128 bytes of
// lower-case ASCII letters, digits, '.', '_' and '-', and does not start with
// '.'. The last rule rules out "." and "..", and keeps the hidden files a store
// may use for its own bookkeeping out of the name space. Because every byte is
// ASCII, a valid name is valid UTF-8. Letters are lower case only because the default file
// systems of macOS and Windows treat names that differ only in case as one
// name, and a store must not depend on the file system it sits on.
//
// A prefix given to [Store.List] follows the same rules, except that it may be
// empty, may end in a single slash, and may end partway through an element.
func ValidName(name string) error {
	if name == "" {
		return invalid("name is empty")
	}
	if len(name) > maxNameLen {
		return invalid("name is longer than %d bytes", maxNameLen)
	}
	return validElements(name)
}

func validElements(s string) error {
	for {
		element, rest, more := strings.Cut(s, "/")
		if err := validElement(element); err != nil {
			return err
		}
		if !more {
			return nil
		}
		s = rest
	}
}

func validElement(e string) error {
	switch {
	case e == "":
		return invalid("an element is empty (leading, trailing or doubled slash)")
	case len(e) > maxElementLen:
		return invalid("an element is longer than %d bytes", maxElementLen)
	case e[0] == '.':
		return invalid("an element starts with '.'")
	}
	for i := 0; i < len(e); i++ {
		c := e[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == '-':
		default:
			return invalid("byte 0x%02x is not one of a-z 0-9 . _ -", c)
		}
	}
	return nil
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidName, fmt.Sprintf(format, args...))
}
