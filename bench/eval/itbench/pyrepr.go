package itbench

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// parseMap reads a map of strings to strings as the exports write one: the
// repr of a Python dictionary, such as {'a': 'b', 'c': "it's"}. It accepts
// nothing else (no number, list or nested map), and reports where in the text
// it stopped, never what was there.
func parseMap(s string) (map[string]string, error) {
	p := &pyParser{s: s}
	p.space()
	if !p.eat('{') {
		return nil, p.fail("a map must start with {")
	}
	out := map[string]string{}
	p.space()
	if p.eat('}') {
		return out, p.end()
	}
	for {
		p.space()
		k, err := p.str()
		if err != nil {
			return nil, err
		}
		p.space()
		if !p.eat(':') {
			return nil, p.fail("expected :")
		}
		p.space()
		v, err := p.str()
		if err != nil {
			return nil, err
		}
		out[k] = v
		p.space()
		if p.eat(',') {
			p.space()
			if p.eat('}') { // a trailing comma
				return out, p.end()
			}
			continue
		}
		if p.eat('}') {
			return out, p.end()
		}
		return nil, p.fail("expected , or }")
	}
}

type pyParser struct {
	s string
	i int
}

func (p *pyParser) fail(msg string) error {
	return fmt.Errorf("itbench: a map: %s at byte %d", msg, p.i)
}

func (p *pyParser) space() {
	for p.i < len(p.s) && (p.s[p.i] == ' ' || p.s[p.i] == '\t' || p.s[p.i] == '\n' || p.s[p.i] == '\r') {
		p.i++
	}
}

func (p *pyParser) eat(c byte) bool {
	if p.i < len(p.s) && p.s[p.i] == c {
		p.i++
		return true
	}
	return false
}

func (p *pyParser) end() error {
	p.space()
	if p.i != len(p.s) {
		return p.fail("text after the map")
	}
	return nil
}

// str reads a quoted string with Python's escapes.
func (p *pyParser) str() (string, error) {
	if p.i >= len(p.s) || (p.s[p.i] != '\'' && p.s[p.i] != '"') {
		return "", p.fail("expected a quoted string")
	}
	q := p.s[p.i]
	p.i++
	var b strings.Builder
	for p.i < len(p.s) {
		c := p.s[p.i]
		switch {
		case c == q:
			p.i++
			return b.String(), nil
		case c != '\\':
			b.WriteByte(c)
			p.i++
			continue
		}
		p.i++
		if p.i >= len(p.s) {
			break
		}
		e := p.s[p.i]
		p.i++
		switch e {
		case '\\', '\'', '"':
			b.WriteByte(e)
		case 'n':
			b.WriteByte('\n')
		case 't':
			b.WriteByte('\t')
		case 'r':
			b.WriteByte('\r')
		case 'a':
			b.WriteByte(7)
		case 'b':
			b.WriteByte(8)
		case 'f':
			b.WriteByte(12)
		case 'v':
			b.WriteByte(11)
		case '0', '1', '2', '3', '4', '5', '6', '7':
			j := p.i
			for j < len(p.s) && j < p.i+2 && p.s[j] >= '0' && p.s[j] <= '7' {
				j++
			}
			n, err := strconv.ParseUint(p.s[p.i-1:j], 8, 32)
			if err != nil {
				return "", p.fail("a bad octal escape")
			}
			p.i = j
			b.WriteRune(rune(n))
		case 'x', 'u', 'U':
			width := map[byte]int{'x': 2, 'u': 4, 'U': 8}[e]
			if p.i+width > len(p.s) {
				return "", p.fail("a short escape")
			}
			n, err := strconv.ParseUint(p.s[p.i:p.i+width], 16, 32)
			if err != nil || !utf8.ValidRune(rune(n)) {
				return "", p.fail("a bad escape")
			}
			p.i += width
			b.WriteRune(rune(n))
		case '\n':
			// a line continuation
		default:
			b.WriteByte('\\')
			b.WriteByte(e)
		}
	}
	return "", errors.New("itbench: a map: an unterminated string")
}
