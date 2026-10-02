package identity

// Exposed to the black-box tests only.
type HashFunc = hashFunc

var WithHash = withHash

// Decode exposes the structural decoder, which knows no catalog.
func Decode(canonical string) error {
	_, _, err := decode(canonical)
	return err
}
