package kafdeck

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// ValidateJSON rejects duplicate keys and unreasonable nesting before decoding
// typed API/import data. Unknown additive API properties remain compatible.
func ValidateJSON(b []byte) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	nodes := 0
	var walk func(int) error
	walk = func(depth int) error {
		nodes++
		if depth > 16 || nodes > 4096 {
			return errors.New("bounded JSON contract exceeded")
		}
		v, e := dec.Token()
		if e != nil {
			return e
		}
		if d, ok := v.(json.Delim); ok {
			switch d {
			case '{':
				seen := map[string]bool{}
				for dec.More() {
					key, e := dec.Token()
					if e != nil {
						return e
					}
					s, ok := key.(string)
					if !ok || seen[s] {
						return errors.New("duplicate or invalid JSON key")
					}
					seen[s] = true
					if e = walk(depth + 1); e != nil {
						return e
					}
				}
				_, e = dec.Token()
				return e
			case '[':
				for dec.More() {
					if e = walk(depth + 1); e != nil {
						return e
					}
				}
				_, e = dec.Token()
				return e
			default:
				return errors.New("invalid JSON delimiter")
			}
		}
		return nil
	}
	if e := walk(0); e != nil {
		return e
	}
	if _, e := dec.Token(); e != io.EOF {
		return errors.New("trailing JSON content")
	}
	return nil
}
