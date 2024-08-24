package jsondiff

import (
	"strconv"
	"strings"
)

const jsonPointerSep = "/"

var (
	// rfc6901Replacer is a replacer used to escape JSON
	// pointer strings in compliance with the JavaScript
	// Object Notation Pointer syntax.
	// https://tools.ietf.org/html/rfc6901
	rfc6901Replacer = strings.NewReplacer("~", "~0", "/", "~1")

	// dotPathReplacer converts a RFC6901 JSON pointer to
	// a JSON path, while also escaping any existing dot
	// characters present in the original pointer.
	dotPathReplacer = strings.NewReplacer(".", "\\.", "/", ".")
)

type jsonNode struct {
	ptr pointer
	val interface{}
}

// pointer represents a RFC6901 JSON Pointer.
type pointer string

const emptyPtr = pointer("")

// String implements the fmt.Stringer interface.
func (p pointer) String() string {
	return string(p)
}

func (p pointer) toJSONPath() string {
	if len(p) > 0 {
		return dotPathReplacer.Replace(string(p)[1:])
	}
	// @this is a special modifier that can
	// be used to retrieve the root path.
	return "@this"
}

func (p pointer) appendKey(key string) pointer {
	return pointer(string(p) + jsonPointerSep + rfc6901Replacer.Replace(key))
}

func (p pointer) appendIndex(idx int) pointer {
	return pointer(string(p) + jsonPointerSep + strconv.Itoa(idx))
}

func (p pointer) isRoot() bool {
	return len(p) == 0
}
