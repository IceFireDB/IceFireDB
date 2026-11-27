package mysql

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/mysql"
)

// errArgCountMismatch is returned when a prepared statement placeholder count and
// the supplied argument count disagree.
var errArgCountMismatch = errors.New("prepared statement placeholder count does not match argument count")

// materialize inlines prepared statement arguments into the query text, producing a
// statement that executes to the same effect on a peer. The p2p replication path can
// only carry SQL text, so a bare "INSERT ... VALUES (?)" would be rejected (or worse,
// inserted as a literal '?') by the remote node.
//
// Only placeholders outside quotes, identifiers and comments are replaced.
func materialize(query string, args []any) (string, error) {
	values := make([]string, len(args))
	for i, arg := range args {
		literal, err := sqlLiteral(arg)
		if err != nil {
			return "", fmt.Errorf("argument %d: %w", i+1, err)
		}
		values[i] = literal
	}

	var b strings.Builder
	b.Grow(len(query) + 8*len(values))

	next := 0
	for i := 0; i < len(query); {
		switch {
		case query[i] == '\'' || query[i] == '"' || query[i] == '`':
			end := skipQuoted(query, i)
			b.WriteString(query[i:end])
			i = end
		case query[i] == '-' && i+1 < len(query) && query[i+1] == '-' &&
			(i+2 == len(query) || isSpace(query[i+2])):
			end := skipLineComment(query, i)
			b.WriteString(query[i:end])
			i = end
		case query[i] == '#':
			end := skipLineComment(query, i)
			b.WriteString(query[i:end])
			i = end
		case query[i] == '/' && i+1 < len(query) && query[i+1] == '*':
			end := skipBlockComment(query, i)
			if end < 0 {
				// Unterminated comment: copy the remainder verbatim.
				end = len(query)
			}
			b.WriteString(query[i:end])
			i = end
		case query[i] == '?':
			if next >= len(values) {
				return "", errArgCountMismatch
			}
			b.WriteString(values[next])
			next++
			i++
		default:
			b.WriteByte(query[i])
			i++
		}
	}
	if next != len(values) {
		return "", errArgCountMismatch
	}
	return b.String(), nil
}

// sqlLiteral renders one argument as a MySQL literal. Unknown types are refused
// rather than silently turned into something that changes meaning.
func sqlLiteral(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "NULL", nil
	case bool:
		if x {
			return "1", nil
		}
		return "0", nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", x), nil
	case float32:
		return formatFloat(float64(x), 32, x)
	case float64:
		return formatFloat(x, 64, x)
	case string:
		return quoteLiteral(x), nil
	case []byte:
		// The wire protocol decodes text-ish values (dates, decimals, JSON) as
		// []byte. Keep those readable as strings and fall back to a hex literal
		// only for genuine binary payloads, which have no charset to convert.
		if utf8.Valid(x) {
			return quoteLiteral(string(x)), nil
		}
		return "X'" + hex.EncodeToString(x) + "'", nil
	case time.Time:
		return quoteLiteral(x.Format("2006-01-02 15:04:05.999999")), nil
	default:
		return "", fmt.Errorf("unsupported prepared statement argument type %T", v)
	}
}

func formatFloat(f float64, bitSize int, original any) (string, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "", fmt.Errorf("unsupported floating point argument %v", original)
	}
	return strconv.FormatFloat(f, 'g', -1, bitSize), nil
}

// quoteLiteral single-quotes and escapes a string using MySQL's default
// (non-NO_BACKSLASH_ESCAPES) rules.
func quoteLiteral(s string) string {
	return "'" + mysql.Escape(s) + "'"
}
