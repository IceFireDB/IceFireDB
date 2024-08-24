package redcon

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// Type of RESP
type Type byte

// Various RESP kinds
const (
	Integer = ':'
	String  = '+'
	Bulk    = '$'
	Array   = '*'
	Error   = '-'
)

type RESP struct {
	Type  Type
	Raw   []byte
	Data  []byte
	Count int
}

// ForEach iterates over each Array element
func (r RESP) ForEach(iter func(resp RESP) bool) {
	data := r.Data
	for i := 0; i < r.Count; i++ {
		n, resp := ReadNextRESP(data)
		if !iter(resp) {
			return
		}
		data = data[n:]
	}
}

func (r RESP) Bytes() []byte {
	return r.Data
}

func (r RESP) String() string {
	return string(r.Data)
}

func (r RESP) Int() int64 {
	x, _ := strconv.ParseInt(r.String(), 10, 64)
	return x
}

func (r RESP) Float() float64 {
	x, _ := strconv.ParseFloat(r.String(), 10)
	return x
}

// Map returns a key/value map of an Array.
// The receiver RESP must be an Array with an equal number of values, where
// the value of the key is followed by the key.
// Example: key1,value1,key2,value2,key3,value3
func (r RESP) Map() map[string]RESP {
	if r.Type != Array {
		return nil
	}
	var n int
	var key string
	m := make(map[string]RESP)
	r.ForEach(func(resp RESP) bool {
		if n&1 == 0 {
			key = resp.String()
		} else {
			m[key] = resp
		}
		n++
		return true
	})
	return m
}

func (r RESP) MapGet(key string) RESP {
	if r.Type != Array {
		return RESP{}
	}
	var val RESP
	var n int
	var ok bool
	r.ForEach(func(resp RESP) bool {
		if n&1 == 0 {
			ok = resp.String() == key
		} else if ok {
			val = resp
			return false
		}
		n++
		return true
	})
	return val
}

func (r RESP) Exists() bool {
	return r.Type != 0
}

// ReadNextRESP returns the next resp in b and returns the number of bytes the
// took up the result.
func ReadNextRESP(b []byte) (n int, resp RESP) {
	if len(b) == 0 {
		return 0, RESP{} // no data to read
	}
	resp.Type = Type(b[0])
	switch resp.Type {
	case Integer, String, Bulk, Array, Error:
	default:
		return 0, RESP{} // invalid kind
	}
	// read to end of line
	i := 1
	for ; ; i++ {
		if i == len(b) {
			return 0, RESP{} // not enough data
		}
		if b[i] == '\n' {
			if b[i-1] != '\r' {
				return 0, RESP{} //, missing CR character
			}
			i++
			break
		}
	}
	resp.Raw = b[0:i]
	resp.Data = b[1 : i-2]
	if resp.Type == Integer {
		// Integer
		if len(resp.Data) == 0 {
			return 0, RESP{} //, invalid integer
		}
		var j int
		if resp.Data[0] == '-' {
			if len(resp.Data) == 1 {
				return 0, RESP{} //, invalid integer
			}
			j++
		}
		for ; j < len(resp.Data); j++ {
			if resp.Data[j] < '0' || resp.Data[j] > '9' {
				return 0, RESP{} // invalid integer
			}
		}
		return len(resp.Raw), resp
	}
	if resp.Type == String || resp.Type == Error {
		// String, Error
		return len(resp.Raw), resp
	}
	var err error
	resp.Count, err = strconv.Atoi(string(resp.Data))
	if resp.Type == Bulk {
		// Bulk
		if err != nil {
			return 0, RESP{} // invalid number of bytes
		}
		if resp.Count < 0 {
			resp.Data = nil
			resp.Count = 0
			return len(resp.Raw), resp
		}
		if len(b) < i+resp.Count+2 {
			return 0, RESP{} // not enough data
		}
		if b[i+resp.Count] != '\r' || b[i+resp.Count+1] != '\n' {
			return 0, RESP{} // invalid end of line
		}
		resp.Data = b[i : i+resp.Count]
		resp.Raw = b[0 : i+resp.Count+2]
		resp.Count = 0
		return len(resp.Raw), resp
	}
	// Array
	if err != nil {
		return 0, RESP{} // invalid number of elements
	}
	var tn int
	sdata := b[i:]
	for j := 0; j < resp.Count; j++ {
		rn, rresp := ReadNextRESP(sdata)
		if rresp.Type == 0 {
			return 0, RESP{}
		}
		tn += rn
		sdata = sdata[rn:]
	}
	resp.Data = b[i : i+tn]
	resp.Raw = b[0 : i+tn]
	return len(resp.Raw), resp
}

// Kind is the kind of command
type Kind int

const (
	// Redis is returned for Redis protocol commands
	Redis Kind = iota
	// Tile38 is returnd for Tile38 native protocol commands
	Tile38
	// Telnet is returnd for plain telnet commands
	Telnet
)

var errInvalidMessage = &errProtocol{"invalid message"}

// ReadNextCommand reads the next command from the provided packet. It's
// possible that the packet contains multiple commands, or zero commands
// when the packet is incomplete.
// 'argsbuf' is an optional reusable buffer and it can be nil.
// 'complete' indicates that a command was read. false means no more commands.
// 'args' are the output arguments for the command.
// 'kind' is the type of command that was read.
// 'leftover' is any remaining unused bytes which belong to the next command.
// 'err' is returned when a protocol error was encountered.
func ReadNextCommand(packet []byte, argsbuf [][]byte) (
	complete bool, args [][]byte, kind Kind, leftover []byte, err error,
) {
	args = argsbuf[:0]
	if len(packet) > 0 {
		if packet[0] != '*' {
			if packet[0] == '$' {
				return readTile38Command(packet, args)
			}
			return readTelnetCommand(packet, args)
		}
		// standard redis command
		for s, i := 1, 1; i < len(packet); i++ {
			if packet[i] == '\n' {
				if packet[i-1] != '\r' {
					return false, args[:0], Redis, packet, errInvalidMultiBulkLength
				}
				count, ok := parseInt(packet[s : i-1])
				if !ok || count < 0 {
					return false, args[:0], Redis, packet, errInvalidMultiBulkLength
				}
				i++
				if count == 0 {
					return true, args[:0], Redis, packet[i:], nil
				}
			nextArg:
				for j := 0; j < count; j++ {
					if i == len(packet) {
						break
					}
					if packet[i] != '$' {
						return false, args[:0], Redis, packet,
							&errProtocol{"expected '$', got '" +
								string(packet[i]) + "'"}
					}
					for s := i + 1; i < len(packet); i++ {
						if packet[i] == '\n' {
							if packet[i-1] != '\r' {
								return false, args[:0], Redis, packet, errInvalidBulkLength
							}
							n, ok := parseInt(packet[s : i-1])
							if !ok || count <= 0 {
								return false, args[:0], Redis, packet, errInvalidBulkLength
							}
							i++
							if len(packet)-i >= n+2 {
								if packet[i+n] != '\r' || packet[i+n+1] != '\n' {
									return false, args[:0], Redis, packet, errInvalidBulkLength
								}
								args = append(args, packet[i:i+n])
								i += n + 2
								if j == count-1 {
									// done reading
									return true, args, Redis, packet[i:], nil
								}
								continue nextArg
							}
							break
						}
					}
					break
				}
				break
			}
		}
	}
	return false, args[:0], Redis, packet, nil
}

func readTile38Command(packet []byte, argsbuf [][]byte) (
	complete bool, args [][]byte, kind Kind, leftover []byte, err error,
) {
	for i := 1; i < len(packet); i++ {
		if packet[i] == ' ' {
			n, ok := parseInt(packet[1:i])
			if !ok || n < 0 {
				return false, args[:0], Tile38, packet, errInvalidMessage
			}
			i++
			if len(packet) >= i+n+2 {
				if packet[i+n] != '\r' || packet[i+n+1] != '\n' {
					return false, args[:0], Tile38, packet, errInvalidMessage
				}
				line := packet[i : i+n]
			reading:
				for len(line) != 0 {
					if line[0] == '{' {
						// The native protocol cannot understand json boundaries so it assumes that
						// a json element must be at the end of the line.
						args = append(args, line)
						break
					}
					if line[0] == '"' && line[len(line)-1] == '"' {
						if len(args) > 0 &&
							strings.ToLower(string(args[0])) == "set" &&
							strings.ToLower(string(args[len(args)-1])) == "string" {
							// Setting a string value that is contained inside double quotes.
							// This is only because of the boundary issues of the native protocol.
							args = append(args, line[1:len(line)-1])
							break
						}
					}
					i := 0
					for ; i < len(line); i++ {
						if line[i] == ' ' {
							value := line[:i]
							if len(value) > 0 {
								args = append(args, value)
							}
							line = line[i+1:]
							continue reading
						}
					}
					args = append(args, line)
					break
				}
				return true, args, Tile38, packet[i+n+2:], nil
			}
			break
		}
	}
	return false, args[:0], Tile38, packet, nil
}
func readTelnetCommand(packet []byte, argsbuf [][]byte) (
	complete bool, args [][]byte, kind Kind, leftover []byte, err error,
) {
	// just a plain text command
	for i := 0; i < len(packet); i++ {
		if packet[i] == '\n' {
			var line []byte
			if i > 0 && packet[i-1] == '\r' {
				line = packet[:i-1]
			} else {
				line = packet[:i]
			}
			var quote bool
			var quotech byte
			var escape bool
		outer:
			for {
				nline := make([]byte, 0, len(line))
				for i := 0; i < len(line); i++ {
					c := line[i]
					if !quote {
						if c == ' ' {
							if len(nline) > 0 {
								args = append(args, nline)
							}
							line = line[i+1:]
							continue outer
						}
						if c == '"' || c == '\'' {
							if i != 0 {
								return false, args[:0], Telnet, packet, errUnbalancedQuotes
							}
							quotech = c
							quote = true
							line = line[i+1:]
							continue outer
						}
					} else {
						if escape {
							escape = false
							switch c {
							case 'n':
								c = '\n'
							case 'r':
								c = '\r'
							case 't':
								c = '\t'
							}
						} else if c == quotech {
							quote = false
							quotech = 0
							args = append(args, nline)
							line = line[i+1:]
							if len(line) > 0 && line[0] != ' ' {
								return false, args[:0], Telnet, packet, errUnbalancedQuotes
							}
							continue outer
						} else if c == '\\' {
							escape = true
							continue
						}
					}
					nline = append(nline, c)
				}
				if quote {
					return false, args[:0], Telnet, packet, errUnbalancedQuotes
				}
				if len(line) > 0 {
					args = append(args, line)
				}
				break
			}
			return true, args, Telnet, packet[i+1:], nil
		}
	}
	return false, args[:0], Telnet, packet, nil
}

// appendPrefix will append a "$3\r\n" style redis prefix for a message.
func appendPrefix(b []byte, c byte, n int64) []byte {
	if n >= 0 && n <= 9 {
		return append(b, c, byte('0'+n), '\r', '\n')
	}
	b = append(b, c)
	b = strconv.AppendInt(b, n, 10)
	return append(b, '\r', '\n')
}

// AppendUint appends a Redis protocol uint64 to the input bytes.
func AppendUint(b []byte, n uint64) []byte {
	b = append(b, ':')
	b = strconv.AppendUint(b, n, 10)
	return append(b, '\r', '\n')
}

// AppendInt appends a Redis protocol int64 to the input bytes.
func AppendInt(b []byte, n int64) []byte {
	return appendPrefix(b, ':', n)
}

// AppendArray appends a Redis protocol array to the input bytes.
func AppendArray(b []byte, n int) []byte {
	return appendPrefix(b, '*', int64(n))
}

// AppendBulk appends a Redis protocol bulk byte slice to the input bytes.
func AppendBulk(b []byte, bulk []byte) []byte {
	b = appendPrefix(b, '$', int64(len(bulk)))
	b = append(b, bulk...)
	return append(b, '\r', '\n')
}

// AppendBulkString appends a Redis protocol bulk string to the input bytes.
func AppendBulkString(b []byte, bulk string) []byte {
	b = appendPrefix(b, '$', int64(len(bulk)))
	b = append(b, bulk...)
	return append(b, '\r', '\n')
}

// AppendString appends a Redis protocol string to the input bytes.
func AppendString(b []byte, s string) []byte {
	b = append(b, '+')
	b = append(b, stripNewlines(s)...)
	return append(b, '\r', '\n')
}

// AppendError appends a Redis protocol error to the input bytes.
func AppendError(b []byte, s string) []byte {
	b = append(b, '-')
	b = append(b, stripNewlines(s)...)
	return append(b, '\r', '\n')
}

// AppendOK appends a Redis protocol OK to the input bytes.
func AppendOK(b []byte) []byte {
	return append(b, '+', 'O', 'K', '\r', '\n')
}
func stripNewlines(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == '\r' || s[i] == '\n' {
			s = strings.Replace(s, "\r", " ", -1)
			s = strings.Replace(s, "\n", " ", -1)
			break
		}
	}
	return s
}

// AppendTile38 appends a Tile38 message to the input bytes.
func AppendTile38(b []byte, data []byte) []byte {
	b = append(b, '$')
	b = strconv.AppendInt(b, int64(len(data)), 10)
	b = append(b, ' ')
	b = append(b, data...)
	return append(b, '\r', '\n')
}

// AppendNull appends a Redis protocol null to the input bytes.
func AppendNull(b []byte) []byte {
	return append(b, '$', '-', '1', '\r', '\n')
}

// AppendBulkFloat appends a float64, as bulk bytes.
func AppendBulkFloat(dst []byte, f float64) []byte {
	return AppendBulk(dst, strconv.AppendFloat(nil, f, 'f', -1, 64))
}

// AppendBulkInt appends an int64, as bulk bytes.
func AppendBulkInt(dst []byte, x int64) []byte {
	return AppendBulk(dst, strconv.AppendInt(nil, x, 10))
}

// AppendBulkUint appends an uint64, as bulk bytes.
func AppendBulkUint(dst []byte, x uint64) []byte {
	return AppendBulk(dst, strconv.AppendUint(nil, x, 10))
}

func prefixERRIfNeeded(msg string) string {
	msg = strings.TrimSpace(msg)
	firstWord := strings.Split(msg, " ")[0]
	addERR := len(firstWord) == 0
	for i := 0; i < len(firstWord); i++ {
		if firstWord[i] < 'A' || firstWord[i] > 'Z' {
			addERR = true
			break
		}
	}
	if addERR {
		msg = strings.TrimSpace("ERR " + msg)
	}
	return msg
}

// SimpleString is for representing a non-bulk representation of a string
// from an *Any call.
type SimpleString string

// SimpleInt is for representing a non-bulk representation of a int
// from an *Any call.
type SimpleInt int

// SimpleError is for representing an error without adding the "ERR" prefix
// from an *Any call.
type SimpleError error

// Marshaler is the interface implemented by types that
// can marshal themselves into a Redis response type from an *Any call.
// The return value is not check for validity.
type Marshaler interface {
	MarshalRESP() []byte
}

// AppendAny appends any type to valid Redis type.
//   nil             -> null
//   error           -> error (adds "ERR " when first word is not uppercase)
//   string          -> bulk-string
//   numbers         -> bulk-string
//   []byte          -> bulk-string
//   bool            -> bulk-string ("0" or "1")
//   slice           -> array
//   map             -> array with key/value pairs
//   SimpleString    -> string
//   SimpleInt       -> integer
//   Marshaler       -> raw bytes
//   everything-else -> bulk-string representation using fmt.Sprint()
func AppendAny(b []byte, v interface{}) []byte {
	switch v := v.(type) {
	case SimpleString:
		b = AppendString(b, string(v))
	case SimpleInt:
		b = AppendInt(b, int64(v))
	case SimpleError:
		b = AppendError(b, v.Error())
	case nil:
		b = AppendNull(b)
	case error:
		b = AppendError(b, prefixERRIfNeeded(v.Error()))
	case string:
		b = AppendBulkString(b, v)
	case []byte:
		b = AppendBulk(b, v)
	case bool:
		if v {
			b = AppendBulkString(b, "1")
		} else {
			b = AppendBulkString(b, "0")
		}
	case int:
		b = AppendBulkInt(b, int64(v))
	case int8:
		b = AppendBulkInt(b, int64(v))
	case int16:
		b = AppendBulkInt(b, int64(v))
	case int32:
		b = AppendBulkInt(b, int64(v))
	case int64:
		b = AppendBulkInt(b, int64(v))
	case uint:
		b = AppendBulkUint(b, uint64(v))
	case uint8:
		b = AppendBulkUint(b, uint64(v))
	case uint16:
		b = AppendBulkUint(b, uint64(v))
	case uint32:
		b = AppendBulkUint(b, uint64(v))
	case uint64:
		b = AppendBulkUint(b, uint64(v))
	case float32:
		b = AppendBulkFloat(b, float64(v))
	case float64:
		b = AppendBulkFloat(b, float64(v))
	case Marshaler:
		b = append(b, v.MarshalRESP()...)
	default:
		vv := reflect.ValueOf(v)
		switch vv.Kind() {
		case reflect.Slice:
			n := vv.Len()
			b = AppendArray(b, n)
			for i := 0; i < n; i++ {
				b = AppendAny(b, vv.Index(i).Interface())
			}
		case reflect.Map:
			n := vv.Len()
			b = AppendArray(b, n*2)
			var i int
			var strKey bool
			var strsKeyItems []strKeyItem

			iter := vv.MapRange()
			for iter.Next() {
				key := iter.Key().Interface()
				if i == 0 {
					if _, ok := key.(string); ok {
						strKey = true
						strsKeyItems = make([]strKeyItem, n)
					}
				}
				if strKey {
					strsKeyItems[i] = strKeyItem{
						key.(string), iter.Value().Interface(),
					}
				} else {
					b = AppendAny(b, key)
					b = AppendAny(b, iter.Value().Interface())
				}
				i++
			}
			if strKey {
				sort.Slice(strsKeyItems, func(i, j int) bool {
					return strsKeyItems[i].key < strsKeyItems[j].key
				})
				for _, item := range strsKeyItems {
					b = AppendBulkString(b, item.key)
					b = AppendAny(b, item.value)
				}
			}
		default:
			b = AppendBulkString(b, fmt.Sprint(v))
		}
	}
	return b
}

type strKeyItem struct {
	key   string
	value interface{}
}
