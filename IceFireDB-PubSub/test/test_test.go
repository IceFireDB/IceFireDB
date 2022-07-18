package test

import (
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/IceFireDB/IceFireDB-Proxy/test/proto"
)

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	tb.Helper()
	if !condition {
		tb.Errorf(msg, v...)
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Errorf("unexpected error: %s", err.Error())
	}
}

func equalFloat(tb testing.TB, exp, act float64) {
	if exp-act > 0.00000001 || exp-act < -0.00000001 {
		tb.Errorf("expected: %#v got: %#v", exp, act)
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	tb.Helper()
	if exp, oke := exp.(string); oke {
		if act, ok := act.(string); ok {
			eq := strings.EqualFold(exp, act)
			if !eq {
				tb.Errorf("expected: %#v got: %#v", exp, act)
			}
			return
		}
	}
	/*if _, ok := exp.([]string); ok {
		sort.Strings(exp.([]string))
	}
	if _, ok := act.([]string); ok {
		sort.Strings(act.([]string))
	}*/
	if !reflect.DeepEqual(exp, act) {
		tb.Errorf("expected: %#v got: %#v", exp, act)
	}
}

func equalStr(tb testing.TB, want, have string) {
	tb.Helper()
	if have != want {
		tb.Errorf("want: %q have: %q", want, have)
	}
}

// mustFail compares the error strings
func mustFail(tb testing.TB, err error, want string) {
	tb.Helper()
	if err == nil {
		tb.Errorf("expected an error, but got a nil")
		return
	}

	if have := err.Error(); have != want {
		tb.Errorf("have %q, want %q", have, want)
	}
}

// execute a Do(args[,-1]...), which needs to be the same as the last arg.
func mustDo(tb testing.TB, c *proto.Client, args ...string) {
	tb.Helper()
	args, want := args[:len(args)-1], args[len(args)-1]
	res, err := c.Do(args...)
	if !(proto.IsError(want)) {
		ok(tb, err)
	}
	equals(tb, want, res)
}

// mustOK is a mustDo() which expects an "OK" response
func mustOK(tb testing.TB, c *proto.Client, args ...string) {
	tb.Helper()
	mustDo(tb, c, append(args, proto.Inline("OK"))...)
}

// mustNil is a mustDo() which expects a nil response
func mustNil(tb testing.TB, c *proto.Client, args ...string) {
	tb.Helper()
	mustDo(tb, c, append(args, proto.Nil)...)
}

// mustNilList is a mustDo() which expects a list nil (-1) response
func mustNilList(tb testing.TB, c *proto.Client, args ...string) {
	tb.Helper()
	mustDo(tb, c, append(args, proto.NilList)...)
}

// must0 is a mustDo() which expects a `0` response
func must0(tb testing.TB, c *proto.Client, args ...string) {
	tb.Helper()
	mustDo(tb, c, append(args, proto.Int(0))...)
}

// must1 is a mustDo() which expects a `1` response
func must1(tb testing.TB, c *proto.Client, args ...string) {
	tb.Helper()
	mustDo(tb, c, append(args, proto.Int(1))...)
}

// execute a Read()
func mustRead(tb testing.TB, c *proto.Client, want string) {
	tb.Helper()
	res, err := c.Read()
	ok(tb, err)
	equals(tb, want, res)
}

// execute a Do(args[,-1]...), which result needs to Contain() the same as the last arg.
func mustContain(tb testing.TB, c *proto.Client, args ...string) {
	tb.Helper()
	args, want := args[:len(args)-1], args[len(args)-1]

	res, err := c.Do(args...)
	ok(tb, err)
	if !strings.Contains(res, want) {
		tb.Errorf("expected %q in %q", want, res)
	}
}

/*func useRESP3(t *testing.T, c *proto.Client) {
	mustContain(t, c, "HELLO", "3", "miniredis")
}*/

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomStr(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func directDoErr(tb testing.TB, c *proto.Client, args ...string) error {
	tb.Helper()
	// args, _ = args[:len(args)-1], args[len(args)-1]

	_, err := c.Do(args...)
	/*ok(tb, err)
	equals(tb, want, res)*/
	return err
}

func interfaceToString(args []interface{}) []string {
	argList := make([]string, len(args))
	for k, v := range args {
		switch val := v.(type) {
		case time.Duration:
			argList[k] = val.String()
		case string:
			argList[k] = val
		case int:
			argList[k] = strconv.Itoa(val)
		default:
			panic(reflect.TypeOf(v))
		}
	}
	return argList
}

func directDo(tb testing.TB, c *proto.Client, args ...interface{}) {
	tb.Helper()
	argList := interfaceToString(args)
	_, err := c.Do(argList...)
	ok(tb, err)
}

func directDoNotParse(tb testing.TB, c *proto.Client, args ...interface{}) (string, error) {
	tb.Helper()
	argList := interfaceToString(args)
	return c.Do(argList...)
}

func directDoString(tb testing.TB, c *proto.Client, args ...interface{}) string {
	tb.Helper()
	argList := interfaceToString(args)

	res, err := c.Do(argList...)
	ok(tb, err)
	str, err := proto.Parse(res)
	ok(tb, err)
	s, _ := str.(string)
	return s
}

func directDoStringArrErr(tb testing.TB, c *proto.Client, args ...interface{}) ([]string, error) {
	tb.Helper()
	argList := interfaceToString(args)

	res, err := c.Do(argList...)
	ok(tb, err)
	return proto.ReadArray(res)
}

func directDoStringErr(tb testing.TB, c *proto.Client, args ...interface{}) (string, error) {
	tb.Helper()
	argList := interfaceToString(args)

	res, err := c.Do(argList...)
	ok(tb, err)
	str, err := proto.Parse(res)
	ok(tb, err)
	if err, ok := str.(error); ok {
		return "", err
	}
	s, _ := str.(string)
	return s, err
}

func directDoInt(tb testing.TB, c *proto.Client, args ...interface{}) int {
	tb.Helper()
	argList := interfaceToString(args)

	res, err := c.Do(argList...)
	ok(tb, err)
	i, err := proto.Parse(res)
	if err, ok := i.(error); ok {
		tb.Errorf("parse int error:%s", err.Error())
		return 0
	}
	ok(tb, err)
	return i.(int)
}

func directDoIntErr(tb testing.TB, c *proto.Client, args ...interface{}) (int, error) {
	tb.Helper()
	argList := interfaceToString(args)

	res, err := c.Do(argList...)
	ok(tb, err)
	i, err := proto.Parse(res)
	ok(tb, err)
	if err, ok := i.(error); ok {
		return 0, err
	}
	return i.(int), err
}

func directDoFloatErr(tb testing.TB, c *proto.Client, args ...interface{}) (float64, error) {
	tb.Helper()
	argList := interfaceToString(args)

	res, err := c.Do(argList...)
	ok(tb, err)
	i, err := proto.ReadString(res)
	ok(tb, err)
	return strconv.ParseFloat(i, 64)
}

func directDoBool(tb testing.TB, c *proto.Client, args ...interface{}) bool {
	tb.Helper()
	argList := interfaceToString(args)

	res, err := c.Do(argList...)
	ok(tb, err)
	i, err := proto.Parse(res)
	ok(tb, err)
	v, _ := i.(int)
	return v > 0
}

func directDoBoolErr(tb testing.TB, c *proto.Client, args ...interface{}) (bool, error) {
	tb.Helper()
	argList := interfaceToString(args)

	res, err := c.Do(argList...)
	ok(tb, err)
	i, err := proto.Parse(res)
	ok(tb, err)
	v, _ := i.(int)
	return v > 0, err
}

func directDoList(tb testing.TB, c *proto.Client, args ...interface{}) (interface{}, error) {
	tb.Helper()
	argList := interfaceToString(args)

	res, err := c.Do(argList...)
	ok(tb, err)
	return res, err
}
