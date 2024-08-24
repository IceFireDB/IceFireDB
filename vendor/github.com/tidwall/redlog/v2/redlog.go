// Package redlog provides a Redis compatible logger.
//   http://build47.com/redis-log-format-levels/
package redlog

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ssh/terminal"
)

const (
	levelDebug   = 0 // '.'
	levelVerbose = 1 // '-'
	levelNotice  = 2 // '*'
	levelWarning = 3 // '#'
	levelError   = 4 // '#' special condition, red
)

var levelChars = []byte{'.', '-', '*', '#', '#'}
var levelColors = []string{"35", "", "1", "33", "31"}

// Options ...
type Options struct {
	Level      int
	Filter     func(line string, tty bool) (msg string, app byte, level int)
	PostFilter func(line string, tty bool) string
	TimeFormat string
	App        byte
}

// DefaultOptions ...
var DefaultOptions = &Options{
	Level:      2,
	Filter:     nil,
	PostFilter: nil,
	App:        'M',
	TimeFormat: "02 Jan 2006 15:04:05.000",
}

// Logger ...
type Logger struct {
	appch      uint32
	tty        bool
	level      int
	pid        int
	timeFormat string
	filter     func(line string, tty bool) (msg string, app byte, level int)
	postFilter func(line string, tty bool) string

	mu sync.Mutex
	wr io.Writer
}

// New sets the level of the logger.
//   0 - Debug
//   1 - Verbose
//   2 - Notice
//   3 - Warning
func New(wr io.Writer, opts *Options) *Logger {
	if wr == nil {
		wr = ioutil.Discard
	}
	if opts == nil {
		opts = DefaultOptions
	}
	if opts.Level < levelDebug || opts.Level > levelWarning {
		panic("invalid level")
	}
	if opts.App == 0 {
		opts.App = DefaultOptions.App
	}
	if opts.TimeFormat == "" {
		opts.TimeFormat = DefaultOptions.TimeFormat
	}
	l := new(Logger)
	l.timeFormat = opts.TimeFormat
	l.wr = wr
	l.filter = opts.Filter
	l.postFilter = opts.PostFilter
	l.SetApp(opts.App)
	l.level = opts.Level
	l.pid = os.Getpid()
	if f, ok := wr.(*os.File); ok && terminal.IsTerminal(int(f.Fd())) {
		l.tty = true
	}
	return l
}

func logPostFilter(line string) string {
	a := strings.IndexByte(line, ':')
	b := strings.IndexByte(line, ' ')
	c := b + 25
	if a == -1 || b == -1 || b != a+2 || c >= len(line) || line[c] != ' ' {
		return line
	}
	clr := ""
	switch line[a+1] {
	default:
		return line
	case 'S':
		clr = "\x1b[31m"
	case 'L':
		clr = "\x1b[32m"
	case 'C':
		clr = "\x1b[36m"
	case 'F':
		clr = "\x1b[33m"
	case 'M':
		clr = "\x1b[35m"
	}
	line = clr + line[:b] + "\x1b[0m\x1b[2m" + line[b:c] + "\x1b[0m" + line[c:]
	return line
}

// SetApp sets the app character
func (l *Logger) SetApp(app byte) {
	atomic.StoreUint32(&l.appch, uint32(app))
}

// App returns the app character
func (l *Logger) App() byte {
	return byte(atomic.LoadUint32(&l.appch))
}

// Debugf ...
func (l *Logger) Debugf(format string, args ...interface{}) {
	if levelDebug >= l.level {
		l.writef(levelDebug, format, args)
	}
}

// Debug ...
func (l *Logger) Debug(args ...interface{}) {
	if levelDebug >= l.level {
		l.write(levelDebug, args)
	}
}

// Debugln ...
func (l *Logger) Debugln(args ...interface{}) {
	if levelDebug >= l.level {
		l.write(levelDebug, args)
	}
}

// Verbf ...
func (l *Logger) Verbf(format string, args ...interface{}) {
	if levelVerbose >= l.level {
		l.writef(levelVerbose, format, args)
	}
}

// Verb ...
func (l *Logger) Verb(args ...interface{}) {
	if levelVerbose >= l.level {
		l.write(levelVerbose, args)
	}
}

// Verbln ...
func (l *Logger) Verbln(args ...interface{}) {
	if levelVerbose >= l.level {
		l.write(levelVerbose, args)
	}
}

// Noticef ...
func (l *Logger) Noticef(format string, args ...interface{}) {
	l.writef(levelNotice, format, args)
}

// Notice ...
func (l *Logger) Notice(args ...interface{}) {
	l.write(levelNotice, args)
}

// Noticeln ...
func (l *Logger) Noticeln(args ...interface{}) {
	l.write(levelNotice, args)
}

// Printf ...
func (l *Logger) Printf(format string, args ...interface{}) {
	l.writef(levelNotice, format, args)
}

// Print ...
func (l *Logger) Print(args ...interface{}) {
	l.write(levelNotice, args)
}

// Println ...
func (l *Logger) Println(args ...interface{}) {
	l.write(levelNotice, args)
}

// Warningf ...
func (l *Logger) Warningf(format string, args ...interface{}) {
	l.writef(levelWarning, format, args)
}

// Warning ...
func (l *Logger) Warning(args ...interface{}) {
	l.write(levelWarning, args)
}

// Warningln ...
func (l *Logger) Warningln(args ...interface{}) {
	l.write(levelWarning, args)
}

// Fatalf ...
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.writef(levelError, format, args)
	os.Exit(1)
}

// Fatal ...
func (l *Logger) Fatal(args ...interface{}) {
	l.write(levelError, args)
	os.Exit(1)
}

// Fatalln ...
func (l *Logger) Fatalln(args ...interface{}) {
	l.write(levelError, args)
	os.Exit(1)
}

// Panicf ...
func (l *Logger) Panicf(format string, args ...interface{}) {
	l.writef(levelError, format, args)
	panic("")
}

// Panic ...
func (l *Logger) Panic(args ...interface{}) {
	l.write(levelError, args)
	panic("")
}

// Panicln ...
func (l *Logger) Panicln(args ...interface{}) {
	l.write(levelError, args)
	panic("")
}

// Errorf ...
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.writef(levelError, format, args)
}

// Error ...
func (l *Logger) Error(args ...interface{}) {
	l.write(levelError, args)
}

// Errorln ...
func (l *Logger) Errorln(args ...interface{}) {
	l.write(levelError, args)
}

// Write writes to the log
func (l *Logger) Write(p []byte) (int, error) {
	level := l.level
	app := l.App()
	line := string(p)
	if l.filter != nil {
		line, app, level = l.filter(line, l.tty)
		if app == 0 {
			app = l.App()
		}
		if level < levelDebug {
			level = levelDebug
		} else if level > levelWarning {
			level = levelWarning
		}
	}
	if level >= l.level {
		write(false, l, app, level, "", []interface{}{line})
	}
	return len(p), nil
}

func (l *Logger) writef(level int, format string, args []interface{}) {
	if level >= l.level {
		write(true, l, l.App(), level, format, args)
	}
}

//go:noinline
func (l *Logger) write(level int, args []interface{}) {
	if level >= l.level {
		write(false, l, l.App(), level, "", args)
	}
}

//go:noinline
func write(useFormat bool, l *Logger, app byte, level int, format string,
	args []interface{}) {
	if l.wr == ioutil.Discard {
		return
	}
	var prefix []byte
	now := time.Now()
	prefix = strconv.AppendInt(prefix, int64(l.pid), 10)
	prefix = append(prefix, ':', app, ' ')
	prefix = now.AppendFormat(prefix, l.timeFormat)
	prefix = append(prefix, ' ')
	if l.tty && levelColors[level] != "" {
		prefix = append(prefix, "\x1b["+levelColors[level]+"m"...)
		prefix = append(prefix, levelChars[level])
		prefix = append(prefix, "\x1b[0m"...)
	} else {
		prefix = append(prefix, levelChars[level])
	}
	var msg string
	if useFormat {
		msg = fmt.Sprintf(format, args...)
	} else {
		msg = fmt.Sprint(args...)
	}
	for len(msg) > 0 {
		switch msg[len(msg)-1] {
		case '\t', ' ', '\r', '\n':
			msg = msg[:len(msg)-1]
			continue
		}
		break
	}
	line := strings.TrimSpace(fmt.Sprintf("%s %s", prefix, msg))
	if l.postFilter != nil {
		line = strings.TrimSpace(l.postFilter(line, l.tty))
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.tty {
		line = logPostFilter(line)
	}
	fmt.Fprintf(l.wr, "%s\n", line)
}

// HashicorpRaftFilter is used as a filter to convert a log message
// from the hashicorp/raft package into redlog structured message.
var HashicorpRaftFilter func(line string, tty bool) (msg string, app byte,
	level int)

func init() {
	HashicorpRaftFilter = func(line string, tty bool) (msg string, app byte,
		level int) {
		msg = string(line)
		idx := strings.IndexByte(msg, ' ')
		if idx != -1 {
			msg = msg[idx+1:]
		}
		idx = strings.IndexByte(msg, ']')
		if idx != -1 && msg[0] == '[' {
			switch msg[1] {
			default: // -> verbose
				level = levelVerbose
			case 'W': // warning -> warning
				level = levelWarning
			case 'E': // error -> warning
				level = levelWarning
			case 'D': // debug -> debug
				level = levelDebug
			case 'V': // verbose -> verbose
				level = levelVerbose
			case 'I': // info -> notice
				level = levelNotice
			}
			msg = msg[idx+1:]
			for len(msg) > 0 && msg[0] == ' ' {
				msg = msg[1:]
			}
		}
		if tty {
			msg = strings.Replace(msg, "[Leader]",
				"\x1b[32m[Leader]\x1b[0m", 1)
			msg = strings.Replace(msg, "[Follower]",
				"\x1b[33m[Follower]\x1b[0m", 1)
			msg = strings.Replace(msg, "[Candidate]",
				"\x1b[36m[Candidate]\x1b[0m", 1)
		}
		idx = strings.Index(msg, "raft: entering ")
		if idx != -1 {
			if strings.Index(msg[idx:], " state:") != -1 {
				level = levelWarning
			}
		}
		return msg, app, level
	}
}

// RedisLogColorizer filters the Redis log output and colorizes it.
func RedisLogColorizer(wr io.Writer) io.Writer {
	if f, ok := wr.(*os.File); !ok || !terminal.IsTerminal(int(f.Fd())) {
		return wr
	}
	pr, pw := io.Pipe()
	go func() {
		rd := bufio.NewReader(pr)
		for {
			line, err := rd.ReadString('\n')
			if err != nil {
				return
			}
			parts := strings.Split(line, " ")
			if len(parts) > 6 {
				var color string
				switch parts[5] {
				case ".":
					color = "\x1b[35m"
				case "-":
					color = ""
				case "*":
					color = "\x1b[1m"
				case "#":
					color = "\x1b[33m"
				}
				if color != "" {
					parts[5] = color + parts[5] + "\x1b[0m"
					line = strings.Join(parts, " ")
				}
			}
			os.Stdout.Write([]byte(logPostFilter(line)))
			continue
		}
	}()
	return pw
}

// GoLogger returns a standard Go log.Logger which when used, will print
// in the Redlog format.
func (l *Logger) GoLogger() *log.Logger {
	rd, wr := io.Pipe()
	gl := log.New(wr, "", 0)
	go func() {
		brd := bufio.NewReader(rd)
		for {
			line, err := brd.ReadBytes('\n')
			if err != nil {
				continue
			}
			l.Printf("%s", line[:len(line)-1])
		}
	}()
	return gl
}
