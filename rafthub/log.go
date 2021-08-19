// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package rafthub

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/hashicorp/go-hclog"
	"github.com/tidwall/redlog/v2"
)

// Logger is the logger used by rafthub for printing all console messages.
type Logger interface {
	Debugf(format string, args ...interface{})
	Debug(args ...interface{})
	Debugln(args ...interface{})
	Verbf(format string, args ...interface{})
	Verb(args ...interface{})
	Verbln(args ...interface{})
	Noticef(format string, args ...interface{})
	Notice(args ...interface{})
	Noticeln(args ...interface{})
	Printf(format string, args ...interface{})
	Print(args ...interface{})
	Println(args ...interface{})
	Warningf(format string, args ...interface{})
	Warning(args ...interface{})
	Warningln(args ...interface{})
	Fatalf(format string, args ...interface{})
	Fatal(args ...interface{})
	Fatalln(args ...interface{})
	Panicf(format string, args ...interface{})
	Panic(args ...interface{})
	Panicln(args ...interface{})
	Errorf(format string, args ...interface{})
	Error(args ...interface{})
	Errorln(args ...interface{})
}

func logInit(conf Config) (hclog.Logger, *redlog.Logger) {
	var log *redlog.Logger
	logLevel := conf.LogLevel
	wr := conf.LogOutput
	lopts := *redlog.DefaultOptions
	lopts.Filter =
		func(line string, tty bool) (msg string, app byte, level int) {
			line = stateChangeFilter(line, log)
			return redlog.HashicorpRaftFilter(line, tty)
		}
	lopts.App = 'S'
	switch logLevel {
	case "debug":
		lopts.Level = 0
	case "verbose", "verb":
		lopts.Level = 1
	case "notice", "info":
		lopts.Level = 2
	case "warning", "warn":
		lopts.Level = 3
	case "quiet", "silent":
		lopts.Level = 3
		wr = ioutil.Discard
	default:
		fmt.Fprintf(os.Stderr, "invalid -loglevel: %s\n", logLevel)
		os.Exit(1)
	}
	log = redlog.New(wr, &lopts)
	hclopts := *hclog.DefaultOptions
	hclopts.Color = hclog.ColorOff
	hclopts.Output = log
	if conf.LogReady != nil {
		conf.LogReady(log)
	}
	log.Warningf("starting %s", versline(conf))
	return hclog.New(&hclopts), log
}
