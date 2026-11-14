package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/internal/mysql"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/config"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

// Set by the Makefile through -ldflags; empty in a plain `go build`.
var (
	BuildVersion = ""
	BuildDate    = ""
)

func main() {
	app := cli.NewApp()

	app.Name = "IceFireDB-SQLProxy"
	app.Version = version()
	app.Usage = "MySQL protocol proxy that replicates writes across an IceFireDB p2p network"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:     "config, c",
			Usage:    "config file path",
			Value:    "config/config.yaml",
			Required: false,
		},
		cli.StringFlag{
			Name:  "log,l",
			Usage: "log level: debug,info,warning,error",
			Value: "info",
		},
	}

	app.Before = func(c *cli.Context) error {
		log.SetFlags(log.Llongfile)
		// init log
		lv, err := logrus.ParseLevel(c.String("log"))
		if err != nil {
			return err
		}
		logrus.SetLevel(lv)
		// init config
		return config.InitConfig(c.String("config"))
	}
	app.Action = func(c *cli.Context) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		watchSignals(cancel)
		return mysql.Run(ctx)
	}
	if err := app.Run(os.Args); err != nil {
		logrus.Errorf("failed to run application: %v", err)
		os.Exit(1)
	}
}

// version renders the release version plus the build identity, so a bug report names the
// exact commit it came from.
func version() string {
	v := mysql.Version
	if BuildVersion != "" {
		v += " (" + BuildVersion + ")"
	}
	if BuildDate != "" {
		v += " built " + BuildDate
	}
	return v
}

// watchSignals cancels ctx on the first termination signal so that connections drain and
// the upstream pools close. A second signal exits immediately, for operators who cannot
// wait for the drain.
func watchSignals(cancel context.CancelFunc) {
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		shuttingDown := false
		for sig := range sigs {
			switch sig {
			case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
				if shuttingDown {
					logrus.Warnf("second %s received, exiting without draining", sig)
					os.Exit(1)
				}
				shuttingDown = true
				logrus.Infof("%s received, shutting down", sig)
				cancel()
			case syscall.SIGHUP:
				logrus.Info("SIGHUP ignored: the configuration is read at startup only")
			default:
				logrus.Infof("ignoring signal %s", sig)
			}
		}
	}()
}
