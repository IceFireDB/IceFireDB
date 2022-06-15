package main

import (
	"context"
	"fmt"
	"github.com/IceFireDB/IceFireDB-SQLProxy/internal/mysql"
	"github.com/IceFireDB/IceFireDB-SQLProxy/pkg/config"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	app := cli.NewApp()

	app.Name = "IceFireDB-SQLProxy"

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
		confPath := c.String("config")
		config.InitConfig(confPath)
		return nil
	}
	app.Action = func(c *cli.Context) error {
		ctx, cancel := context.WithCancel(context.TODO())
		go exitSignal(cancel)
		if err := mysql.Run(ctx); err != nil {
			return err
		}
		return nil
	}
	if err := app.Run(os.Args); err != nil {
		panic(fmt.Sprintf("app run error: %v", err))
	}
}

func exitSignal(cancel context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for sig := range sigs {
		switch sig {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			cancel()
			fmt.Println("Bye!")
			os.Exit(0)
		case syscall.SIGHUP:
			fmt.Println("+++++++++++++++++++++++++++++")
		default:
			fmt.Println(sig)
		}
	}
}
