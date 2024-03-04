package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IceFireDB/IceFireDB-SQLite/internal/mysql"
	"github.com/IceFireDB/IceFireDB-SQLite/pkg/config"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()

	app.Name = "github.com/IceFireDB/IceFireDB-SQLite"

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
		stop := make(chan struct{})

		go func() {
			ms := mysql.NewMysqlProxy()
			ms.Run(ctx)
			stop <- struct{}{}
		}()

		return exitSignal(cancel, stop)
	}
	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}
}

func exitSignal(cancel context.CancelFunc, stop chan struct{}) error {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for sig := range sigs {
		switch sig {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			cancel()

			select {
			case <-stop:
				fmt.Println("shutdown")
			case <-time.After(time.Second * 5):
				fmt.Println("timeout forced shutdown")
			}
			os.Exit(0)
		case syscall.SIGHUP:
			fmt.Println("catch syscall.SIGHUP")
		}
	}
	return nil
}
