package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/IceFireDB/IceFireDB/IceFireDB-PubSub/pkg/config"
	"github.com/IceFireDB/IceFireDB/IceFireDB-PubSub/proxy"
	"github.com/IceFireDB/IceFireDB/IceFireDB-PubSub/utils"
	"github.com/spf13/viper"
	"github.com/urfave/cli"
)

// BuildDate: Binary file compilation time
// BuildVersion: Binary compiled GIT version
var (
	BuildDate    string
	BuildVersion string
)

func main() {
	app := cli.NewApp()
	app.Name = "IceFireDB-PubSub"
	app.Description = "IceFireDB-PubSub, easier to use P2P Events, support resp protocol."
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:     "config,c",
			Usage:    "config file",
			Required: false,
			Value:    "config/config.yaml",
		},
	}
	app.Before = initConfig
	app.Action = start
	err := app.Run(os.Args)
	if err != nil {
		logrus.Errorf("failed to run application: %v", err)
		os.Exit(1)
	}
}

func start(c *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := sync.WaitGroup{}
	errSignal := make(chan error)
	p, err := proxy.New()
	if err != nil {
		return err
	}

	wg.Add(1)
	fmt.Println("listener port:", config.Get().Proxy.LocalPort)
	utils.GoWithRecover(func() {
		defer wg.Done()
		p.Run(ctx, errSignal)
	}, nil)
	err = <-errSignal

	if err != nil {
		return err
	}

	// Listening to the offline
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for sig := range sigs {
		switch sig {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			logrus.Info("Received shutdown signal, initiating graceful shutdown...")
			cancel()

			// Use sync.Once to ensure shutdown is only performed once
			var once sync.Once
			once.Do(func() {
				ok := make(chan struct{})
				go func() {
					wg.Wait()
					close(ok)
				}()

				select {
				case <-ok:
					logrus.Info("All goroutines have gracefully shut down.")
				case <-time.After(time.Second * 5):
					logrus.Warn("Context deadline exceeded, forcing shutdown.")
				}

				os.Exit(0)
			})

		case syscall.SIGHUP:
			logrus.Info("Received SIGHUP signal, performing reload or reconfiguration if supported.")
		}
	}

	return nil
}

func initConfig(c *cli.Context) error {
	// Read configuration file configuration
	viper.SetConfigFile(c.String("config"))
	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	// Map configuration file content to structure
	err := config.InitConfig()
	if err != nil {
		return err
	}
	debug()

	return nil
}

func debug() {
	// Open pprof
	if config.Get().PprofDebug.Enable {
		utils.GoWithRecover(func() {
			addr := strconv.Itoa(int(config.Get().PprofDebug.Port))
			_ = http.ListenAndServe(":"+addr, nil)
		}, nil)
	}
}
