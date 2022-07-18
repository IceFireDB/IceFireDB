/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

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

	"github.com/IceFireDB/IceFireDB-Proxy/pkg/config"
	"github.com/IceFireDB/IceFireDB-Proxy/proxy"
	"github.com/IceFireDB/IceFireDB-Proxy/utils"
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
	app.Name = "redis proxy"
	app.Description = "IceFireDB proxy, easier to use IceFireDB, support resp protocol."
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:     "config,c",
			Usage:    "config file",
			Required: false,
			Value:    "config/config.yaml",
		},
	}
	//showBanner()
	logrus.Println("Starting Pubsub ...")
	app.Before = initConfig
	app.Action = start
	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}
}

func start(c *cli.Context) error {
	ctx, cancel := context.WithCancel(context.TODO())
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
			cancel()
			ok := make(chan struct{})
			go func() {
				wg.Wait()
				ok <- struct{}{}
			}()
			select {
			case <-ok:
				logrus.Info("shutdown proxy ！！！！")
			case <-time.After(time.Second * 5):
				logrus.Info("context deadline exceeded ！！！！")
			}
			os.Exit(0)
		case syscall.SIGHUP:
			logrus.Info("+++++++++++++++++++++++++++++")
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

func showBanner() {
	logo := `╦═╗┌─┐┌┬┐┬┌─┐  ╔═╗┬─┐┌─┐─┐ ┬┬ ┬
╠╦╝├┤  │││└─┐  ╠═╝├┬┘│ │┌┴┬┘└┬┘
╩╚═└─┘─┴┘┴└─┘  ╩  ┴└─└─┘┴ └─ ┴ `
	fmt.Println(logo)
	fmt.Println("Build Version: ", BuildVersion, "  Date: ", BuildDate)
}
