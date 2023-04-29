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

package proxy

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/IceFireDB/IceFireDB/IceFireDB-PubSub/pkg/ppubsub"
	"github.com/sirupsen/logrus"

	"github.com/IceFireDB/IceFireDB-Proxy/pkg/cache"
	"github.com/IceFireDB/IceFireDB/IceFireDB-PubSub/pkg/p2p"

	"github.com/IceFireDB/IceFireDB/IceFireDB-PubSub/pkg/bareneter"
	"github.com/IceFireDB/IceFireDB/IceFireDB-PubSub/pkg/config"
	"github.com/IceFireDB/IceFireDB/IceFireDB-PubSub/pkg/router"
	proxycluster "github.com/IceFireDB/IceFireDB/IceFireDB-PubSub/pkg/router/redisCluster"
	proxynode "github.com/IceFireDB/IceFireDB/IceFireDB-PubSub/pkg/router/redisNode"
	rediscluster "github.com/chasex/redis-go-cluster"
	redisclient "github.com/gomodule/redigo/redis"
)

type Proxy struct {
	Cache        *cache.Cache
	proxyCluster rediscluster.Cluster
	proxyClient  *redisclient.Pool
	server       *bareneter.Server
	router       router.IRoutes
	P2pHost      *p2p.P2P
	P2pSubPub    *p2p.PubSub
}

func New() (*Proxy, error) {
	p := &Proxy{}
	var err error
	if config.Get().RedisDB.Type == config.TypeNode {
		p.proxyClient = &redisclient.Pool{
			MaxIdle:     config.Get().RedisDB.ConnPoolSize,
			IdleTimeout: time.Duration(config.Get().RedisDB.ConnAliveTimeOut) * time.Second,
			Dial: func() (redisclient.Conn, error) {
				c, err := redisclient.Dial("tcp", config.Get().RedisDB.StartNodes,
					redisclient.DialConnectTimeout(time.Duration(config.Get().RedisDB.ConnTimeOut)*time.Second),
					redisclient.DialReadTimeout(time.Duration(config.Get().RedisDB.ConnReadTimeOut)*time.Second),
					redisclient.DialWriteTimeout(time.Duration(config.Get().RedisDB.ConnWriteTimeOut)*time.Second))
				if err != nil {
					return nil, err
				}
				return c, err
			},
			TestOnBorrow: func(c redisclient.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		}
		p.router = proxynode.NewRouter(p.proxyClient)
	} else {
		p.proxyCluster, err = rediscluster.NewCluster(
			&rediscluster.Options{
				StartNodes:   strings.Split(config.Get().RedisDB.StartNodes, ","),
				ConnTimeout:  time.Duration(config.Get().RedisDB.ConnTimeOut) * time.Second,
				ReadTimeout:  time.Duration(config.Get().RedisDB.ConnReadTimeOut) * time.Second,
				WriteTimeout: time.Duration(config.Get().RedisDB.ConnWriteTimeOut) * time.Second,
				KeepAlive:    config.Get().RedisDB.ConnPoolSize,
				AliveTime:    time.Duration(config.Get().RedisDB.ConnAliveTimeOut) * time.Second,
			})
		if err != nil {
			return nil, err
		}
		p.router = proxycluster.NewRouter(p.proxyCluster)
	}

	// if enable p2p command pubsub mode,then create p2p pubsub handle
	if config.Get().P2P.Enable {
		logrus.Println("Starting Pubsub ...")
		// create p2p element
		p2phost := p2p.NewP2P(config.Get().P2P.ServiceDiscoveryID) // create p2p
		p.P2pHost = p2phost

		logrus.Println("Completed P2P Setup")

		// Connect to peers with the chosen discovery method
		switch strings.ToLower(config.Get().P2P.ServiceDiscoverMode) {
		case "announce":
			p2phost.AnnounceConnect() // KadDHT p2p net create
		case "advertise":
			p2phost.AdvertiseConnect()
		default:
			p2phost.AdvertiseConnect()
		}

		logrus.Println("Connected to P2P Service Peers")

		p.P2pSubPub, err = p2p.JoinPubSub(p.P2pHost, "redis-client", config.Get().P2P.ServiceCommandTopic)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		log.Printf("Successfully joined [%s] P2P channel. \n", config.Get().P2P.ServiceCommandTopic)
		//init ppubsub
		ppubsub.InitPubSub(context.Background(), p.P2pHost)
	}

	p.router.Use(router.IgnoreCMDMiddleware(config.Get().IgnoreCMD.Enable, config.Get().IgnoreCMD.CMDList))

	if config.Get().P2P.Enable {
		p.router.Use(router.PubSubMiddleware(p.router, p.P2pSubPub))
	}
	p.router.InitCMD()

	p.server = bareneter.NewServerNetwork("tcp",
		fmt.Sprintf(":%d", config.Get().Proxy.LocalPort),
		p.handle,
		p.accept,
		p.closed)
	return p, nil
}

func (p *Proxy) Run(ctx context.Context, errSignal chan error) {
	go func() {
		select {
		case <-ctx.Done():
			_ = p.server.Close()
		}
	}()
	_ = p.server.ListenServeAndSignal(errSignal)
}
