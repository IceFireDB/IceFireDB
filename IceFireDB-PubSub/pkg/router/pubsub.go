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

package router

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/IceFireDB/IceFireDB/IceFireDB-PubSub/utils"

	"github.com/IceFireDB/IceFireDB/IceFireDB-PubSub/pkg/p2p"
)

// PubSubMiddleware Synchronize notification to peer node middleware
func PubSubMiddleware(router IRoutes, pubSub *p2p.PubSub) HandlerFunc {
	subscribe(router, pubSub)
	return func(context *Context) error {
		// sync write operate cmd and remove 'publish' command
		if context.Op.IsMasterOnly() && strings.ToUpper(context.Cmd) != "PUBLISH" {
			args := make([]string, len(context.Args))
			for k, v := range context.Args {
				args[k] = string(v.([]byte))
			}
			s, _ := json.Marshal(args)
			pubSub.Outbound <- string(s)
			logrus.Info("outbound: ", string(s))
		}
		return context.Next()
	}
}

func subscribe(router IRoutes, pubSub *p2p.PubSub) {
	utils.GoWithRecover(func() {
		for args := range pubSub.Inbound {
			logrus.Info("inbound: ", args.Message)
			var data []interface{}
			err := json.Unmarshal([]byte(args.Message), &data)
			if err != nil {
				logrus.Errorf("subscribe error: %v", err)
				continue
			}
			err = router.Sync(data)
			if err != nil {
				logrus.Errorf("subscribe sync error: %v", err)
			}
		}
	}, func(r interface{}) {
		time.Sleep(time.Second)
		subscribe(router, pubSub)
	})
}
