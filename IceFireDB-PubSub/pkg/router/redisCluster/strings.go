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

package redisCluster

import (
	"github.com/IceFireDB/IceFireDB/IceFireDB-PubSub/pkg/router"
	rediscluster "github.com/chasex/redis-go-cluster"
)

func (r *Router) cmdMGET(s *router.Context) error {
	batch := r.redisCluster.NewBatch()
	argLen := len(s.Args)
	for i := 1; i < argLen; i++ {
		err := batch.Put("GET", s.Args[i])
		if err != nil {
			return err
		}
	}
	var err error
	reply, err := rediscluster.Values(r.redisCluster.RunBatch(batch))
	if err != nil && err != rediscluster.ErrNil {
		return router.WriteError(s.Writer, err)
	}

	if len(reply) == 1 {
		if err, ok := reply[0].(error); ok {
			return router.WriteError(s.Writer, err)
		}
	}
	s.Reply = reply
	return router.RecursivelyWriteObjects(s.Writer, reply...)
}

func (r *Router) cmdDEL(s *router.Context) error {
	argLen := len(s.Args)

	if argLen == 2 {
		reply, err := rediscluster.Int64(r.redisCluster.Do(s.Cmd, s.Args[1]))
		if err != nil {
			return router.WriteError(s.Writer, err)
		}
		return router.WriteInt(s.Writer, reply)
	}

	batch := r.redisCluster.NewBatch()
	for i := 1; i < argLen; i++ {
		err := batch.Put("DEL", s.Args[i])
		if err != nil {
			return err
		}
	}
	reply, err := rediscluster.Values(r.redisCluster.RunBatch(batch))
	if err != nil && err != rediscluster.ErrNil {
		return router.WriteError(s.Writer, err)
	}

	var delCount int64
	for _, valInterface := range reply {
		if count, ok := valInterface.(int64); ok {
			delCount += count
		}
	}
	s.Reply = delCount
	return router.WriteInt(s.Writer, delCount)
}

func (r *Router) cmdEXISTS(s *router.Context) error {
	argLen := len(s.Args)

	if argLen == 2 {
		reply, err := rediscluster.Int64(r.redisCluster.Do(s.Cmd, s.Args[1]))
		if err != nil {
			return router.WriteError(s.Writer, err)
		}
		return router.WriteInt(s.Writer, reply)
	}

	batch := r.redisCluster.NewBatch()
	for i := 1; i < argLen; i++ {
		err := batch.Put(s.Cmd, s.Args[i])
		if err != nil {
			return err
		}
	}
	reply, err := rediscluster.Values(r.redisCluster.RunBatch(batch))
	if err != nil && err != rediscluster.ErrNil {
		return router.WriteError(s.Writer, err)
	}

	var existsCount int64
	for _, valInterface := range reply {
		if count, ok := valInterface.(int64); ok {
			existsCount += count
		}
	}
	s.Reply = existsCount
	return router.WriteInt(s.Writer, existsCount)
}
