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

package rediscluster

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Batch pack multiple commands, which should be supported by Do method.
type Batch struct {
	cluster        *Cluster
	batches        []nodeBatch
	index          []int
	isSlaveOperate bool
	// Quantity seed: Can be used for randomization to avoid performance overhead
	numSeed uint64
}

type nodeBatch struct {
	node *redisNode
	// Master node: Only the master node is populated in case the slave node requests demotion
	masterNode *redisNode
	cmds       []nodeCommand

	err  error
	done chan int
}

type nodeCommand struct {
	cmd   string
	args  []interface{}
	reply interface{}
	err   error
}

// NewBatch create a new batch to pack mutiple commands.
func (cluster *Cluster) NewBatch(numSeed uint64) *Batch {
	return &Batch{
		cluster:        cluster,
		batches:        make([]nodeBatch, 0),
		index:          make([]int, 0),
		isSlaveOperate: false,
		numSeed:        numSeed,
	}
}

func (batch *Batch) SetIsSlaveOperate() {
	if batch.cluster.IsSlaveOperate() {
		batch.isSlaveOperate = true
	} else {
		batch.isSlaveOperate = false
	}
}

func (batch *Batch) GetIsSlaveOperate() bool {
	return batch.isSlaveOperate
}

// Put add a redis command to batch, DO NOT put MGET/MSET/MSETNX.
func (batch *Batch) Put(isSlaveOperate bool, cmd string, args ...interface{}) error {
	if len(args) < 1 {
		return fmt.Errorf("Put: no key found in args")
	}

	if cmd == "MGET" || cmd == "MSET" || cmd == "MSETNX" {
		return fmt.Errorf("Put: %s not supported", cmd)
	}

	var node *redisNode
	var masterNode *redisNode
	var err error

	switch strings.ToUpper(cmd) {
	case "GET":
		// If read/write separation is enabled and the current probability of performing slave operations is correct, select an available slave node
		// If the operation is performed on the slave node
		if isSlaveOperate {
			node, masterNode, err = batch.cluster.getAnSlaveNodeByNumSeed(args[0], batch.numSeed)
			// If no slave nodes are available, for example, the number of slave nodes is 0
			if err != nil {
				go batch.cluster.UpdateSlotsByRandomMasterNode()

				node, err = batch.cluster.getNodeByKey(args[0])
			}
		} else {
			node, err = batch.cluster.getNodeByKey(args[0])
		}
	default:
		node, err = batch.cluster.getNodeByKey(args[0])
	}
	if err != nil {
		return fmt.Errorf("Put: %v", err)
	}

	var i int
	batchesLen := len(batch.batches)

	for i = 0; i < batchesLen; i++ {
		if batch.batches[i].node == node {
			batch.batches[i].cmds = append(batch.batches[i].cmds,
				nodeCommand{cmd: cmd, args: args})

			batch.index = append(batch.index, i)
			break
		}
	}

	if i == batchesLen {
		batch.batches = append(batch.batches,
			nodeBatch{
				node:       node,
				masterNode: masterNode,
				cmds:       []nodeCommand{{cmd: cmd, args: args}},
				done:       make(chan int),
			})
		batch.index = append(batch.index, i)
	}

	return nil
}

// RunBatch execute commands in batch simutaneously. If multiple commands are
// directed to the same node, they will be merged and sent at once using pipeling.
func (cluster *Cluster) RunBatch(bat *Batch) ([]interface{}, error) {
	batchesCount := len(bat.batches)

	var batchesWg sync.WaitGroup
	batchesWg.Add(batchesCount)

	for i := range bat.batches {
		go doBatch(cluster, &bat.batches[i], &batchesWg, bat.isSlaveOperate)
	}

	batchesWg.Wait()

	var replies []interface{}

	for _, i := range bat.index {
		if checkReply(bat.batches[i].cmds[0].reply) == kRespClusterDown {
			bat.batches[i].cmds[0].reply = nil
			cluster.UpdateSlotsInfoByRandomNode(bat.batches[i].node)
			return nil, Cluster_Down_Error
		}

		if bat.batches[i].err != nil {
			bat.batches[i].cmds[0].reply = nil
			cluster.UpdateSlotsInfoByRandomNode(bat.batches[i].node)
		}

		replies = append(replies, bat.batches[i].cmds[0].reply)
		bat.batches[i].cmds = bat.batches[i].cmds[1:]
	}

	return replies, nil
}

func doBatch(cluster *Cluster, batch *nodeBatch, wg *sync.WaitGroup, isSlaveOperate bool) {
	defer wg.Done()

	node := batch.node

	conn, err := node.getConn()
	if err != nil {
		// If the slave node fails, it sends a cluster update signal
		go cluster.UpdateSlotsByRandomMasterNode()

		if node.NodeType == SLAVE_NODE && batch.masterNode != nil {
			node = batch.masterNode
			conn, err = node.getConn()

			if err != nil {
				batch.err = err
				return
			}
		} else {
			batch.err = err
			return
		}
	}

	// If read/write separation is enabled on the secondary node, the readonly command is sent
	if node.NodeType == SLAVE_NODE {
		err = conn.send("READONLY")

		if err != nil {
			batch.err = err
			conn.shutdown()
			return
		}
	}

	for i := range batch.cmds {
		err = conn.send(batch.cmds[i].cmd, batch.cmds[i].args...)

		if err != nil {
			batch.err = err
			conn.shutdown()
			return
		}
	}

	// Flushing the CONN buffer
	err = conn.flush()
	if err != nil {
		batch.err = err
		conn.shutdown()
		return
	}

	// In the case of slave read, the READONLY command is used first
	if node.NodeType == SLAVE_NODE {
		reply, err := conn.receive()
		if err != nil {
			batch.err = err
			conn.shutdown()
			return
		}

		if checkReply(reply) != kRespOK {
			batch.err = errors.New("doBatch : [READONLY command] -> Reply is not OK.")
			conn.shutdown()
			return
		}
	}

	// Read modified version of data: increase support for move ask and other instructions and node update induction
	for i := range batch.cmds {
		reply, err := conn.receive()
		if err != nil {
			batch.err = err
			conn.shutdown()
			return
		}

		// Check the reply type: Perform actions according to the type: MOVE, ASK, TIMEOUT
		resp := checkReply(reply)

		switch resp {
		case kRespOK, kRespError:
			err = nil
		case kRespMove:
			for {
				reply, err = cluster.handleMove(node, reply.(redisError).Error(), batch.cmds[i].cmd, batch.cmds[i].args)
				respType := checkReply(reply)
				if respType != kRespMove {
					switch respType {
					case kRespOK, kRespError:
						err = nil
					case kRespAsk:
						reply, err = cluster.handleAsk(node, reply.(redisError).Error(), batch.cmds[i].cmd, batch.cmds[i].args)
					case kRespConnTimeout:
						reply, err = cluster.handleConnTimeout(node, batch.cmds[i].cmd, batch.cmds[i].args)
					case kRespClusterDown:
						err = Cluster_Down_Error
					}
					break
				}
			}
		case kRespAsk:
			reply, err = cluster.handleAsk(node, reply.(redisError).Error(), batch.cmds[i].cmd, batch.cmds[i].args)
		case kRespConnTimeout:
			reply, err = cluster.handleConnTimeout(node, batch.cmds[i].cmd, batch.cmds[i].args)
		case kRespClusterDown:
			err = Cluster_Down_Error
		}

		batch.cmds[i].reply, batch.cmds[i].err = reply, err
	}

	node.releaseConn(conn)
}
