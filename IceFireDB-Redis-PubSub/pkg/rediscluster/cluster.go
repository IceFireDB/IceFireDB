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
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/valyala/fastrand"
)

// Options is used to initialize a new redis cluster.
type Options struct {
	StartNodes []string // Startup nodes

	ConnTimeout  time.Duration // Connection timeout
	ReadTimeout  time.Duration // Read timeout
	WriteTimeout time.Duration // Write timeout

	KeepAlive int           // Maximum keep alive connecion in each node
	AliveTime time.Duration // Keep alive timeout

	SlaveOperateRate       int // 从节点承载的读流量百分比
	ClusterUpdateHeartbeat int // redis 集群的状态更新心跳间隔：只针对读写分离开启的场景生效
}

var Cluster_Down_Error = errors.New("CLUSTERDOWN")

// Cluster is a redis client that manage connections to redis nodes,
// cache and update cluster info, and execute all kinds of commands.
// Multiple goroutines may invoke methods on a cluster simutaneously.
type Cluster struct {
	slots      [kClusterSlots]*redisNode   // redis主节点槽位分布数组
	slaveslots [kClusterSlots][]*redisNode // redis从节点槽位分布数组 由于一个主节点可能有多个从节点，所以同一个slot可能对应多个从服务器，因此需要Slice

	nodes      map[string]*redisNode // redis主节点map
	slaveNodes map[string]*redisNode // redis从节点map：这个用来存储从节点地址的映射关系

	connTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration

	keepAlive int
	aliveTime time.Duration

	updateTime time.Time
	updateList chan updateMesg

	rwLock sync.RWMutex

	closed bool

	isSlaveReadAble  bool // 集群从节点是否开启读取功能
	slaveOperateRate int  // 从节点承载的读流量百分比

	clusterUpdateHeartbeat int // redis集群的状态更新心跳间隔：只针对读写分离开启的场景生效

}

// 集群更新信号的数据结构
type updateMesg struct {
	node      *redisNode // 进行更新的redis node节点
	movedTime time.Time  // 当前信号产生的时间：用于信号限流
}

/*
 *NewCluster()
 *通过Options属性创建Redis Cluster对象
 */
func NewCluster(options *Options) (*Cluster, error) {
	cluster := &Cluster{
		nodes:                  make(map[string]*redisNode),
		slaveNodes:             make(map[string]*redisNode),
		connTimeout:            options.ConnTimeout,
		readTimeout:            options.ReadTimeout,
		writeTimeout:           options.WriteTimeout,
		keepAlive:              options.KeepAlive,
		aliveTime:              options.AliveTime,
		updateList:             make(chan updateMesg),
		isSlaveReadAble:        false,
		slaveOperateRate:       0,
		clusterUpdateHeartbeat: options.ClusterUpdateHeartbeat,
	}

	// 如果开启了读流量百分比参数 则证明开启集群读写分离
	if options.SlaveOperateRate > 0 {
		cluster.isSlaveReadAble = true
		cluster.slaveOperateRate = options.SlaveOperateRate
	}

	// 初始化从节点的槽位数组,由于一个master可能会有多个slave，所以建立slice级别数据结构
	// for i := 0; i < kClusterSlots; i++ {
	// 	cluster.slaveslots[i] = make([]*redisNode, 0)
	// }

	// 遍历集群配置中的startNode节点，通过节点进行集群状态信息的获取工作，只要有一次获取成功则退出
	for i := range options.StartNodes {
		node := &redisNode{
			address:      options.StartNodes[i],
			connTimeout:  options.ConnTimeout,
			readTimeout:  options.ReadTimeout,
			writeTimeout: options.WriteTimeout,
			keepAlive:    options.KeepAlive,
			aliveTime:    options.AliveTime,
		}

		// 根据当前的redis节点进行集群状态更新
		err := cluster.Update(node)
		if err != nil {
			// 当前节点无效、继续尝试下一个节点
			continue
		} else {
			// 获取集群状态信息成功，异步线程进行更新信号监听。
			go cluster.handleUpdate()

			if cluster.isSlaveReadAble {
				// 如果开启了读写分离，则开辟一个单独的协程进行心跳周期的redis集群状态更新工作
				go func() {
					for {
						// KafkaLoger.CInfof("Cluster Update Slots Heartbeat.")
						// 从主节点中随机选择一个进行更新
						cluster.UpdateSlotsByRandomMasterNode()
						time.Sleep(time.Second * time.Duration(cluster.clusterUpdateHeartbeat))
					}
				}()
			}

			return cluster, nil
		}
	}

	return nil, fmt.Errorf("NewCluster: no valid node in %v", options.StartNodes)
}

// Do excute a redis command with random number arguments. First argument will
// be used as key to hash to a slot, so it only supports a subset of redis
// commands.
///
// SUPPORTED: most commands of keys, strings, lists, sets, sorted sets, hashes.
// NOT SUPPORTED: scripts, transactions, clusters.
//
// Particularly, MSET/MSETNX/MGET are supported using result aggregation.
// To MSET/MSETNX, there's no atomicity gurantee that given keys are set at once.
// It's possible that some keys are set, while others not.
//
// See README.md for more details.
// See full redis command list: http://www.redis.io/commands
func (cluster *Cluster) Do(cmd string, args ...interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("Do: no key found in args")
	}

	if cmd == "MSET" || cmd == "MSETNX" {
		return cluster.multiSet(cmd, args...)
	}

	if cmd == "MGET" {
		return cluster.multiGet(cmd, args...)
	}

	node, err := cluster.getNodeByKey(args[0])
	if err != nil {
		return nil, fmt.Errorf("Do->cluster.getNodeByKey() %w", err)
	}

	reply, err := node.do(cmd, args...)
	if err != nil {
		// 这里的err不为空，一定是网络读取错误，但是一条TCP出错不应该直接调用槽位更新: 可能是此条TCP出现问题
		// cluster.UpdateSlotsInfoByRandomNode(node)
		return nil, fmt.Errorf("Do->node.do(%s) %w", cmd, err)
	}

	resp := checkReply(reply)

	// 检查resp类型
	switch resp {
	case kRespOK, kRespError:
		return reply, nil
	case kRespMove:
		// 此处在高并发+slots循环多次集中迁移时，会出现数据的多级别MOVE，对于多级别MOVE 要进行到底，一般频率为20万次中出现10次
		// 所以采用循环进行多级MOVE处理
		for {
			// 尝试第一次MOVE，并对结果进行判断，如果reply类型不再是MOVE类型，则证明摆脱多级MOVE，则把结果返回出去
			// 由于结果可能会发生变化，因此再进行判断
			reply, err = cluster.handleMove(node, reply.(redisError).Error(), cmd, args)

			respType := checkReply(reply)

			// 如果reply类型不是MOVE类型，则 准备跳出循环、对结果进行判断，选择条件返回
			if respType != kRespMove {

				switch respType {
				case kRespOK, kRespError:
					return reply, nil
				case kRespAsk:
					return cluster.handleAsk(node, reply.(redisError).Error(), cmd, args)
				case kRespConnTimeout:
					return cluster.handleConnTimeout(node, cmd, args)
				case kRespClusterDown: // 如果redis集群宕机，则返回宕机错误
					// 选取可用的节点 更新集群状态信息
					cluster.UpdateSlotsInfoByRandomNode(node)
					return reply, Cluster_Down_Error
				}

				// 此处return为了跳出多级MOVE的for循环
				return reply, err
			}

		}
		// return cluster.handleMove(node, reply.(redisError).Error(), cmd, args)
	case kRespAsk:
		return cluster.handleAsk(node, reply.(redisError).Error(), cmd, args)
	case kRespConnTimeout:
		return cluster.handleConnTimeout(node, cmd, args)
	case kRespClusterDown: // 如果redis集群宕机，则返回宕机错误
		// 选取可用的节点 更新集群状态信息
		cluster.UpdateSlotsInfoByRandomNode(node)
		return reply, Cluster_Down_Error
	}

	return nil, fmt.Errorf("Do: unreachable")
}

func (cluster *Cluster) SlaveDo(cmd string, args ...interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("Do: no key found in args")
	}

	if cmd == "MSET" || cmd == "MSETNX" {
		return cluster.multiSet(cmd, args...)
	}

	if cmd == "MGET" {
		return cluster.multiGet(cmd, args...)
	}

	// 选择一个slave节点
	node, err := cluster.getAnRandomSlaveNodeByKey(args[0])
	// 如果从节点选择失败，则发送集群更新信号，并选择一个master节点承载请求
	if err != nil {
		// 更新集群槽位信息: 随机选择一个master节点进行槽位更新
		go cluster.UpdateSlotsByRandomMasterNode()
		// KafkaLoger.CErrorf("RedisCluster Cluster-Do-SLAVE -> getAnRandomSlaveNodeByKey() [%s] -> %s.", args[0], err.Error())

		// 选择cluser中的master节点进行key的服务承载
		node, err = cluster.getNodeByKey(args[0])
		if err != nil {
			return nil, fmt.Errorf("SlaveDo->cluster.getNodeByKey() %w", err)
		}
	}

	// 从node中获取一条conn链接
	conn, err := node.getConn()
	if err != nil {
		return nil, fmt.Errorf("SlaveDo->node.getConn() %w", err)
	}

	switch cmd {
	case "GET": // 如果是从节点，则发送READONLY指令
		if node.NodeType == SLAVE_NODE {
			err = conn.send("READONLY")
			if err != nil {
				conn.shutdown()
				return nil, fmt.Errorf("SlaveDo->node.send->READONLY() %w", err)
			}
		}
	}

	err = conn.send(cmd, args...)
	if err != nil {
		conn.shutdown()
		// 这里的err不为空，一定是网络读取错误，但是一条TCP出错不应该直接调用槽位更新: 可能是此条TCP出现问题
		return nil, fmt.Errorf("SlaveDo->conn.send(%s) %w", cmd, err)
	}

	// 刷新conn缓冲区
	err = conn.flush()
	if err != nil {
		conn.shutdown()
		return nil, fmt.Errorf("SlaveDo->conn.flush(%s) %w", cmd, err)
	}

	switch cmd {
	case "GET": // 如果是从节点，则处理前面的READONLY指令响应
		if node.NodeType == SLAVE_NODE {

			reply, err := conn.receive()
			if err != nil {
				conn.shutdown()
				return nil, fmt.Errorf("SlaveDo->node.receive->READONLY() %w", err)
			}

			if checkReply(reply) != kRespOK {
				conn.shutdown()
				return nil, fmt.Errorf("SlaveDo->node.receive->READONLY() :[READONLY command] -> Reply is not OK.")
			}
		}
	}

	reply, err := conn.receive()
	if err != nil {
		conn.shutdown()
		return nil, fmt.Errorf("SlaveDo->node.receive->READONLY() %w", err)
	}

	// 回收conn连接
	node.releaseConn(conn)

	resp := checkReply(reply)
	// 检查resp类型
	switch resp {
	case kRespOK, kRespError:
		return reply, nil
	case kRespMove:
		// 此处在高并发+slots循环多次集中迁移时，会出现数据的多级别MOVE，对于多级别MOVE 要进行到底，一般频率为20万次中出现10次
		// 所以采用循环进行多级MOVE处理
		for {
			// 尝试第一次MOVE，并对结果进行判断，如果reply类型不再是MOVE类型，则证明摆脱多级MOVE，则把结果返回出去
			// 由于结果可能会发生变化，因此再进行判断
			reply, err = cluster.handleMove(node, reply.(redisError).Error(), cmd, args)

			respType := checkReply(reply)

			// 如果reply类型不是MOVE类型，则 准备跳出循环、对结果进行判断，选择条件返回
			if respType != kRespMove {

				switch respType {
				case kRespOK, kRespError:
					return reply, nil
				case kRespAsk:
					return cluster.handleAsk(node, reply.(redisError).Error(), cmd, args)
				case kRespConnTimeout:
					return cluster.handleConnTimeout(node, cmd, args)
				case kRespClusterDown: // 如果redis集群宕机，则返回宕机错误
					// 选取可用的节点 更新集群状态信息
					cluster.UpdateSlotsInfoByRandomNode(node)
					return reply, Cluster_Down_Error
				}

				// 此处return为了跳出多级MOVE的for循环
				return reply, err
			}

		}
		// return cluster.handleMove(node, reply.(redisError).Error(), cmd, args)
	case kRespAsk:
		return cluster.handleAsk(node, reply.(redisError).Error(), cmd, args)
	case kRespConnTimeout:
		return cluster.handleConnTimeout(node, cmd, args)
	case kRespClusterDown: // 如果redis集群宕机，则返回宕机错误
		// 选取可用的节点 更新集群状态信息
		cluster.UpdateSlotsInfoByRandomNode(node)
		return reply, Cluster_Down_Error
	}

	return nil, fmt.Errorf("Do: unreachable")
}

// Close cluster connection, any subsequent method call will fail.
func (cluster *Cluster) Close() {
	cluster.rwLock.Lock()
	defer cluster.rwLock.Unlock()

	for addr, node := range cluster.nodes {
		node.shutdown()
		delete(cluster.nodes, addr)
	}

	cluster.closed = true
}

func (cluster *Cluster) handleMove(node *redisNode, replyMsg, cmd string, args []interface{}) (interface{}, error) {
	fields := strings.Split(replyMsg, " ")
	if len(fields) != 3 {
		return nil, fmt.Errorf("handleMove: invalid response \"%s\"", replyMsg)
	}

	// cluster has changed, inform update routine
	Inform(cluster, node)

	newNode, err := cluster.getNodeByAddr(fields[2])
	if err != nil {
		return nil, fmt.Errorf("handleMove: %w", err)
	}

	return newNode.do(cmd, args...)
}

func (cluster *Cluster) handleAsk(node *redisNode, replyMsg, cmd string, args []interface{}) (interface{}, error) {
	fields := strings.Split(replyMsg, " ")
	if len(fields) != 3 {
		return nil, fmt.Errorf("handleAsk: invalid response \"%s\"", replyMsg)
	}

	newNode, err := cluster.getNodeByAddr(fields[2])
	if err != nil {
		return nil, fmt.Errorf("handleAsk: %w", err)
	}

	conn, err := newNode.getConn()
	if err != nil {
		return nil, fmt.Errorf("handleAsk: %w", err)
	}

	conn.send("ASKING")
	conn.send(cmd, args...)

	err = conn.flush()
	if err != nil {
		conn.shutdown()
		return nil, fmt.Errorf("handleAsk: %w", err)
	}

	re, err := String(conn.receive())
	if err != nil || re != "OK" {
		conn.shutdown()
		return nil, fmt.Errorf("handleAsk: %w", err)
	}

	reply, err := conn.receive()
	if err != nil {
		conn.shutdown()
		return nil, fmt.Errorf("handleAsk: %w", err)
	}

	newNode.releaseConn(conn)

	return reply, nil
}

func (cluster *Cluster) handleConnTimeout(node *redisNode, cmd string, args []interface{}) (interface{}, error) {
	var randomNode *redisNode

	// choose a random node other than previous one
	cluster.rwLock.RLock()
	for _, randomNode = range cluster.nodes {
		if randomNode.address != node.address {
			break
		}
	}
	cluster.rwLock.RUnlock()

	reply, err := randomNode.do(cmd, args...)
	if err != nil {
		return nil, fmt.Errorf("handleConnTimeout: %w", err)
	}

	if _, ok := reply.(redisError); !ok {
		// we happen to choose the right node, which means
		// that cluster has changed, so inform update routine.
		Inform(cluster, randomNode)
		return reply, nil
	}

	// ignore replies other than MOVED
	errMsg := reply.(redisError).Error()
	if len(errMsg) < 5 || string(errMsg[:5]) != "MOVED" {
		return nil, errors.New(errMsg)
	}

	// When MOVED received, we check wether move adress equal to
	// previous one. If equal, then it's just an connection timeout
	// error, return error and carry on. If not, then the master may
	// down or unreachable, a new master has served the slot, request
	// new master and update cluster info.
	//
	// TODO: At worst case, it will request redis 3 times on a single
	// command, will this be a problem?
	fields := strings.Split(errMsg, " ")
	if len(fields) != 3 {
		return nil, fmt.Errorf("handleConnTimeout: invalid response \"%s\"", errMsg)
	}

	if fields[2] == node.address {
		return nil, fmt.Errorf("handleConnTimeout: %s connection timeout", node.address)
	}

	// cluster change, inform back routine to update
	Inform(cluster, randomNode)

	newNode, err := cluster.getNodeByAddr(fields[2])
	if err != nil {
		return nil, fmt.Errorf("handleConnTimeout: %w", err)
	}

	return newNode.do(cmd, args...)
}

const (
	kClusterSlots = 16384

	kRespOK          = 0
	kRespMove        = 1
	kRespAsk         = 2
	kRespConnTimeout = 3
	kRespError       = 4
	kRespClusterDown = 5
)

func checkReply(reply interface{}) int {
	if _, ok := reply.(redisError); !ok {
		return kRespOK
	}

	errMsg := reply.(redisError).Error()

	if len(errMsg) >= 3 && string(errMsg[:3]) == "ASK" {
		return kRespAsk
	}

	if len(errMsg) >= 5 && string(errMsg[:5]) == "MOVED" {
		return kRespMove
	}

	if len(errMsg) >= 12 && string(errMsg[:12]) == "ECONNTIMEOUT" {
		return kRespConnTimeout
	}

	if len(errMsg) >= 11 && string(errMsg[:11]) == "CLUSTERDOWN" {
		return kRespClusterDown
	}

	return kRespError
}

/* 3主3从 集群槽位信息
10.100.22.218:9001> CLUSTER slots
1) 1) (integer) 5461       起始槽位
   2) (integer) 10922      终止槽位
   3) 1) "10.100.22.218"   master IP
      2) (integer) 9002	   master Port
      3) "760c3b31549d991e2723704d1c44f6b8dd2a7cff" master node hash
   4) 1) "10.100.22.218"   Slave IP
      2) (integer) 9006    Slave Port
      3) "f304507524166f9a4e6b691821952a1c79ba2dbc" Slave node hash
2) 1) (integer) 0
   2) (integer) 5460
   3) 1) "10.100.22.218"
      2) (integer) 9001
      3) "9fd4d7a3a8f12692ff642d325e73014904ada214"
   4) 1) "10.100.22.218"
      2) (integer) 9005
      3) "d1cdfbdebfba07c631b05a9b13ffda0950df2149"
3) 1) (integer) 10923
   2) (integer) 16383
   3) 1) "10.100.22.218"
      2) (integer) 9003
      3) "812d2735e1171de59a9c7d675d54925777f6385a"
   4) 1) "10.100.22.218"
      2) (integer) 9004
	  3) "34d383f2f55bc8ad7f609e3bd9683081e99ef26a"

[
	[5461 10922
	 [MASTER
		[49 48 46 53 51 46 50 53 53 46 53] 9005 [54 56 100 54 49 56 97 98 51 48 51 56 102 52 102 100 50 55 54 99 97 52 98 48 97 53 102 100 56 48 49 53 48 50 52 52 100 48 52 50]
	 ]

	 [SLAVE
		[49 48 46 53 51 46 50 53 53 46 53] 9002 [55 54 97 53 52 53 49 52 99 49 57 55 54 102 54 57 54 97 99 101 48 55 52 49 55 99 101 101 55 56 57 54 48 51 53 54 51 49 56 102]
	 ]
	]

	[10923 16383
	 [MASTER
		[49 48 46 53 51 46 50 53 53 46 53] 9001 [49 98 55 56 50 57 51 100 55 54 99 101 100 54 56 50 100 100 54 97 99 49 55 48 53 53 48 57 56 49 97 57 102 50 101 99 48 101 98 56]
	 ]

	 [SLAVE1
		[49 48 46 53 51 46 50 53 53 46 53] 9003 [53 97 100 54 99 97 99 99 52 57 54 98 53 49 56 100 99 53 54 101 57 57 55 53 57 56 55 53 49 50 56 55 97 100 56 101 99 102 99 53]
	 ]

	 [SLAVE2
		[49 48 46 53 51 46 50 53 53 46 53] 9006 [54 97 100 55 55 50 54 99 99 49 98 48 48 56 50 56 55 49 54 98 100 52 101 56 56 98 101 53 50 50 99 52 52 52 56 50 100 49 101 55]
	 ]
	]

	[0 5460
	 [MASTER
		[49 48 46 53 51 46 50 53 53 46 53] 9004 [57 100 98 51 99 53 52 101 57 101 54 54 55 53 102 49 54 57 50 51 51 57 56 48 99 55 48 48 51 53 48 55 53 52 57 54 48 51 101 48]
	 ]

	 [SLAVE1
		[49 48 46 53 51 46 50 53 53 46 53] 9007 [57 57 48 53 51 54 102 98 54 100 49 50 97 51 57 100 48 53 49 49 54 56 100 48 53 53 99 97 100 54 98 100 52 48 52 55 99 101 49 57]
	 ]

	 [SLAVE2
		[49 48 46 53 51 46 50 53 53 46 53] 9008 [55 52 100 102 51 97 100 57 49 56 52 57 101 51 52 97 98 98 99 101 57 50 97 98 102 97 100 98 50 55 100 56 99 50 52 49 48 48 48 48]
	 ]
	]
]
*/

func (cluster *Cluster) Update(node *redisNode) error {
	// 如果node指针为空 则返回错误
	if node == nil {
		return fmt.Errorf("Cluster Update error.")
	}

	info, err := Values(node.do("CLUSTER", "SLOTS")) // 向node发送cluster slots命令，获取集群的槽位分布数据
	if err != nil {
		return err
	}

	errFormat := fmt.Errorf("update: %s invalid response", node.address)

	var nslots int // 总槽位统计量：目前必须要全覆盖 16384

	// 主节点slots分布存储地址: map[address][start1,end1][start2,end2][start3,end3]
	slots := make(map[string][]uint16)

	// 从节点slots分布存储地址
	slaveSlots := make(map[string][]uint16)

	for _, i := range info {
		m, err := Values(i, err)
		// 当前 slot 的数据集，数据先后分布如： startOffset endOffset masterAddr\port\hash sloveAdrr\port\hash slaveAdrr\port\hash ...
		slotInfoLen := len(m) // slot-start、slot-end、masterInfo、slaveInfo1、slaveInfo2

		// 针对信息进行长度安全校验，保障最低有一个主节点
		if err != nil || slotInfoLen < 3 {
			return errFormat
		}

		// 数据槽 start offset
		start, err := Int(m[0], err)
		if err != nil {
			return errFormat
		}

		// 数据槽 end offset
		end, err := Int(m[1], err)
		if err != nil {
			return errFormat
		}

		// 主节点 信息 结构：host port node_hash
		t, err := Values(m[2], err)

		if err != nil || len(t) < 2 {
			return errFormat
		}

		var ip string
		var port int

		_, err = Scan(t, &ip, &port)
		if err != nil {
			return errFormat
		}
		addr := fmt.Sprintf("%s:%d", ip, port)

		// 增加主节点槽位信息
		slot, ok := slots[addr]
		if !ok {
			slot = make([]uint16, 0, 2)
		}

		nslots += end - start + 1

		slot = append(slot, uint16(start))
		slot = append(slot, uint16(end))

		// 增加主节点槽位分布数据
		slots[addr] = slot

		// 计算从节点数据
		slaveNodesNum := slotInfoLen - 3 // 计算当前node的从节点数据
		for i := 0; i < slaveNodesNum; i++ {
			slaveNodeInfoOffset := i + 3                   // 计算从节点信息偏移量
			st, err := Values(m[slaveNodeInfoOffset], err) // 获取从节点信息

			if err != nil || len(st) < 2 {
				return errFormat
			}

			var slaveIp string
			var slavePort int

			// 获取从节点IP、Port信息
			_, err = Scan(st, &slaveIp, &slavePort)
			if err != nil {
				return errFormat
			}

			slaveAddr := fmt.Sprintf("%s:%d", slaveIp, slavePort)

			// 增加从节点节点槽位信息
			slaveSlot, ok := slaveSlots[slaveAddr]
			if !ok {
				slaveSlot = make([]uint16, 0, 2)
			}

			slaveSlot = append(slaveSlot, uint16(start))
			slaveSlot = append(slaveSlot, uint16(end))

			// 存储从节点 槽位分布数据
			slaveSlots[slaveAddr] = slaveSlot
		}
	}

	// 槽位数据获取结束 更新了master 和 slave 的槽位MAP数据
	// TODO: Is full coverage really needed? 判断槽位是不是全部覆盖
	if nslots != kClusterSlots {
		return fmt.Errorf("update: %s slots not full covered", node.address)
	}

	// 获取当前时间
	t := time.Now()

	// 为了并发安全，增加读写锁，以下部分是低频内存操作，所以性能损耗忽略
	cluster.rwLock.Lock()
	defer cluster.rwLock.Unlock()

	cluster.updateTime = t

	// 遍历master-slots信息，更新集群中的master节点信息
	for addr, slot := range slots {
		node, ok := cluster.nodes[addr]
		if !ok {
			node = &redisNode{
				address:      addr,
				connTimeout:  cluster.connTimeout,
				readTimeout:  cluster.readTimeout,
				writeTimeout: cluster.writeTimeout,
				keepAlive:    cluster.keepAlive,
				aliveTime:    cluster.aliveTime,
				NodeType:     MASTER_NODE, // 设置节点的类型为master
			}
		}

		n := len(slot)
		for i := 0; i < n-1; i += 2 {
			start := slot[i]
			end := slot[i+1]

			// 空间换时间，针对主节点的槽位分布进行数组map，便于快速查询
			for j := start; j <= end; j++ {
				cluster.slots[j] = node
			}
		}

		node.updateTime = t
		cluster.nodes[addr] = node
	}

	// 针对集群中的master节点进行剔除操作
	for addr, node := range cluster.nodes {
		if node.updateTime != t {
			node.shutdown()

			delete(cluster.nodes, addr)
		}
	}

	// 更新集群中的从节点信息-Slave
	// 第一步：清除cluster.slaveslots的槽位数组数据，因为后面会追加数据，如果不清空就会产生累积数据，避免脏数据
	for i := 0; i < kClusterSlots; i++ {
		cluster.slaveslots[i] = nil
		cluster.slaveslots[i] = make([]*redisNode, 0)
	}

	// 第二步：根据从节点槽位信息
	for addr, slot := range slaveSlots {
		node, ok := cluster.slaveNodes[addr]
		if !ok {
			node = &redisNode{
				address:      addr,
				connTimeout:  cluster.connTimeout,
				readTimeout:  cluster.readTimeout,
				writeTimeout: cluster.writeTimeout,
				keepAlive:    cluster.keepAlive,
				aliveTime:    cluster.aliveTime,
				NodeType:     SLAVE_NODE, // 设置节点的类型为slave类型
			}
		}

		// 遍历从节点槽位数据，并填充从节点槽位数组，由于一个master可能附着若干个slave，所以需要使用slice追加
		n := len(slot)
		for i := 0; i < n-1; i += 2 {
			start := slot[i]
			end := slot[i+1]

			// 空间换时间，针对从节点的槽位分布进行数组map，便于快速查询
			for j := start; j <= end; j++ {
				// 如果直接 cluster.slaveslots[j] = node ，会造成覆盖，因为针对同一个槽位区间，可能有多个从节点，所以需要追加
				cluster.slaveslots[j] = append(cluster.slaveslots[j], node)
			}
		}

		node.updateTime = t
		cluster.slaveNodes[addr] = node
	}

	// 针对集群中的slave节点进行剔除操作
	for addr, node := range cluster.slaveNodes {
		if node.updateTime != t {
			node.shutdown()

			delete(cluster.slaveNodes, addr)
		}
	}
	return nil
}

func (cluster *Cluster) handleUpdate() {
	for {
		msg := <-cluster.updateList

		// TODO: control update frequency by updateTime and movedTime?
		cluster.rwLock.RLock()
		clusterLastUpdateTime := cluster.updateTime
		cluster.rwLock.RUnlock()

		// 如果集群的上一次更新时间 加上窗口值（1s） 小于 此次MOVE指令产生的时间: 证明MOVE频率过高
		if clusterLastUpdateTime.Add(1 * time.Second).Before(msg.movedTime) {

			err := cluster.Update(msg.node)
			if err != nil {
				log.Printf("handleUpdate: %v\n", err)
				// KafkaLoger.CErrorf("redistun handleUpdate wrong. err: %s", err.Error())
			}

		}
	}
}

func Inform(cluster *Cluster, node *redisNode) {
	mesg := updateMesg{
		node:      node,
		movedTime: time.Now(),
	}

	select {
	case cluster.updateList <- mesg:
		// Push update message, no more to do.
	default:
		// Update channel full, just carry on.
	}
}

// 根据cluster中的nodes进行同步模式节点更新
func (cluster *Cluster) UpdateSlotsInfoByRandomNode(node *redisNode) {
	var randomNode *redisNode

	// choose a random node other than previous one
	cluster.rwLock.RLock()
	for _, randomNode = range cluster.nodes {
		if randomNode.address != node.address {
			break
		}
	}
	cluster.rwLock.RUnlock()

	// 通知更新节点
	Inform(cluster, randomNode)
}

// 从cluster的master node中 随机选择一个master进行集群分槽数据更新
func (cluster *Cluster) UpdateSlotsByRandomMasterNode() {
	var randomNode *redisNode
	i := 0
	rand.Seed(time.Now().UnixNano())

	cluster.rwLock.RLock()

	randPerm := rand.Perm(len(cluster.nodes)) // 根据master节点的长度创建一个随机Perm数组
	// 随机选择一个master node进行更新状态
	for _, randomNode = range cluster.nodes {
		// 随机数组里 当 元素 数值为0时 跳出循环，模拟node的随机选择
		if randPerm[i] == 0 {
			break
		}
		i++
	}

	cluster.rwLock.RUnlock()

	// 通知更新节点
	Inform(cluster, randomNode)
}

func (cluster *Cluster) getNodeByAddr(addr string) (*redisNode, error) {
	cluster.rwLock.RLock()
	defer cluster.rwLock.RUnlock()

	if cluster.closed {
		return nil, fmt.Errorf("getNodeByAddr: cluster has been closed")
	}

	node, ok := cluster.nodes[addr]
	if !ok {
		return nil, fmt.Errorf("getNodeByAddr: %s not found", addr)
	}

	return node, nil
}

func (cluster *Cluster) getNodeByKey(arg interface{}) (*redisNode, error) {
	key, err := key(arg)
	if err != nil {
		return nil, fmt.Errorf("getNodeByKey: invalid key %v", key)
	}

	slot := hash(key)

	cluster.rwLock.RLock()
	defer cluster.rwLock.RUnlock()

	if cluster.closed {
		return nil, fmt.Errorf("getNodeByKey: cluster has been closed")
	}

	node := cluster.slots[slot]
	if node == nil {
		return nil, fmt.Errorf("getNodeByKey: %s[%d] no node found", key, slot)
	}

	return node, nil
}

// 结合cluster内部参数 判断当前是否应该slave进行操作
func (cluster *Cluster) IsSlaveOperate() bool {
	// 如果集群状态数据中的从节点数据为空，则证明没有slave节点，则返回false，强行走master节点
	cluster.rwLock.RLock()
	slaveNodeCount := len(cluster.slaveNodes)
	isSlaveReadAble := cluster.isSlaveReadAble
	rate := cluster.slaveOperateRate
	cluster.rwLock.RUnlock()

	if slaveNodeCount == 0 {
		return false
	}

	// 如果cluster中的配置 IsSlaveReadAble 为false
	if isSlaveReadAble == false {
		return false
	}

	// 从 1-100 获取随机数
	// rand.Seed(time.Now().UnixNano())
	randNum := fastrand.Uint32n(100) + 1

	if randNum <= uint32(rate) {
		return true // 如果rate低于设置的百分比，则证明需要slave操作
	}

	return false
}

// 根据key 随机的获取某一个key获取从节点 ：
// 情况缺陷，如果一个master有多个slave，则会把batch分散到多个slave节点上，造成node分散,从而增加网络开销
func (cluster *Cluster) getAnRandomSlaveNodeByKey(arg interface{}) (*redisNode, error) {
	key, err := key(arg) // 格式化key 处理redis key-tag
	if err != nil {
		return nil, fmt.Errorf("getAnRandomSlaveNodeByKey: invalid key %v", key)
	}

	slot := hash(key)

	cluster.rwLock.RLock()
	defer cluster.rwLock.RUnlock()

	if cluster.closed {
		return nil, fmt.Errorf("getAnRandomSlaveNodeByKey: cluster has been closed")
	}

	nodes := cluster.slaveslots[slot] // 这是一个数据元素

	nodesCount := len(nodes)
	if nodesCount == 0 { // 从节点列表为空
		masterAddress := ""
		if cluster.slots[slot] != nil {
			masterAddress = cluster.slots[slot].address
		}

		return nil, fmt.Errorf("getAnRandomSlaveNodeByKey: %s[%d] no alive slave of %s master found", key, slot, masterAddress)
	}

	// 在从节点列表中随机选择一个node出来
	// rand.Seed(time.Now().UnixNano())
	var nodeId uint32
	if nodesCount == 1 {
		nodeId = 0
	} else {
		nodeId = fastrand.Uint32n(uint32(nodesCount))
	}

	return nodes[nodeId], nil
}

// 根据numSeed 取余操作进行slave节点的选择，解决getAnRandomSlaveNodeByKey函数的缺陷 ：返回masterNode 主要为了slaveNode不可用的时候降级
func (cluster *Cluster) getAnSlaveNodeByNumSeed(arg interface{}, numSeed uint64) (slaveNode *redisNode, masterNode *redisNode, err error) {
	key, err := key(arg) // 格式化key 处理redis key-tag
	if err != nil {
		return nil, nil, fmt.Errorf("getAnSlaveNodeByNumSeed: invalid key %v", key)
	}

	slot := hash(key)

	cluster.rwLock.RLock()
	defer cluster.rwLock.RUnlock()

	if cluster.closed {
		return nil, nil, fmt.Errorf("getAnSlaveNodeByNumSeed: cluster has been closed")
	}

	nodes := cluster.slaveslots[slot] // 获取次slot槽位中存储的redis slave node节点slice

	nodesCount := len(nodes)
	if nodesCount == 0 { // 从节点列表为空：证明没有可用的slave节点，返回错误。
		masterAddress := ""
		if cluster.slots[slot] != nil {
			masterAddress = cluster.slots[slot].address
		}

		return nil, nil, fmt.Errorf("getAnSlaveNodeByNumSeed: %s[%d] no alive slave of %s master found", key, slot, masterAddress)
	}

	nodeId := 0
	if nodesCount == 1 {
		nodeId = 0
	} else {
		// 由于numSeed在一个batch内是相同的，所以这个numSeed取余运算 可以保障一个batch内 对于多个slave节点均选择相同的一个slave node,降低网络损耗
		nodeId = int(numSeed % uint64(nodesCount))
	}

	masterNode = cluster.slots[slot] // 获取slave对应的master节点

	return nodes[nodeId], masterNode, nil
}

func key(arg interface{}) (string, error) {
	switch arg := arg.(type) {
	case int:
		return strconv.Itoa(arg), nil
	case int64:
		return strconv.Itoa(int(arg)), nil
	case float64:
		return strconv.FormatFloat(arg, 'g', -1, 64), nil
	case string:
		return arg, nil
	case []byte:
		return string(arg), nil
	default:
		return "", fmt.Errorf("key: unknown type %T", arg)
	}
}

func hash(key string) uint16 {
	var s, e int
	for s = 0; s < len(key); s++ {
		if key[s] == '{' {
			break
		}
	}

	if s == len(key) {
		return crc16(key) & (kClusterSlots - 1)
	}

	for e = s + 1; e < len(key); e++ {
		if key[e] == '}' {
			break
		}
	}

	if e == len(key) || e == s+1 {
		return crc16(key) & (kClusterSlots - 1)
	}

	return crc16(key[s+1:e]) & (kClusterSlots - 1)
}

/*func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}*/
