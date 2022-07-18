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

package monitor

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/IceFireDB/IceFireDB-Proxy/pkg/netstat"
)

var ethInterface string

type NetworkStat struct {
	RxBytes uint64
	TxBytes uint64
}
type NetStatCallback func(stat *NetworkStat, err error)

var folder string

func init() {
	ethInterface = "eth0"
	if val := os.Getenv("MSP_ETH_INTERFACE_NAME"); len(val) > 0 {
		ethInterface = val
	}
	folder = "/sys/class/net/" + ethInterface + "/statistics/"

	cmd := exec.Command("ip", "-o", "-4", "route", "show", "to", "default")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return
	}
	parts := strings.Split(strings.TrimSpace(out.String()), " ")
	if len(parts) < 5 {
		fmt.Println(fmt.Errorf("invalid result from \"ip -o -4 route show to default\": %s", out.String()))
		return
	}
	ethInterface = strings.TrimSpace(parts[4])
	folder = "/sys/class/net/" + ethInterface + "/statistics/"
}

func GetNetstat() (tcp map[string]int, err error) {
	socks, err := netstat.TCPSocks(netstat.NoopFilter)
	if err != nil {
		logrus.Error("获取服务TCP连接失败 ", err)
		return
	}
	tcp = make(map[string]int)
	if len(socks) > 0 {
		for _, value := range socks {
			state := value.State.String()
			tcp[state]++
		}
	}
	return
}

func CurrentNetworkStatInputByte() float64 {
	rxBytes, _ := ReadNumberFromFile(folder + "rx_bytes")
	return rxBytes
}

func CurrentNetworkStatOutputByte() float64 {
	txBytes, _ := ReadNumberFromFile(folder + "tx_bytes")
	return txBytes
}

func ReadNumberFromFile(name string) (n float64, err error) {
	out, err := ioutil.ReadFile(name)
	if err != nil {
		return 0, err
	}

	n, err = strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		return n, err
	}
	return n, nil
}
