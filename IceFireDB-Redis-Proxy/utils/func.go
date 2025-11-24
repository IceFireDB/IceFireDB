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

package utils

import (
	"log"
	"os"
	"runtime/debug"
	"slices"
	"strconv"
	"time"
)

func GoWithRecover(handler func(), recoverHandler func(r any)) {
	go func() {
		defer func() {
			if r := recover(); r != nil {

				log.Printf("%s goroutine panic: %v\n%s\n", time.Now().Format(time.DateTime), r, string(debug.Stack()))
				if recoverHandler != nil {
					go func() {
						defer func() {
							if p := recover(); p != nil {
								log.Printf("recover goroutine panic:%v\n%s\n", p, string(debug.Stack()))
							}
						}()
						recoverHandler(r)
					}()
				}
			}
		}()
		handler()
	}()
}

func GetInterfaceString(param any) string {
	switch param := param.(type) {
	case []byte:
		return string(param)
	case string:
		return param
	case int:
		return strconv.Itoa(param)
	case float64:
		return strconv.Itoa(int(param))
	}
	return ""
}

func InArray(in string, array []string) bool {
	return slices.Contains(array, in)
}

var _hostname string

func GetHostname() string {
	if _hostname != "" {
		return _hostname
	}

	hostname, err := os.Hostname()
	if err != nil {
		return ""
	}
	_hostname = hostname
	return _hostname
}
