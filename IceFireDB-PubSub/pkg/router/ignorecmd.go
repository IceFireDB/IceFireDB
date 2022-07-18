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
	"fmt"

	"github.com/IceFireDB/IceFireDB-Proxy/utils"
)

// Start the middleware according to the configuration
func IgnoreCMDMiddleware(enable bool, cmdList []string) HandlerFunc {
	return func(context *Context) error {
		// Ignore custom commands
		if enable && len(cmdList) > 0 {
			if utils.InArray(context.Cmd, cmdList) {
				return fmt.Errorf(ErrUnknownCommand, context.Cmd)
			}
		}
		return context.Next()
	}
}
