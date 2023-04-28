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
	"errors"
	"fmt"

	"github.com/IceFireDB/IceFireDB-Proxy/pkg/RedSHandle"
	"github.com/IceFireDB/IceFireDB/IceFireDB-Redis-Proxy/pkg/bareneter"
	"github.com/IceFireDB/IceFireDB/IceFireDB-Redis-Proxy/pkg/codis/credis"
	"github.com/IceFireDB/IceFireDB/IceFireDB-Redis-Proxy/pkg/router"
	"github.com/sirupsen/logrus"
)

func (p *Proxy) handle(conn bareneter.Conn) {
	defer func() {
		_ = conn.Close()
	}()
	localConn := conn.NetConn()
	localWriteHandle := RedSHandle.NewWriterHandle(localConn)
	decoder := credis.NewDecoderSize(localConn, 1024)
	for {
		resp, err := decoder.Decode()
		if err != nil {
			/*if err.Error() != io.EOF.Error() && strings.Index(err.Error(), net.ErrClosed.Error()) == -1 {
				logrus.Errorf("RESP fail:%v", err)
			}*/
			return
		}

		if resp.Type != credis.TypeArray {
			_ = router.WriteError(localWriteHandle, fmt.Errorf(router.ErrUnknownCommand, "cmd"))
			return
		}

		respCount := len(resp.Array)

		if respCount < 1 {
			_ = router.WriteError(localWriteHandle, fmt.Errorf(router.ErrArguments, "cmd"))
			return
		}

		if resp.Array[0].Type != credis.TypeBulkBytes {
			_ = router.WriteError(localWriteHandle, router.ErrCmdTypeWrong)
			return
		}

		commandArgs := make([]interface{}, respCount)
		for i := 0; i < respCount; i++ {
			commandArgs[i] = resp.Array[i].Value
		}
		err = p.router.Handle(localWriteHandle, commandArgs)

		if err != nil {
			if errors.Is(err, router.ErrLocalWriter) || errors.Is(err, router.ErrLocalFlush) {
				return
			}
			_ = router.WriteError(localWriteHandle, err)
			logrus.Errorf("resp command exec fail:%s , %v", commandArgs, err)
			return
		}
	}
}
