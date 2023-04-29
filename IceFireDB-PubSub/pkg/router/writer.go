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
	"bytes"

	"github.com/sirupsen/logrus"

	"github.com/IceFireDB/components-go/RESPHandle"
)

func WriteSimpleString(local *RESPHandle.WriterHandle, reply string) error {
	err := local.WriteSimpleString(reply)
	if err != nil {
		logrus.Error("client write error:", err)
		return ErrLocalWriter
	}
	err = local.Flush()
	if err != nil {
		logrus.Error("Client flush error:", err)
		return ErrLocalFlush
	}
	return nil
}

func WriteBulk(local *RESPHandle.WriterHandle, reply []byte) error {
	err := local.WriteBulk(reply)
	if err != nil {
		logrus.Error("client write error:", err)
		return ErrLocalWriter
	}
	err = local.Flush()
	if err != nil {
		logrus.Error("Client flush error:", err)
		return ErrLocalFlush
	}
	return nil
}

func WriteObjects(local *RESPHandle.WriterHandle, reply ...interface{}) (err error) {
	if len(reply) > 1 {
		var memoryWriterBuffer bytes.Buffer
		memoryWriterHandle := RESPHandle.NewWriterHandle(&memoryWriterBuffer)
		err = memoryWriterHandle.WriteObjects(reply...)

		if err != nil {
			logrus.Error("mget memoryWriterHandle error:", err)
			return err
		}
		err = memoryWriterHandle.Flush()
		if err != nil {
			logrus.Error("mget memoryWriterHandle flush error:", err)
			return err
		}
		_, err = local.Write(memoryWriterBuffer.Bytes())
	} else {
		err = local.WriteObjects(reply...)
	}
	if err != nil {
		logrus.Error("Client write error:", err)
		return ErrLocalWriter
	}
	err = local.Flush()
	if err != nil {
		logrus.Error("Client flush error:", err)
		return ErrLocalFlush
	}
	return nil
}

func RecursivelyWriteObjects(local *RESPHandle.WriterHandle, reply ...interface{}) (err error) {
	if len(reply) > 1 {
		var memoryWriterBuffer bytes.Buffer
		memoryWriterHandle := RESPHandle.NewWriterHandle(&memoryWriterBuffer)
		err = memoryWriterHandle.RecursivelyWriteObjects(reply...)

		if err != nil {
			logrus.Error("mget memoryWriterHandle error:", err)
			return err
		}
		err = memoryWriterHandle.Flush()
		if err != nil {
			logrus.Error("mget memoryWriterHandle flush error:", err)
			return err
		}
		_, err = local.Write(memoryWriterBuffer.Bytes())
	} else {
		err = local.RecursivelyWriteObjects(reply...)
	}

	if err != nil {
		logrus.Error("Client write error:", err)
		return ErrLocalWriter
	}
	err = local.Flush()
	if err != nil {
		logrus.Error("Client flush error:", err)
		return ErrLocalFlush
	}
	return nil
}

func WriteBulkStrings(local *RESPHandle.WriterHandle, reply []string) error {
	err := local.WriteBulkStrings(reply)
	if err != nil {
		logrus.Error("Client write error:", err)
		return ErrLocalWriter
	}
	err = local.Flush()
	if err != nil {
		logrus.Error("Client flush error:", err)
		return ErrLocalFlush
	}
	return nil
}

func WriteInt(local *RESPHandle.WriterHandle, reply int64) error {
	err := local.WriteInt(reply)
	if err != nil {
		logrus.Error("Client write error:", err)
		return ErrLocalWriter
	}
	err = local.Flush()
	if err != nil {
		logrus.Error("Client flush error:", err)
		return ErrLocalFlush
	}
	return nil
}

func WriteError(local *RESPHandle.WriterHandle, err error) error {
	local.WriteError(err.Error())
	return local.Flush()
}
