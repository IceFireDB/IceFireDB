// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cdata

import (
	"context"
	"fmt"
	"runtime/cgo"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// #include <stdlib.h>
// #include <errno.h>
// #include "arrow/c/abi.h"
// #include "arrow/c/helpers.h"
//
// typedef const char cchar_t;
// extern int streamGetSchema(struct ArrowArrayStream*, struct ArrowSchema*);
// extern int streamGetNext(struct ArrowArrayStream*, struct ArrowArray*);
// extern const char* streamGetError(struct ArrowArrayStream*);
// extern void streamRelease(struct ArrowArrayStream*);
// extern int asyncStreamOnSchema(struct ArrowAsyncDeviceStreamHandler*, struct ArrowSchema*);
// extern int asyncStreamOnNextTask(struct ArrowAsyncDeviceStreamHandler*, struct ArrowAsyncTask*, char*);
// extern void asyncStreamOnError(struct ArrowAsyncDeviceStreamHandler*, int, char*, char*);
// extern void asyncStreamRelease(struct ArrowAsyncDeviceStreamHandler*);
// extern void asyncProducerRequest(struct ArrowAsyncProducer*, int64_t);
// extern void asyncProducerCancel(struct ArrowAsyncProducer*);
// extern int asyncTaskExtract(struct ArrowAsyncTask*, struct ArrowDeviceArray*);
// // XXX(https://github.com/apache/arrow-adbc/issues/729)
// int streamGetSchemaTrampoline(struct ArrowArrayStream* stream, struct ArrowSchema* out);
// int streamGetNextTrampoline(struct ArrowArrayStream* stream, struct ArrowArray* out);
// int asyncTaskExtractTrampoline(struct ArrowAsyncTask* task, struct ArrowDeviceArray* out);
//
// static void goCallRequest(struct ArrowAsyncProducer* producer, int64_t n) {
//  	producer->request(producer, n);
// }
// static int goCallOnSchema(struct ArrowAsyncDeviceStreamHandler* handler, struct ArrowSchema* schema) {
//   	return handler->on_schema(handler, schema);
// }
// static void goCallOnError(struct ArrowAsyncDeviceStreamHandler* handler, int code, char* message, char* metadata) {
//   	handler->on_error(handler, code, message, metadata);
// }
// static int goCallOnNextTask(struct ArrowAsyncDeviceStreamHandler* handler, struct ArrowAsyncTask* task, char* metadata) {
//   	return handler->on_next_task(handler, task, metadata);
// }
//
// static struct ArrowAsyncProducer* get_producer() {
//   	struct ArrowAsyncProducer* out = (struct ArrowAsyncProducer*)malloc(sizeof(struct ArrowAsyncProducer));
//   	memset(out, 0, sizeof(struct ArrowAsyncProducer));
//   	return out;
// }
//
// static void goReleaseAsyncHandler(struct ArrowAsyncDeviceStreamHandler* handler) {
// 	handler->release(handler);
// }
//
import "C"

//export releaseExportedSchema
func releaseExportedSchema(schema *CArrowSchema) {
	if C.ArrowSchemaIsReleased(schema) == 1 {
		return
	}
	defer C.ArrowSchemaMarkReleased(schema)

	C.free(unsafe.Pointer(schema.name))
	C.free(unsafe.Pointer(schema.format))
	C.free(unsafe.Pointer(schema.metadata))

	if schema.n_children == 0 {
		return
	}

	if schema.dictionary != nil {
		C.ArrowSchemaRelease(schema.dictionary)
		C.free(unsafe.Pointer(schema.dictionary))
	}

	children := unsafe.Slice(schema.children, schema.n_children)
	for _, c := range children {
		C.ArrowSchemaRelease(c)
	}

	C.free(unsafe.Pointer(children[0]))
	C.free(unsafe.Pointer(schema.children))
}

// apache/arrow#33864: allocate a new cgo.Handle and store its address
// in a heap-allocated uintptr_t.
func createHandle(hndl cgo.Handle) unsafe.Pointer {
	// uintptr_t* hptr = malloc(sizeof(uintptr_t));
	hptr := (*C.uintptr_t)(C.malloc(C.sizeof_uintptr_t))
	// *hptr = (uintptr)hndl;
	*hptr = C.uintptr_t(uintptr(hndl))
	return unsafe.Pointer(hptr)
}

func getHandle(ptr unsafe.Pointer) cgo.Handle {
	// uintptr_t* hptr = (uintptr_t*)ptr;
	hptr := (*C.uintptr_t)(ptr)
	return cgo.Handle((uintptr)(*hptr))
}

//export releaseExportedArray
func releaseExportedArray(arr *CArrowArray) {
	if C.ArrowArrayIsReleased(arr) == 1 {
		return
	}
	defer C.ArrowArrayMarkReleased(arr)

	if arr.n_buffers > 0 {
		C.free(unsafe.Pointer(arr.buffers))
	}

	if arr.dictionary != nil {
		C.ArrowArrayRelease(arr.dictionary)
		C.free(unsafe.Pointer(arr.dictionary))
	}

	if arr.n_children > 0 {
		children := unsafe.Slice(arr.children, arr.n_children)

		for _, c := range children {
			C.ArrowArrayRelease(c)
		}
		C.free(unsafe.Pointer(children[0]))
		C.free(unsafe.Pointer(arr.children))
	}

	h := getHandle(arr.private_data)
	h.Value().(arrow.ArrayData).Release()
	h.Delete()
	C.free(unsafe.Pointer(arr.private_data))
}

//export streamGetSchema
func streamGetSchema(handle *CArrowArrayStream, out *CArrowSchema) C.int {
	h := getHandle(handle.private_data)
	rdr := h.Value().(cRecordReader)
	return C.int(rdr.getSchema(out))
}

//export streamGetNext
func streamGetNext(handle *CArrowArrayStream, out *CArrowArray) C.int {
	h := getHandle(handle.private_data)
	rdr := h.Value().(cRecordReader)
	return C.int(rdr.next(out))
}

//export streamGetError
func streamGetError(handle *CArrowArrayStream) *C.cchar_t {
	h := getHandle(handle.private_data)
	rdr := h.Value().(cRecordReader)
	return rdr.getLastError()
}

//export streamRelease
func streamRelease(handle *CArrowArrayStream) {
	h := getHandle(handle.private_data)
	h.Value().(cRecordReader).release()
	h.Delete()
	C.free(unsafe.Pointer(handle.private_data))
	handle.release = nil
	handle.private_data = nil
}

func exportStream(rdr array.RecordReader, out *CArrowArrayStream) {
	out.get_schema = (*[0]byte)(C.streamGetSchemaTrampoline)
	out.get_next = (*[0]byte)(C.streamGetNextTrampoline)
	out.get_last_error = (*[0]byte)(C.streamGetError)
	out.release = (*[0]byte)(C.streamRelease)
	rdr.Retain()
	h := cgo.NewHandle(cRecordReader{rdr: rdr, err: nil})
	out.private_data = createHandle(h)
}

type cAsyncState struct {
	ch        chan AsyncRecordBatchStream
	queueSize uint64
	ctx       context.Context
}

type taskState struct {
	task CArrowAsyncTask
	meta arrow.Metadata
	err  error
}

//export asyncStreamOnSchema
func asyncStreamOnSchema(self *CArrowAsyncDeviceStreamHandler, schema *CArrowSchema) C.int {
	h := getHandle(self.private_data)
	handler := h.Value().(cAsyncState)
	defer close(handler.ch)

	if self.producer.device_type != C.ARROW_DEVICE_CPU {
		handler.ch <- AsyncRecordBatchStream{Err: fmt.Errorf("unsupported device type")}
		return C.EINVAL
	}

	sc, err := ImportCArrowSchema(schema)
	if err != nil {
		handler.ch <- AsyncRecordBatchStream{Err: err}
		return C.EINVAL
	}

	var meta arrow.Metadata
	if self.producer.additional_metadata != nil {
		meta = decodeCMetadata(self.producer.additional_metadata)
	}

	recordStream := make(chan RecordMessage, handler.queueSize)
	taskQueue := make(chan taskState, handler.queueSize)
	handler.ch <- AsyncRecordBatchStream{Schema: sc,
		AdditionalMetadata: meta, Stream: recordStream}

	self.private_data = createHandle(cgo.NewHandle(&cAsyncStreamHandler{
		producer:  self.producer,
		ctx:       handler.ctx,
		taskQueue: taskQueue,
	}))
	defer h.Delete()

	C.goCallRequest(self.producer, C.int64_t(handler.queueSize))
	go asyncTaskQueue(handler.ctx, sc, recordStream, taskQueue, self.producer)
	return 0
}

//export asyncStreamOnNextTask
func asyncStreamOnNextTask(self *CArrowAsyncDeviceStreamHandler, task *CArrowAsyncTask, metadata *C.char) C.int {
	h := getHandle(self.private_data)
	handler := h.Value().(*cAsyncStreamHandler)
	return handler.onNextTask(task, metadata)
}

//export asyncStreamOnError
func asyncStreamOnError(self *CArrowAsyncDeviceStreamHandler, code C.int, message, metadata *C.char) {
	h := getHandle(self.private_data)
	switch handler := h.Value().(type) {
	case *cAsyncStreamHandler:
		handler.onError(code, message, metadata)
	case cAsyncState:
		handler.ch <- AsyncRecordBatchStream{Err: AsyncStreamError{
			Code:     int(code),
			Msg:      C.GoString(message),
			Metadata: C.GoString(metadata),
		}}
		close(handler.ch)
	}
}

//export asyncStreamRelease
func asyncStreamRelease(self *CArrowAsyncDeviceStreamHandler) {
	h := getHandle(self.private_data)
	if handler, ok := h.Value().(*cAsyncStreamHandler); ok {
		handler.release()
	}

	h.Delete()
	C.free(unsafe.Pointer(self.private_data))
	self.release = nil
	self.private_data = nil
}

func exportAsyncHandler(state cAsyncState, out *CArrowAsyncDeviceStreamHandler) {
	out.on_schema = (*[0]byte)(C.asyncStreamOnSchema)
	out.on_next_task = (*[0]byte)(C.asyncStreamOnNextTask)
	out.on_error = (*[0]byte)(C.asyncStreamOnError)
	out.release = (*[0]byte)(C.asyncStreamRelease)
	out.private_data = createHandle(cgo.NewHandle(state))
}

//export asyncProducerRequest
func asyncProducerRequest(producer *CArrowAsyncProducer, n C.int64_t) {
	h := getHandle(producer.private_data)
	handler := h.Value().(*cAsyncProducer)
	if handler.reqChan != nil {
		handler.reqChan <- int64(n)
	}
}

//export asyncProducerCancel
func asyncProducerCancel(producer *CArrowAsyncProducer) {
	h := getHandle(producer.private_data)
	handler := h.Value().(*cAsyncProducer)
	if handler.done != nil {
		close(handler.done)
		handler.done, handler.reqChan = nil, nil
	}
}

//export asyncTaskExtract
func asyncTaskExtract(task *CArrowAsyncTask, out *CArrowDeviceArray) C.int {
	h := getHandle(task.private_data)
	rec := h.Value().(arrow.Record)
	defer rec.Release()

	out.device_id, out.device_type = C.int64_t(-1), C.ARROW_DEVICE_CPU
	ExportArrowRecordBatch(rec, &out.array, nil)
	return C.int(0)
}

type cAsyncProducer struct {
	reqChan chan int64
	done    chan error
}

func exportAsyncProducer(schema *arrow.Schema, stream <-chan RecordMessage, handler *CArrowAsyncDeviceStreamHandler) error {
	defer C.goReleaseAsyncHandler(handler)

	if schema == nil {
		err := fmt.Errorf("%w: must have non-nil schema", arrow.ErrInvalid)
		errmsg := C.CString(err.Error())
		C.goCallOnError(handler, C.EINVAL, errmsg, nil)
		C.free(unsafe.Pointer(errmsg))
		return err
	}

	reqChan, done := make(chan int64, 5), make(chan error, 1)
	prodHandle := cgo.NewHandle(&cAsyncProducer{reqChan: reqChan, done: done})
	cproducer := prodHandle.Value().(*cAsyncProducer)
	defer func() {
		close(reqChan)
		cproducer.reqChan = nil
		if cproducer.done != nil {
			close(cproducer.done)
			cproducer.done = nil
		}

		prodHandle.Delete()
	}()

	producer := C.get_producer()
	defer C.free(unsafe.Pointer(producer))

	producer.device_type = C.ARROW_DEVICE_CPU
	producer.request = (*[0]byte)(C.asyncProducerRequest)
	producer.cancel = (*[0]byte)(C.asyncProducerCancel)
	producer.private_data = createHandle(prodHandle)
	producer.additional_metadata = nil
	handler.producer = producer

	var s CArrowSchema
	ExportArrowSchema(schema, &s)
	if status := C.goCallOnSchema(handler, &s); status != C.int(0) {
		releaseExportedSchema(&s)
		return fmt.Errorf("on_schema failed with status %d", status)
	}

	var pending int64 = 0
	for {
		select {
		case err, ok := <-done:
			if !ok {
				return nil
			}

			return err
		case req := <-reqChan:
			pending += req
		default:
		}

		if pending > 0 {
			select {
			case msg, ok := <-stream:
				if !ok {
					if status := C.goCallOnNextTask(handler, nil, nil); status != C.int(0) {
						return fmt.Errorf("on_next_task with nil task failed with status %d", status)
					}
					return nil
				}

				pending--
				if msg.Err != nil {
					errmsg := C.CString(msg.Err.Error())
					C.goCallOnError(handler, C.EINVAL, errmsg, nil)
					C.free(unsafe.Pointer(errmsg))
					return msg.Err
				}

				var task CArrowAsyncTask
				task.extract_data = (*[0]byte)(C.asyncTaskExtractTrampoline)
				task.private_data = createHandle(cgo.NewHandle(msg.Record))

				var encoded []byte
				if msg.AdditionalMetadata.Len() != 0 {
					encoded = encodeCMetadata(msg.AdditionalMetadata.Keys(),
						msg.AdditionalMetadata.Values())
				}

				status := C.goCallOnNextTask(handler, &task,
					(*C.char)(unsafe.Pointer(unsafe.SliceData(encoded))))
				if status != C.int(0) {
					msg.Record.Release()
					getHandle(task.private_data).Delete()
					return fmt.Errorf("on_next_task failed with status %d", status)
				}
			default:
			}
		}
	}
}
