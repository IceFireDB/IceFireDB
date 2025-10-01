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

#include <string.h>

#include "arrow/c/abi.h"

int streamGetSchema(struct ArrowArrayStream*, struct ArrowSchema*);
int streamGetNext(struct ArrowArrayStream*, struct ArrowArray*);
int asyncTaskExtract(struct ArrowAsyncTask*, struct ArrowDeviceArray*);

int streamGetSchemaTrampoline(struct ArrowArrayStream* stream, struct ArrowSchema* out) {
  // XXX(https://github.com/apache/arrow-adbc/issues/729)
  memset(out, 0, sizeof(*out));
  return streamGetSchema(stream, out);
}

int streamGetNextTrampoline(struct ArrowArrayStream* stream, struct ArrowArray* out) {
  // XXX(https://github.com/apache/arrow-adbc/issues/729)
  memset(out, 0, sizeof(*out));
  return streamGetNext(stream, out);
}

int asyncTaskExtractTrampoline(struct ArrowAsyncTask* task, struct ArrowDeviceArray* out) {
  memset(out, 0, sizeof(*out));
  return asyncTaskExtract(task, out);
}
