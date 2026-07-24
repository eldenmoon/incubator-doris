// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message_lite.h>

#include <string>

namespace doris {

// Deterministic for the same protobuf schema and runtime; not canonical across builds or
// protobuf implementations.
inline bool serialize_protobuf_deterministically(const google::protobuf::MessageLite& message,
                                                 std::string* output) {
    output->clear();
    google::protobuf::io::StringOutputStream string_output(output);
    google::protobuf::io::CodedOutputStream coded_output(&string_output);
    coded_output.SetSerializationDeterministic(true);
    const bool serialized = message.SerializeToCodedStream(&coded_output);
    const bool had_error = coded_output.HadError();
    return serialized && !had_error;
}

} // namespace doris
