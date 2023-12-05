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
#include <brpc/controller.h>
#include <bthread/types.h>
#include <butil/errno.h>
#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "exec/tablet_info.h"
#include "gutil/ref_counted.h"
#include "olap/wal_writer.h"
#include "runtime/decimalv2_value.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "util/countdown_latch.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"
#include "util/stopwatch.hpp"
#include "vec/columns/column.h"
#include "vec/common/allocator.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/sink/async_writer_sink.h"
#include "vec/sink/writer/vtablet_writer.h"

namespace doris::vectorized {

inline constexpr char VOLAP_TABLE_SINK[] = "VOlapTableSink";
// Write block data to Olap Table.
// When OlapTableSink::open() called, there will be a consumer thread running in the background.
// When you call VOlapTableSink::send(), you will be the producer who products pending batches.
// Join the consumer thread in close().
class VOlapTableSink final : public AsyncWriterSink<VTabletWriter, VOLAP_TABLE_SINK> {
public:
    // Construct from thrift struct which is generated by FE.
    VOlapTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                   const std::vector<TExpr>& texprs);
    // the real writer will construct in (actually, father's) init but not constructor
    Status init(const TDataSink& sink) override;

    Status close(RuntimeState* state, Status exec_status) override;

private:
    ObjectPool* _pool = nullptr;

    Status _close_status = Status::OK();
};

} // namespace doris::vectorized
