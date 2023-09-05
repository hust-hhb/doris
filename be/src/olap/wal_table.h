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
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/stream_load_context.h"
namespace doris {
class WalTable {
public:
    WalTable(ExecEnv* exec_env, int64_t db_id, int64_t table_id);
    ~WalTable();
    // <retry_num, start_time_ms, is_doing_replay>
    using replay_wal_info = std::tuple<int64_t, int64_t, bool>;
    // used when be start and there are wals need to do recovery
    void add_wals(std::vector<std::string> wals);
    Status check_wal(int64_t wal_id, bool& needRecovery);
    Status replay_wals();
    Status begin_txn(std::shared_ptr<StreamLoadContext>& stream_load_ctx, int64_t wal_id);
    Status update_wal_map(std::shared_ptr<StreamLoadContext>& stream_load_ctx);
    Status generate_stream_load_put_request(std::shared_ptr<StreamLoadContext>& stream_load_ctx,
                                            TStreamLoadPutRequest& request);
    Status execute_plan_fragment(std::shared_ptr<StreamLoadContext> stream_load_ctx,
                                 TStreamLoadPutRequest& request);
    int64_t get_wal_id(const std::string& wal);
    std::string get_tmp_path(const std::string wal);
    size_t size();
    Status send_request();

private:
    ExecEnv* _exec_env;
    int64_t _db_id;
    int64_t _table_id;
//    std::string _wal_dir;
//    std::string _tmp_dir;
    std::string _relay = "relay";
    std::string _split = "_";
    mutable std::mutex _replay_wal_lock;
    // key is wal_id
    std::map<std::string, replay_wal_info> _replay_wal_map;
    bool need_replay(const replay_wal_info& info);
    Status replay_wal_internal(const std::string& wal);
};
} // namespace doris