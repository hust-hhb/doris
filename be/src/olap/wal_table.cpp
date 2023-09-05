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

#include "olap/wal_table.h"

#include <thrift/protocol/TDebugProtocol.h>

#include "http/action/stream_load.h"
#include "io/fs/local_file_system.h"
#include "olap/wal_manager.h"
#include "runtime/client_cache.h"
#include "runtime/fragment_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "http/ev_http_server.h"
#include "util/path_util.h"
#include "util/thrift_rpc_helper.h"
#include "vec/exec/format/wal/wal_reader.h"

#include <event2/event_struct.h>
#include <event2/event.h>
#include <event2/bufferevent.h>
#include <event2/http.h>
#include "evhttp.h"

namespace doris {

#ifdef BE_TEST
TCheckWalResult k_check_wal_result;
TUpdateWalMapResult k_update_wal_map_result;
#endif

WalTable::WalTable(ExecEnv* exec_env, int64_t db_id, int64_t table_id)
        : _exec_env(exec_env), _db_id(db_id), _table_id(table_id) {}
WalTable::~WalTable() {}

#ifdef BE_TEST
TStreamLoadPutResult wal_stream_load_put_result;
#endif

void WalTable::add_wals(std::vector<std::string> wals) {
    std::lock_guard<std::mutex> lock(_replay_wal_lock);
    for (const auto& wal : wals) {
        LOG(INFO) << "add replay wal " << wal;
        _replay_wal_map.emplace(wal, replay_wal_info {0, UnixMillis(), false});
    }
}
Status WalTable::replay_wals() {
    std::vector<std::string> need_replay_wals;
    {
        std::lock_guard<std::mutex> lock(_replay_wal_lock);
        if (_replay_wal_map.empty()) {
            return Status::OK();
        }
        VLOG_DEBUG << "Start replay wals for db=" << _db_id << ", table=" << _table_id
                   << ", wal size=" << _replay_wal_map.size();
        for (auto& [wal, info] : _replay_wal_map) {
            auto& [retry_num, start_ts, replaying] = info;
            if (replaying) {
                continue;
            }
            if (retry_num >= config::group_commit_replay_wal_retry_num) {
                LOG(WARNING) << "All replay wal failed, db=" << _db_id << ", table=" << _table_id
                             << ", wal=" << wal
                             << ", retry_num=" << config::group_commit_replay_wal_retry_num;
                std::string rename_path = get_tmp_path(wal);
                LOG(INFO) << "rename wal from " << wal << " to " << rename_path;
                std::rename(wal.c_str(), rename_path.c_str());
                _replay_wal_map.erase(wal);
                continue;
            }
            if (need_replay(info)) {
                replaying = true;
                need_replay_wals.push_back(wal);
            }
        }
        std::sort(need_replay_wals.begin(), need_replay_wals.end());
    }
    for (const auto& wal : need_replay_wals) {
        auto st = replay_wal_internal(wal);
        if (!st.ok()) {
            std::lock_guard<std::mutex> lock(_replay_wal_lock);
            auto it = _replay_wal_map.find(wal);
            if (it != _replay_wal_map.end()) {
                auto& [retry_num, start_time, replaying] = it->second;
                replaying = false;
            }
            LOG(WARNING) << "failed replay wal, drop this round, db=" << _db_id
                         << ", table=" << _table_id << ", wal=" << wal << ", st=" << st.to_string();
            break;
        }
        VLOG_NOTICE << "replay wal, db=" << _db_id << ", table=" << _table_id << ", label=" << wal
                    << ", st=" << st.to_string();
    }
    return Status::OK();
}

std::string WalTable::get_tmp_path(const std::string wal) {
    std::vector<std::string> path_element;
    doris::vectorized::WalReader::string_split(wal, "/", path_element);
    std::stringstream ss;
    int index = 0;
    while (index < path_element.size() - 3) {
        ss << path_element[index] << "/";
        index++;
    }
    ss << "tmp/";
    while (index < path_element.size()) {
        if (index != path_element.size() - 1) {
            ss << path_element[index] << "_";
        } else {
            ss << path_element[index];
        }
        index++;
    }
    return ss.str();
}

bool WalTable::need_replay(const doris::WalTable::replay_wal_info& info) {
#ifndef BE_TEST
    auto& [retry_num, start_ts, replaying] = info;
    auto replay_interval =
            pow(2, retry_num) * config::group_commit_replay_wal_retry_interval_seconds * 1000;
    return UnixMillis() - start_ts >= replay_interval;
#else
    return true;
#endif
}

Status WalTable::replay_wal_internal(const std::string& wal) {
    LOG(INFO) << "Start replay wal for db=" << _db_id << ", table=" << _table_id << ", wal=" << wal;
    // start a new stream load
    {
        std::lock_guard<std::mutex> lock(_replay_wal_lock);
        auto it = _replay_wal_map.find(wal);
        if (it != _replay_wal_map.end()) {
            auto& [retry_num, start_time, replaying] = it->second;
            ++retry_num;
            replaying = true;
        } else {
            LOG(WARNING) << "can not find wal in stream load replay map. db=" << _db_id
                         << ", table=" << _table_id << ", wal=" << wal;
            return Status::OK();
        }
    }
    // check whether wal need to do recover or not
    bool needRecovery = true;
    int64_t wal_id = get_wal_id(wal);
    auto st = check_wal(wal_id, needRecovery);
    if (!st.ok()) {
        LOG(WARNING) << "fail to check status for wal=" << wal << ", st=" << st.to_string();
        return st;
    } else if (!needRecovery) {
        LOG(INFO) << "wal is already committed, skip recovery, wal=" << wal_id;
        _exec_env->wal_mgr()->delete_wal(wal_id);

//        RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(std::to_string(wal_id)));
//        LOG(INFO) << "delete wal=" << wal;
        std::lock_guard<std::mutex> lock(_replay_wal_lock);
        _replay_wal_map.erase(wal);
        return Status::OK();
    }
    // generate and execute a plan fragment
    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
    TStreamLoadPutRequest request;
    st = begin_txn(ctx, wal_id);
    if (!st.ok()) {
        LOG(WARNING) << "fail to begin txn files for wal=" << wal << ", st=" << st.to_string();
        if (ctx->need_rollback) {
            _exec_env->stream_load_executor()->rollback_txn(ctx.get());
            ctx->need_rollback = false;
        }
        return st;
    } else {
        LOG(INFO) << "begin txn files for wal=" << wal << ", st=" << st.to_string();
    }
    ctx->wal_path = wal;
    RETURN_IF_ERROR(update_wal_map(ctx));
    RETURN_IF_ERROR(generate_stream_load_put_request(ctx, request));
    RETURN_IF_ERROR(execute_plan_fragment(ctx, request));
    RETURN_IF_ERROR(send_request());
    return Status::OK();
}

int64_t WalTable::get_wal_id(const std::string& wal) {
    std::vector<std::string> path_element;
    doris::vectorized::WalReader::string_split(wal, "/", path_element);
    int64_t wal_id = std::strtoll(path_element[path_element.size() - 1].c_str(), NULL, 10);
    return wal_id;
}

Status WalTable::begin_txn(std::shared_ptr<StreamLoadContext>& stream_load_ctx, int64_t wal_id) {
    stream_load_ctx->use_streaming = true;
    stream_load_ctx->load_type = TLoadType::type::MANUL_LOAD;
    stream_load_ctx->load_src_type = TLoadSourceType::type::RAW;
    stream_load_ctx->auth.user = "root";
    stream_load_ctx->auth.passwd = "";
    stream_load_ctx->db_id = _db_id;
    stream_load_ctx->table_id = _table_id;
    stream_load_ctx->wal_id = wal_id;
    stream_load_ctx->label = _relay + _split + std::to_string(_db_id) + _split +
                             std::to_string(_table_id) + _split + std::to_string(wal_id) + _split +
                             std::to_string(UnixMillis());
    stream_load_ctx->id = UniqueId::gen_uid();             // use a new id
    stream_load_ctx->format = TFileFormatType::FORMAT_WAL; // this is fake
    stream_load_ctx->max_filter_ratio = 1.0;
    RETURN_IF_ERROR(_exec_env->stream_load_executor()->begin_txn(stream_load_ctx.get()));
    return Status::OK();
}

//Status WalTable::update_wal_map(std::shared_ptr<StreamLoadContext>& stream_load_ctx) {
//    TUpdateWalMapRequest request;
//    request.__set_table_id(stream_load_ctx->table_id);
//    request.__set_wal_id(stream_load_ctx->wal_id);
//    request.__set_db_id(stream_load_ctx->db_id);
//    request.__set_be_id(_exec_env->master_info()->backend_id);
//    request.__set_txn_id(stream_load_ctx->txn_id);
//    TUpdateWalMapResult result;
//    Status status;
//    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
//    if (master_addr.hostname.empty() || master_addr.port == 0) {
//        status = Status::ServiceUnavailable("Have not get FE Master heartbeat yet");
//    } else {
//#ifndef BE_TEST
//        RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
//                master_addr.hostname, master_addr.port,
//                [&request, &result](FrontendServiceConnection& client) {
//                    client->updateWalMap(result, request);
//                }));
//#else
//        result = k_update_wal_map_result;
//#endif
//        status = Status(result.status);
//    }
//    return status;
//}

Status WalTable::update_wal_map(std::shared_ptr<StreamLoadContext>& stream_load_ctx) {
    return Status::OK();
}

void http_request_done(struct evhttp_request *req, void *arg){

}

Status WalTable::send_request() {
    struct event_base* base;
    struct evhttp_connection* conn;
    struct evhttp_request* req;

    base = event_base_new();
    conn = evhttp_connection_new("127.0.0.1", doris::config::webserver_port);
    evhttp_connection_set_base(conn, base);

    req = evhttp_request_new(http_request_done, base);
    evhttp_add_header(req->output_headers, "format", "WAL");
    evhttp_make_request(conn, req, EVHTTP_REQ_PUT, "/api/{db}/{table}/_stream_load");
    evhttp_connection_set_timeout(req->evcon, 600);

    event_base_dispatch(base);
    evhttp_connection_free(conn);
    event_base_free(base);

    return Status::OK();
}

Status WalTable::generate_stream_load_put_request(
        std::shared_ptr<StreamLoadContext>& stream_load_ctx, TStreamLoadPutRequest& request) {
    request.user = stream_load_ctx->auth.user;
    request.passwd = stream_load_ctx->auth.passwd;
    request.db = stream_load_ctx->db;
    request.tbl = stream_load_ctx->table;
    request.__set_table_id(stream_load_ctx->table_id);
    request.txnId = stream_load_ctx->txn_id;
    request.__set_loadId(stream_load_ctx->id.to_thrift());
    request.__set_thrift_rpc_timeout_ms(config::thrift_rpc_timeout_ms);
    request.formatType = stream_load_ctx->format;
    request.fileType = TFileType::FILE_STREAM; // this is fake
    return Status::OK();
}

Status WalTable::execute_plan_fragment(std::shared_ptr<StreamLoadContext> ctx,
                                       TStreamLoadPutRequest& request) {
#ifndef BE_TEST
    // plan this load
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    int64_t stream_load_put_start_time = MonotonicNanos();
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, ctx](FrontendServiceConnection& client) {
                client->streamLoadPut(ctx->put_result, request);
            }));
    ctx->stream_load_put_cost_nanos = MonotonicNanos() - stream_load_put_start_time;
#else
    ctx->put_result = wal_stream_load_put_result;
#endif
    Status plan_status(Status::create(ctx->put_result.status));
    if (!plan_status.ok()) {
        LOG(WARNING) << "plan streaming load failed. errmsg=" << plan_status << ctx->brief();
        return plan_status;
    }
    ctx->put_result.params.__set_import_label(ctx->label);
    VLOG_NOTICE << "params is " << apache::thrift::ThriftDebugString(ctx->put_result.params);

    ctx->start_write_data_nanos = MonotonicNanos();
    LOG(INFO) << "begin to execute job. label=" << ctx->label << ", txn_id=" << ctx->txn_id
              << ", query_id=" << print_id(ctx->put_result.params.params.query_id);
    std::shared_ptr<bool> running(new bool(true));
    auto st = _exec_env->stream_load_executor()->execute_plan_fragment(ctx, [ctx, running, this]() {
#ifndef BE_TEST
#else
        ctx->status=ctx->future.get();
#endif
        if (ctx->status.ok()) {
            // If put file success we need commit this load
            int64_t commit_and_publish_start_time = MonotonicNanos();
            LOG(INFO) << "commit txn:" << ctx->txn_id;
            ctx->status = _exec_env->stream_load_executor()->commit_txn(ctx.get());
            ctx->commit_and_publish_txn_cost_nanos =
                    MonotonicNanos() - commit_and_publish_start_time;
        }
        if (!ctx->status.ok() && ctx->status.code() != TStatusCode::PUBLISH_TIMEOUT) {
            LOG(WARNING) << "handle relay wal load failed, id=" << ctx->id
                         << ", need_rollback=" << ctx->need_rollback
                         << ", errmsg=" << ctx->status.to_string();
            if (ctx->need_rollback) {
                _exec_env->stream_load_executor()->rollback_txn(ctx.get());
                ctx->need_rollback = false;
            }
        }
        auto fragment_instance_id = ctx->put_result.params.params.fragment_instance_id;
        ctx->load_cost_millis = UnixMillis() - ctx->start_millis;
        LOG(INFO) << "finish replay wal stream load, fragment=" << fragment_instance_id
                  << ", id=" << ctx->id << ", label=" << ctx->label
                  << ", cost=" << ctx->load_cost_millis << " ms, body_size=" << ctx->body_bytes
                  << " bytes, load_rows=" << ctx->number_loaded_rows
                  << ", st=" << ctx->status.to_string();
        if (ctx->status.ok() || ctx->status.code() == TStatusCode::PUBLISH_TIMEOUT) {
            // delete WAL
//            auto st = io::global_local_filesystem()->delete_file(ctx->wal_path);
//            if (st.ok()) {
//                LOG(INFO) << "delete wal=" << ctx->wal_path;
//            } else {
//                LOG(WARNING) << "fail to delete wal=" << ctx->wal_path;
//            }
            _exec_env->wal_mgr()->delete_wal(ctx->wal_id);
            std::lock_guard<std::mutex> lock(_replay_wal_lock);
            _replay_wal_map.erase(ctx->wal_path);
        } else {
            // recovery WAL if failed
            std::lock_guard<std::mutex> lock(_replay_wal_lock);
            auto it = _replay_wal_map.find(ctx->wal_path);
            if (it != _replay_wal_map.end()) {
                auto& [retry_num, start_time, replaying] = it->second;
                replaying = false;
            } else {
                _replay_wal_map.emplace(ctx->wal_path, replay_wal_info {0, UnixMillis(), false});
            }
        }
        *running.get() = false;
    });
    while (st.ok() && *running.get()) {
        LOG(INFO) << "wait for relay wal done, wal=" << ctx->wal_id;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    RETURN_IF_ERROR(st);
    LOG(INFO) << "WalManager::execute_plan_fragment, id=" << ctx->id
              << ", instanceId=" << ctx->put_result.params.params.fragment_instance_id
              << ", status=" << ctx->status;
    st = ctx->status.code() == TStatusCode::PUBLISH_TIMEOUT ? Status::OK() : ctx->status;
    return st;
}

size_t WalTable::size() {
    std::lock_guard<std::mutex> lock(_replay_wal_lock);
    return _replay_wal_map.size();
}

//Status WalTable::check_wal(int64_t wal_id, bool& needRecovery) {
//    TCheckWalRequest request;
//    request.__set_table_id(_table_id);
//    request.__set_wal_id(wal_id);
//    request.__set_db_id(_db_id);
//    TCheckWalResult result;
//    Status status;
//    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
//    if (master_addr.hostname.empty() || master_addr.port == 0) {
//        status = Status::ServiceUnavailable("Have not get FE Master heartbeat yet");
//    } else {
//#ifndef BE_TEST
//        RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
//                master_addr.hostname, master_addr.port,
//                [&request, &result](FrontendServiceConnection& client) {
//                    client->checkWal(result, request);
//                }));
//#else
//        result = k_check_wal_result;
//#endif
//        needRecovery = result.need_recovery;
//        status = Status(result.status);
//    }
//    return status;
//}

Status WalTable::check_wal(int64_t wal_id, bool& needRecovery) {
    TCheckWalResult result;
    Status status;
#ifndef BE_TEST
#else
    result = k_check_wal_result;
#endif
    needRecovery = result.need_recovery;
    status = Status(result.status);
    return status;
}
} // namespace doris