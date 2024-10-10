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

#include <string>
#include <vector>

#include "common/status.h"
#include "io/io_common.h"
#include "olap/compaction.h"
#include "olap/olap_common.h"

namespace doris {

class CumulativeCompaction final : public CompactionMixin {
public:
    CumulativeCompaction(StorageEngine& engine, const TabletSharedPtr& tablet);

    ~CumulativeCompaction() override;

    Status prepare_compact() override;

    Status execute_compact() override;

private:
    std::string_view compaction_name() const override { return "cumulative compaction"; }

    ReaderType compaction_type() const override { return ReaderType::READER_CUMULATIVE_COMPACTION; }

    Status pick_rowsets_to_compact();

    void find_longest_consecutive_version(std::vector<RowsetSharedPtr>* rowsets,
                                          std::vector<Version>* missing_version);

    void process_old_version_delete_bitmap();

    Version _last_delete_version {-1, -1};
};

} // namespace doris
