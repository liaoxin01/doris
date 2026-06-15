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

#include <gen_cpp/Types_types.h>

#include <memory>

namespace doris {

namespace io {
// Cold-read IO scheduler (forward decl): see io/scheduler/segment_read_session.h.
class SegmentReadSession;
// Scanner-owned slot tracking the current segment's read session, so the scan scheduler
// can park a scanner on pending IO. Defined in io/scheduler/segment_read_session.h.
struct IOBarrierSlot;
} // namespace io

enum class ReaderType : uint8_t {
    READER_QUERY = 0,
    READER_ALTER_TABLE = 1,
    READER_BASE_COMPACTION = 2,
    READER_CUMULATIVE_COMPACTION = 3,
    READER_CHECKSUM = 4,
    READER_COLD_DATA_COMPACTION = 5,
    READER_SEGMENT_COMPACTION = 6,
    READER_FULL_COMPACTION = 7,
    READER_BINLOG_COMPACTION = 8,
    READER_BINLOG = 9,
    UNKNOWN = 10
};

namespace io {

struct FileReaderStats {
    size_t read_calls = 0;
    size_t read_bytes = 0;
    int64_t read_time_ns = 0;
    size_t read_rows = 0;
};

struct FileCacheStatistics {
    int64_t num_local_io_total = 0;
    int64_t num_remote_io_total = 0;
    int64_t num_peer_io_total = 0;
    int64_t local_io_timer = 0;
    int64_t bytes_read_from_local = 0;
    int64_t bytes_read_from_remote = 0;
    int64_t bytes_read_from_peer = 0;
    int64_t remote_io_timer = 0;
    int64_t peer_io_timer = 0;
    int64_t remote_wait_timer = 0;
    int64_t write_cache_io_timer = 0;
    int64_t bytes_write_into_cache = 0;
    int64_t num_skip_cache_io_total = 0;
    int64_t read_cache_file_directly_timer = 0;
    int64_t cache_get_or_set_timer = 0;
    int64_t lock_wait_timer = 0;
    int64_t get_timer = 0;
    int64_t set_timer = 0;

    int64_t inverted_index_num_local_io_total = 0;
    int64_t inverted_index_num_remote_io_total = 0;
    int64_t inverted_index_num_peer_io_total = 0;
    int64_t inverted_index_bytes_read_from_local = 0;
    int64_t inverted_index_bytes_read_from_remote = 0;
    int64_t inverted_index_bytes_read_from_peer = 0;
    int64_t inverted_index_local_io_timer = 0;
    int64_t inverted_index_remote_io_timer = 0;
    int64_t inverted_index_peer_io_timer = 0;
    int64_t inverted_index_io_timer = 0;
};

struct IOContext {
    ReaderType reader_type = ReaderType::UNKNOWN;
    // FIXME(plat1ko): Seems `is_disposable` can be inferred from the `reader_type`?
    bool is_disposable = false;
    bool is_index_data = false;
    bool read_file_cache = true;
    // TODO(lightman): use following member variables to control file cache
    bool is_persistent = false;
    // stop reader when reading, used in some interrupted operations
    bool should_stop = false;
    int64_t expiration_time = 0;
    const TUniqueId* query_id = nullptr;             // Ref
    FileCacheStatistics* file_cache_stats = nullptr; // Ref
    FileReaderStats* file_reader_stats = nullptr;    // Ref
    bool is_inverted_index = false;
    // if is_dryrun, read IO will download data to cache but return no data to reader
    // useful to skip cache data read from local disk to accelarate warm up
    bool is_dryrun = false;
    // if `is_warmup` == true, this I/O request is from a warm up task
    bool is_warmup {false};
    int64_t condition_cache_filtered_rows = 0;
    // Cold-read IO scheduler: when non-null (and config::enable_io_scheduler),
    // reads first try this session for a buffer-direct hit before the legacy cache path.
    // Lifetime is owned by the SegmentIterator that set it; copied by value with IOContext.
    SegmentReadSession* read_session = nullptr;
    // Cold-read IO scheduler BYPASS (TopN / point lookup): when true (and the scheduler
    // switch is on), reads go straight to the raw remote reader for exactly the requested
    // bytes -- skipping get_or_set, so no 1MB block amplification and no cache write-back.
    bool bypass_cache = false;
    // Scanner-owned (lives across segments); the SegmentIterator registers its read_session
    // here so the scan scheduler can park the scanner on pending IO instead of blocking a
    // worker thread. Null unless the scanner opted into the IO gate. Copied by value (shared).
    std::shared_ptr<IOBarrierSlot> io_barrier_slot;
};

} // namespace io
} // namespace doris
