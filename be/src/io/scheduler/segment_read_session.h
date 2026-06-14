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

// Cold-read IO subsystem POC -- L1 SegmentReadSession.
// See cold-read-poc-implementation.md (section 5.2).
//
// At segment-iterator init the query declares the exact byte ranges it will read for
// the whole segment (1MB block granularity, inherited from SegmentPrefetcher). The
// session hands them to the L2 IOScheduler and keeps an interval map offset -> future.
// PageIO reads later resolve against that map: a hit copies the slice straight out of
// the scheduler buffer (no cache round-trip); a miss returns NotFound so the caller
// transparently falls back to the legacy cached read path.

#pragma once

#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/scheduler/io_scheduler.h"
#include "util/slice.h"

namespace doris::io {

class SegmentReadSession {
public:
    // `inner_no_cache` MUST bypass the file cache (the raw S3 reader); `file_key` is the
    // cache hash key string used by the L3 sink for write-back.
    SegmentReadSession(std::string file_key, FileReaderSPtr inner_no_cache);
    ~SegmentReadSession();

    // Register block ranges and hand them to the scheduler. After coalescing, one
    // returned extent may cover several registered ranges; the interval map keys on the
    // *merged* extent geometry so try_read can locate the covering extent.
    void submit(std::vector<FetchRange> ranges, const FetchHints& hints);

    // The actual read entry point (called from the injection layer):
    //   1. interval map hit -> future.get() -> memcpy the requested slice -> OK
    //   2. miss -> NotFound (caller falls back to legacy path), counts PocSessionMissBytes
    Status try_read(size_t offset, Slice result, size_t* bytes_read);

    // Called on iterator destruction / query cancel: marks in-flight tasks abandoned so
    // not-yet-started reads are skipped. Buffers already issued are kept alive by the
    // shared_ptr held inside the futures.
    void cancel();

private:
    std::string _file_key;
    FileReaderSPtr _inner;
    std::shared_ptr<std::atomic_bool> _abandoned;

    // offset -> (end_exclusive, future). submit writes; try_read reads concurrently.
    std::map<size_t, std::pair<size_t, SharedListenableFuture<ExtentSPtr>>> _intervals;
    std::shared_mutex _mu;
};
using SegmentReadSessionSPtr = std::shared_ptr<SegmentReadSession>;

} // namespace doris::io
