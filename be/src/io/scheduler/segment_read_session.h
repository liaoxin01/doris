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

// Cold-read IO subsystem -- L1 SegmentReadSession.
// See cold-read-io-redesign-v2.md (section 3.1).
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
#include <mutex>
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
    //   2. miss -> NotFound (caller falls back to legacy path), counts SessionMissBytes
    Status try_read(size_t offset, Slice result, size_t* bytes_read);

    // Called on iterator destruction / query cancel: marks in-flight tasks abandoned so
    // not-yet-started reads are skipped. Buffers already issued are kept alive by the
    // shared_ptr held inside the futures.
    void cancel();

    // --- IO-readiness, for the scheduling-boundary IO gate (pipeline IODependency) ---
    // The scanner reads roughly by increasing offset; `_consume_cursor` tracks how far it
    // has read. These let the scan scheduler park a scanner on pending IO (and reschedule
    // when it completes) instead of blocking a worker thread inside try_read.

    // True if some submitted extent at/after the read frontier has not been fetched yet.
    bool has_pending_io() const;

    // A Void future that fires when the earliest still-pending extent at/after the read
    // frontier completes; an already-ready future when nothing is pending. The caller may
    // park on it (process_for) so the worker is released during the wait.
    SharedListenableFuture<Void> io_barrier();

private:
    std::string _file_key;
    FileReaderSPtr _inner;
    std::shared_ptr<std::atomic_bool> _abandoned;

    // offset -> (end_exclusive, future). submit writes; try_read reads concurrently.
    std::map<size_t, std::pair<size_t, SharedListenableFuture<ExtentSPtr>>> _intervals;
    mutable std::shared_mutex _mu;
    // Highest end-offset served by try_read so far; the read frontier (hint, relaxed).
    std::atomic<size_t> _consume_cursor {0};
};
using SegmentReadSessionSPtr = std::shared_ptr<SegmentReadSession>;

// Scanner-owned slot (referenced from IOContext) holding the current segment's read session.
// The SegmentIterator publishes its session here at init; the scan scheduler reads it to
// decide whether to park the scanner on pending IO. Weak so a finished segment's session is
// naturally dropped. Thread-safe: written on segment open, read on the scanner thread.
struct IOBarrierSlot {
    void publish(const SegmentReadSessionSPtr& s) {
        std::lock_guard<std::mutex> l(mu);
        active = s;
    }
    SegmentReadSessionSPtr current() const {
        std::lock_guard<std::mutex> l(mu);
        return active.lock();
    }

    mutable std::mutex mu;
    std::weak_ptr<SegmentReadSession> active;
};

} // namespace doris::io
