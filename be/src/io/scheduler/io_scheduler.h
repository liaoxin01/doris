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

// Cold-read IO subsystem -- L2 IOScheduler.
// See cold-read-io-redesign-v2.md (section 3.2) for the design rationale.
//
// The scheduler takes a batch of byte ranges that belong to one file, sorts and
// coalesces them (512KB gap / 8MB quantum by default), then drives the merged
// requests through a fixed IO thread pool against a *cache-bypassing* raw reader.
// Each merged request is materialized into an `Extent` (a shared buffer) that is
// handed back to the query through a future and, optionally, to the L3 CacheSink.

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/scan/task_executor/listenable_future.h"
#include "io/fs/file_reader.h"

namespace doris {
class ThreadPool;
class MemTrackerLimiter;
namespace io {
struct IOContext;
class CacheSink;

// A raw byte range the query wants to read (within a single file).
struct FetchRange {
    size_t offset = 0;
    size_t len = 0;
};

// One coalesced network request unit. The buffer is allocated by the scheduler and
// its lifetime is governed by the shared_ptr: query future, scheduler task and sink
// may all hold a reference, so a late-arriving cancel never frees data in use.
struct Extent {
    size_t offset = 0;
    size_t len = 0;
    std::unique_ptr<char[]> data;
    Status status;
};
using ExtentSPtr = std::shared_ptr<Extent>;

// Cache policy carried alongside a submit. Only DISPOSABLE / BYPASS are used.
enum class CachePolicy : int8_t {
    KEEP = 0,
    DISPOSABLE = 1,
    BYPASS = 2,
};

struct FetchHints {
    CachePolicy cache_policy = CachePolicy::DISPOSABLE;
    bool allow_coalesce = true;
};

// Result of submit(): the merged extent geometry plus the future that will be
// satisfied once the IO thread has filled it. The geometry is known eagerly (at
// merge time) so the caller can build its interval map before the data is ready.
struct MergedFetch {
    size_t offset = 0;
    size_t len = 0;
    SharedListenableFuture<ExtentSPtr> future;
};

// Process-wide bvar counters (defined in io_scheduler.cpp).
void io_scheduler_add_submitted_bytes(int64_t n);
void io_scheduler_add_fetched_bytes(int64_t n);
void io_scheduler_add_coalesced_requests(int64_t n);
void io_scheduler_add_session_miss_bytes(int64_t n);
void io_scheduler_add_sink_dropped_bytes(int64_t n);
// Bytes of ranges skipped at submit because they were already in the local file cache (P1-5).
void io_scheduler_add_cached_skipped_bytes(int64_t n);

class IOScheduler {
public:
    static IOScheduler* instance();

    // Creates the IO thread pool (config::io_scheduler_thread_num). Idempotent.
    Status init();
    void stop();

    // Submit a batch of ranges that belong to `file_key` (read through `inner`, which
    // MUST be a cache-bypassing raw reader -- see section 6.3). Internally: sort ->
    // coalesce -> enqueue. submit() never blocks; back-pressure manifests as a growing
    // queue and futures that stay not-ready. The merged extents (with their futures)
    // are appended to `out`.
    // `abandoned` is owned by the caller (the session); setting it to true causes any
    // not-yet-started task from this submit to be skipped.
    void submit(std::string file_key, FileReaderSPtr inner, std::vector<FetchRange> ranges,
                const FetchHints& hints, std::shared_ptr<std::atomic_bool> abandoned,
                std::vector<MergedFetch>* out);

    void register_sink(CacheSink* sink) { _sink = sink; }

    // Coalesce ranges: sort, dedup-overlap, merge while gap <= io_scheduler_coalesce_gap and the
    // merged length stays <= io_scheduler_coalesce_quantum. Exposed for unit testing.
    static std::vector<FetchRange> coalesce(std::vector<FetchRange> ranges, size_t gap,
                                            size_t quantum, bool allow_coalesce);

    int64_t inflight_bytes() const { return _inflight_bytes.load(); }
    int64_t inflight_bytes_peak() const { return _inflight_bytes_peak.load(); }

private:
    IOScheduler() = default;

    struct IOTask {
        std::string file_key;
        FileReaderSPtr inner;
        ExtentSPtr extent;
        SharedListenableFuture<ExtentSPtr> future;
        FetchHints hints;
        // Set when the owning session is cancelled. An abandoned task that has not
        // started yet is skipped (saving bandwidth); the buffer is freed via shared_ptr.
        std::shared_ptr<std::atomic_bool> abandoned;
    };

    // Runs one merged request: budget acquire -> raw read -> fulfil future -> sink -> release.
    void _run_task(IOTask task);

    // Target routing: drop ranges already DOWNLOADED in the local file cache (they fall
    // through to the fast local read path), returning only the cold ranges to schedule.
    std::vector<FetchRange> _filter_cold_ranges(const std::string& file_key,
                                                std::vector<FetchRange> ranges);

    // Byte budget shared across all in-flight extents -- the memory bound of the scheduler.
    // Acquire is done inside the IO thread (after dequeue), so submit() never blocks.
    void _budget_acquire(size_t n);
    void _budget_release(size_t n);

    std::unique_ptr<doris::ThreadPool> _pool;
    std::atomic_bool _inited {false};
    std::atomic_bool _stopped {false};

    // Byte budget (counting-semaphore style, supports arbitrary acquire sizes).
    std::mutex _budget_mu;
    std::condition_variable _budget_cv;
    std::atomic<int64_t> _inflight_bytes {0};
    std::atomic<int64_t> _inflight_bytes_peak {0};
    // Mirrors the in-flight byte budget so the memory shows up under a named tracker.
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;

    CacheSink* _sink = nullptr;
};

} // namespace io
} // namespace doris
