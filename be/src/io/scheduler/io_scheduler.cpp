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

#include "io/scheduler/io_scheduler.h"

#include <bvar/bvar.h>
#include <glog/logging.h>

#include <algorithm>

#include "common/config.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/scheduler/cache_sink.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris::io {

// ---------------------------------------------------------------------------
// bvar counters
// ---------------------------------------------------------------------------
bvar::Adder<int64_t> g_io_scheduler_submitted_bytes("io_scheduler_submitted_bytes");
bvar::Adder<int64_t> g_io_scheduler_fetched_bytes("io_scheduler_fetched_bytes");
bvar::Adder<int64_t> g_io_scheduler_coalesced_requests("io_scheduler_coalesced_requests");
bvar::Adder<int64_t> g_io_scheduler_session_miss_bytes("io_scheduler_session_miss_bytes");
bvar::Adder<int64_t> g_io_scheduler_sink_dropped_bytes("io_scheduler_sink_dropped_bytes");
bvar::Adder<int64_t> g_io_scheduler_cached_skipped_bytes("io_scheduler_cached_skipped_bytes");
bvar::Status<int64_t> g_io_scheduler_inflight_bytes_peak("io_scheduler_inflight_bytes_peak", 0);

void io_scheduler_add_submitted_bytes(int64_t n) {
    g_io_scheduler_submitted_bytes << n;
}
void io_scheduler_add_fetched_bytes(int64_t n) {
    g_io_scheduler_fetched_bytes << n;
}
void io_scheduler_add_coalesced_requests(int64_t n) {
    g_io_scheduler_coalesced_requests << n;
}
void io_scheduler_add_session_miss_bytes(int64_t n) {
    g_io_scheduler_session_miss_bytes << n;
}
void io_scheduler_add_sink_dropped_bytes(int64_t n) {
    g_io_scheduler_sink_dropped_bytes << n;
}
void io_scheduler_add_cached_skipped_bytes(int64_t n) {
    g_io_scheduler_cached_skipped_bytes << n;
}

// ---------------------------------------------------------------------------
// IOScheduler
// ---------------------------------------------------------------------------
IOScheduler* IOScheduler::instance() {
    static IOScheduler s_instance;
    return &s_instance;
}

Status IOScheduler::init() {
    bool expected = false;
    if (!_inited.compare_exchange_strong(expected, true)) {
        return Status::OK();
    }
    int thread_num = static_cast<int>(config::io_scheduler_thread_num);
    if (thread_num <= 0) {
        thread_num = 128;
    }
    RETURN_IF_ERROR(ThreadPoolBuilder("IOScheduler")
                            .set_min_threads(thread_num)
                            .set_max_threads(thread_num)
                            .build(&_pool));
    _mem_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL,
                                                    "IOSchedulerInflight");
    LOG(INFO) << "IOScheduler initialized, io_thread_num=" << thread_num;
    return Status::OK();
}

void IOScheduler::stop() {
    _stopped.store(true);
    {
        // Wake up anything parked on the budget so it can observe _stopped.
        std::lock_guard<std::mutex> l(_budget_mu);
        _budget_cv.notify_all();
    }
    if (_pool) {
        _pool->shutdown();
    }
}

std::vector<FetchRange> IOScheduler::coalesce(std::vector<FetchRange> ranges, size_t gap,
                                              size_t quantum, bool allow_coalesce) {
    std::vector<FetchRange> out;
    if (ranges.empty()) {
        return out;
    }
    std::sort(ranges.begin(), ranges.end(),
              [](const FetchRange& a, const FetchRange& b) { return a.offset < b.offset; });

    FetchRange cur = ranges.front();
    auto flush = [&]() { out.push_back(cur); };
    for (size_t i = 1; i < ranges.size(); ++i) {
        const FetchRange& r = ranges[i];
        size_t cur_end = cur.offset + cur.len;        // exclusive
        size_t r_end = r.offset + r.len;              // exclusive
        // Overlapping or within-gap, and merging keeps us under the quantum.
        bool within_gap = r.offset <= cur_end || (r.offset - cur_end) <= gap;
        size_t merged_len = (std::max(cur_end, r_end)) - cur.offset;
        if (allow_coalesce && within_gap && merged_len <= quantum) {
            cur.len = merged_len;
        } else {
            flush();
            cur = r;
        }
    }
    flush();
    return out;
}

std::vector<FetchRange> IOScheduler::_filter_cold_ranges(const std::string& file_key,
                                                         std::vector<FetchRange> ranges) {
    // No file cache configured (e.g. unit tests): treat everything as cold. Guard against an
    // empty factory because get_by_path() indexes by `hash % _caches.size()`.
    if (FileCacheFactory::instance()->get_cache_instance_size() == 0) {
        return ranges;
    }
    UInt128Wrapper hash = BlockFileCache::hash(file_key);
    BlockFileCache* cache = FileCacheFactory::instance()->get_by_path(hash);
    if (cache == nullptr) {
        return ranges;
    }
    std::map<size_t, FileBlockSPtr> cached = cache->get_blocks_by_key(hash);
    std::vector<FetchRange> cold;
    cold.reserve(ranges.size());
    int64_t skipped = 0;
    for (const auto& r : ranges) {
        bool is_cached = false;
        if (auto it = cached.upper_bound(r.offset); it != cached.begin()) {
            --it; // the cached block whose start offset <= r.offset
            const auto& range = it->second->range();
            if (range.left <= r.offset && r.offset + r.len - 1 <= range.right) {
                is_cached = true;
            }
        }
        if (is_cached) {
            skipped += static_cast<int64_t>(r.len);
        } else {
            cold.push_back(r);
        }
    }
    if (skipped > 0) {
        io_scheduler_add_cached_skipped_bytes(skipped);
    }
    return cold;
}

void IOScheduler::submit(std::string file_key, FileReaderSPtr inner, std::vector<FetchRange> ranges,
                         const FetchHints& hints, std::shared_ptr<std::atomic_bool> abandoned,
                         std::vector<MergedFetch>* out) {
    if (ranges.empty() || inner == nullptr) {
        return;
    }

    // Target routing (cold-block filter): ranges already present (DOWNLOADED) in the local
    // file cache are not re-fetched from remote -- they fall through to the fast local-cache
    // read path at read time. Owning this here keeps the data-source decision (remote vs
    // local) in the IO layer; the session declares ranges without any cache knowledge.
    // TODO: instead of dropping cached ranges, route them to a local-cache target and merge
    // them through the same scheduler (sequential big-block reads off the cache disk).
    if (config::io_scheduler_submit_only_cold_blocks) {
        ranges = _filter_cold_ranges(file_key, std::move(ranges));
        if (ranges.empty()) {
            return;
        }
    }

    size_t submitted = 0;
    for (const auto& r : ranges) {
        submitted += r.len;
    }
    io_scheduler_add_submitted_bytes(static_cast<int64_t>(submitted));

    std::vector<FetchRange> merged =
            coalesce(std::move(ranges), static_cast<size_t>(config::io_scheduler_coalesce_gap),
                     static_cast<size_t>(config::io_scheduler_coalesce_quantum), hints.allow_coalesce);
    io_scheduler_add_coalesced_requests(static_cast<int64_t>(merged.size()));

    out->reserve(out->size() + merged.size());
    for (const auto& m : merged) {
        auto extent = std::make_shared<Extent>();
        extent->offset = m.offset;
        extent->len = m.len;

        SharedListenableFuture<ExtentSPtr> future;

        IOTask task;
        task.file_key = file_key;
        task.inner = inner;
        task.extent = extent;
        task.future = future;
        task.hints = hints;
        task.abandoned = abandoned;

        out->push_back(MergedFetch {m.offset, m.len, future});

        // submit_func never blocks on the budget; the budget is acquired inside the task.
        Status st = _pool->submit_func([this, t = std::move(task)]() mutable { _run_task(std::move(t)); });
        if (!st.ok()) {
            // Pool is shutting down: fail the future so try_read falls back to legacy path.
            future.set_error(st);
            LOG_EVERY_N(WARNING, 100) << "IOScheduler submit_func failed: " << st;
        }
    }
}

void IOScheduler::_budget_acquire(size_t n) {
    int64_t budget = config::io_scheduler_inflight_bytes_budget;
    int64_t want = static_cast<int64_t>(n);
    // A single request larger than the whole budget is still allowed through (capped),
    // otherwise it would deadlock waiting for room that can never exist.
    if (want > budget) {
        want = budget;
    }
    std::unique_lock<std::mutex> l(_budget_mu);
    _budget_cv.wait(l, [&]() {
        return _stopped.load() || (_inflight_bytes.load() + want) <= budget;
    });
    int64_t now = _inflight_bytes.fetch_add(want) + want;
    if (_mem_tracker) {
        _mem_tracker->consume(want);
    }
    int64_t peak = _inflight_bytes_peak.load();
    while (now > peak && !_inflight_bytes_peak.compare_exchange_weak(peak, now)) {
    }
    if (now > peak) {
        g_io_scheduler_inflight_bytes_peak.set_value(now);
    }
}

void IOScheduler::_budget_release(size_t n) {
    int64_t budget = config::io_scheduler_inflight_bytes_budget;
    int64_t amt = static_cast<int64_t>(n);
    if (amt > budget) {
        amt = budget;
    }
    {
        std::lock_guard<std::mutex> l(_budget_mu);
        _inflight_bytes.fetch_sub(amt);
    }
    if (_mem_tracker) {
        _mem_tracker->release(amt);
    }
    _budget_cv.notify_all();
}

void IOScheduler::_run_task(IOTask task) {
    ExtentSPtr& extent = task.extent;

    if (_stopped.load() || (task.abandoned && task.abandoned->load())) {
        extent->status = Status::Cancelled("io scheduler task abandoned");
        task.future.set_value(extent);
        return;
    }

    _budget_acquire(extent->len);
    Defer release {[&]() { _budget_release(extent->len); }};

    extent->data.reset(new char[extent->len]);
    size_t bytes_read = 0;
    // Read through the raw (cache-bypassing) reader; io_ctx is intentionally null so the
    // task does not depend on the (possibly destroyed) query IOContext.
    Status st = task.inner->read_at(extent->offset, Slice(extent->data.get(), extent->len),
                                    &bytes_read, nullptr);
    if (st.ok() && bytes_read != extent->len) {
        st = Status::IOError("short read: want {} got {} at offset {}", extent->len, bytes_read,
                             extent->offset);
    }
    extent->status = st;
    if (st.ok()) {
        io_scheduler_add_fetched_bytes(static_cast<int64_t>(extent->len));
    }

    // Hand the buffer to the query first (latency), then offer it to the sink.
    task.future.set_value(extent);

    if (st.ok() && _sink != nullptr && task.hints.cache_policy != CachePolicy::BYPASS) {
        _sink->on_fetched(task.file_key, extent, task.hints);
    }
}

} // namespace doris::io
