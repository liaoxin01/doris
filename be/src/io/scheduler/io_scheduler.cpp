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
#include "io/scheduler/cache_sink.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris::io {

// ---------------------------------------------------------------------------
// bvar counters
// ---------------------------------------------------------------------------
bvar::Adder<int64_t> g_poc_submitted_bytes("poc_submitted_bytes");
bvar::Adder<int64_t> g_poc_fetched_bytes("poc_fetched_bytes");
bvar::Adder<int64_t> g_poc_coalesced_requests("poc_coalesced_requests");
bvar::Adder<int64_t> g_poc_session_miss_bytes("poc_session_miss_bytes");
bvar::Adder<int64_t> g_poc_sink_dropped_bytes("poc_sink_dropped_bytes");
bvar::Adder<int64_t> g_poc_cached_skipped_bytes("poc_cached_skipped_bytes");
bvar::Status<int64_t> g_poc_inflight_bytes_peak("poc_inflight_bytes_peak", 0);

void poc_add_submitted_bytes(int64_t n) {
    g_poc_submitted_bytes << n;
}
void poc_add_fetched_bytes(int64_t n) {
    g_poc_fetched_bytes << n;
}
void poc_add_coalesced_requests(int64_t n) {
    g_poc_coalesced_requests << n;
}
void poc_add_session_miss_bytes(int64_t n) {
    g_poc_session_miss_bytes << n;
}
void poc_add_sink_dropped_bytes(int64_t n) {
    g_poc_sink_dropped_bytes << n;
}
void poc_add_cached_skipped_bytes(int64_t n) {
    g_poc_cached_skipped_bytes << n;
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
    int thread_num = static_cast<int>(config::poc_io_thread_num);
    if (thread_num <= 0) {
        thread_num = 128;
    }
    RETURN_IF_ERROR(ThreadPoolBuilder("PocIOScheduler")
                            .set_min_threads(thread_num)
                            .set_max_threads(thread_num)
                            .build(&_pool));
    _mem_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL,
                                                    "PocIOSchedulerInflight");
    LOG(INFO) << "IOScheduler POC initialized, io_thread_num=" << thread_num;
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

void IOScheduler::submit(std::string file_key, FileReaderSPtr inner, std::vector<FetchRange> ranges,
                         const FetchHints& hints, std::shared_ptr<std::atomic_bool> abandoned,
                         std::vector<MergedFetch>* out) {
    if (ranges.empty() || inner == nullptr) {
        return;
    }
    size_t submitted = 0;
    for (const auto& r : ranges) {
        submitted += r.len;
    }
    poc_add_submitted_bytes(static_cast<int64_t>(submitted));

    std::vector<FetchRange> merged =
            coalesce(std::move(ranges), static_cast<size_t>(config::poc_coalesce_gap),
                     static_cast<size_t>(config::poc_coalesce_quantum), hints.allow_coalesce);
    poc_add_coalesced_requests(static_cast<int64_t>(merged.size()));

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
    int64_t budget = config::poc_inflight_bytes_budget;
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
        g_poc_inflight_bytes_peak.set_value(now);
    }
}

void IOScheduler::_budget_release(size_t n) {
    int64_t budget = config::poc_inflight_bytes_budget;
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
        poc_add_fetched_bytes(static_cast<int64_t>(extent->len));
    }

    // Hand the buffer to the query first (latency), then offer it to the sink.
    task.future.set_value(extent);

    if (st.ok() && _sink != nullptr && task.hints.cache_policy != PocCachePolicy::BYPASS) {
        _sink->on_fetched(task.file_key, extent, task.hints);
    }
}

} // namespace doris::io
