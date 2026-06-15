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

// Cold-read IO subsystem -- L3 CacheSink.
// See cold-read-io-redesign-v2.md (section 3.3).
//
// In the new architecture the file cache leaves the read path and becomes a bypass
// subscriber: the IO scheduler offers each fetched extent to the sink, which writes it
// back to the block cache asynchronously, under a rate limit, as DISPOSABLE blocks.
// BYPASS extents (TopN / point lookup) are dropped immediately. When the write-back
// queue is full the sink drops the extent (sacrificing fill ratio, never blocking IO).

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "io/scheduler/io_scheduler.h"

namespace doris {
class ThreadPool;
namespace io {

class CacheSink {
public:
    static CacheSink* instance();

    Status init(); // creates the 2-thread write-back pool
    void stop();

    // IO-thread callback (non-blocking). BYPASS returns immediately; otherwise the extent
    // is enqueued (its shared_ptr keeps the buffer alive). If the queue is over its cap the
    // extent is dropped and SinkDroppedBytes is bumped.
    void on_fetched(const std::string& file_key, const ExtentSPtr& e, const FetchHints& h);

private:
    CacheSink() = default;

    void _write_back(std::string file_key, ExtentSPtr e);

    // Token bucket: blocks until `bytes` tokens are available (rate = io_scheduler_cache_fill_rate_mbps).
    void _rate_limit(size_t bytes);

    std::unique_ptr<doris::ThreadPool> _pool;
    std::atomic_bool _inited {false};
    std::atomic_bool _stopped {false};

    // Bounded write-back queue accounting (cap = kQueueCapBytes).
    static constexpr int64_t kQueueCapBytes = 256LL * 1024 * 1024;
    std::atomic<int64_t> _queued_bytes {0};

    // Token bucket state.
    std::mutex _tb_mu;
    double _tokens = 0;
    std::chrono::steady_clock::time_point _last_refill;
};

} // namespace io
} // namespace doris
