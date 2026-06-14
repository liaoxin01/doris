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

#include "io/scheduler/cache_sink.h"

#include <glog/logging.h>

#include <thread>

#include "common/config.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "util/defer_op.h"
#include "util/slice.h"
#include "util/threadpool.h"

namespace doris::io {

CacheSink* CacheSink::instance() {
    static CacheSink s_instance;
    return &s_instance;
}

Status CacheSink::init() {
    bool expected = false;
    if (!_inited.compare_exchange_strong(expected, true)) {
        return Status::OK();
    }
    RETURN_IF_ERROR(ThreadPoolBuilder("PocCacheSink")
                            .set_min_threads(2)
                            .set_max_threads(2)
                            .build(&_pool));
    _last_refill = std::chrono::steady_clock::now();
    LOG(INFO) << "CacheSink POC initialized";
    return Status::OK();
}

void CacheSink::stop() {
    _stopped.store(true);
    if (_pool) {
        _pool->shutdown();
    }
}

void CacheSink::on_fetched(const std::string& file_key, const ExtentSPtr& e, const FetchHints& h) {
    if (_stopped.load() || _pool == nullptr) {
        return;
    }
    if (h.cache_policy == PocCachePolicy::BYPASS) {
        return; // TopN / point lookup: never pollute the cache
    }
    if (e == nullptr || !e->status.ok() || e->data == nullptr) {
        return;
    }
    int64_t len = static_cast<int64_t>(e->len);
    if (_queued_bytes.load() + len > kQueueCapBytes) {
        poc_add_sink_dropped_bytes(len);
        return; // queue full: drop, do not block the IO thread
    }
    _queued_bytes.fetch_add(len);
    ExtentSPtr extent = e; // keep buffer alive
    Status st = _pool->submit_func([this, key = file_key, extent = std::move(extent)]() mutable {
        _write_back(std::move(key), std::move(extent));
    });
    if (!st.ok()) {
        _queued_bytes.fetch_sub(len);
        poc_add_sink_dropped_bytes(len);
    }
}

void CacheSink::_rate_limit(size_t bytes) {
    double rate = static_cast<double>(config::poc_cache_fill_rate_mbps) * 1024.0 * 1024.0;
    if (rate <= 0) {
        return; // unlimited
    }
    const double burst = rate; // allow up to 1 second of accumulated tokens
    for (;;) {
        double sleep_sec = 0;
        {
            std::lock_guard<std::mutex> l(_tb_mu);
            auto now = std::chrono::steady_clock::now();
            double elapsed = std::chrono::duration<double>(now - _last_refill).count();
            _last_refill = now;
            _tokens = std::min(burst, _tokens + elapsed * rate);
            if (_tokens >= static_cast<double>(bytes)) {
                _tokens -= static_cast<double>(bytes);
                return;
            }
            sleep_sec = (static_cast<double>(bytes) - _tokens) / rate;
        }
        std::this_thread::sleep_for(std::chrono::duration<double>(sleep_sec));
    }
}

void CacheSink::_write_back(std::string file_key, ExtentSPtr e) {
    Defer dec {[&]() { _queued_bytes.fetch_sub(static_cast<int64_t>(e->len)); }};
    if (_stopped.load()) {
        return;
    }

    UInt128Wrapper hash = BlockFileCache::hash(file_key);
    BlockFileCache* cache = FileCacheFactory::instance()->get_by_path(hash);
    if (cache == nullptr) {
        return;
    }

    const size_t block_size = static_cast<size_t>(config::file_cache_each_block_size);
    if (block_size == 0) {
        return;
    }
    const size_t ext_begin = e->offset;
    const size_t ext_end = e->offset + e->len; // exclusive

    // First block-aligned offset at/after ext_begin.
    size_t blk = (ext_begin + block_size - 1) / block_size * block_size;

    ReadStatistics stats;
    CacheContext ctx;
    ctx.cache_type = FileCacheType::DISPOSABLE; // bypass-subscriber write-back
    ctx.stats = &stats;

    for (; blk + block_size <= ext_end; blk += block_size) {
        if (_stopped.load()) {
            return;
        }
        _rate_limit(block_size);
        FileBlocksHolder holder = cache->get_or_set(hash, blk, block_size, ctx);
        for (auto& block : holder.file_blocks) {
            if (block->state() != FileBlock::State::EMPTY) {
                continue; // someone is downloading / already cached -> skip
            }
            block->get_or_set_downloader();
            if (!block->is_downloader()) {
                continue;
            }
            size_t left = block->range().left;
            size_t sz = block->range().size();
            if (left < ext_begin || left + sz > ext_end) {
                continue; // partial coverage at the edge -> POC does not patch holes
            }
            const char* src = e->data.get() + (left - ext_begin);
            Status st = block->append(Slice(src, sz));
            if (st.ok()) {
                st = block->finalize();
            }
            if (!st.ok()) {
                LOG_EVERY_N(WARNING, 100) << "CacheSink write-back failed: " << st.msg();
            }
        }
    }
}

} // namespace doris::io
