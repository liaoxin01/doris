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

#include "io/scheduler/segment_read_session.h"

#include <bthread/bthread.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstring>

#include "common/config.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/file_system.h"
#include "util/async_io.h"

namespace doris::io {

SegmentReadSession::SegmentReadSession(std::string file_key, FileReaderSPtr inner_no_cache)
        : _file_key(std::move(file_key)),
          _inner(std::move(inner_no_cache)),
          _abandoned(std::make_shared<std::atomic_bool>(false)) {}

SegmentReadSession::~SegmentReadSession() {
    cancel();
}

void SegmentReadSession::submit(std::vector<FetchRange> ranges, const FetchHints& hints) {
    if (ranges.empty() || _inner == nullptr) {
        return;
    }
    // The prefetcher emits 1MB-aligned ranges; the file's last block may extend past EOF.
    // Clamp to the real file size so the raw read never short-reads (which would otherwise
    // fail the extent and force a fallback for the tail block).
    const size_t file_size = _inner->size();
    std::vector<FetchRange> clamped;
    clamped.reserve(ranges.size());
    for (auto& r : ranges) {
        if (r.offset >= file_size) {
            continue;
        }
        r.len = std::min(r.len, file_size - r.offset);
        if (r.len > 0) {
            clamped.push_back(r);
        }
    }
    ranges = std::move(clamped);
    if (ranges.empty()) {
        return;
    }

    // P1-5: drop ranges already present (DOWNLOADED) in the local file cache, so only cold
    // blocks go through the scheduler (concurrent remote fetch). Cached blocks then miss the
    // session at read time and fall through to the fast local-cache read path -- this avoids
    // re-fetching warm data from remote, which otherwise makes warm scans slower than legacy.
    if (config::poc_submit_only_cold_blocks) {
        UInt128Wrapper hash = BlockFileCache::hash(_file_key);
        if (BlockFileCache* cache = FileCacheFactory::instance()->get_by_path(hash);
            cache != nullptr) {
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
                poc_add_cached_skipped_bytes(skipped);
            }
            ranges = std::move(cold);
            if (ranges.empty()) {
                return;
            }
        }
    }

    std::vector<MergedFetch> merged;
    IOScheduler::instance()->submit(_file_key, _inner, std::move(ranges), hints, _abandoned,
                                    &merged);
    std::unique_lock<std::shared_mutex> l(_mu);
    for (auto& m : merged) {
        // end is exclusive
        _intervals.emplace(m.offset, std::make_pair(m.offset + m.len, std::move(m.future)));
    }
}

Status SegmentReadSession::try_read(size_t offset, Slice result, size_t* bytes_read) {
    size_t want = result.size;
    size_t want_end = offset + want; // exclusive
    SharedListenableFuture<ExtentSPtr> future;
    bool found = false;
    {
        std::shared_lock<std::shared_mutex> l(_mu);
        // The first interval whose start <= offset.
        auto it = _intervals.upper_bound(offset);
        if (it != _intervals.begin()) {
            --it;
            size_t ext_start = it->first;
            size_t ext_end = it->second.first; // exclusive
            if (ext_start <= offset && want_end <= ext_end) {
                future = it->second.second;
                found = true;
            }
        }
    }
    if (!found) {
        poc_add_session_miss_bytes(static_cast<int64_t>(want));
        return Status::NotFound<false>("session miss");
    }

    // Wait for the extent. The future is a std::condition_variable wait, which on a bthread
    // (scanners run on bthreads) would pin the bthread's worker pthread and starve other
    // scan bthreads. Following doris's own IO pattern, offload the wait to a pthread via
    // AsyncIO so the bthread worker is released ("returned") during the IO wait -- this is
    // the async IODependency at the bthread level. On a pthread, just wait inline.
    Result<ExtentSPtr> res;
    if (bthread_self() == 0) {
        res = future.get();
    } else {
        auto task = [&]() { res = future.get(); };
        AsyncIO::run_task(task, io::FileSystemType::S3);
    }
    if (!res.has_value()) {
        poc_add_session_miss_bytes(static_cast<int64_t>(want));
        return Status::NotFound("session extent fetch failed: {}", res.error().to_string());
    }
    const ExtentSPtr& extent = res.value();
    if (!extent->status.ok() || extent->data == nullptr) {
        poc_add_session_miss_bytes(static_cast<int64_t>(want));
        return Status::NotFound("session extent not usable");
    }
    DCHECK(extent->offset <= offset && want_end <= extent->offset + extent->len);
    std::memcpy(result.data, extent->data.get() + (offset - extent->offset), want);
    *bytes_read = want;
    return Status::OK();
}

void SegmentReadSession::cancel() {
    if (_abandoned) {
        _abandoned->store(true);
    }
}

} // namespace doris::io
