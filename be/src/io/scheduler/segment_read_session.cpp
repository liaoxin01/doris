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

#include <glog/logging.h>

#include <algorithm>
#include <cstring>

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

    // L1 declares every range it intends to read; deciding which ranges are already local
    // (and so should not be re-fetched from remote) is the scheduler's job -- see
    // IOScheduler::submit's cold-block filter / target routing. The session stays free of any
    // file-cache knowledge.
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
        io_scheduler_add_session_miss_bytes(static_cast<int64_t>(want));
        return Status::NotFound<false>("session miss");
    }

    Result<ExtentSPtr> res = future.get();
    if (!res.has_value()) {
        io_scheduler_add_session_miss_bytes(static_cast<int64_t>(want));
        return Status::NotFound("session extent fetch failed: {}", res.error().to_string());
    }
    const ExtentSPtr& extent = res.value();
    if (!extent->status.ok() || extent->data == nullptr) {
        io_scheduler_add_session_miss_bytes(static_cast<int64_t>(want));
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
