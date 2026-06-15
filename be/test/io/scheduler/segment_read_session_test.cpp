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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "io/fs/file_reader.h"
#include "common/config.h"
#include "io/scheduler/io_scheduler.h"

namespace doris::io {

class SessionMockReader : public FileReader {
public:
    static uint8_t pattern(size_t i) { return static_cast<uint8_t>((i * 2654435761u) & 0xff); }

    explicit SessionMockReader(size_t size) : _size(size) {}

    Status close() override { return Status::OK(); }
    const Path& path() const override { return _path; }
    size_t size() const override { return _size; }
    bool closed() const override { return false; }
    int64_t mtime() const override { return 0; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext*) override {
        size_t n = std::min(result.size, _size - offset);
        for (size_t i = 0; i < n; ++i) {
            result.data[i] = static_cast<char>(pattern(offset + i));
        }
        *bytes_read = n;
        return Status::OK();
    }

private:
    Path _path {"/mock/seg"};
    size_t _size;
};

class SegmentReadSessionTest : public testing::Test {
protected:
    void SetUp() override {
        ASSERT_TRUE(IOScheduler::instance()->init().ok());
        // The cold-block filter queries the global FileCacheFactory, which isn't initialized
        // in this unit test; disable it so submit() doesn't touch the cache.
        config::io_scheduler_submit_only_cold_blocks = false;
    }

    static void check_slice(SegmentReadSession& s, size_t off, size_t len) {
        std::vector<char> buf(len);
        size_t br = 0;
        Status st = s.try_read(off, Slice(buf.data(), len), &br);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(br, len);
        for (size_t i = 0; i < len; ++i) {
            ASSERT_EQ(static_cast<uint8_t>(buf[i]), SessionMockReader::pattern(off + i));
        }
    }
};

TEST_F(SegmentReadSessionTest, hit_returns_buffer) {
    auto reader = std::make_shared<SessionMockReader>(1 << 20);
    SegmentReadSession session("seg_key", reader);
    session.submit({{0, 4096}, {4096, 4096}}, FetchHints {});
    check_slice(session, 0, 100);
    check_slice(session, 4000, 200);    // crosses the two coalesced ranges
    check_slice(session, 8191 - 1, 1);  // last byte of registered region
}

TEST_F(SegmentReadSessionTest, miss_returns_not_found) {
    auto reader = std::make_shared<SessionMockReader>(1 << 20);
    SegmentReadSession session("seg_key", reader);
    session.submit({{0, 4096}}, FetchHints {});

    std::vector<char> buf(100);
    size_t br = 0;
    // Offset beyond any registered range -> miss.
    Status st = session.try_read(1 << 19, Slice(buf.data(), 100), &br);
    EXPECT_TRUE(st.is<ErrorCode::NOT_FOUND>());
}

TEST_F(SegmentReadSessionTest, partial_overrun_is_miss) {
    auto reader = std::make_shared<SessionMockReader>(1 << 20);
    SegmentReadSession session("seg_key", reader);
    session.submit({{0, 4096}}, FetchHints {});

    std::vector<char> buf(200);
    size_t br = 0;
    // Read starts inside the range but overruns its end -> not fully covered -> miss.
    Status st = session.try_read(4000, Slice(buf.data(), 200), &br);
    EXPECT_TRUE(st.is<ErrorCode::NOT_FOUND>());
}

TEST_F(SegmentReadSessionTest, cancel_before_read_is_safe) {
    auto reader = std::make_shared<SessionMockReader>(1 << 20);
    SegmentReadSession session("seg_key", reader);
    session.submit({{0, 4096}}, FetchHints {});
    session.cancel();
    // After cancel, try_read may hit (if already fetched) or miss; must not crash.
    std::vector<char> buf(100);
    size_t br = 0;
    (void)session.try_read(0, Slice(buf.data(), 100), &br);
}

} // namespace doris::io
