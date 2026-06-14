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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "common/config.h"
#include "io/fs/file_reader.h"

namespace doris::io {

// A deterministic in-memory reader: byte at absolute file offset i == pattern(i).
// Optionally sleeps per read to exercise concurrency / back-pressure.
class MockFileReader : public FileReader {
public:
    static uint8_t pattern(size_t i) { return static_cast<uint8_t>((i * 1315423911u) & 0xff); }

    explicit MockFileReader(size_t size, int sleep_ms = 0) : _size(size), _sleep_ms(sleep_ms) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }
    const Path& path() const override { return _path; }
    size_t size() const override { return _size; }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 0; }

    int read_count() const { return _read_count.load(); }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext*) override {
        _read_count.fetch_add(1);
        if (_sleep_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(_sleep_ms));
        }
        size_t n = std::min(result.size, _size - offset);
        for (size_t i = 0; i < n; ++i) {
            result.data[i] = static_cast<char>(pattern(offset + i));
        }
        *bytes_read = n;
        return Status::OK();
    }

private:
    Path _path {"/mock/file"};
    size_t _size;
    int _sleep_ms;
    bool _closed = false;
    std::atomic<int> _read_count {0};
};

class IOSchedulerTest : public testing::Test {
protected:
    void SetUp() override { ASSERT_TRUE(IOScheduler::instance()->init().ok()); }
};

// ---- coalesce ----

TEST_F(IOSchedulerTest, coalesce_merges_within_gap) {
    std::vector<FetchRange> in {{0, 100}, {120, 100}}; // gap 20 <= 512
    auto out = IOScheduler::coalesce(in, 512, 8192, true);
    ASSERT_EQ(out.size(), 1);
    EXPECT_EQ(out[0].offset, 0);
    EXPECT_EQ(out[0].len, 220); // 0..220
}

TEST_F(IOSchedulerTest, coalesce_splits_beyond_gap) {
    std::vector<FetchRange> in {{0, 100}, {2000, 100}}; // gap 1900 > 512
    auto out = IOScheduler::coalesce(in, 512, 8192, true);
    ASSERT_EQ(out.size(), 2);
}

TEST_F(IOSchedulerTest, coalesce_respects_quantum) {
    // Adjacent ranges, but merging would exceed the quantum of 150.
    std::vector<FetchRange> in {{0, 100}, {100, 100}};
    auto out = IOScheduler::coalesce(in, 512, 150, true);
    ASSERT_EQ(out.size(), 2);
}

TEST_F(IOSchedulerTest, coalesce_handles_overlap) {
    std::vector<FetchRange> in {{0, 100}, {50, 100}};
    auto out = IOScheduler::coalesce(in, 0, 8192, true);
    ASSERT_EQ(out.size(), 1);
    EXPECT_EQ(out[0].offset, 0);
    EXPECT_EQ(out[0].len, 150);
}

TEST_F(IOSchedulerTest, coalesce_disabled_keeps_each_range) {
    std::vector<FetchRange> in {{0, 100}, {100, 100}};
    auto out = IOScheduler::coalesce(in, 512, 8192, false);
    ASSERT_EQ(out.size(), 2);
}

// ---- submit / fetch ----

static void verify_extent(const ExtentSPtr& e) {
    ASSERT_TRUE(e->status.ok());
    ASSERT_NE(e->data, nullptr);
    for (size_t i = 0; i < e->len; ++i) {
        ASSERT_EQ(static_cast<uint8_t>(e->data[i]), MockFileReader::pattern(e->offset + i));
    }
}

TEST_F(IOSchedulerTest, submit_fetches_correct_bytes) {
    auto reader = std::make_shared<MockFileReader>(1 << 20);
    auto abandoned = std::make_shared<std::atomic_bool>(false);
    std::vector<FetchRange> ranges {{0, 4096}, {4096, 4096}, {1 << 19, 4096}};
    std::vector<MergedFetch> out;
    IOScheduler::instance()->submit("k", reader, ranges, FetchHints {}, abandoned, &out);
    ASSERT_FALSE(out.empty());
    for (auto& m : out) {
        auto res = m.future.get();
        ASSERT_TRUE(res.has_value());
        verify_extent(res.value());
    }
}

TEST_F(IOSchedulerTest, abandoned_tasks_are_skipped) {
    auto reader = std::make_shared<MockFileReader>(1 << 20, /*sleep_ms=*/50);
    auto abandoned = std::make_shared<std::atomic_bool>(false);
    abandoned->store(true); // cancel before tasks run
    std::vector<FetchRange> ranges {{0, 4096}};
    std::vector<MergedFetch> out;
    IOScheduler::instance()->submit("k", reader, ranges, FetchHints {}, abandoned, &out);
    ASSERT_EQ(out.size(), 1);
    auto res = out[0].future.get();
    ASSERT_TRUE(res.has_value());
    EXPECT_FALSE(res.value()->status.ok()); // cancelled, never read
}

TEST_F(IOSchedulerTest, budget_backpressure_completes) {
    // Tiny budget forces serialization, but every future must still resolve.
    auto old_budget = config::poc_inflight_bytes_budget;
    config::poc_inflight_bytes_budget = 4096;
    auto reader = std::make_shared<MockFileReader>(1 << 20, /*sleep_ms=*/5);
    auto abandoned = std::make_shared<std::atomic_bool>(false);
    std::vector<FetchRange> ranges;
    for (int i = 0; i < 16; ++i) {
        ranges.push_back({static_cast<size_t>(i * 8192), 4096}); // 4096 gap -> no merge
    }
    std::vector<MergedFetch> out;
    IOScheduler::instance()->submit("k", reader, ranges, FetchHints {.allow_coalesce = false},
                                    abandoned, &out);
    ASSERT_EQ(out.size(), 16);
    for (auto& m : out) {
        auto res = m.future.get();
        ASSERT_TRUE(res.has_value());
        verify_extent(res.value());
    }
    // The byte budget is enforced inside _budget_acquire (a counting-semaphore style wait);
    // here we assert the post-condition that every acquire was matched by a release, so the
    // in-flight accounting drains back to zero (no leak). The release runs in a Defer *after*
    // future.set_value(), so poll briefly for the last task(s) to finish releasing.
    // NOTE: inflight_bytes_peak() is a process-wide monotonic high-watermark on the singleton
    // scheduler -- it accumulates across tests and cannot be asserted per-test.
    for (int i = 0; i < 100 && IOScheduler::instance()->inflight_bytes() != 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_EQ(IOScheduler::instance()->inflight_bytes(), 0);
    config::poc_inflight_bytes_budget = old_budget;
}

} // namespace doris::io
