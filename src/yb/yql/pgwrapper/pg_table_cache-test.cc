// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <ranges>
#include <span>
#include <vector>

#include "yb/client/table.h"

#include "yb/common/pg_types.h"

#include "yb/gutil/dynamic_annotations.h"
#include "yb/gutil/thread_annotations.h"

#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/pg_client_service.h"
#include "yb/tserver/pg_table_cache.h"
#include "yb/tserver/tablet_server.h"

#include "yb/util/countdown_latch.h"
#include "yb/util/scope_exit.h"
#include "yb/util/sync_point.h"
#include "yb/util/test_thread_holder.h"
#include "yb/util/test_util.h"
#include "yb/util/tsan_util.h"

#include "yb/yql/pgwrapper/pg_mini_test_base.h"
#include "yb/yql/pgwrapper/pg_test_utils.h"

using namespace std::literals;

DECLARE_int32(master_inject_latency_on_tablet_lookups_ms);
DECLARE_bool(TEST_enable_pg_client_mock);
DECLARE_int32(rpc_workers_limit);

namespace yb::pgwrapper {
using tserver::PgTableCache;
using tserver::PgTablesQueryListener;
using tserver::PgTablesQueryResult;
using tserver::TabletServer;
using tserver::PgClientServiceMockImpl;

namespace {

class PgTableCacheTest : public PgMiniTestBase {
 protected:
  size_t NumTabletServers() override { return 1; }

  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_enable_pg_client_mock) = true;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_rpc_workers_limit) = 32;
    PgMiniTestBase::SetUp();
    table_cache_ = &GetServer().TEST_GetPgClientService()->TEST_TableCache();
  }

  TabletServer& GetServer() {
    return *CHECK_NOTNULL(cluster_->mini_tablet_server(0)->server());
  }

  PgClientServiceMockImpl& ClientServiceMock() {
    return *CHECK_NOTNULL(GetServer().TEST_GetPgClientServiceMock());
  }

  Result<PgOid> CurrentDatabaseOid(PGConn& conn) {
    return conn.FetchRow<PGOid>("SELECT oid FROM pg_database WHERE datname = current_database()");
  }

  using ResultCheck = std::function<Status(const PgTablesQueryResult&)>;

  void TestGetTablesWhileLoading(const TableId& table_id, const ResultCheck& check);

  PgTableCache* table_cache_{nullptr};
};

class MockPgTablesQueryListener : public PgTablesQueryListener {
 public:
  using OnReadyFunctor = std::function<void(PgTablesQueryResult&&)>;

  explicit MockPgTablesQueryListener(OnReadyFunctor&& on_ready) : on_ready_(std::move(on_ready)) {}

  void Ready(PgTablesQueryResult&& result) override { on_ready_(std::move(result)); }

 private:
  OnReadyFunctor on_ready_;
};

// The results of GetTables calls; shared with their listeners, which may outlive a failed test.
struct GetTablesResults {
  explicit GetTablesResults(size_t expected) : latch(expected) {}

  void Add(Status status) {
    {
      std::lock_guard lock(mutex);
      statuses.push_back(std::move(status));
    }
    latch.CountDown();
  }

  std::vector<Status> Take() {
    std::lock_guard lock(mutex);
    return std::move(statuses);
  }

  CountDownLatch latch;
  std::mutex mutex;
  std::vector<Status> statuses GUARDED_BY(mutex);
};

} // namespace

// Misses table_id with one GetTables call, which starts the load of its entry, and holds that load
// while more calls for table_id are made from concurrent threads. They must find the loading entry
// under the shared side of the cache's mutex and wait on it, rather than each take the exclusive
// side: the exclusive pass runs only for the miss. Every call's result must pass check.
void PgTableCacheTest::TestGetTablesWhileLoading(
    const TableId& table_id, const ResultCheck& check) {
  constexpr size_t kConcurrentCallers = 8;
  const auto kWaitTimeout = 30s * kTimeMultiplier;

  std::atomic<size_t> exclusive_passes{0};
  std::atomic<size_t> loads{0};
  CountDownLatch release_load{1};
  SyncPointScope sync_point;
  // Before sync_point's destructor, which waits for its running callbacks.
  ScopeExit release_load_on_exit{[&release_load] { release_load.CountDown(); }};

  sync_point->SetCallBack("PgTableCache::GetTables:Exclusive", [&](void* arg) {
    const auto& ids = *static_cast<std::span<const TableId>*>(arg);
    if (std::ranges::find(ids, table_id) != ids.end()) {
      ++exclusive_passes;
    }
  });
  sync_point->SetCallBack("PgTableCache::LoadEntry:BeforeSetValue", [&](void* arg) {
    if (*static_cast<TableIdView*>(arg) != table_id) {
      return;
    }
    ++loads;
    // Runs on a client callback thread: bounded, so that a failed test does not hold it forever.
    release_load.WaitFor(kWaitTimeout);
  });
  sync_point->EnableProcessing();

  auto get_tables = [this, &check](
      const TableId& id, const std::shared_ptr<GetTablesResults>& results) {
    table_cache_->GetTables(
        {&id, 1},
        std::make_shared<MockPgTablesQueryListener>([results, check](PgTablesQueryResult&& r) {
          results->Add(check(r));
        }));
  };

  auto results = std::make_shared<GetTablesResults>(1 + kConcurrentCallers);
  {
    // The caller's ids may be gone before the load completes, so the load must not refer to them:
    // the load hook above only holds a load whose id is table_id.
    TableId caller_table_id = table_id;
    get_tables(caller_table_id, results);
    std::ranges::fill(caller_table_id, 'x');
  }
  ASSERT_EQ(exclusive_passes.load(), 1);
  {
    TestThreadHolder threads;
    for (size_t i = 0; i != kConcurrentCallers; ++i) {
      threads.AddThreadFunctor(
          [&get_tables, &table_id, results] { get_tables(table_id, results); });
    }
    threads.JoinAll();
  }
  // The load is held, so no call can have its result yet.
  ASSERT_EQ(results->latch.count(), 1 + kConcurrentCallers);
  ASSERT_EQ(exclusive_passes.load(), 1);

  release_load.CountDown();
  ASSERT_TRUE(results->latch.WaitFor(kWaitTimeout));
  ASSERT_EQ(loads.load(), 1);
  for (const auto& status : results->Take()) {
    ASSERT_OK(status);
  }
}

// The test checks that listener is notified if GetTables is called with empty table_ids
TEST_F(PgTableCacheTest, GetEmptyTables) {
  CountDownLatch call_latch{1};
  auto listener = std::make_shared<MockPgTablesQueryListener>(
      [&call_latch](PgTablesQueryResult&& result) {
        ScopeExit se{[&call_latch] { call_latch.CountDown(); }};
        // Check result is usable (no crash on access)
        ASSERT_NOK(result.Get(PgObjectId{1, 1}.GetYbTableId()));
      });
  table_cache_->GetTables({}, listener);
  ASSERT_TRUE(call_latch.WaitFor(15s));
}

// The test checks multiple waiters on same table cache entry will not block each other.
// I.e. system will not get stuck when the last waiter owns session lock and other waiters tries to
// take same lock.
TEST_F(PgTableCacheTest, MultipleWaitersWhenLastOwnsSessionLock) {
  constexpr auto kMaxInFlightOps = 200;
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("CREATE TABLE t(k INT PRIMARY KEY, v INT)"));
  const auto table_id = PgObjectId{
      ASSERT_RESULT(conn.FetchRow<PGOid>(
          "SELECT oid FROM pg_database WHERE datname = current_database()")),
      ASSERT_RESULT(conn.FetchRow<PGOid>("SELECT 't'::regclass::oid"))}.GetYbTableId();
  auto inserter = [&conn, kMaxInFlightOps]() -> Status {
    RETURN_NOT_OK(conn.StartTransaction(IsolationLevel::SNAPSHOT_ISOLATION));
    RETURN_NOT_OK(conn.ExecuteFormat(
        "INSERT INTO t SELECT s, s FROM generate_series(1, $0) AS s", kMaxInFlightOps));
    return conn.RollbackTransaction();
  };

  // Warm up cache
  ASSERT_OK(inserter());

  ASSERT_GT(kMaxInFlightOps, FLAGS_rpc_workers_limit);
  ASSERT_OK(conn.ExecuteFormat(
      "SET ysql_max_in_flight_ops = $0;"
      "SET ysql_session_max_batch_size = 1",
      kMaxInFlightOps));
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_master_inject_latency_on_tablet_lookups_ms) =
      2000 * kTimeMultiplier;

  auto mock_perform = ClientServiceMock().MockPerformBefore(
    [table_cache = table_cache_, table_id](auto* req, auto*, const auto* ctx) -> Status {
      const auto& ops = req->ops();
      DCHECK(!ops.empty());
      const auto& op = ops.front();
      if (op.has_write() && op.write().table_id() == table_id) {
        if (!ctx->rpc) {
          // Invalidate the table and significantly slowdown shared exchange perform query to make
          // its waiter the last one in the table cache entry.
          table_cache->Invalidate(table_id);
          std::this_thread::sleep_until(
              NextDiscreteTimePoint(1000ms * kTimeMultiplier) + 500ms * kTimeMultiplier);
        } else {
          std::this_thread::sleep_for(100ms * kTimeMultiplier);
        }
      }
      return Status::OK();
    });
  for ([[maybe_unused]] auto _ : std::views::iota(1, 3)) {
    ASSERT_OK(inserter());
  }
}

// The test checks that concurrent GetTables calls for a table whose entry is loading wait on that
// entry under the shared side of the cache's mutex and all get the table.
TEST_F(PgTableCacheTest, ConcurrentGetTablesWhileLoading) {
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("CREATE TABLE t(k INT PRIMARY KEY, v INT)"));
  const auto table_id = PgObjectId{
      ASSERT_RESULT(CurrentDatabaseOid(conn)),
      ASSERT_RESULT(conn.FetchRow<PGOid>("SELECT 't'::regclass::oid"))}.GetYbTableId();
  table_cache_->Invalidate(table_id);

  ASSERT_NO_FATALS(TestGetTablesWhileLoading(
      table_id, [table_id](const PgTablesQueryResult& result) -> Status {
        const auto& table = VERIFY_RESULT_REF(result.Get(table_id));
        SCHECK_EQ(table->id(), table_id, IllegalState, "Wrong table");
        return Status::OK();
      }));
}

// The test checks that Get finds an entry that is loading, and one that is loaded, under the shared
// side of the cache's mutex: only the miss that starts the load takes the exclusive side.
TEST_F(PgTableCacheTest, GetOfLoadingAndLoadedTable) {
  constexpr size_t kConcurrentCallers = 4;
  const auto kWaitTimeout = 30s * kTimeMultiplier;

  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.Execute("CREATE TABLE t(k INT PRIMARY KEY, v INT)"));
  const auto table_id = PgObjectId{
      ASSERT_RESULT(CurrentDatabaseOid(conn)),
      ASSERT_RESULT(conn.FetchRow<PGOid>("SELECT 't'::regclass::oid"))}.GetYbTableId();
  table_cache_->Invalidate(table_id);

  std::atomic<size_t> exclusive_gets{0};
  std::atomic<size_t> loads{0};
  CountDownLatch release_load{1};
  SyncPointScope sync_point;
  // Before sync_point's destructor, which waits for its running callbacks.
  ScopeExit release_load_on_exit{[&release_load] { release_load.CountDown(); }};

  sync_point->SetCallBack("PgTableCache::GetEntry:Exclusive", [&](void* arg) {
    if (*static_cast<TableIdView*>(arg) == table_id) {
      ++exclusive_gets;
    }
  });
  sync_point->SetCallBack("PgTableCache::LoadEntry:BeforeSetValue", [&](void* arg) {
    if (*static_cast<TableIdView*>(arg) != table_id) {
      return;
    }
    ++loads;
    // Runs on a client callback thread: bounded, so that a failed test does not hold it forever.
    release_load.WaitFor(kWaitTimeout);
  });
  sync_point->EnableProcessing();

  auto check_get = [this, &table_id]() -> Status {
    auto table = VERIFY_RESULT(table_cache_->Get(table_id));
    SCHECK_EQ(table->id(), table_id, IllegalState, "Wrong table");
    return Status::OK();
  };

  // The miss, which starts the load.
  table_cache_->GetTables(
      {&table_id, 1}, std::make_shared<MockPgTablesQueryListener>([](PgTablesQueryResult&&) {}));
  {
    TestThreadHolder threads;
    std::vector<Status> statuses(kConcurrentCallers);
    for (auto& status : statuses) {
      // Finds the entry loading, and waits for the held load, unless the thread starts late.
      threads.AddThreadFunctor([&check_get, &status] { status = check_get(); });
    }
    release_load.CountDown();
    threads.JoinAll();
    for (const auto& status : statuses) {
      ASSERT_OK(status);
    }
  }
  ASSERT_OK(check_get());
  ASSERT_EQ(loads.load(), 1);
  ASSERT_EQ(exclusive_gets.load(), 0);
}

// The test checks that concurrent GetTables calls for a table whose entry is loading all get the
// error when the load fails, and that a later call loads the table again.
TEST_F(PgTableCacheTest, ConcurrentGetTablesWhileLoadFails) {
  // A table oid that no table has in the database.
  constexpr PgOid kMissingTableOid = 65000;
  auto conn = ASSERT_RESULT(Connect());
  const auto table_id =
      PgObjectId{ASSERT_RESULT(CurrentDatabaseOid(conn)), kMissingTableOid}.GetYbTableId();

  auto check = [table_id](const PgTablesQueryResult& result) -> Status {
    SCHECK(!result.Get(table_id).ok(), IllegalState, "Missing table loaded");
    return Status::OK();
  };
  ASSERT_NO_FATALS(TestGetTablesWhileLoading(table_id, check));

  // The failed entry is replaced, so this call loads the table again rather than get the cached
  // error.
  std::atomic<size_t> loads{0};
  SyncPointScope sync_point;
  sync_point->SetCallBack("PgTableCache::LoadEntry:BeforeSetValue", [&](void* arg) {
    if (*static_cast<TableIdView*>(arg) == table_id) {
      ++loads;
    }
  });
  sync_point->EnableProcessing();
  auto result = ASSERT_RESULT(
      table_cache_->GetTables({&table_id, 1}, CoarseMonoClock::now() + 30s * kTimeMultiplier));
  ASSERT_OK(check(result));
  ASSERT_EQ(loads.load(), 1);
}

} // namespace yb::pgwrapper
