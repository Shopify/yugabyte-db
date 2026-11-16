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

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "yb/rocksdb/statistics.h"
#include "yb/rocksdb/util/statistics.h"

#include "yb/util/countdown_latch.h"
#include "yb/util/flags.h"
#include "yb/util/jsonwriter.h"
#include "yb/util/metrics.h"
#include "yb/util/metrics_writer.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_thread_holder.h"
#include "yb/util/test_util.h"

DECLARE_bool(rocksdb_striped_tickers);

namespace rocksdb {

class StripedTickersTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_rocksdb_striped_tickers) = true;
    table_entity_ = table_prototype_.Instantiate(&registry_, kTableId, attributes_);
  }

  scoped_refptr<yb::MetricEntity> NewTabletEntity() {
    auto attributes = attributes_;
    attributes["tablet_id"] = yb::Format("tablet_$0", next_tablet_++);
    return tablet_prototype_.Instantiate(&registry_, attributes["tablet_id"], attributes);
  }

  std::shared_ptr<Statistics> NewStatistics(const scoped_refptr<yb::MetricEntity>& tablet) {
    return CreateDBStatistics(table_entity_, tablet, false);
  }

  // Returns the table-level value of rocksdb_block_cache_hit in the scrape output.
  int64_t ScrapedTotal() {
    yb::MetricPrometheusOptions opts;
    std::stringstream output;
    yb::PrometheusWriter writer(&output, opts);
    EXPECT_OK(registry_.WriteForPrometheus(&writer, opts));
    std::string line;
    while (std::getline(output, line)) {
      if (line.starts_with("rocksdb_block_cache_hit{") &&
          line.find("table_id=\"" + kTableId + "\"") != std::string::npos) {
        return std::stoll(line.substr(line.rfind(' ', line.rfind(' ') - 1) + 1));
      }
    }
    ADD_FAILURE() << "No rocksdb_block_cache_hit series in:\n" << output.str();
    return -1;
  }

  // Scrapes from two threads, checking that each sees a non-decreasing total, while `mutator`
  // runs.
  void ScrapeConcurrently(std::function<void()> mutator) {
    yb::CountDownLatch ready(3);
    yb::TestThreadHolder threads;
    for (int scraper = 0; scraper != 2; ++scraper) {
      threads.AddThreadFunctor([this, &ready] {
        ready.CountDown();
        ready.Wait();
        int64_t previous = 0;
        for (int i = 0; i != 100; ++i) {
          const auto current = ScrapedTotal();
          ASSERT_GE(current, previous);
          previous = current;
        }
      });
    }
    threads.AddThreadFunctor([&ready, &mutator] {
      ready.CountDown();
      ready.Wait();
      mutator();
    });
    threads.JoinAll();
  }

  const std::string kTableId = "striped_table_id";
  google::FlagSaver flag_saver_;
  yb::MetricRegistry registry_;
  yb::MetricEntityPrototype table_prototype_{"table"};
  yb::MetricEntityPrototype tablet_prototype_{"tablet"};
  yb::MetricEntity::AttributeMap attributes_{
      {"table_id", kTableId}, {"table_name", "t"}, {"table_type", "PGSQL_TABLE_TYPE"}};
  scoped_refptr<yb::MetricEntity> table_entity_;
  int next_tablet_ = 0;
};

TEST_F(StripedTickersTest, ExactCountsAndJson) {
  constexpr int kThreads = 8;
  constexpr uint64_t kIncrementsPerThread = 10000;
  std::vector<scoped_refptr<yb::MetricEntity>> entities{NewTabletEntity(), NewTabletEntity()};
  std::vector<std::shared_ptr<Statistics>> tablets{
      NewStatistics(entities[0]), NewStatistics(entities[1])};
  {
    yb::TestThreadHolder threads;
    for (int t = 0; t != kThreads; ++t) {
      threads.AddThreadFunctor([&tablets, t] {
        for (uint64_t i = 0; i != kIncrementsPerThread; ++i) {
          tablets[t % 2]->recordTick(BLOCK_CACHE_HIT, 1);
        }
      });
    }
    threads.JoinAll();
  }
  const uint64_t per_tablet = kThreads / 2 * kIncrementsPerThread;
  ASSERT_EQ(tablets[0]->getTickerCount(BLOCK_CACHE_HIT), per_tablet);
  ASSERT_EQ(tablets[1]->getTickerCount(BLOCK_CACHE_HIT), per_tablet);

  std::stringstream json;
  yb::JsonWriter json_writer(&json, yb::JsonWriter::COMPACT);
  ASSERT_OK(entities[0]->WriteAsJson(&json_writer, yb::MetricJsonOptions()));
  rapidjson::Document document;
  document.Parse(json.str().c_str());
  ASSERT_FALSE(document.HasParseError());
  bool found = false;
  for (const auto& metric : document["metrics"].GetArray()) {
    if (std::string(metric["name"].GetString()) == "rocksdb_block_cache_hit") {
      ASSERT_EQ(metric["value"].GetUint64(), per_tablet);
      found = true;
    }
  }
  ASSERT_TRUE(found);
  ASSERT_EQ(ScrapedTotal(), 2 * per_tablet);
}

TEST_F(StripedTickersTest, PreAggregationDeferredToScrape) {
  auto entity = NewTabletEntity();
  auto tablet = NewStatistics(entity);
  ASSERT_EQ(ScrapedTotal(), 0);
  tablet->recordTick(BLOCK_CACHE_HIT, 5);
  ASSERT_EQ(registry_.TEST_metrics_aggregator()
                ->TEST_GetMetricPreAggregatedValue("rocksdb_block_cache_hit", kTableId),
            0);
  ASSERT_EQ(ScrapedTotal(), 5);
  ASSERT_EQ(ScrapedTotal(), 5);
}

TEST_F(StripedTickersTest, Reset) {
  std::vector<scoped_refptr<yb::MetricEntity>> entities{NewTabletEntity(), NewTabletEntity()};
  std::vector<std::shared_ptr<Statistics>> tablets;
  for (const auto& entity : entities) {
    tablets.push_back(NewStatistics(entity));
    tablets.back()->recordTick(BLOCK_CACHE_HIT, 10);
  }
  ASSERT_EQ(ScrapedTotal(), 20);
  tablets[0]->setTickerCount(BLOCK_CACHE_HIT, 3);
  ASSERT_EQ(ScrapedTotal(), 13);
  tablets[0]->setTickerCount(BLOCK_CACHE_HIT, 10);
  ASSERT_EQ(ScrapedTotal(), 20);
}

TEST_F(StripedTickersTest, SharedEntity) {
  auto entity = NewTabletEntity();
  auto tablet = NewStatistics(entity);
  tablet->recordTick(BLOCK_CACHE_HIT, 10);
  ASSERT_EQ(ScrapedTotal(), 10);
  // Reusing an entity shares its gauge, not an independent flush cursor.
  auto shared = NewStatistics(entity);
  ASSERT_EQ(shared->getTickerCount(BLOCK_CACHE_HIT), 10);
  shared->recordTick(BLOCK_CACHE_HIT, 2);
  shared.reset();
  ASSERT_EQ(ScrapedTotal(), 12);
  ASSERT_EQ(tablet->getTickerCount(BLOCK_CACHE_HIT), 12);
}

TEST_F(StripedTickersTest, RetirementKeepsUnflushedCounts) {
  auto kept_entity = NewTabletEntity();
  auto kept = NewStatistics(kept_entity);
  {
    auto entity = NewTabletEntity();
    auto retired = NewStatistics(entity);
    retired->recordTick(BLOCK_CACHE_HIT, 7);
  }
  kept->recordTick(BLOCK_CACHE_HIT, 5);
  ASSERT_EQ(ScrapedTotal(), 12);
}

TEST_F(StripedTickersTest, ConcurrentScrapeUpdateDestroy) {
  auto entity = NewTabletEntity();
  auto tablet = NewStatistics(entity);
  ScrapeConcurrently([&tablet] {
    for (int i = 0; i != 10000; ++i) {
      tablet->recordTick(BLOCK_CACHE_HIT, 1);
    }
    tablet.reset();
  });
  ASSERT_EQ(ScrapedTotal(), 10000);
}

TEST_F(StripedTickersTest, ConcurrentScrapeCreateDestroy) {
  constexpr int kIterations = 1000;
  auto entity = NewTabletEntity();
  // Keeps the gauges registered, so each destruction flushes them while scrapes flush them too.
  auto kept = NewStatistics(entity);
  ScrapeConcurrently([this, &entity] {
    for (int i = 0; i != kIterations; ++i) {
      NewStatistics(entity)->recordTick(BLOCK_CACHE_HIT, 1);
    }
  });
  ASSERT_EQ(ScrapedTotal(), kIterations);
}

TEST_F(StripedTickersTest, ExcludedTickersKeepGaugeSemantics) {
  auto entity = NewTabletEntity();
  auto tablet = NewStatistics(entity);
  // A ticker that goes up and down stays a plain gauge.
  tablet->recordTick(NO_ITERATORS, 1);
  tablet->recordTick(NO_ITERATORS, -1);
  ASSERT_EQ(tablet->getTickerCount(NO_ITERATORS), 0);
  // Tickers that DBImpl sets keep set semantics.
  tablet->setTickerCount(CURRENT_VERSION_NUM_SST_FILES, 7);
  tablet->setTickerCount(CURRENT_VERSION_NUM_SST_FILES, 3);
  ASSERT_EQ(tablet->getTickerCount(CURRENT_VERSION_NUM_SST_FILES), 3);
}

}  // namespace rocksdb
