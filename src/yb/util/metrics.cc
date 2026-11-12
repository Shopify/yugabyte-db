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
//
// The following only applies to changes made to this file as part of YugabyteDB development.
//
// Portions Copyright (c) YugabyteDB, Inc.
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

#include "yb/util/metrics.h"

#include <cstring>
#include <map>
#include <optional>
#include <set>

#include "yb/gutil/atomicops.h"
#include "yb/gutil/casts.h"
#include "yb/gutil/map-util.h"

#include "yb/util/aggregate_stats.h"
#include "yb/util/debug-util.h"
#include "yb/util/format.h"
#include "yb/util/hdr_histogram.h"
#include "yb/util/histogram.pb.h"
#include "yb/util/jsonwriter.h"
#include "yb/util/lanes.h"
#include "yb/util/locks.h"
#include "yb/util/status.h"
#include "yb/util/status_log.h"
#include "yb/util/stopwatch.h"
#include "yb/util/threadpool.h"
#include "yb/util/flags.h"

DEFINE_UNKNOWN_bool(expose_metric_histogram_percentiles, true,
            "Should we expose the percentiles information for metrics histograms.");

DEPRECATE_FLAG(int32, max_tables_metrics_breakdowns, "08_2023"
             "The maxmimum number of tables to retrieve metrics for");

DEFINE_test_flag(bool, pause_flush_aggregated_metrics, false,
                 "If true, pause before flushing aggregated metrics.");

// Process/server-wide metrics should go into the 'server' entity.
// More complex applications will define other entities.
METRIC_DEFINE_entity(server);

namespace yb {

void RegisterMetricPrototype(const MetricPrototype* prototype);

using std::string;

//
// MetricUnit
//

const char* MetricUnit::Name(Type unit) {
  switch (unit) {
    case kCacheHits:
      return "hits";
    case kCacheQueries:
      return "queries";
    case kBytes:
      return "bytes";
    case kRequests:
      return "requests";
    case kEntries:
      return "entries";
    case kRows:
      return "rows";
    case kCells:
      return "cells";
    case kConnections:
      return "connections";
    case kOperations:
      return "operations";
    case kProbes:
      return "probes";
    case kNanoseconds:
      return "nanoseconds";
    case kMicroseconds:
      return "microseconds";
    case kMilliseconds:
      return "milliseconds";
    case kSeconds:
      return "seconds";
    case kThreads:
      return "threads";
    case kTransactions:
      return "transactions";
    case kUnits:
      return "units";
    case kMaintenanceOperations:
      return "operations";
    case kBlocks:
      return "blocks";
    case kLogBlockContainers:
      return "log block containers";
    case kTasks:
      return "tasks";
    case kMessages:
      return "messages";
    case kContextSwitches:
      return "context switches";
    case kKeys:
      return "keys";
    default:
      return "UNKNOWN UNIT";
  }
}

//
// MetricType
//

const char* const MetricType::kGaugeType = "gauge";
const char* const MetricType::kCounterType = "counter";
const char* const MetricType::kHistogramType = "histogram";
const char* const MetricType::kEventStatsType = "event stats";
const char* MetricType::Name(MetricType::Type type) {
  switch (type) {
    case kGauge:
      return kGaugeType;
    case kCounter:
      return kCounterType;
    case kHistogram:
      return kHistogramType;
    default:
      return "UNKNOWN TYPE";
  }
}

// For prometheus # TYPE.
const char* MetricType::PrometheusType(MetricType::Type type) {
  switch (type) {
    case kGauge: case kLag:
      return kGaugeType;
    case kCounter:
      return kCounterType;
    default:
      LOG(DFATAL) << Format("$0 type can't be exported to prometheus # TYPE",
          Name(type));
      return "UNKNOWN TYPE";
  }
}

namespace {

const char* MetricLevelName(MetricLevel level) {
  switch (level) {
    case MetricLevel::kDebug:
      return "debug";
    case MetricLevel::kInfo:
      return "info";
    case MetricLevel::kWarn:
      return "warn";
    default:
      return "UNKNOWN LEVEL";
  }
}

} // anonymous namespace

//
// MetricRegistry
//

MetricRegistry::MetricRegistry() {
  CHECK_OK(ThreadPoolBuilder("metric cleanup").set_max_threads(1).Build(&metric_cleanup_pool_));
}

MetricRegistry::~MetricRegistry() {
  metric_cleanup_pool_->Shutdown();
}

Status MetricRegistry::SubmitAMetricCleanupTask() const {
  return metric_cleanup_pool_->SubmitClosure(
      Bind(&MetricRegistry::RetireOldMetricsAndCleanupMetricAggregator, Unretained(this)));
}

bool MetricRegistry::TabletHasBeenShutdown(const scoped_refptr<MetricEntity> entity) const {
    if (strcmp(entity->prototype_->name(), "tablet") == 0 && tablets_shutdown_find(entity->id())) {
      DVLOG(5) << "Do not report metrics for shutdown tablet " << entity->id();
      return true;
    }

    return false;
}

Status MetricRegistry::WriteAsJson(JsonWriter* writer,
                                   const MetricJsonOptions& opts) const {
  EntityMap entities;
  {
    std::lock_guard l(lock_);
    entities = entities_;
  }

  writer->StartArray();
  for (const EntityMap::value_type& e : entities) {
    if (TabletHasBeenShutdown(e.second)) {
      continue;
    }

    WARN_NOT_OK(e.second->WriteAsJson(writer, opts),
        Format("Failed to write entity $0 as JSON", e.second->id()));
  }
  writer->EndArray();

  // Rather than having a thread poll metrics periodically to retire old ones,
  // we'll just retire them here. The only downside is that, if no one is polling
  // metrics, we may end up leaving them around indefinitely; however, metrics are
  // small, and one might consider it a feature: if monitoring stops polling for
  // metrics, we should keep them around until the next poll.
  entities.clear(); // necessary to deref metrics we just dumped before doing retirement scan.
  RETURN_NOT_OK(SubmitAMetricCleanupTask());
  return Status::OK();
}

Status MetricRegistry::WriteForPrometheus(PrometheusWriter* writer,
                                          const MetricPrometheusOptions& opts) const {
  EntityMap entities;
  VLOG_TIMING(1, "Handle non-pre-aggregated metrics") {
    // Non-pre-aggregated metrics are processed as follows:
    // - Metrics that need to be aggregated at scrape time are aggregated in this step.
    // - Metrics that do not require aggregation are flushed directly in this step.
    {
      std::lock_guard l(lock_);
      entities = entities_;
    }

    for (const EntityMap::value_type& e : entities) {
      if (TabletHasBeenShutdown(e.second)) {
        continue;
      }

      WARN_NOT_OK(e.second->WriteForPrometheus(writer, opts),
                  Format("Failed to write entity $0 as Prometheus", e.second->id()));
    }
  }

  TEST_PAUSE_IF_FLAG(TEST_pause_flush_aggregated_metrics);

  VLOG_TIMING(1, "flush aggregated metrics") {
    // Pre-aggregated and scrape-time-aggregated metrics are flushed in this step.
    RETURN_NOT_OK(writer->Finish(metrics_aggregator_));
  }

  // Rather than having a thread poll metrics periodically to retire old ones,
  // we'll just retire them here. The only downside is that, if no one is polling
  // metrics, we may end up leaving them around indefinitely; however, metrics are
  // small, and one might consider it a feature: if monitoring stops polling for
  // metrics, we should keep them around until the next poll.
  entities.clear(); // necessary to deref metrics we just dumped before doing retirement scan.
  RETURN_NOT_OK(SubmitAMetricCleanupTask());

  return Status::OK();
}

void MetricRegistry::RetireOldMetricsAndCleanupMetricAggregator() const {
  auto mutable_this = const_cast<MetricRegistry*>(this);
  mutable_this->RetireOldMetrics();
  mutable_this->metrics_aggregator_.CleanupRetiredMetricsAndCorrespondingAttributes();
}

void MetricRegistry::get_all_prototypes(std::set<std::string>& prototypes) const {
  EntityMap entities;
  {
    std::lock_guard l(lock_);
    entities = entities_;
  }
  for (const EntityMap::value_type& e : entities) {
    prototypes.insert(e.second->prototype().name());
  }
}

void MetricRegistry::RetireOldMetrics() {
  std::lock_guard l(lock_);
  for (auto it = entities_.begin(); it != entities_.end();) {
    it->second->RetireOldMetrics();

    if (it->second->num_metrics() == 0 && it->second->HasOneRef()) {
      // No metrics and no external references to this entity, so we can retire it.
      // Unlike retiring the metrics themselves, we don't wait for any timeout
      // to retire them -- we assume that that timed retention has been satisfied
      // by holding onto the metrics inside the entity.

      // For a tablet that has been shutdown, metrics are being deleted. So do not track
      // the tablet anymore.
      if (strcmp(it->second->prototype_->name(), "tablet") == 0) {
        DVLOG(3) << "T " << it->first << ": "
          << "Remove from set of tablets that have been shutdown so as to be freed";
        tablets_shutdown_erase(it->first);
      }

      entities_.erase(it++);
    } else {
      ++it;
    }
  }
}

//
// MetricPrototype
//
MetricPrototype::MetricPrototype(CtorArgs args) : args_(std::move(args)) {
  RegisterMetricPrototype(this);
}

void MetricPrototype::WriteFields(JsonWriter* writer,
                                  const MetricJsonOptions& opts) const {
  writer->String("name");
  writer->String(name());

  if (opts.include_schema_info) {
    writer->String("label");
    writer->String(label());

    writer->String("type");
    writer->String(MetricType::Name(type()));

    writer->String("unit");
    writer->String(MetricUnit::Name(unit()));

    writer->String("description");
    writer->String(description());

    writer->String("level");
    writer->String(MetricLevelName(level()));
  }
}

//
// FunctionGaugeDetacher
//

scoped_refptr<MetricEntity> MetricRegistry::FindOrCreateEntity(
    const MetricEntityPrototype* prototype,
    const std::string& id,
    const MetricEntity::AttributeMap& initial_attributes,
    std::shared_ptr<MemTracker> mem_tracker) {
  std::lock_guard l(lock_);
  scoped_refptr<MetricEntity> e = FindPtrOrNull(entities_, id);
  if (!e) {
    e = new MetricEntity(
        prototype, id, initial_attributes, &metrics_aggregator_, std::move(mem_tracker));
    InsertOrDie(&entities_, id, e);
  } else {
    CHECK_STREQ(e->prototype().name(), prototype->name())
        << "Metric entity " << id << " already exists with type "
        << e->prototype().name() << ", cannot re-instantiate as type "
        << prototype->name();
    e->SetAttributes(initial_attributes);
  }
  return e;
}

//
// Metric
//
Metric::Metric(const MetricPrototype* prototype)
  : prototype_(prototype) {
}

Metric::Metric(std::shared_ptr<MetricPrototype> prototype)
  : prototype_holder_(std::move(prototype)), prototype_(prototype_holder_.get()) {
}

Metric::~Metric() {
}

Status Metric::DoWriteForPrometheus(
    PrometheusWriter* writer,
    const MetricEntity::AttributeMap& attributes,
    AggregationLevels default_aggregation_levels,
    int64_t value) const {
  return writer->WriteSingleEntry(
      attributes, prototype_->name(), value, prototype_->aggregation_function(),
      default_aggregation_levels, prototype_->entity_type(),
      MetricType::PrometheusType(prototype_->type()),
      prototype_->description(), prototype_holder_);
}
//
// Gauge
//

Status Gauge::WriteAsJson(JsonWriter* writer,
                          const MetricJsonOptions& opts) const {
  if (prototype_->level() < opts.level) {
    return Status::OK();
  }

  writer->StartObject();

  prototype_->WriteFields(writer, opts);

  writer->String("value");
  WriteValue(writer);

  writer->EndObject();
  return Status::OK();
}

//
// StringGauge
//

StringGauge::StringGauge(const GaugePrototype<string>* proto,
                         string initial_value)
    : Gauge(proto), value_(std::move(initial_value)) {}

std::string StringGauge::value() const {
  std::lock_guard l(lock_);
  return value_;
}

void StringGauge::set_value(const std::string& value) {
  std::lock_guard l(lock_);
  value_ = value;
}

void StringGauge::WriteValue(JsonWriter* writer) const {
  writer->String(value());
}

Status StringGauge::WriteForPrometheus(
    PrometheusWriter* writer,
    const MetricEntity::AttributeMap& attributes,
    const MetricPrometheusOptions& opts,
    AggregationLevels default_aggregation_levels) const {
  return Status::OK();
}

//
// Counter
//
// This implementation is optimized by using a striped counter. See LongAdder for details.

scoped_refptr<Counter> CounterPrototype::Instantiate(
    const scoped_refptr<MetricEntity>& entity) const {
  return entity->FindOrCreateMetric<Counter>(this);
}

Counter::Counter(const CounterPrototype* proto) : Metric(proto) {
}

Counter::Counter(std::shared_ptr<CounterPrototype> proto) : Metric(std::move(proto)) {
}

int64_t Counter::value() const {
  return value_.Value();
}

void Counter::Increment() {
  IncrementBy(1);
}

void Counter::IncrementBy(int64_t amount) {
  value_.IncrementBy(amount);

  if (aggregated_prometheus_value_holder_ != nullptr) {
    aggregated_prometheus_value_holder_->IncrementBy(amount);
  }
}

Status Counter::WriteAsJson(JsonWriter* writer,
                            const MetricJsonOptions& opts) const {
  if (prototype_->level() < opts.level) {
    return Status::OK();
  }

  writer->StartObject();

  prototype_->WriteFields(writer, opts);

  writer->String("value");
  writer->Int64(value());

  writer->EndObject();
  return Status::OK();
}

Status Counter::WriteForPrometheus(
    PrometheusWriter* writer,
    const MetricEntity::AttributeMap& attributes,
    const MetricPrometheusOptions& opts,
    AggregationLevels default_aggregation_levels) const {
  if (IsPreAggregated() || prototype_->level() < opts.level) {
    return Status::OK();
  }

  return DoWriteForPrometheus(writer, attributes, default_aggregation_levels, value());
}

Status Counter::SetUpPreAggregationForPrometheus(
    MetricsAggregator* metrics_aggregator,
    const MetricEntity::AttributeMap& attributes,
    AggregationLevels default_aggregation_levels,
    const std::string& aggregation_id) {
  if (!metrics_aggregator->IsPreAggregationSupported(prototype_, default_aggregation_levels)) {
    return Status::OK();
  }

  aggregated_prometheus_value_holder_ =
      metrics_aggregator->CreateOrFindPreAggregatedMetricValueHolder(
          attributes, prototype_holder_, prototype_->name(),
          default_aggregation_levels, prototype_->entity_type(), aggregation_id,
          MetricType::PrometheusType(prototype_->type()), prototype_->description());

  aggregated_prometheus_value_holder_->IncrementBy(value());

  return Status::OK();
}

bool Counter::IsPreAggregated() const {
  return aggregated_prometheus_value_holder_ != nullptr;
}

//
// MillisLag
//

scoped_refptr<MillisLag> MillisLagPrototype::Instantiate(
    const scoped_refptr<MetricEntity>& entity) const {
  return entity->FindOrCreateMetric<MillisLag>(this);
}

MillisLag::MillisLag(const MillisLagPrototype* proto)
  : Metric(proto),
    timestamp_ms_(static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count())) {
}

Status MillisLag::WriteAsJson(JsonWriter* writer, const MetricJsonOptions& opts) const {
  if (prototype_->level() < opts.level) {
    return Status::OK();
  }

  writer->StartObject();

  prototype_->WriteFields(writer, opts);

  writer->String("value");
  writer->Int64(lag_ms());

  writer->EndObject();
  return Status::OK();
}

Status MillisLag::WriteForPrometheus(
    PrometheusWriter* writer,
    const MetricEntity::AttributeMap& attributes,
    const MetricPrometheusOptions& opts,
    AggregationLevels default_aggregation_levels) const {
  if (prototype_->level() < opts.level) {
    return Status::OK();
  }

  return DoWriteForPrometheus(writer, attributes, default_aggregation_levels, lag_ms());
}

AtomicMillisLag::AtomicMillisLag(const MillisLagPrototype* proto)
  : MillisLag(proto),
    atomic_timestamp_ms_(static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count())) {
}

Status AtomicMillisLag::WriteAsJson(JsonWriter* writer, const MetricJsonOptions& opts) const {
  if (prototype_->level() < opts.level) {
    return Status::OK();
  }

  writer->StartObject();

  prototype_->WriteFields(writer, opts);

  writer->String("value");
  writer->Int64(this->lag_ms());

  writer->EndObject();
  return Status::OK();
}

/////////////////////////////////////////////////
// Lane cells
/////////////////////////////////////////////////

namespace metrics_internal {

class LaneValueCells {
 public:
  explicit LaneValueCells(size_t num_lanes) {
    cells_.Allocate(num_lanes);
  }

  const LaneSharded<std::atomic<int64_t>>& cells() const { return cells_; }

 private:
  LaneSharded<std::atomic<int64_t>> cells_;
};

class LaneStatsCells {
 public:
  explicit LaneStatsCells(size_t num_lanes) {
    cells_.Allocate(num_lanes);
  }

  const LaneSharded<AggregateStats>& cells() const { return cells_; }

 private:
  LaneSharded<AggregateStats> cells_;
};

// A lane's histogram is created when a thread of the lane first records, so methods that are
// never called from a lane cost only the pointer array.
class LaneHistogramCells {
 public:
  LaneHistogramCells(uint64_t highest_trackable_value, int num_significant_digits, size_t num_lanes)
      : highest_trackable_value_(highest_trackable_value),
        num_significant_digits_(num_significant_digits),
        num_lanes_(num_lanes),
        cells_(new std::atomic<Cell*>[num_lanes]()) {}

  ~LaneHistogramCells() {
    for (size_t i = 0; i != num_lanes_; ++i) {
      delete cells_[i].load(std::memory_order_acquire);
    }
  }

  HdrHistogram* LaneLocal() {
    if (!LaneModeActive()) {
      return nullptr;
    }
    const auto lane = ThisThreadLane();
    if (lane >= num_lanes_) {
      return nullptr;
    }
    auto& entry = cells_[lane];
    auto* cell = entry.load(std::memory_order_acquire);
    if (!cell) {
      auto* created = new Cell(highest_trackable_value_, num_significant_digits_);
      if (entry.compare_exchange_strong(cell, created, std::memory_order_acq_rel)) {
        cell = created;
      } else {
        delete created;
      }
    }
    return &cell->histogram;
  }

  template <class F>
  void ForEach(const F& f) const {
    for (size_t i = 0; i != num_lanes_; ++i) {
      if (auto* cell = cells_[i].load(std::memory_order_acquire)) {
        f(cell->histogram);
      }
    }
  }

 private:
  struct alignas(CACHELINE_SIZE) Cell {
    Cell(uint64_t highest_trackable_value, int num_significant_digits)
        : histogram(highest_trackable_value, num_significant_digits) {}

    HdrHistogram histogram;
  };

  const uint64_t highest_trackable_value_;
  const int num_significant_digits_;
  const size_t num_lanes_;
  const std::unique_ptr<std::atomic<Cell*>[]> cells_;
};

bool LaneCellsAllowed(const MetricPrototype* prototype) {
  return strcmp(prototype->entity_type(), "server") == 0;
}

LaneValueCells* NewLaneValueCells() {
  return LaneModeActive() ? new LaneValueCells(GetLaneTopology().num_lanes()) : nullptr;
}

void DeleteLaneValueCells(LaneValueCells* cells) {
  delete cells;
}

bool AddToLaneValueCell(LaneValueCells* cells, int64_t amount) {
  auto* cell = cells->cells().LaneLocal();
  if (!cell) {
    return false;
  }
  cell->fetch_add(amount, std::memory_order_relaxed);
  return true;
}

int64_t SumLaneValueCells(const LaneValueCells* cells) {
  int64_t result = 0;
  cells->cells().ForEach([&result](std::atomic<int64_t>& cell) {
    result += cell.load(std::memory_order_relaxed);
  });
  return result;
}

int64_t DrainLaneValueCells(LaneValueCells* cells) {
  int64_t result = 0;
  cells->cells().ForEach([&result](std::atomic<int64_t>& cell) {
    result += cell.exchange(0, std::memory_order_relaxed);
  });
  return result;
}

bool AddToLaneStatsCell(LaneStatsCells* cells, int64_t value, uint64_t count) {
  auto* cell = cells->cells().LaneLocal();
  if (!cell) {
    return false;
  }
  cell->IncrementBy(value, count);
  return true;
}

bool AddToLaneHistogramCell(LaneHistogramCells* cells, int64_t value, int64_t count) {
  auto* cell = cells->LaneLocal();
  if (!cell) {
    return false;
  }
  cell->IncrementBy(value, count);
  return true;
}

} // namespace metrics_internal

namespace {

template <class F>
void ForEachLaneCell(const metrics_internal::LaneHistogramCells* cells, const F& f) {
  if (cells) {
    cells->ForEach(f);
  }
}

template <class F>
void ForEachLaneCell(const metrics_internal::LaneStatsCells* cells, const F& f) {
  if (cells) {
    cells->cells().ForEach(f);
  }
}

template <class Cells, class Create>
void InstallLaneCells(
    const MetricPrototype* prototype, std::atomic<Cells*>* holder, const Create& create) {
  if (holder->load(std::memory_order_acquire) || !LaneModeActive() ||
      !metrics_internal::LaneCellsAllowed(prototype)) {
    return;
  }
  auto cells = create(GetLaneTopology().num_lanes());
  Cells* expected = nullptr;
  if (holder->compare_exchange_strong(expected, cells.get(), std::memory_order_acq_rel)) {
    cells.release();
  }
}

} // namespace

/////////////////////////////////////////////////
// HistogramPrototype
/////////////////////////////////////////////////

HistogramPrototype::HistogramPrototype(const MetricPrototype::CtorArgs& args,
                                       uint64_t max_trackable_value, int num_sig_digits)
  : MetricPrototype(args),
    max_trackable_value_(max_trackable_value),
    num_sig_digits_(num_sig_digits) {
  // Better to crash at definition time that at instantiation time.
  CHECK(HdrHistogram::IsValidHighestTrackableValue(max_trackable_value))
      << Format("Invalid max trackable value on histogram $0: $1",
                args.name_, max_trackable_value);
  CHECK(HdrHistogram::IsValidNumSignificantDigits(num_sig_digits))
      << Format("Invalid number of significant digits on histogram $0: $1",
                args.name_, num_sig_digits);
}

scoped_refptr<Histogram> HistogramPrototype::Instantiate(
    const scoped_refptr<MetricEntity>& entity) const {
  return entity->FindOrCreateMetric<Histogram>(this);
}

/////////////////////////////////////////////////
// EventStatsPrototype
/////////////////////////////////////////////////

EventStatsPrototype::EventStatsPrototype(
    const MetricPrototype::CtorArgs& args)
  : MetricPrototype(args) {
}

scoped_refptr<EventStats> EventStatsPrototype::Instantiate(
    const scoped_refptr<MetricEntity>& entity) const {
  return entity->FindOrCreateMetric<EventStats>(this);
}

/////////////////////////////////////////////////
// BaseStats
/////////////////////////////////////////////////

template<typename Stats>
void BaseStats<Stats>::Reset() const {
  auto derived = const_cast<Stats*>(static_cast<const Stats*>(this));
  derived->mutable_underlying()->Reset();
  ForEachLaneCell(derived->lane_cells_.load(std::memory_order_acquire), [](auto& cell) {
    cell.Reset();
  });
}

template<typename Stats>
uint64_t BaseStats<Stats>::TotalCount() const {
  const auto* stats = AsConstStats();
  uint64_t result = stats->underlying()->TotalCount();
  ForEachLaneCell(stats->lane_cells_.load(std::memory_order_acquire), [&result](auto& cell) {
    result += cell.TotalCount();
  });
  return result;
}

template<typename Stats>
uint64_t BaseStats<Stats>::TotalSum() const {
  const auto* stats = AsConstStats();
  uint64_t result = stats->underlying()->TotalSum();
  ForEachLaneCell(stats->lane_cells_.load(std::memory_order_acquire), [&result](auto& cell) {
    result += cell.TotalSum();
  });
  return result;
}

template<typename Stats>
uint64_t BaseStats<Stats>::MinValue() const {
  const auto* stats = AsConstStats();
  const auto* cells = stats->lane_cells_.load(std::memory_order_acquire);
  if (!cells) {
    return stats->underlying()->MinValue();
  }
  // Only cells that recorded since the last reset have a minimum.
  using Value = decltype(stats->underlying()->MinValue());
  std::optional<Value> result;
  auto add = [&result](const auto& cell) {
    if (cell.CurrentCount() != 0) {
      const Value value = cell.MinValue();
      result = result ? std::min(*result, value) : value;
    }
  };
  add(*stats->underlying());
  ForEachLaneCell(cells, add);
  return result ? *result : 0;
}

template<typename Stats>
uint64_t BaseStats<Stats>::MaxValue() const {
  const auto* stats = AsConstStats();
  const auto* cells = stats->lane_cells_.load(std::memory_order_acquire);
  if (!cells) {
    return stats->underlying()->MaxValue();
  }
  using Value = decltype(stats->underlying()->MaxValue());
  std::optional<Value> result;
  auto add = [&result](const auto& cell) {
    if (cell.CurrentCount() != 0) {
      const Value value = cell.MaxValue();
      result = result ? std::max(*result, value) : value;
    }
  };
  add(*stats->underlying());
  ForEachLaneCell(cells, add);
  return result ? *result : 0;
}

template<typename Stats>
double BaseStats<Stats>::MeanValue() const {
  const auto* stats = AsConstStats();
  const auto* cells = stats->lane_cells_.load(std::memory_order_acquire);
  if (!cells) {
    return stats->underlying()->MeanValue();
  }
  auto sum = stats->underlying()->CurrentSum();
  auto count = stats->underlying()->CurrentCount();
  ForEachLaneCell(cells, [&sum, &count](const auto& cell) {
    sum += cell.CurrentSum();
    count += cell.CurrentCount();
  });
  return count == 0 ? 0.0 : static_cast<double>(sum) / count;
}

template<typename Stats>
Status BaseStats<Stats>::WriteAsJson(
    JsonWriter* writer, const MetricJsonOptions& opts) const {
  if (prototype_->level() < opts.level) {
    return Status::OK();
  }

  HistogramSnapshotPB snapshot;
  RETURN_NOT_OK(AsConstStats()->GetAndResetHistogramSnapshotPB(
      &snapshot, opts));
  writer->Protobuf(snapshot);
  return Status::OK();
}

template<typename Stats>
Status BaseStats<Stats>::WriteForPrometheus(
    PrometheusWriter* writer,
    const MetricEntity::AttributeMap& attributes,
    const MetricPrometheusOptions& opts,
    AggregationLevels default_aggregation_levels) const {
  if (AsConstStats()->IsPreAggregated() || prototype_->level() < opts.level) {
    return Status::OK();
  }

  // Representing the sum and count require suffixed names.
  std::string histogram_name = prototype_->name();
  const char* description = prototype_->description();
  const char* counter_type = MetricType::PrometheusType(MetricType::kCounter);
  auto copy_of_attributes = attributes;
  // For #HELP and #TYPE, we need to print them for each entry, since our
  // histogram doesn't really get exported as histograms.
  RETURN_NOT_OK(writer->WriteSingleEntry(
        copy_of_attributes, histogram_name + "_sum", TotalSum(),
        prototype()->aggregation_function(), default_aggregation_levels, prototype_->entity_type(),
        counter_type, description, prototype_holder_));
  RETURN_NOT_OK(writer->WriteSingleEntry(
        copy_of_attributes, histogram_name + "_count", TotalCount(),
        prototype()->aggregation_function(), default_aggregation_levels, prototype_->entity_type(),
        counter_type, description, prototype_holder_));

  if (FLAGS_expose_metric_histogram_percentiles) {
    RETURN_NOT_OK(AsConstStats()->WritePercentilesForPrometheus(
            writer, std::move(copy_of_attributes), default_aggregation_levels));
  }

  // HdrHistogram reports percentiles based on all the data points from the
  // begining of time. We are interested in the percentiles based on just
  // the "newly-arrived" data. So, in the defualt setting, we will reset
  // the histogram's percentiles between each invocation. User also has the
  // option to set the url parameter reset_histograms=false
  if (opts.reset_histograms) {
    Reset();
  }

  return Status::OK();
}

template<typename Stats>
Status BaseStats<Stats>::GetAndResetHistogramSnapshotPB(
    HistogramSnapshotPB* snapshot_pb, const MetricJsonOptions& opts) const {
  snapshot_pb->set_name(prototype_->name());
  if (opts.include_schema_info) {
    snapshot_pb->set_type(MetricType::Name(prototype_->type()));
    snapshot_pb->set_label(prototype_->label());
    snapshot_pb->set_unit(MetricUnit::Name(prototype_->unit()));
    snapshot_pb->set_description(prototype_->description());
    snapshot_pb->set_level(MetricLevelName(prototype_->level()));
  }
  snapshot_pb->set_total_count(TotalCount());
  snapshot_pb->set_total_sum(TotalSum());
  snapshot_pb->set_min(MinValue());
  snapshot_pb->set_mean(MeanValue());
  snapshot_pb->set_max(MaxValue());

  // HdrHistogram reports percentiles based on all the data points from the
  // begining of time. We are interested in the percentiles based on just
  // the "newly-arrived" data. So, in the defualt setting, we will reset
  // the histogram's percentiles between each invocation. User also has the
  // option to set the url parameter reset_histograms=false
  if (opts.reset_histograms) {
    Reset();
  }

  return Status::OK();
}

template<typename Stats>
Status BaseStats<Stats>::SetUpPreAggregationForPrometheus(
    MetricsAggregator* metrics_aggregator,
    const MetricEntity::AttributeMap& attributes,
    AggregationLevels default_aggregation_levels,
    const std::string& aggregation_id) {
  return AsStats()->SetUpPreAggregationForPrometheus(
      metrics_aggregator, attributes, default_aggregation_levels, aggregation_id);
}

template class BaseStats<Histogram>;
template class BaseStats<EventStats>;

/////////////////////////////////////////////////
// Histogram
/////////////////////////////////////////////////

Histogram::Histogram(const HistogramPrototype* proto)
  : BaseStats<Histogram>(proto),
    histogram_(new HdrHistogram(proto->max_trackable_value(), proto->num_sig_digits())) {
}

Histogram::Histogram(std::shared_ptr<HistogramPrototype> proto)
  : BaseStats<Histogram>(std::move(proto)),
    histogram_(new HdrHistogram(
        down_cast<const HistogramPrototype*>(prototype_)->max_trackable_value(),
        down_cast<const HistogramPrototype*>(prototype_)->num_sig_digits())) {
}

Histogram::~Histogram() {
  delete lane_cells_.load(std::memory_order_acquire);
}

void Histogram::EnableLaneCells() {
  InstallLaneCells(prototype_, &lane_cells_, [this](size_t num_lanes) {
    return std::make_unique<metrics_internal::LaneHistogramCells>(
        histogram_->highest_trackable_value(), histogram_->num_significant_digits(), num_lanes);
  });
}

uint64_t Histogram::CountInBucketForValueForTests(uint64_t value) const {
  auto result = histogram_->CountInBucketForValue(value);
  ForEachLaneCell(lane_cells_.load(std::memory_order_acquire), [&result, value](auto& cell) {
    result += cell.CountInBucketForValue(value);
  });
  return result;
}

Status Histogram::SetUpPreAggregationForPrometheus(
    MetricsAggregator* metrics_aggregator,
    const MetricEntity::AttributeMap& attributes,
    AggregationLevels default_aggregation_levels,
    const std::string& aggregation_id) {
  return Status::OK();
}

Status Histogram::WritePercentilesForPrometheus(
    PrometheusWriter* writer,
    MetricEntity::AttributeMap attributes,
    AggregationLevels default_aggregation_levels) const {
  std::string histogram_name = prototype_->name();
  const char* description = prototype_->description();
  const char* gauge_type = MetricType::PrometheusType(MetricType::kGauge);

  auto WriteQuantile = [&](const std::string& quantile_name, double value) -> yb::Status {
    attributes["quantile"] = quantile_name;
    return writer->WriteSingleEntry(
        attributes, histogram_name, value, prototype_->aggregation_function(),
        default_aggregation_levels, prototype_->entity_type(), gauge_type,
        description, prototype_holder_);
  };

  const HdrHistogram* histogram = underlying();
  std::optional<HdrHistogram> merged;
  if (const auto* cells = lane_cells_.load(std::memory_order_acquire)) {
    merged.emplace(*histogram_);
    ForEachLaneCell(cells, [&merged](const HdrHistogram& cell) {
      merged->MergeFrom(cell);
    });
    histogram = &*merged;
  }
  RETURN_NOT_OK(WriteQuantile("p50", histogram->ValueAtPercentile(50)));
  RETURN_NOT_OK(WriteQuantile("p95", histogram->ValueAtPercentile(95)));
  RETURN_NOT_OK(WriteQuantile("p99", histogram->ValueAtPercentile(99)));
  RETURN_NOT_OK(WriteQuantile("mean", histogram->MeanValue()));
  RETURN_NOT_OK(WriteQuantile("max", histogram->MaxValue()));
  RETURN_NOT_OK(WriteQuantile("min", histogram->MinValue()));
  return Status::OK();
}

Status Histogram::GetAndResetHistogramSnapshotPB(
    HistogramSnapshotPB* snapshot_pb, const MetricJsonOptions& opts) const {
  HdrHistogram snapshot(*histogram_);
  ForEachLaneCell(lane_cells_.load(std::memory_order_acquire), [&snapshot](const auto& cell) {
    snapshot.MergeFrom(cell);
  });
  if (opts.include_schema_info) {
    snapshot_pb->set_max_trackable_value(snapshot.highest_trackable_value());
    snapshot_pb->set_num_significant_digits(snapshot.num_significant_digits());
  }
  snapshot_pb->set_percentile_75(snapshot.ValueAtPercentile(75));
  snapshot_pb->set_percentile_95(snapshot.ValueAtPercentile(95));
  snapshot_pb->set_percentile_99(snapshot.ValueAtPercentile(99));
  snapshot_pb->set_percentile_99_9(snapshot.ValueAtPercentile(99.9));
  snapshot_pb->set_percentile_99_99(snapshot.ValueAtPercentile(99.99));

  if (opts.include_raw_histograms) {
    RecordedValuesIterator iter(&snapshot);
    while (iter.HasNext()) {
      HistogramIterationValue value;
      RETURN_NOT_OK(iter.Next(&value));
      snapshot_pb->add_values(value.value_iterated_to);
      snapshot_pb->add_counts(value.count_at_value_iterated_to);
    }
  }
  return BaseStats<Histogram>::GetAndResetHistogramSnapshotPB(snapshot_pb, opts);
}

/////////////////////////////////////////////////
// EventStats
/////////////////////////////////////////////////

EventStats::EventStats(const EventStatsPrototype* proto)
  : BaseStats<EventStats>(proto),
    stats_(new AggregateStats()) {
}

EventStats::EventStats(std::shared_ptr<EventStatsPrototype> proto)
  : BaseStats<EventStats>(std::move(proto)),
    stats_(new AggregateStats()) {
}

Status EventStats::WritePercentilesForPrometheus(
    PrometheusWriter* writer,
    MetricEntity::AttributeMap attributes,
    AggregationLevels default_aggregation_levels) const {
  return Status::OK();
}

Status EventStats::SetUpPreAggregationForPrometheus(
    MetricsAggregator* metrics_aggregator,
    const MetricEntity::AttributeMap& attributes,
    AggregationLevels default_aggregation_levels,
    const std::string& aggregation_id) {
  if (!metrics_aggregator->IsPreAggregationSupported(prototype_, default_aggregation_levels)) {
    return Status::OK();
  }

  std::string event_stats_name = prototype_->name();
  const char* description = prototype_->description();
  const char* counter_type = MetricType::PrometheusType(MetricType::kCounter);

  auto aggregated_prometheus_sum_value_holder =
      metrics_aggregator->CreateOrFindPreAggregatedMetricValueHolder(
          attributes, prototype_holder_, event_stats_name + "_sum", default_aggregation_levels,
          prototype_->entity_type(), aggregation_id, counter_type, description);

  auto aggregated_prometheus_count_value_holder =
      metrics_aggregator->CreateOrFindPreAggregatedMetricValueHolder(
          attributes, prototype_holder_, event_stats_name + "_count", default_aggregation_levels,
          prototype_->entity_type(), aggregation_id, counter_type, description);

  // Let stats_ maintain the pre-aggregated sum and count values for prometheus.
  return stats_->SetUpPreAggregationForPrometheus(
      aggregated_prometheus_sum_value_holder, aggregated_prometheus_count_value_holder);
}

EventStats::~EventStats() {
  delete lane_cells_.load(std::memory_order_acquire);
}

void EventStats::EnableLaneCells() {
  InstallLaneCells(prototype_, &lane_cells_, [](size_t num_lanes) {
    return std::make_unique<metrics_internal::LaneStatsCells>(num_lanes);
  });
}

/////////////////////////////////////////////////
// ScopedLatencyMetric
/////////////////////////////////////////////////

template<typename Stats>
ScopedLatencyMetric<Stats>::ScopedLatencyMetric(
    const scoped_refptr<Stats>& latency_stats, Auto automatic)
    : latency_stats_(latency_stats), auto_(automatic) {
  Restart();
}

template<typename Stats>
ScopedLatencyMetric<Stats>::ScopedLatencyMetric(ScopedLatencyMetric&& rhs)
    : latency_stats_(std::move(rhs.latency_stats_)), time_started_(rhs.time_started_),
      auto_(rhs.auto_) {
}

template<typename Stats>
void ScopedLatencyMetric<Stats>::operator=(ScopedLatencyMetric&& rhs) {
  if (auto_) {
    Finish();
  }

  latency_stats_ = std::move(rhs.latency_stats_);
  time_started_ = rhs.time_started_;
  auto_ = rhs.auto_;
}

template<typename Stats>
ScopedLatencyMetric<Stats>::~ScopedLatencyMetric() {
  if (auto_) {
    Finish();
  }
}

template<typename Stats>
void ScopedLatencyMetric<Stats>::Restart() {
  if (latency_stats_) {
    time_started_ = MonoTime::Now();
  }
}

template<typename Stats>
void ScopedLatencyMetric<Stats>::Finish() {
  if (latency_stats_ != nullptr) {
    auto passed = (MonoTime::Now() - time_started_).ToMicroseconds();
    latency_stats_->Increment(passed);
  }
}

template class ScopedLatencyMetric<Histogram>;
template class ScopedLatencyMetric<EventStats>;

// Replace specific chars with underscore to pass PrometheusNameRegex().
void EscapeMetricNameForPrometheus(std::string *id) {
  std::replace(id->begin(), id->end(), ' ', '_');
  std::replace(id->begin(), id->end(), '.', '_');
  std::replace(id->begin(), id->end(), '-', '_');
}

} // namespace yb
