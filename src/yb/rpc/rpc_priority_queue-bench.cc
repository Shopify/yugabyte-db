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

// Latency benchmark for RpcPriorityQueue ordering, isolated from networking.
//
// Three arms share one worker pool configuration (same worker count, permit budget and callback
// reserve), so the only variable is how waiting work is ordered:
//   direct    tasks go straight to the pool (its own FIFO); the no-queue reference
//   fifo      tasks go through an RpcPriorityQueue but all at kNormal, so the queue is a FIFO
//   priority  same queue, probe tasks at kHigh and bulk tasks at kNormal
//
// Two scenarios:
//   unloaded   one task at a time: measures the queue's fast-path overhead (Enqueue call cost and
//              submit-to-start latency) against the direct pool
//   contended  closed-loop kNormal bulk saturating the pool, plus an open-loop probe stream:
//              measures submit-to-start latency of probe and bulk work, and bulk throughput

#include <algorithm>
#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "yb/rpc/rpc_priority_queue.h"
#include "yb/rpc/thread_pool.h"

#include "yb/gutil/stringprintf.h"

#include "yb/util/countdown_latch.h"
#include "yb/util/flags.h"
#include "yb/util/format.h"
#include "yb/util/logging.h"
#include "yb/util/monotime.h"
#include "yb/util/status.h"
#include "yb/util/test_util.h"

DEFINE_NON_RUNTIME_int32(bench_workers, 8,
    "Pool workers; also the queue's permit budget.");
DEFINE_NON_RUNTIME_int32(bench_callback_reserve, 0,
    "Permits reserved for callbacks (reduces the inbound cap in both queue arms).");
DEFINE_NON_RUNTIME_int32(bench_service_time_us, 200,
    "Busy-spin service time of every task.");
DEFINE_NON_RUNTIME_int32(bench_bulk_concurrency, 64,
    "Closed-loop bulk tasks kept outstanding in the contended scenario.");
DEFINE_NON_RUNTIME_int32(bench_probe_interval_us, 1000,
    "Open-loop probe inter-arrival time in the contended scenario.");
DEFINE_NON_RUNTIME_int32(bench_warmup_ms, 300,
    "Contended scenario: time excluded from measurement after start.");
DEFINE_NON_RUNTIME_int32(bench_duration_ms, 2000,
    "Contended scenario: measured duration after warmup.");
DEFINE_NON_RUNTIME_int32(bench_unloaded_iterations, 20000,
    "Unloaded scenario: sequential task submissions per arm.");

using namespace std::literals;

namespace yb::rpc {

namespace {

enum class Arm { kDirect, kFifo, kPriority };

const char* ArmName(Arm arm) {
  switch (arm) {
    case Arm::kDirect: return "direct";
    case Arm::kFifo: return "fifo";
    case Arm::kPriority: return "priority";
  }
  return "?";
}

struct Percentiles {
  size_t count = 0;
  int64_t p50 = 0;
  int64_t p90 = 0;
  int64_t p99 = 0;
  int64_t p999 = 0;
  int64_t max = 0;
  double mean = 0;
};

Percentiles Summarize(std::vector<int64_t> samples) {
  Percentiles result;
  if (samples.empty()) {
    return result;
  }
  std::sort(samples.begin(), samples.end());
  auto at = [&samples](double p) {
    const auto index = std::min(
        samples.size() - 1, static_cast<size_t>(p * static_cast<double>(samples.size())));
    return samples[index];
  };
  result.count = samples.size();
  result.p50 = at(0.50);
  result.p90 = at(0.90);
  result.p99 = at(0.99);
  result.p999 = at(0.999);
  result.max = samples.back();
  int64_t sum = 0;
  for (auto v : samples) {
    sum += v;
  }
  result.mean = static_cast<double>(sum) / static_cast<double>(samples.size());
  return result;
}

std::string Describe(const Percentiles& p) {
  auto us = [](double ns) { return StringPrintf("%.1f", ns / 1000.0); };
  return Format(
      "n=$0 mean=$1us p50=$2us p90=$3us p99=$4us p99.9=$5us max=$6us",
      p.count, us(p.mean), us(p.p50), us(p.p90), us(p.p99), us(p.p999), us(p.max));
}

void Spin(MonoDelta duration) {
  const auto deadline = MonoTime::Now() + duration;
  while (MonoTime::Now() < deadline) {
  }
}

// Submits a task either straight to the pool or through the queue, depending on the arm.
class Submitter {
 public:
  Submitter(Arm arm, ThreadPoolPtr pool, RpcPriorityQueue* queue)
      : arm_(arm), pool_(std::move(pool)), queue_(queue) {}

  // Enqueue + the time the caller spent inside it.
  bool Submit(ThreadPoolTask* task, RpcPriority priority, int64_t* enqueue_ns) {
    if (arm_ == Arm::kFifo) {
      priority = RpcPriority::kNormal;
    }
    const auto before = MonoTime::Now();
    bool ok;
    if (arm_ == Arm::kDirect) {
      ok = pool_->Enqueue(task);
    } else {
      ok = queue_->Enqueue(task, priority, RpcTaskClass::kInbound, pool_);
    }
    if (enqueue_ns != nullptr) {
      *enqueue_ns = (MonoTime::Now() - before).ToNanoseconds();
    }
    return ok;
  }

 private:
  const Arm arm_;
  const ThreadPoolPtr pool_;
  RpcPriorityQueue* const queue_;
};

// One contended-scenario bulk slot: resubmits itself from Done() so the pool stays saturated
// without extra producer threads. Resubmission happens before the queue releases this task's
// permit, so in the queue arms the new task always registers as waiting and receives a permit via
// hand-off; that is the saturated steady state being measured.
class BulkTask : public ThreadPoolTask {
 public:
  BulkTask(Submitter* submitter, MonoDelta service_time, std::atomic<bool>* stop,
           CountDownLatch* retired)
      : submitter_(submitter), service_time_(service_time), stop_(stop), retired_(retired) {
    waits_.reserve(1 << 16);
  }

  void Start() {
    submitted_at_ = MonoTime::Now();
    submitter_->Submit(this, RpcPriority::kNormal, nullptr);
  }

  void Run() override {
    const auto started = MonoTime::Now();
    waits_.push_back({started, (started - submitted_at_).ToNanoseconds()});
    Spin(service_time_);
  }

  void Done(const Status& status) override {
    if (!status.ok() || stop_->load(std::memory_order_acquire)) {
      retired_->CountDown();
      return;
    }
    Start();
  }

  struct Sample {
    MonoTime started;
    int64_t wait_ns;
  };
  const std::vector<Sample>& waits() const { return waits_; }

 private:
  Submitter* const submitter_;
  const MonoDelta service_time_;
  std::atomic<bool>* const stop_;
  CountDownLatch* const retired_;
  MonoTime submitted_at_;
  std::vector<Sample> waits_;
};

// Fire-and-forget probe. Records its own submit-to-start wait; the array of probes is read after
// all have completed.
class ProbeTask : public ThreadPoolTask {
 public:
  explicit ProbeTask(MonoDelta service_time) : service_time_(service_time) {}

  void MarkSubmitted() { submitted_at_ = MonoTime::Now(); }

  void Run() override {
    started_at_ = MonoTime::Now();
    Spin(service_time_);
  }

  void Done(const Status& status) override {
    done_ = true;
    status_ = status;
    completed_.CountDown();
  }

  bool WaitDone(MonoDelta timeout) { return completed_.WaitFor(timeout); }
  bool ok() const { return done_ && status_.ok(); }
  MonoTime submitted_at() const { return submitted_at_; }
  int64_t wait_ns() const { return (started_at_ - submitted_at_).ToNanoseconds(); }

 private:
  const MonoDelta service_time_;
  MonoTime submitted_at_;
  MonoTime started_at_;
  bool done_ = false;
  Status status_;
  CountDownLatch completed_{1};
};

class RpcPriorityQueueBench : public YBTest {
 protected:
  struct Fixture {
    ThreadPoolPtr pool;
    std::unique_ptr<RpcPriorityQueue> queue;
    std::unique_ptr<Submitter> submitter;

    ~Fixture() {
      if (queue) {
        queue->StartShutdown();
        queue->CompleteShutdown();
      }
      pool->Shutdown();
    }
  };

  std::unique_ptr<Fixture> MakeFixture(Arm arm) {
    auto fixture = std::make_unique<Fixture>();
    const auto workers = static_cast<size_t>(FLAGS_bench_workers);
    fixture->pool = std::make_shared<ThreadPool>(ThreadPoolOptions {
      .name = Format("bench_$0", ArmName(arm)),
      .max_workers = workers,
      // Pre-spawned so idle-worker creation does not enter the latency samples.
      .min_workers = workers,
    });
    if (arm != Arm::kDirect) {
      fixture->queue = std::make_unique<RpcPriorityQueue>(
          Format("bench_$0", ArmName(arm)), workers,
          static_cast<size_t>(FLAGS_bench_callback_reserve), /* metric_entity= */ nullptr);
    }
    fixture->submitter = std::make_unique<Submitter>(arm, fixture->pool, fixture->queue.get());
    return fixture;
  }

  MonoDelta ServiceTime() const {
    return MonoDelta::FromMicroseconds(FLAGS_bench_service_time_us);
  }

  void RunUnloaded(Arm arm) {
    auto fixture = MakeFixture(arm);
    const auto iterations = static_cast<size_t>(FLAGS_bench_unloaded_iterations);
    std::vector<int64_t> enqueue_ns;
    std::vector<int64_t> wait_ns;
    enqueue_ns.reserve(iterations);
    wait_ns.reserve(iterations);
    // Unmeasured warmup so page faults and worker wake-up paths are hot.
    const size_t warmup = std::min<size_t>(iterations / 10, 1000);
    for (size_t i = 0; i < iterations + warmup; ++i) {
      ProbeTask task(MonoDelta::kZero);
      int64_t enqueue = 0;
      task.MarkSubmitted();
      ASSERT_TRUE(fixture->submitter->Submit(&task, RpcPriority::kHigh, &enqueue));
      ASSERT_TRUE(task.WaitDone(10s));
      ASSERT_TRUE(task.ok());
      if (i >= warmup) {
        enqueue_ns.push_back(enqueue);
        wait_ns.push_back(task.wait_ns());
      }
    }
    LOG(INFO) << "BENCH unloaded arm=" << ArmName(arm) << " enqueue_call "
              << Describe(Summarize(std::move(enqueue_ns)));
    LOG(INFO) << "BENCH unloaded arm=" << ArmName(arm) << " submit_to_start "
              << Describe(Summarize(std::move(wait_ns)));
  }

  void RunContended(Arm arm) {
    auto fixture = MakeFixture(arm);
    const auto service_time = ServiceTime();
    const auto bulk_concurrency = static_cast<size_t>(FLAGS_bench_bulk_concurrency);
    const auto warmup = MonoDelta::FromMilliseconds(FLAGS_bench_warmup_ms);
    const auto duration = MonoDelta::FromMilliseconds(FLAGS_bench_duration_ms);
    const auto probe_interval = MonoDelta::FromMicroseconds(FLAGS_bench_probe_interval_us);

    std::atomic<bool> stop{false};
    CountDownLatch bulk_retired(bulk_concurrency);
    std::vector<std::unique_ptr<BulkTask>> bulk;
    bulk.reserve(bulk_concurrency);
    for (size_t i = 0; i < bulk_concurrency; ++i) {
      bulk.push_back(std::make_unique<BulkTask>(
          fixture->submitter.get(), service_time, &stop, &bulk_retired));
    }

    const size_t probe_count =
        static_cast<size_t>((warmup + duration).ToMicroseconds() / probe_interval.ToMicroseconds())
        + 1;
    std::vector<std::unique_ptr<ProbeTask>> probes;
    probes.reserve(probe_count);
    for (size_t i = 0; i < probe_count; ++i) {
      probes.push_back(std::make_unique<ProbeTask>(service_time));
    }
    std::vector<int64_t> probe_enqueue_ns(probe_count, 0);

    const auto start = MonoTime::Now();
    const auto measure_from = start + warmup;
    const auto end = measure_from + duration;

    for (auto& task : bulk) {
      task->Start();
    }
    // Open-loop probe stream on a fixed schedule; late arrivals are not skipped, so the offered
    // probe rate is constant across arms.
    size_t probes_submitted = 0;
    std::thread prober([&] {
      for (size_t i = 0; i < probe_count; ++i) {
        const auto due = start + MonoDelta::FromMicroseconds(
            static_cast<int64_t>(i) * probe_interval.ToMicroseconds());
        while (MonoTime::Now() < due) {
          std::this_thread::yield();
        }
        if (MonoTime::Now() >= end) {
          break;
        }
        probes[i]->MarkSubmitted();
        fixture->submitter->Submit(probes[i].get(), RpcPriority::kHigh, &probe_enqueue_ns[i]);
        ++probes_submitted;
      }
    });
    prober.join();
    stop.store(true, std::memory_order_release);
    ASSERT_TRUE(bulk_retired.WaitFor(30s)) << "bulk tasks did not retire";
    for (size_t i = 0; i < probes_submitted; ++i) {
      ASSERT_TRUE(probes[i]->WaitDone(30s));
      ASSERT_TRUE(probes[i]->ok());
    }

    std::vector<int64_t> bulk_waits;
    size_t bulk_completed = 0;
    for (const auto& task : bulk) {
      for (const auto& sample : task->waits()) {
        if (sample.started >= measure_from && sample.started < end) {
          bulk_waits.push_back(sample.wait_ns);
          ++bulk_completed;
        }
      }
    }
    std::vector<int64_t> probe_waits;
    std::vector<int64_t> probe_enqueue;
    for (size_t i = 0; i < probes_submitted; ++i) {
      const auto& probe = probes[i];
      if (probe->submitted_at() >= measure_from && probe->submitted_at() < end) {
        probe_waits.push_back(probe->wait_ns());
        probe_enqueue.push_back(probe_enqueue_ns[i]);
      }
    }
    const double seconds = duration.ToSeconds();
    LOG(INFO) << "BENCH contended arm=" << ArmName(arm)
              << " workers=" << FLAGS_bench_workers
              << " reserve=" << FLAGS_bench_callback_reserve
              << " service_us=" << FLAGS_bench_service_time_us
              << " bulk_concurrency=" << bulk_concurrency
              << " probe_interval_us=" << FLAGS_bench_probe_interval_us
              << " bulk_throughput=" << StringPrintf("%.1f", bulk_completed / seconds) << "/s"
              << " probes=" << probe_waits.size();
    LOG(INFO) << "BENCH contended arm=" << ArmName(arm) << " probe_submit_to_start "
              << Describe(Summarize(std::move(probe_waits)));
    LOG(INFO) << "BENCH contended arm=" << ArmName(arm) << " probe_enqueue_call "
              << Describe(Summarize(std::move(probe_enqueue)));
    LOG(INFO) << "BENCH contended arm=" << ArmName(arm) << " bulk_submit_to_start "
              << Describe(Summarize(std::move(bulk_waits)));
  }
};

TEST_F(RpcPriorityQueueBench, Unloaded) {
  for (auto arm : {Arm::kDirect, Arm::kFifo, Arm::kPriority}) {
    ASSERT_NO_FATALS(RunUnloaded(arm));
  }
}

TEST_F(RpcPriorityQueueBench, Contended) {
  for (auto arm : {Arm::kDirect, Arm::kFifo, Arm::kPriority}) {
    ASSERT_NO_FATALS(RunContended(arm));
  }
}

} // namespace

} // namespace yb::rpc
