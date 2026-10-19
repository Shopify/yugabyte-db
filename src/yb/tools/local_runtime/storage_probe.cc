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

// Storage-only resource experiment. This does not instantiate YSQL, TabletPeer, or transactions.

#include <sys/resource.h>

#ifdef __APPLE__
#include <mach/mach.h>
#endif

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include "yb/docdb/docdb_rocksdb_util.h"
#include "yb/dockv/doc_key.h"
#include "yb/dockv/primitive_value.h"
#include "yb/rocksdb/db.h"
#include "yb/rocksdb/iterator.h"
#include "yb/rocksdb/listener.h"
#include "yb/rocksdb/write_batch.h"
#include "yb/util/flags.h"
#include "yb/util/format.h"
#include "yb/util/jsonwriter.h"
#include "yb/util/logging.h"
#include "yb/util/status_format.h"
#include "yb/util/tcmalloc_util.h"

DEFINE_NON_RUNTIME_string(probe_dir, "", "New directory; existing with --probe_verify.");
DEFINE_NON_RUNTIME_int32(probe_tablets, 2000, "Number of logical storage namespaces.");
DEFINE_NON_RUNTIME_int32(probe_stores, 1, "Physical regular/intents DB pairs; <= probe_tablets.");
DEFINE_NON_RUNTIME_int32(probe_rows, 4, "Records per logical namespace.");
DEFINE_NON_RUNTIME_bool(probe_verify, false, "Reopen existing stores and verify every record.");
DEFINE_NON_RUNTIME_int32(probe_pause_seconds, 0, "Pause after durable writes, before close/flush.");
DEFINE_NON_RUNTIME_bool(probe_wal, true, "Sync WAL; false requires a clean flush.");
DEFINE_NON_RUNTIME_int32(probe_idle_seconds, 0, "Idle CPU sampling duration after flushing.");

DECLARE_bool(rocksdb_disable_compactions);

namespace yb::tools {
namespace {

using Clock = std::chrono::steady_clock;

struct Store {
  std::unique_ptr<rocksdb::DB> regular;
  std::unique_ptr<rocksdb::DB> intents;
};

int64_t ResidentBytes() {
#ifdef __APPLE__
  mach_task_basic_info_data_t info;
  mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
  if (task_info(
          mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count) !=
      KERN_SUCCESS) {
    return -1;
  }
  return info.resident_size;
#elif defined(__linux__)
  std::ifstream status("/proc/self/status");
  std::string line;
  while (std::getline(status, line)) {
    if (line.starts_with("VmRSS:")) {
      std::istringstream value(line.substr(6));
      int64_t kib;
      if (value >> kib) {
        return kib * 1024;
      }
    }
  }
  return -1;
#else
  return -1;
#endif
}

int64_t OpenFileDescriptors() {
#ifdef __APPLE__
  const char* path = "/dev/fd";
#else
  const char* path = "/proc/self/fd";
#endif
  std::error_code error;
  const std::filesystem::directory_iterator entries(path, error);
  if (error) {
    return -1;
  }
  return std::distance(entries, std::filesystem::directory_iterator());
}

void Report(const char* phase, Clock::time_point started) {
  struct rusage usage;
  CHECK_EQ(getrusage(RUSAGE_SELF, &usage), 0);
  std::stringstream out;
  JsonWriter json(&out, JsonWriter::COMPACT);
  json.StartObject();
  json.String("phase");
  json.String(phase);
  json.String("reopening");
  json.Bool(FLAGS_probe_verify);
  json.String("logical_tablets");
  json.Int(FLAGS_probe_tablets);
  json.String("physical_db_pairs");
  json.Int(FLAGS_probe_stores);
  json.String("rows_per_tablet");
  json.Int(FLAGS_probe_rows);
  json.String("wal_enabled");
  json.Bool(FLAGS_probe_wal);
  json.String("compactions_disabled");
  json.Bool(FLAGS_rocksdb_disable_compactions);
  json.String("elapsed_seconds");
  json.Double(std::chrono::duration<double>(Clock::now() - started).count());
  json.String("rss_bytes");
  json.Int64(ResidentBytes());
  json.String("peak_rss_bytes");
#ifdef __APPLE__
  json.Int64(usage.ru_maxrss);
#else
  json.Int64(usage.ru_maxrss * 1024);
#endif
  json.String("allocated_bytes");
  json.Int64(GetTCMallocCurrentAllocatedBytes());
  json.String("allocator_physical_bytes");
  json.Int64(GetTCMallocPhysicalBytesUsed());
  json.String("cpu_seconds");
  json.Double(
      usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6 + usage.ru_stime.tv_sec +
      usage.ru_stime.tv_usec / 1e6);
  json.String("open_file_descriptors");
  json.Int64(OpenFileDescriptors());
  json.EndObject();
  std::cout << out.str() << std::endl;
}

std::string Key(int tablet, int row) {
  // Both layouts use identical DocDB-encoded keys. Repeated hash/key components deliberately
  // collide across namespaces. This is physical key namespacing, not SQL colocation support.
  return dockv::SubDocKey(
             dockv::DocKey(
                 static_cast<ColocationId>(tablet + 1), 42, {dockv::KeyEntryValue::Int32(row)}),
             HybridTime(1000))
      .Encode()
      .ToStringBuffer();
}

std::string Value(int tablet, int row) {
  return std::string(1, static_cast<char>(dockv::ValueEntryType::kString)) +
         Format("tablet=$0,row=$1,", tablet, row) + std::string(64, 'v');
}

Status Verify(const std::vector<Store>& stores) {
  for (int tablet = 0; tablet < FLAGS_probe_tablets; ++tablet) {
    auto& db = *stores[tablet % stores.size()].regular;
    for (int row = 0; row < FLAGS_probe_rows; ++row) {
      std::string value;
      RETURN_NOT_OK(db.Get(rocksdb::ReadOptions(), Key(tablet, row), &value));
      SCHECK_EQ(value, Value(tablet, row), Corruption, "Namespace value mismatch");
    }
  }
  // Point lookups alone would not detect unexpected extra records or broken sorted iteration.
  uint64_t records = 0;
  for (const auto& store : stores) {
    std::unique_ptr<rocksdb::Iterator> iter(store.regular->NewIterator(rocksdb::ReadOptions()));
    std::string previous;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      SCHECK(
          previous.empty() || iter->key().compare(previous) > 0, Corruption,
          "Iterator keys are not strictly increasing");
      previous = iter->key().ToBuffer();
      ++records;
    }
    RETURN_NOT_OK(iter->status());
  }
  SCHECK_EQ(
      records, static_cast<uint64_t>(FLAGS_probe_tablets) * FLAGS_probe_rows, Corruption,
      "Unexpected record count");
  return Status::OK();
}

Status Run() {
  SCHECK(!FLAGS_probe_dir.empty(), InvalidArgument, "--probe_dir is required");
  SCHECK(
      FLAGS_probe_tablets > 0 && FLAGS_probe_stores > 0 &&
          FLAGS_probe_stores <= FLAGS_probe_tablets && FLAGS_probe_rows > 0 &&
          FLAGS_probe_pause_seconds >= 0 && FLAGS_probe_idle_seconds >= 0,
      InvalidArgument, "Invalid probe dimensions");
  SCHECK(
      FLAGS_probe_wal || FLAGS_probe_pause_seconds == 0, InvalidArgument,
      "Crash-recovery experiments require --probe_wal");
  const auto started = Clock::now();
  const std::filesystem::path root(FLAGS_probe_dir);
  if (!FLAGS_probe_verify) {
    SCHECK(
        std::filesystem::create_directory(root), AlreadyPresent,
        "Refusing to overwrite an existing probe directory");
  } else {
    SCHECK(std::filesystem::is_directory(root), NotFound, "Probe directory does not exist");
  }

  // Production already shares the block cache. Do not exaggerate savings by allocating one
  // cache per DB in the separate-store control.
  tablet::TabletOptions tablet_options;
  tablet_options.block_cache = rocksdb::NewLRUCache(64 * 1024 * 1024);
  Report("baseline", started);
  std::vector<Store> stores(FLAGS_probe_stores);
  for (int i = 0; i < FLAGS_probe_stores; ++i) {
    for (const auto type : {docdb::StorageDbType::kRegular, docdb::StorageDbType::kIntents}) {
      const bool regular = type == docdb::StorageDbType::kRegular;
      const auto name = Format("$0-$1", i, regular ? "regular" : "intents");
      rocksdb::Options options;
      docdb::InitRocksDBOptions(&options, name, name, nullptr, tablet_options, type);
      options.create_if_missing = !FLAGS_probe_verify;
      options.error_if_exists = !FLAGS_probe_verify;
      // A production tablet relies on its Raft WAL. Here a RocksDB WAL supplies durability,
      // or --noprobe_wal isolates DB overhead without claiming crash safety.
      options.disable_wal = !FLAGS_probe_wal;
      rocksdb::DB* db = nullptr;
      RETURN_NOT_OK(rocksdb::DB::Open(options, (root / name).string(), &db));
      (regular ? stores[i].regular : stores[i].intents).reset(db);
    }
  }
  Report("opened", started);
  if (!FLAGS_probe_verify) {
    rocksdb::WriteOptions options;
    options.sync = FLAGS_probe_wal;
    options.disableWAL = !FLAGS_probe_wal;
    for (int tablet = 0; tablet < FLAGS_probe_tablets; ++tablet) {
      rocksdb::WriteBatch batch;
      for (int row = 0; row < FLAGS_probe_rows; ++row) {
        batch.Put(Key(tablet, row), Value(tablet, row));
      }
      RETURN_NOT_OK(stores[tablet % stores.size()].regular->Write(options, &batch));
    }
    Report("written", started);
    if (FLAGS_probe_pause_seconds) {
      SleepFor(MonoDelta::FromSeconds(FLAGS_probe_pause_seconds));
    }
  }
  RETURN_NOT_OK(Verify(stores));
  Report("verified", started);
  if (!FLAGS_probe_verify) {
    rocksdb::FlushOptions options(rocksdb::FlushReason::kTestOnly);
    options.wait = true;
    for (const auto& store : stores) {
      RETURN_NOT_OK(store.regular->Flush(options));
    }
    Report("flushed", started);
  }
  if (FLAGS_probe_idle_seconds) {
    Report("idle_begin", started);
    SleepFor(MonoDelta::FromSeconds(FLAGS_probe_idle_seconds));
    Report("idle_end", started);
  }
  stores.clear();
  Report("closed", started);
  return Status::OK();
}

}  // namespace
}  // namespace yb::tools

int main(int argc, char** argv) {
  FLAGS_logtostderr = true;
  FLAGS_rocksdb_disable_compactions = true;
  yb::ParseCommandLineFlags(&argc, &argv, true);
  yb::InitGoogleLoggingSafeBasic(argv[0]);
  try {
    const auto status = yb::tools::Run();
    if (!status.ok()) {
      std::cerr << status << std::endl;
      return 1;
    }
  } catch (const std::exception& error) {
    std::cerr << error.what() << std::endl;
    return 1;
  }
  return 0;
}
