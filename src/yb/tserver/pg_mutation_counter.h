#pragma once

#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "yb/common/entity_ids_types.h"

#include "yb/gutil/thread_annotations.h"

#include "yb/util/lanes.h"
#include "yb/util/shared_lock.h"

namespace yb::tserver {

using TableMutationCounts = std::unordered_map<TableId, std::atomic<uint64_t>>;

// Counts the mutations of each table since the last GetAndClear().
//
// Every write statement of every session increases the count of its table. In lane mode the counts
// are therefore kept per lane, and added up by GetAndClear(): a thread of a lane takes the lock of
// its lane's shard and adds to its lane's count of the table, so that neither the lock nor the
// count is a word that the sessions of all lanes write. Threads without a lane, and all threads
// with lanes off, use one shared shard.
class PgMutationCounter {
 public:
  void Increase(const TableId& table_id, uint64_t mutation_count);
  TableMutationCounts GetAndClear();

  template <class Batch>
  void IncreaseBatch(const Batch& batch) {
    if (batch.empty()) {
      return;
    }
    auto& shard = LocalShard();
    auto batch_it = batch.begin();
    auto batch_end = batch.end();
    {
      // The mutex is used for only guarding membership changes to the map. A shared lock is needed
      // when checking for membership and an exclusive lock is needed if updating membership i.e.,
      // adding/ removing a key from the map.
      //
      // Incrementing the counters doesn't need a mutex because we use atomic for the counter.
      SharedLock shared_lock(shard.mutex);
      for (; batch_it != batch_end; ++batch_it) {
        const auto& [key, value] = *batch_it;
        auto it = shard.counts.find(key);
        if (it == shard.counts.end()) {
          break;
        }
        it->second += value;
      }
    }

    if (batch_it != batch_end) {
      std::lock_guard lock(shard.mutex);
      for (; batch_it != batch_end; ++batch_it) {
        const auto& [key, value] = *batch_it;
        shard.counts[key] += value;
      }
    }
  }

 private:
  struct Shard {
    std::shared_mutex mutex;
    // Table id is not stored as oid as that will require conversion in each table of each
    // transaction.
    TableMutationCounts counts GUARDED_BY(mutex);
  };

  // The shard of the calling thread's lane; shared_shard_ for a thread without a lane or with lanes
  // off. The lane shards are allocated by the first call from a thread of a lane.
  Shard& LocalShard();

  static void MoveCounts(Shard* shard, TableMutationCounts* out);

  Shard shared_shard_;
  LaneSharded<Shard> lane_shards_;
};

}  // namespace yb::tserver
