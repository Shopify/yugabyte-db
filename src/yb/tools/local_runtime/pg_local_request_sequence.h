// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <optional>

#include "yb/util/status_format.h"
#include "yb/util/result.h"

namespace yb::local_runtime {

// One admitted backend and no parallel workers give one predecessor chain. The service parks
// out-of-order arrivals, without blocking RPC workers. A watermark rejects replay in constant
// space.
class LocalRequestSequence {
 public:
  Result<bool> Ready(uint64_t serial, std::optional<uint64_t> predecessor) const {
    SCHECK(!last_ || serial > *last_, NotSupported, "Local request replay is not supported");
    SCHECK(!predecessor || *predecessor < serial, InvalidArgument, "Invalid request predecessor");
    if (predecessor == last_) return true;
    SCHECK(
        predecessor && (!last_ || *predecessor > *last_), NotSupported,
        "Local request has a stale predecessor");
    return false;
  }

  Status Register(uint64_t serial, std::optional<uint64_t> predecessor) {
    SCHECK(
        VERIFY_RESULT(Ready(serial, predecessor)), NotSupported, "Request predecessor is pending");
    last_ = serial;
    return Status::OK();
  }

  void Reset() { last_.reset(); }

 private:
  std::optional<uint64_t> last_;
};

}  // namespace yb::local_runtime
