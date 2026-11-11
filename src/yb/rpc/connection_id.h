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

#pragma once

#include <cstdint>
#include <string>

#include "yb/rpc/rpc_fwd.h"

#include "yb/util/lanes.h"
#include "yb/util/net/sockaddr.h"

namespace yb::rpc {

// Used to key on Connection information.
// For use as a key in an unordered STL collection, use ConnectionIdHash.
//
// A laned messenger keys connections for calls with a lane hint by (remote, lane, idx), with idx
// below the messenger's connections per (remote, lane); every other connection has lane kNoLane
// and idx below num_connections_to_server.
class ConnectionId {
 public:
  ConnectionId() {}

  ConnectionId(
      const Endpoint& remote, uint8_t idx, const Protocol* protocol, LaneId lane = kNoLane)
      : remote_(remote), idx_(idx), protocol_(protocol), lane_(lane) {}

  // The remote address.
  const Endpoint& remote() const { return remote_; }
  uint8_t idx() const { return idx_; }
  const Protocol* protocol() const { return protocol_; }
  LaneId lane() const { return lane_; }

  // Returns a string representation of the object, not including the password field. The lane is
  // shown only when set.
  std::string ToString() const;

  size_t HashCode() const;

 private:
  // Remember to update HashCode(), operator== and ToString() when new fields are added.
  Endpoint remote_;
  uint8_t idx_ = 0;  // Connection index, used to support multiple connections to the same server.
  const Protocol* protocol_ = nullptr;
  LaneId lane_ = kNoLane;
};

class ConnectionIdHash {
 public:
  std::size_t operator() (const ConnectionId& conn_id) const;
};

inline bool operator==(const ConnectionId& lhs, const ConnectionId& rhs) {
  return lhs.remote() == rhs.remote() && lhs.idx() == rhs.idx() &&
         lhs.protocol() == rhs.protocol() && lhs.lane() == rhs.lane();
}

} // namespace yb::rpc
