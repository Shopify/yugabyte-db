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

#include "yb/common/common_fwd.h"
#include "yb/master/master_heartbeat.fwd.h"
#include "yb/server/server_fwd.h"
#include "yb/tserver/tserver_fwd.h"

namespace yb {
class Cgroup;
} // namespace yb

namespace yb::tserver {

class ConnectivityPoller {
 public:
  // Receives the live tservers of each successful ListTabletServers poll while
  // live_tservers_from_connectivity_check is set.
  using LiveTServersCallback =
      std::function<void(const google::protobuf::RepeatedPtrField<master::TSInformationPB>&)>;

  ConnectivityPoller(
      server::RpcServerBase& server, const std::string& uuid,
      LiveTServersCallback live_tservers_callback);
  ~ConnectivityPoller();

  Status Start(Cgroup* cgroup = nullptr);

  void Shutdown();

  void UpdateMasterAddresses(server::MasterAddressesPtr master_addresses);
  ConnectivityStateResponsePB State();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace yb::tserver
