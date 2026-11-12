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

#pragma once

#include <mutex>
#include <unordered_map>
#include <vector>

#undef EV_ERROR
#include <ev++.h> // NOLINT

#include "yb/gutil/ref_counted.h"
#include "yb/util/lanes.h"
#include "yb/util/net/sockaddr.h"
#include "yb/util/net/socket.h"
#include "yb/util/status_fwd.h"

namespace yb {

class Cgroup;
class Counter;
class MetricEntity;
class Socket;
class Thread;

namespace rpc {

// Called on the acceptor thread with each accepted socket, which it takes over by moving it, and
// the remote address. lane is the lane of the socket's lane preamble, already reduced modulo the
// acceptor's lane count, or kNoLane when the socket is not bound to a lane.
using NewSocketHandler = std::function<void(Socket*, const Endpoint&, LaneId lane)>;

// A acceptor that calls accept() to create new connections.
class Acceptor {
 public:
  // num_lanes is the lane count of a laned messenger whose inbound protocol accepts a lane
  // preamble (rpc/lane_preamble.h). The acceptor then holds each accepted socket in its own loop,
  // never blocking, until the socket's first bytes show whether they are a preamble, for at most
  // --rpc_lane_preamble_peek_timeout_ms; it consumes a preamble and passes its lane, and passes
  // any other socket with nothing consumed. With 0, sockets are passed on as soon as accepted.
  Acceptor(const scoped_refptr<MetricEntity>& metric_entity, NewSocketHandler handler,
           Cgroup* cgroup = nullptr, size_t num_lanes = 0);
  ~Acceptor();

  // Setup acceptor to listen address.
  // Return bound address in bound_address.
  Status Listen(const Endpoint& endpoint, Endpoint* bound_endpoint = nullptr);

  Status Start();
  void Shutdown();

 private:
  void RunThread();
  void IoHandler(ev::io& io, int events); // NOLINT
  void AsyncHandler(ev::async& async, int events); // NOLINT

  struct PendingSocket;

  void PeekLanePreamble(Socket socket, const Endpoint& remote);
  // Peeks at the first bytes of a held socket, passes it on once they decide, or once wait is false
  // or its deadline passed, and otherwise keeps holding it.
  void DecideLanePreamble(PendingSocket* pending, bool wait);
  void PassPending(PendingSocket* pending, LaneId lane);

  struct AcceptingSocket {
    std::unique_ptr<ev::io> io;
    Socket socket;
    Endpoint endpoint;
  };

  NewSocketHandler handler_;
  scoped_refptr<yb::Thread> thread_;
  std::mutex mutex_;
  std::unordered_map<ev::io*, AcceptingSocket> sockets_;

  std::vector<Socket> sockets_to_add_;
  std::vector<Socket> processing_sockets_to_add_;

  scoped_refptr<Counter> rpc_connections_accepted_;

  const size_t num_lanes_;
  // Instantiated only when num_lanes_ is not 0.
  scoped_refptr<Counter> lane_preamble_bound_;
  scoped_refptr<Counter> lane_preamble_absent_;
  scoped_refptr<Counter> lane_preamble_peek_timeouts_;

  bool closing_ = false;
  ev::dynamic_loop loop_;
  ev::async async_;
#ifdef __linux__
  Cgroup* cgroup_ = nullptr;
#endif

  // Sockets held until their first bytes show whether they start with a lane preamble. Used only
  // on the acceptor thread, and declared after loop_ since their watchers stop on destruction.
  std::unordered_map<PendingSocket*, std::unique_ptr<PendingSocket>> pending_sockets_;

  DISALLOW_COPY_AND_ASSIGN(Acceptor);
};

} // namespace rpc
} // namespace yb
