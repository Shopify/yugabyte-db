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

#include "yb/rpc/acceptor.h"

#include <pthread.h>
#include <sys/socket.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest_prod.h>

#include "yb/gutil/ref_counted.h"

#include "yb/rpc/lane_preamble.h"
#include "yb/rpc/reactor.h"

#include "yb/util/logging.h"
#include "yb/util/metrics.h"
#include "yb/util/net/sockaddr.h"
#include "yb/util/cgroups.h"
#include "yb/util/errno.h"
#include "yb/util/flags.h"
#include "yb/util/monotime.h"
#include "yb/util/status.h"
#include "yb/util/status_format.h"
#include "yb/util/status_log.h"
#include "yb/util/thread.h"

using namespace std::literals;

METRIC_DEFINE_counter(server, rpc_connections_accepted,
                      "RPC Connections Accepted",
                      yb::MetricUnit::kConnections,
                      "Number of incoming TCP connections made to the RPC server");

DEFINE_UNKNOWN_int32(rpc_acceptor_listen_backlog, 128,
             "Socket backlog parameter used when listening for RPC connections. "
             "This defines the maximum length to which the queue of pending "
             "TCP connections inbound to the RPC server may grow. If a connection "
             "request arrives when the queue is full, the client may receive "
             "an error. Higher values may help the server ride over bursts of "
             "new inbound connection requests.");
TAG_FLAG(rpc_acceptor_listen_backlog, advanced);

DEFINE_RUNTIME_uint32(rpc_lane_preamble_peek_timeout_ms, 500,
    "How long the acceptor of a laned messenger waits for the first bytes of an accepted "
    "connection to show whether it starts with a lane preamble. A connection that has not decided "
    "by then is assigned to a reactor as without lanes, and is not bound to a lane for its "
    "lifetime.");
TAG_FLAG(rpc_lane_preamble_peek_timeout_ms, advanced);

DEFINE_test_flag(int32, rpc_lane_preamble_max_pending_sockets, 4096,
    "Maximum number of accepted connections the acceptor of a laned messenger holds while waiting "
    "for their lane preamble; further undecided connections are not bound to a lane.");

DECLARE_bool(TEST_rpc_ignore_lane_preamble);

METRIC_DEFINE_counter(server, rpc_lane_preamble_bound,
    "RPC Connections Bound By Lane Preamble", yb::MetricUnit::kConnections,
    "Number of inbound connections that the acceptor of a laned messenger bound to a reactor of "
    "the lane of their lane preamble.");
METRIC_DEFINE_counter(server, rpc_lane_preamble_absent,
    "RPC Connections Without Lane Preamble", yb::MetricUnit::kConnections,
    "Number of inbound connections of a laned messenger whose first bytes were not a lane "
    "preamble, that closed or failed before sending one, or whose preamble the acceptor could not "
    "read in full. They are assigned to a reactor as without lanes.");
METRIC_DEFINE_counter(server, rpc_lane_preamble_peek_timeouts,
    "RPC Lane Preamble Peek Timeouts", yb::MetricUnit::kConnections,
    "Number of inbound connections of a laned messenger whose first bytes had not shown whether "
    "they start with a lane preamble within --rpc_lane_preamble_peek_timeout_ms, or that could not "
    "be held because the acceptor held the maximum number of such connections. They are assigned "
    "to a reactor as without lanes.");
METRIC_DECLARE_counter(rpc_lane_preamble_stripped_late);

namespace yb::rpc {

namespace {

// How often a socket that sent part of a preamble is peeked again. Its read watcher would fire
// continuously while the bytes are left unread. Senders write the preamble with the bytes after
// it, so a split preamble is rare.
const MonoDelta kLanePreambleRepeekInterval = 1ms;

} // namespace

struct Acceptor::PendingSocket {
  PendingSocket(Acceptor* acceptor_, Socket socket_, const Endpoint& remote_, MonoTime deadline_)
      : acceptor(acceptor_), socket(std::move(socket_)), remote(remote_), deadline(deadline_) {}

  Acceptor* const acceptor;
  Socket socket;
  const Endpoint remote;
  const MonoTime deadline;
  ev::io io;
  ev::timer timer;

  void Handler(ev::io&, int) { // NOLINT
    acceptor->DecideLanePreamble(this, /* wait= */ true);
  }

  void TimerHandler(ev::timer&, int) { // NOLINT
    acceptor->DecideLanePreamble(this, /* wait= */ true);
  }
};

Acceptor::Acceptor(
    const scoped_refptr<MetricEntity>& metric_entity, NewSocketHandler handler, Cgroup* cgroup,
    size_t num_lanes)
    : handler_(std::move(handler)),
      rpc_connections_accepted_(METRIC_rpc_connections_accepted.Instantiate(metric_entity)),
      num_lanes_(num_lanes),
      loop_(kDefaultLibEvFlags)
#ifdef __linux__
      , cgroup_(cgroup)
#endif
{
  (void)cgroup;
  if (num_lanes_ != 0) {
    lane_preamble_bound_ = METRIC_rpc_lane_preamble_bound.Instantiate(metric_entity);
    lane_preamble_absent_ = METRIC_rpc_lane_preamble_absent.Instantiate(metric_entity);
    lane_preamble_peek_timeouts_ = METRIC_rpc_lane_preamble_peek_timeouts.Instantiate(
        metric_entity);
    METRIC_rpc_lane_preamble_stripped_late.Instantiate(metric_entity);
  }
}

Acceptor::~Acceptor() {
  Shutdown();
}

Status Acceptor::Listen(const Endpoint& endpoint, Endpoint* bound_endpoint) {
  Socket socket;
  RETURN_NOT_OK(socket.Init(endpoint.address().is_v6() ? Socket::FLAG_IPV6 : 0));
  RETURN_NOT_OK(socket.SetReuseAddr(true));
  RETURN_NOT_OK(socket.Bind(endpoint));
  if (bound_endpoint) {
    RETURN_NOT_OK(socket.GetSocketAddress(bound_endpoint));
  }
  RETURN_NOT_OK(socket.SetNonBlocking(true));
  RETURN_NOT_OK(socket.Listen(FLAGS_rpc_acceptor_listen_backlog));

  bool was_empty;
  {
    std::lock_guard lock(mutex_);
    if (closing_) {
      return STATUS_FORMAT(ServiceUnavailable, "Acceptor closing");
    }
    was_empty = sockets_to_add_.empty();
    sockets_to_add_.push_back(std::move(socket));
  }

  if (was_empty) {
    async_.send();
  }

  return Status::OK();
}

Status Acceptor::Start() {
  async_.set(loop_);
  async_.set<Acceptor, &Acceptor::AsyncHandler>(this);
  async_.start();
  async_.send();
  return yb::Thread::Create("acceptor", "acceptor", &Acceptor::RunThread, this, &thread_);
}

void Acceptor::Shutdown() {
  {
    std::lock_guard lock(mutex_);
    if (closing_) {
      CHECK(sockets_to_add_.empty());
      VLOG(2) << "Acceptor already shut down";
      return;
    }
    closing_ = true;
  }

  if (thread_) {
    async_.send();

    CHECK_OK(ThreadJoiner(thread_.get()).Join());
    thread_.reset();
  } else {
    // This is reached if Start() was never called.
    std::lock_guard lock(mutex_);
    sockets_to_add_.clear();
  }
}

void Acceptor::IoHandler(ev::io& io, int events) {
  auto it = sockets_.find(&io);
  if (it == sockets_.end()) {
    LOG(DFATAL) << "IoHandler for unknown socket: " << &io;
    return;
  }
  Socket& socket = it->second.socket;
  if (events & EV_ERROR) {
    LOG(INFO) << "Acceptor socket failure: " << socket.GetFd()
              << ", endpoint: " << it->second.endpoint;
    sockets_.erase(it);
    return;
  }

  if (events & EV_READ) {
    for (;;) {
      Socket new_sock;
      Endpoint remote;
      VLOG(2) << "calling accept() on socket " << socket.GetFd();
      Status s = socket.Accept(&new_sock, &remote, Socket::FLAG_NONBLOCKING);
      if (!s.ok()) {
        if (!s.IsTryAgain()) {
          LOG(WARNING) << "Acceptor: accept failed: " << s;
        }
        return;
      }
      s = new_sock.SetNoDelay(true);
      if (!s.ok()) {
        LOG(WARNING) << "Acceptor with remote = " << remote
                     << " failed to set TCP_NODELAY on a newly accepted socket: "
                     << s.ToString();
        continue;
      }
      rpc_connections_accepted_->Increment();
      if (num_lanes_ != 0 && !FLAGS_TEST_rpc_ignore_lane_preamble) {
        PeekLanePreamble(std::move(new_sock), remote);
        continue;
      }
      handler_(&new_sock, remote, kNoLane);
    }
  }
}

void Acceptor::PeekLanePreamble(Socket socket, const Endpoint& remote) {
  auto pending = std::make_unique<PendingSocket>(
      this, std::move(socket), remote,
      MonoTime::Now() + FLAGS_rpc_lane_preamble_peek_timeout_ms * 1ms);
  auto* ptr = pending.get();
  ptr->io.set(loop_);
  ptr->io.set<PendingSocket, &PendingSocket::Handler>(ptr);
  ptr->timer.set(loop_);
  ptr->timer.set<PendingSocket, &PendingSocket::TimerHandler>(ptr);
  // A sender usually wrote its first bytes before the connection was accepted.
  auto wait = std::cmp_less(
      pending_sockets_.size(), FLAGS_TEST_rpc_lane_preamble_max_pending_sockets);
  pending_sockets_.emplace(ptr, std::move(pending));
  DecideLanePreamble(ptr, wait);
}

void Acceptor::DecideLanePreamble(PendingSocket* pending, bool wait) {
  uint8_t bytes[kLanePreambleSize];
  const auto fd = pending->socket.GetFd();
  ssize_t peeked;
  do {
    peeked = ::recv(fd, bytes, sizeof(bytes), MSG_PEEK);
  } while (peeked < 0 && errno == EINTR);

  bool partial = false;
  if (peeked < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
    // The reactor fails or closes the connection as it would without lanes.
    VLOG(2) << "Peek failed on connection from " << pending->remote << ": "
            << ErrnoToString(errno);
    lane_preamble_absent_->Increment();
    return PassPending(pending, kNoLane);
  }
  if (peeked == 0) {
    VLOG(2) << "Connection from " << pending->remote << " closed before sending a lane preamble";
    lane_preamble_absent_->Increment();
    return PassPending(pending, kNoLane);
  }
  if (peeked > 0) {
    switch (MatchLanePreamble(Slice(bytes, peeked))) {
      case LanePreambleMatch::kNone:
        lane_preamble_absent_->Increment();
        return PassPending(pending, kNoLane);
      case LanePreambleMatch::kPrefix:
        partial = true;
        break;
      case LanePreambleMatch::kPreamble: {
        // The bytes were peeked, so they are in the receive buffer, but read in a loop since recv
        // may return fewer.
        size_t consumed = 0;
        while (consumed != kLanePreambleSize) {
          auto received = ::recv(fd, bytes + consumed, kLanePreambleSize - consumed, 0);
          if (received < 0 && errno == EINTR) {
            continue;
          }
          if (received <= 0) {
            break;
          }
          consumed += received;
        }
        if (consumed != kLanePreambleSize) {
          // Nothing consumed: the reactor drops the preamble. Part of it consumed: the reactor
          // rejects the stream as it rejects any unknown header.
          LOG(WARNING) << "Consumed " << consumed << " of " << kLanePreambleSize
                       << " lane preamble bytes from " << pending->remote;
          lane_preamble_absent_->Increment();
          return PassPending(pending, kNoLane);
        }
        // The lane byte comes from the network: reduce it before it selects anything.
        auto lane = static_cast<LaneId>(bytes[kLanePreambleSize - 1] % num_lanes_);
        VLOG(2) << "Connection from " << pending->remote << " bound to lane "
                << static_cast<int>(lane);
        lane_preamble_bound_->Increment();
        return PassPending(pending, lane);
      }
    }
  }

  const auto now = MonoTime::Now();
  if (!wait || now >= pending->deadline) {
    VLOG(2) << "No lane preamble from " << pending->remote << " within the peek timeout"
            << (wait ? "" : ", or too many connections waiting for one");
    lane_preamble_peek_timeouts_->Increment();
    return PassPending(pending, kNoLane);
  }
  auto remaining = pending->deadline - now;
  pending->timer.stop();
  if (partial) {
    pending->io.stop();
    pending->timer.start(std::min(remaining, kLanePreambleRepeekInterval).ToSeconds());
  } else {
    pending->io.start(fd, EV_READ);
    pending->timer.start(remaining.ToSeconds());
  }
}

void Acceptor::PassPending(PendingSocket* pending, LaneId lane) {
  auto it = pending_sockets_.find(pending);
  if (it == pending_sockets_.end()) {
    LOG(DFATAL) << "Unknown pending socket: " << pending;
    return;
  }
  auto holder = std::move(it->second);
  pending_sockets_.erase(it);
  Socket socket = std::move(holder->socket);
  auto remote = holder->remote;
  // Stops the watchers, which may be the caller.
  holder.reset();
  handler_(&socket, remote, lane);
}

void Acceptor::AsyncHandler(ev::async& async, int events) {
  bool closing;
  {
    std::lock_guard lock(mutex_);
    closing = closing_;
    sockets_to_add_.swap(processing_sockets_to_add_);
  }

  if (closing) {
    processing_sockets_to_add_.clear();
    sockets_.clear();
    // Closes the sockets still waiting for a lane preamble without passing them on.
    pending_sockets_.clear();
    loop_.break_loop();
    return;
  }

  while (!processing_sockets_to_add_.empty()) {
    auto& socket = processing_sockets_to_add_.back();
    Endpoint endpoint;
    auto status = socket.GetSocketAddress(&endpoint);
    if (!status.ok()) {
      LOG(WARNING) << "Failed to get address for socket: " << socket.GetFd() << ": "
                   << status.ToString();
    }
    VLOG(1) << "Adding socket fd " << socket.GetFd() << " at " << endpoint;
    AcceptingSocket ac{
        .io = std::make_unique<ev::io>(),
        .socket = Socket(std::move(socket)),
        .endpoint = endpoint};
    processing_sockets_to_add_.pop_back();
    ac.io->set(loop_);
    ac.io->set<Acceptor, &Acceptor::IoHandler>(this);
    ac.io->start(ac.socket.GetFd(), EV_READ);
    sockets_.emplace(ac.io.get(), std::move(ac));
  }
}

void Acceptor::RunThread() {
#ifdef __linux__
  if (cgroup_) {
    WARN_NOT_OK(cgroup_->MoveCurrentThreadToGroup(),
                "Failed to move acceptor thread to cgroup");
  }
#endif
  loop_.run();
  VLOG(1) << "Acceptor shutting down.";
}

} // namespace yb::rpc
