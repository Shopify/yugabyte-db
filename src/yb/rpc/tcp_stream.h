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

#include <deque>

#include <ev++.h>

#include "yb/rpc/stream.h"

#include "yb/util/net/socket.h"
#include "yb/util/mem_tracker.h"
#include "yb/util/ref_cnt_buffer.h"

namespace yb {

class Counter;

namespace rpc {

struct TcpStreamSendingData {
  typedef boost::container::small_vector<RefCntSlice, 4> SendingBytes;

  TcpStreamSendingData(OutboundDataPtr data_, const MemTrackerPtr& mem_tracker);

  // Bytes of the stream itself, such as a lane preamble: no data, nothing to report as transferred.
  explicit TcpStreamSendingData(RefCntSlice bytes_);

  size_t bytes_size() const {
    size_t result = 0;
    for (const auto& entry : bytes) {
      result += entry.size();
    }
    return result;
  }

  void ClearBytes() {
    bytes.clear();
    consumption = ScopedTrackedConsumption();
  }

  // Null for bytes of the stream itself.
  OutboundDataPtr data;
  SendingBytes bytes;
  ScopedTrackedConsumption consumption;
  bool skipped = false;
};

// The bottom layer of every stream stack. A client stream with StreamCreateData::lane_preamble
// queues the lane preamble as its first sending_ entry when it starts, ahead of whatever the upper
// layers send once connected or queue before the connect completes. A server stream with
// StreamCreateData::strip_lane_preamble checks the first bytes it reads, before any upper layer
// sees them, and drops a lane preamble the acceptor did not consume.
class TcpStream : public Stream {
 public:
  explicit TcpStream(const StreamCreateData& data);
  ~TcpStream();

  Socket* socket() { return &socket_; }

  size_t GetPendingWriteBytes() override {
    return queued_bytes_to_send_ - send_position_;
  }

  static const rpc::Protocol* StaticProtocol();
  static StreamFactoryPtr Factory();

 private:
  struct FillIovResult {
    int len;
    bool only_heartbeats;
  };

  Status Start(bool connect, ev::loop_ref* loop, StreamContext* context) override;
  void Close() override;
  void Shutdown(const Status& status) override;
  Result<size_t> Send(OutboundDataPtr data) override;
  Status TryWrite() override;
  bool Cancelled(size_t handle) override;

  bool Idle(std::string* reason_not_idle) override;
  bool IsConnected() override { return connected_; }
  void DumpPB(const DumpRunningRpcsRequestPB& req, RpcConnectionPB* resp) override;

  const Endpoint& Remote() const override { return remote_; }
  const Endpoint& Local() const override { return local_; }

  const Protocol* GetProtocol() override {
    return StaticProtocol();
  }

  void ParseReceived() override;

  Status DoWrite();
  void HandleOutcome(const Status& status, bool enqueue);
  void ClearSending(const Status& status);

  void Handler(ev::io& watcher, int revents); // NOLINT
  Status ReadHandler();
  Status WriteHandler(bool just_connected);

  Result<bool> Receive();
  // Try to parse received data and process it.
  Result<bool> TryProcessReceived();

  // Updates listening events.
  void UpdateEvents();

  FillIovResult FillIov(iovec* out);

  void DelayConnectHandler(ev::timer& watcher, int revents); // NOLINT

  Status DoStart(ev::loop_ref* loop, bool connect);

  StreamReadBuffer& ReadBuffer() {
    return context_->ReadBuffer();
  }

  void PopSending();

  void QueueLanePreamble();

  // Drops a lane preamble at the start of the read buffer. Returns false while the bytes read so
  // far are a prefix of a preamble, so that no upper layer sees them yet.
  bool StripLanePreamble();

  // The socket we're communicating on.
  Socket socket_;

  // The remote address we're talking from.
  Endpoint local_;

  // The remote address we're talking to.
  const Endpoint remote_;

  StreamContext* context_ = nullptr;

  // Notifies us when our socket is readable or writable.
  ev::io io_;

  ev::timer connect_delayer_;

  // Set to true when the connection is registered on a loop.
  // This is used for a sanity check in the destructor that we are properly
  // un-registered before shutting down.
  bool is_epoll_registered_ = false;

  bool connected_ = false;

  bool read_buffer_full_ = false;

  // Lane to send in a preamble on start; kNoLane once queued, and for streams that send none.
  LaneId lane_preamble_;
  // Whether the start of the inbound stream is still to be checked for a lane preamble.
  bool strip_lane_preamble_;
  // Set only when strip_lane_preamble_ is, to count stripped preambles.
  scoped_refptr<MetricEntity> metric_entity_;

  std::deque<TcpStreamSendingData> sending_;
  size_t data_blocks_sent_ = 0;
  size_t send_position_ = 0;
  size_t queued_bytes_to_send_ = 0;
  size_t inbound_bytes_to_skip_ = 0;
  bool waiting_write_ready_ = false;
  MemTrackerPtr mem_tracker_;
  scoped_refptr<Counter> bytes_sent_counter_;
  scoped_refptr<Counter> bytes_received_counter_;
};

} // namespace rpc
} // namespace yb
