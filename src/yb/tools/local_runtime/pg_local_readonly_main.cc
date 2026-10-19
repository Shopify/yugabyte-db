// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.  See the License for the specific language governing permissions and
// limitations under the License.

#include <fcntl.h>
#include <unistd.h>

#include <csignal>
#include <cstdint>
#include <iostream>
#include <sstream>
#include <string>

#include "yb/tools/local_runtime/pg_local_readonly.h"

#include "yb/gutil/port.h"
#include "yb/gutil/stringprintf.h"
#include "yb/util/backoff_waiter.h"
#include "yb/util/env_util.h"
#include "yb/util/flags.h"
#include "yb/util/jsonwriter.h"
#include "yb/util/logging.h"
#include "yb/util/status.h"
#include "yb/util/slice.h"
#include "yb/util/status_format.h"
#include "yb/util/thread.h"
#include "yb/yql/pgwrapper/libpq_utils.h"

DEFINE_NON_RUNTIME_string(data_dir, "", "Directory for local readonly runtime data.");
DEFINE_NON_RUNTIME_int32(sql_port, 15433, "SQL port for the local readonly PostgreSQL server.");
DEFINE_NON_RUNTIME_bool(experimental_catalog_writes, false, "Serialized SQL transactions.");

namespace {

volatile sig_atomic_t g_stop = 0;

void HandleSignal(int) { g_stop = 1; }

yb::Status ValidateSqlPort(int32_t port) {
  if (port < 1 || port > 65535) {
    return STATUS_FORMAT(InvalidArgument, "--sql_port must be in range 1..65535, got $0", port);
  }
  return yb::Status::OK();
}

}  // namespace

int main(int argc, char** argv) {
  FLAGS_logtostderr = true;
  yb::ParseCommandLineFlags(&argc, &argv, true);
  yb::InitGoogleLoggingSafeBasic(argv[0]);

  if (FLAGS_data_dir.empty()) {
    LOG(ERROR) << "--data_dir is required";
    return EXIT_FAILURE;
  }
  if (const auto status = ValidateSqlPort(FLAGS_sql_port); !status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

  const std::string snapshot = yb::env_util::GetRootDir("share/initial_sys_catalog_snapshot") +
                               "/share/initial_sys_catalog_snapshot";

  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  // Keep initdb/postgres chatter off the machine-readable readiness stream.
  const int ready_output = fcntl(STDOUT_FILENO, F_DUPFD_CLOEXEC, 3);
  if (ready_output < 0 || dup2(STDERR_FILENO, STDOUT_FILENO) < 0) {
    LOG(ERROR) << "Cannot reserve readiness output";
    return EXIT_FAILURE;
  }

  auto started = yb::local_runtime::ReadonlyPgServer::Start(
      FLAGS_data_dir, snapshot, static_cast<uint16_t>(FLAGS_sql_port),
      FLAGS_experimental_catalog_writes ? yb::local_runtime::CatalogMode::kSerializedTransactions
                                        : yb::local_runtime::CatalogMode::kFrozen);
  if (!started.ok()) {
    LOG(ERROR) << started.status();
    return EXIT_FAILURE;
  }
  auto server = std::move(*started);
  int32_t readiness_backend = 0;
  {
    yb::pgwrapper::PGConnSettings settings{
        .host = "127.0.0.1",
        .port = server->SqlPort(),
        .dbname = "yugabyte",
        .user = "yugabyte",
        .connect_timeout = 30,
        .should_stop = [] { return g_stop != 0; }};
    auto connection = yb::pgwrapper::PGConnBuilder(settings).Connect();
    if (!connection.ok()) {
      LOG(ERROR) << connection.status();
      return EXIT_FAILURE;
    }
    auto directory = connection->FetchRow<std::string>("SHOW data_directory");
    if (!directory.ok() || !server->ValidateConnection(*directory).ok()) {
      LOG(ERROR) << "SQL readiness reached an unowned postmaster";
      return EXIT_FAILURE;
    }
    auto backend = connection->FetchRow<int32_t>("SELECT pg_backend_pid()");
    if (!backend.ok()) {
      LOG(ERROR) << backend.status();
      return EXIT_FAILURE;
    }
    readiness_backend = *backend;
    auto result = connection->FetchRow<int64_t>("SELECT 1::bigint");
    if (!result.ok() || *result != 1) {
      LOG(ERROR) << "SQL readiness query failed";
      return EXIT_FAILURE;
    }
  }
  if (FLAGS_experimental_catalog_writes) {
    // Closing libpq is asynchronous; readiness must release the sole admitted backend slot.
    auto status = yb::WaitFor(
        [&]() -> yb::Result<bool> { return kill(readiness_backend, 0) != 0 && errno == ESRCH; },
        yb::MonoDelta::FromSeconds(5), "releasing SQL readiness session");
    if (!status.ok()) {
      LOG(ERROR) << status;
      return EXIT_FAILURE;
    }
  }
  if (g_stop) return EXIT_SUCCESS;

  if (dup2(ready_output, STDOUT_FILENO) < 0) {
    LOG(ERROR) << "Cannot restore readiness output";
    return EXIT_FAILURE;
  }
  close(ready_output);

  std::stringstream output;
  yb::JsonWriter writer(&output, yb::JsonWriter::COMPACT);
  writer.StartObject();
  writer.String("state");
  writer.String("ready");
  writer.String("host");
  writer.String("127.0.0.1");
  writer.String("sql_port");
  writer.Int(server->SqlPort());
  writer.String("rpc_address");
  writer.String(server->RpcAddress().ToString());
  writer.String("mode");
  writer.String(FLAGS_experimental_catalog_writes ? "experimental-serialized-sql" : "readonly");
  writer.EndObject();
  std::cout << output.str() << '\n' << std::flush;

  while (!g_stop) {
    yb::SleepFor(yb::MonoDelta::FromMilliseconds(100));
  }

  server.reset();
  return EXIT_SUCCESS;
}
