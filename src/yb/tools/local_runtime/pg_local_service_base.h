// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <boost/preprocessor/stringize.hpp>

#include "yb/rpc/rpc_context.h"
#include "yb/tserver/pg_client_service.h"
#include "yb/util/status_format.h"

namespace yb::local_runtime {

class UnsupportedPgClientService : public tserver::PgClientServiceIf {
 public:
  explicit UnsupportedPgClientService(const scoped_refptr<MetricEntity>& entity)
      : tserver::PgClientServiceIf(entity) {}

  void Perform(
      const tserver::PgPerformRequestMsg* /*req*/, tserver::PgPerformResponseMsg* /*resp*/,
      rpc::RpcContext context) override {
    context.RespondFailure(
        STATUS_FORMAT(NotSupported, "Read-only local runtime does not implement Perform"));
  }

#define YB_UNSUPPORTED_PG_CLIENT_METHOD_DECL(r, data, method) \
  void method( \
      const tserver::YB_PG_CLIENT_METHOD_ARG(data, method, Request)* /*req*/, \
      tserver::YB_PG_CLIENT_METHOD_ARG(data, method, Response)* /*resp*/, rpc::RpcContext context) \
      override { \
    context.RespondFailure(STATUS_FORMAT( \
        NotSupported, "Read-only local runtime does not implement $0", \
        BOOST_PP_STRINGIZE(method))); \
  }

#define YB_UNSUPPORTED_PG_CLIENT_TRIVIAL_METHOD_DECL(r, data, method) \
  Result<tserver::YB_PG_CLIENT_METHOD_ARG(data, method, Response)> method( \
      const tserver::YB_PG_CLIENT_METHOD_ARG(data, method, Request) & /*req*/, \
      CoarseTimePoint /*deadline*/) override { \
    return STATUS_FORMAT( \
        NotSupported, "Read-only local runtime does not implement $0", \
        BOOST_PP_STRINGIZE(method)); \
  }

  BOOST_PP_SEQ_FOR_EACH(YB_UNSUPPORTED_PG_CLIENT_METHOD_DECL, BOOST_PP_NIL, YB_PG_CLIENT_METHODS)
  BOOST_PP_SEQ_FOR_EACH(
      YB_UNSUPPORTED_PG_CLIENT_METHOD_DECL, BOOST_PP_NIL, YB_PG_CLIENT_ASYNC_METHODS)
  BOOST_PP_SEQ_FOR_EACH(YB_UNSUPPORTED_PG_CLIENT_METHOD_DECL, (LW), YB_PG_CLIENT_LW_METHODS)
  BOOST_PP_SEQ_FOR_EACH(YB_UNSUPPORTED_PG_CLIENT_METHOD_DECL, (LW), YB_PG_CLIENT_ASYNC_LW_METHODS)
  BOOST_PP_SEQ_FOR_EACH(
      YB_UNSUPPORTED_PG_CLIENT_TRIVIAL_METHOD_DECL, ~, YB_PG_CLIENT_TRIVIAL_METHODS)

#undef YB_UNSUPPORTED_PG_CLIENT_METHOD_DECL
#undef YB_UNSUPPORTED_PG_CLIENT_TRIVIAL_METHOD_DECL
};

}  // namespace yb::local_runtime
