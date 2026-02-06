#
# Copyright (c) YugabyteDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations
# under the License.
#

find_path(OPENTELEMETRY_INCLUDE_DIR opentelemetry/version.h
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

# Core OpenTelemetry libraries
find_library(OPENTELEMETRY_COMMON_LIB opentelemetry_common
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

find_library(OPENTELEMETRY_TRACE_LIB opentelemetry_trace
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

find_library(OPENTELEMETRY_METRICS_LIB opentelemetry_metrics
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

find_library(OPENTELEMETRY_RESOURCES_LIB opentelemetry_resources
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

find_library(OPENTELEMETRY_VERSION_LIB opentelemetry_version
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

# OTLP protocol support
find_library(OPENTELEMETRY_PROTO_LIB opentelemetry_proto
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

find_library(OPENTELEMETRY_OTLP_RECORDABLE_LIB opentelemetry_otlp_recordable
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

# OTLP HTTP exporters
find_library(OPENTELEMETRY_EXPORTER_OTLP_HTTP_LIB opentelemetry_exporter_otlp_http
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

find_library(OPENTELEMETRY_EXPORTER_OTLP_HTTP_CLIENT_LIB opentelemetry_exporter_otlp_http_client
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

find_library(OPENTELEMETRY_EXPORTER_OTLP_HTTP_METRIC_LIB opentelemetry_exporter_otlp_http_metric
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

find_library(OPENTELEMETRY_HTTP_CLIENT_CURL_LIB opentelemetry_http_client_curl
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

# Testing/debugging exporters
find_library(OPENTELEMETRY_EXPORTER_IN_MEMORY_LIB opentelemetry_exporter_in_memory
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

find_library(OPENTELEMETRY_EXPORTER_OSTREAM_SPAN_LIB opentelemetry_exporter_ostream_span
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

find_library(OPENTELEMETRY_EXPORTER_OSTREAM_METRICS_LIB opentelemetry_exporter_ostream_metrics
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(OpenTelemetry REQUIRED_VARS
  OPENTELEMETRY_INCLUDE_DIR
  OPENTELEMETRY_COMMON_LIB
  OPENTELEMETRY_TRACE_LIB
  OPENTELEMETRY_METRICS_LIB
  OPENTELEMETRY_RESOURCES_LIB
  OPENTELEMETRY_VERSION_LIB
  OPENTELEMETRY_PROTO_LIB
  OPENTELEMETRY_OTLP_RECORDABLE_LIB
  OPENTELEMETRY_EXPORTER_OTLP_HTTP_LIB
  OPENTELEMETRY_EXPORTER_OTLP_HTTP_CLIENT_LIB
  OPENTELEMETRY_EXPORTER_OTLP_HTTP_METRIC_LIB
  OPENTELEMETRY_HTTP_CLIENT_CURL_LIB
  OPENTELEMETRY_EXPORTER_IN_MEMORY_LIB
  OPENTELEMETRY_EXPORTER_OSTREAM_SPAN_LIB
  OPENTELEMETRY_EXPORTER_OSTREAM_METRICS_LIB)
