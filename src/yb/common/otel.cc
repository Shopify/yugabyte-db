#include "otel.h"
#include "version_info.h"

#include "opentelemetry/exporters/otlp/otlp_http_exporter.h"
#include "opentelemetry/trace/provider.h"
#include "opentelemetry/sdk/trace/batch_span_processor.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"

#include "yb/util/flags/flag_tags.h"
#include "yb/util/net/net_util.h"

DEFINE_NON_RUNTIME_bool(otel_enable_tracing, false,
    "Enable OpenTelemetry distributed tracing. Requires server restart to take effect.");

// TODO: add sampling configuration to control the amount of tracing data collected.

namespace yb {
namespace common {

namespace otlp = opentelemetry::exporter::otlp;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace resource = opentelemetry::sdk::resource;
namespace trace_api = opentelemetry::trace;

void OpenTelemetry::Init(const std::string& service_name) {
  if (!FLAGS_otel_enable_tracing) {
    LOG(INFO) << "OpenTelemetry tracing is disabled (enable_tracing=false)";
    return;
  }

  tracing_enabled_.store(true);
  LOG(INFO) << "OpenTelemetry tracing is enabled (enable_tracing=true)";

  // Determine version string
  std::string version_string;
  const char* version_env = getenv("YB_VERSION_STRING");
  if (version_env != nullptr) {
    // Postgres backend - tserver passed version via env var
    version_string = version_env;
  } else {
    // Master/tserver - initialize VersionInfo
    CHECK_OK(VersionInfo::Init());
    version_string = VersionInfo::GetShortVersionString();
  }

  main_thread_id_ = std::this_thread::get_id();
  auto exporter = std::make_unique<otlp::OtlpHttpExporter>();

  trace_sdk::BatchSpanProcessorOptions batch_options;
  auto processor = std::make_unique<trace_sdk::BatchSpanProcessor>(
      std::move(exporter), batch_options);

  auto hostname_result = GetHostname();
  std::string hostname = hostname_result.ok() ? *hostname_result : "unknown";

  auto resource_attributes = resource::ResourceAttributes{
      {"service.name", service_name},
      {"service.version", version_string},
      {"process.pid", std::to_string(getpid())},
      {"host.name", hostname}
  };
  auto resource_ptr = resource::Resource::Create(resource_attributes);

  auto provider = opentelemetry::nostd::shared_ptr<trace_api::TracerProvider>(
      new trace_sdk::TracerProvider(std::move(processor), resource_ptr));
  trace_api::Provider::SetTracerProvider(provider);

  LOG(INFO) << "OpenTelemetry initialized";
}

void OpenTelemetry::Shutdown() {
  if (!tracing_enabled_.load()) {
    return;
  }

  ClearAllSpans();
  auto provider = trace_api::Provider::GetTracerProvider();
  if (provider) {
    // Cast using static_cast on the raw pointer, then wrap in shared_ptr
    auto* sdk_provider = static_cast<trace_sdk::TracerProvider *>(provider.get());
    sdk_provider->Shutdown();

    opentelemetry::nostd::shared_ptr<trace_api::TracerProvider> none(nullptr);
    trace_api::Provider::SetTracerProvider(none);

    LOG(INFO) << "OpenTelemetry shutdown completed";
  } else {
    LOG(WARNING) << "No TracerProvider found during OpenTelemetry shutdown";
  }
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> OpenTelemetry::GetTracer(
    const std::string& name) {
  if (!tracing_enabled_.load()) {
    return opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>(nullptr);
  }
  return trace_api::Provider::GetTracerProvider()->GetTracer(name, "1.0.0");
}

OpenTelemetry::SpanWithScopePtr OpenTelemetry::GetSpan(const std::string& tracer_name, const std::string& span_name) {
  if (!tracing_enabled_.load()) {
    return SpanWithScopePtr(nullptr);
  }

  // TODO: add sampling code here. If there is a parent context, create a new span.
  //    Otherwise, use sampling configuration to decide whether to create a new span.

  auto tracer = GetTracer(tracer_name);
  auto span = tracer->StartSpan(span_name);
  return std::make_shared<SpanWithScope>(span);
}

size_t OpenTelemetry::StartSpan(const std::string& tracer_name, const std::string& span_name) {
  if (!tracing_enabled_.load()) {
    return 0;
  }

  CHECK_EQ(std::this_thread::get_id(), main_thread_id_)
      << "StartSpan called from wrong thread - must use postgres backend only";

  auto tracer = GetTracer(tracer_name);

  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span;
  trace_api::StartSpanOptions options;

  span = tracer->StartSpan(span_name, options);

  span_stack_.emplace_back(SpanWithScope{span});
  return span_stack_.size();
}

size_t OpenTelemetry::StartSpanWithTraceParent(const std::string& tracer_name, const std::string& span_name, const std::string& trace_parent) {
  if (!tracing_enabled_.load()) {
    return 0;
  }

  // TODO: add sampling code here for when the parent is empty. Always create a span if there is a parent context.
  CHECK_EQ(std::this_thread::get_id(), main_thread_id_)
      << "StartSpanWithTraceParent called from wrong thread - must use postgres backend only";

  CHECK(span_stack_.empty()) << "StartSpanWithTraceParent should only be called when there are no active spans";

  auto tracer = GetTracer(tracer_name);

  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span;
  trace_api::StartSpanOptions options;
  if (!trace_parent.empty()) {
    options.parent = ParseTraceParent(trace_parent);
  }

  span = tracer->StartSpan(span_name, options);

  span_stack_.emplace_back(SpanWithScope{span});
  return span_stack_.size();
}

OpenTelemetry::SpanWithScopePtr OpenTelemetry::GetSpanWithParentContext(
    const std::string& tracer_name, const std::string& span_name,
    const opentelemetry::trace::SpanContext& parent_context) {

  if (!tracing_enabled_.load()) {
    return SpanWithScopePtr(nullptr);
  }

  auto tracer = GetTracer(tracer_name);

  trace_api::StartSpanOptions options;
  options.parent = parent_context;

  auto span = tracer->StartSpan(span_name, options);
  return std::make_shared<SpanWithScope>(span);
}

void OpenTelemetry::EndSpan(SpanWithScopePtr span_ptr) {
  if (span_ptr == nullptr) {
    return;
  }
  span_ptr->End();
}

void OpenTelemetry::EndSpan(size_t span_id) {
  if (!tracing_enabled_.load() || span_id == 0) {
    return;
  }
  CHECK_EQ(std::this_thread::get_id(), main_thread_id_)
      << "EndSpan called from wrong thread - must use postgres backend only";

  CHECK(span_id > 0) << "span_id should be greater than 0";
  bool normal_return = (span_id == span_stack_.size());

  while (span_stack_.size() >= span_id) {
    auto& span = span_stack_.back().span;
    span->SetAttribute("normal.return", normal_return);
    span->End();
    span_stack_.pop_back();
  }
}

void OpenTelemetry::ClearAllSpans() {
  if (!tracing_enabled_.load()) {
    return;
  }

  CHECK_EQ(std::this_thread::get_id(), main_thread_id_)
      << "ClearAllSpans called from wrong thread - must use postgres backend only";

  // End all spans in the stack, marking them as not having a normal return
  // since we're clearing them without returning from the function they were created in.
  if (!span_stack_.empty()) {
    EndSpan(1);
  }
}

void OpenTelemetry::SetAttribute(size_t span_id, const std::string& key, const opentelemetry::common::AttributeValue &value) {
  if (!tracing_enabled_.load() || span_id == 0) {
    return;
  }

  CHECK_EQ(std::this_thread::get_id(), main_thread_id_)
      << "SetAttribute called from wrong thread - must use postgres backend only";

  CHECK(span_id > 0) << "span_id should be greater than 0";
  CHECK(span_id <= span_stack_.size()) << "span_id should be less than or equal to the current stack size";

  auto& span = span_stack_[span_id - 1].span;
  if (span) {
      span->SetAttribute(key, value);
  }
}

bool OpenTelemetry::IsTracingEnabled() {
  return tracing_enabled_.load();
}

opentelemetry::trace::SpanContext OpenTelemetry::ParseTraceParent(const std::string& traceparent) {
  if (traceparent.length() != 55) {
      return opentelemetry::trace::SpanContext::GetInvalid();
  }

  const char* p = traceparent.c_str();

  // Skip version "00-"
  p += 3;

  // Parse trace ID (32 hex chars -> 16 bytes)
  uint8_t trace_id[16];
  for (int i = 0; i < 16; i++) {
      std::sscanf(p + i*2, "%2hhx", &trace_id[i]);
  }

  p += 33; // Skip trace ID and '-'

  // Parse span ID (16 hex chars -> 8 bytes)
  uint8_t span_id[8];
  for (int i = 0; i < 8; i++) {
      std::sscanf(p + i*2, "%2hhx", &span_id[i]);
  }

  // Skip span ID and '-'
  p += 17;

  // Parse flags (2 hex chars)
  uint8_t flags;
  std::sscanf(p, "%2hhx", &flags);

  return opentelemetry::trace::SpanContext(
      opentelemetry::trace::TraceId(trace_id),
      opentelemetry::trace::SpanId(span_id),
      opentelemetry::trace::TraceFlags(flags),
      true  // is_remote
  );
}

}  // namespace common
}  // namespace yb