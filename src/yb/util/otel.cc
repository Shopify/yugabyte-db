#include "otel.h"

#include "opentelemetry/exporters/otlp/otlp_http_exporter.h"
#include "opentelemetry/sdk/trace/batch_span_processor.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"

#include "yb/util/logging.h"

namespace yb {
namespace util {

namespace otlp = opentelemetry::exporter::otlp;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace resource = opentelemetry::sdk::resource;
namespace trace_api = opentelemetry::trace;

void OpenTelemetry::Init() {

  otlp::OtlpHttpExporterOptions exporter_opts;
  exporter_opts.url = "http://localhost:4318/v1/traces";  // TODO: Make configurable via flag

  auto exporter = std::make_unique<otlp::OtlpHttpExporter>(exporter_opts);
  
  trace_sdk::BatchSpanProcessorOptions batch_options;
  auto processor = std::make_unique<trace_sdk::BatchSpanProcessor>(
      std::move(exporter), batch_options);
  
  auto resource_attributes = resource::ResourceAttributes{
      {"service.name", "yb-server"},
      {"service.version", "1.0.0"}  // TODO: Use actual version
  };
  auto resource_ptr = resource::Resource::Create(resource_attributes);
  
  auto provider = opentelemetry::nostd::shared_ptr<trace_api::TracerProvider>(
      new trace_sdk::TracerProvider(std::move(processor), resource_ptr));
  trace_api::Provider::SetTracerProvider(provider);

  LOG(INFO) << "OpenTelemetry initialized";
}

void OpenTelemetry::Shutdown() {
  auto provider = trace_api::Provider::GetTracerProvider();
  if (provider) {
    // Cast using static_cast on the raw pointer, then wrap in shared_ptr
    auto* sdk_provider = static_cast<trace_sdk::TracerProvider *>(provider.get());
    sdk_provider->Shutdown();

    opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider> none(nullptr);
    trace_api::Provider::SetTracerProvider(none);

    LOG(INFO) << "OpenTelemetry shutdown completed";
  } else {
    LOG(WARNING) << "No TracerProvider found during OpenTelemetry shutdown";
  }
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> OpenTelemetry::GetTracer(
    const std::string& name) {
  return trace_api::Provider::GetTracerProvider()->GetTracer(name, "1.0.0");
}

}  // namespace util
}  // namespace yb