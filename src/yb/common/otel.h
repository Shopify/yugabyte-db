#pragma once

#include <string>
#include <thread>

#include "opentelemetry/trace/scope.h"

namespace yb {
namespace common {

class OpenTelemetry {
public:
  struct SpanWithScope {
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span;
    std::optional<opentelemetry::trace::Scope> scope;
    void SetAttribute(const std::string& key, const opentelemetry::common::AttributeValue &value) {
      if (span) {
        span->SetAttribute(key, value);
      }
    }
    void SetStatus(opentelemetry::v1::trace::StatusCode code, opentelemetry::nostd::string_view description = "") {
      if (span) {
        span->SetStatus(code, description);
      }
    }
    opentelemetry::trace::SpanContext GetContext() const {
      if (span) {
        return span->GetContext();
      }
      return opentelemetry::trace::SpanContext::GetInvalid();
    }
    void End() {
      if (span && span->IsRecording()) {
        scope.reset();
        span->End();
      }
    }

    // Add move constructor and assignment
    SpanWithScope(SpanWithScope&&) = default;
    SpanWithScope& operator=(SpanWithScope&&) = default;

    // Delete copy constructor and assignment
    SpanWithScope(const SpanWithScope&) = delete;
    SpanWithScope& operator=(const SpanWithScope&) = delete;

    SpanWithScope(opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> s)
        : span(std::move(s)), scope(span) {}
    ~SpanWithScope() {
      End();
    }
  };
  using SpanWithScopePtr = std::shared_ptr<SpanWithScope>;

  static void Init(const std::string& service_name);
  static void Shutdown();
  static SpanWithScopePtr
    GetSpan(const std::string& tracer_name, const std::string& span_name);
  static SpanWithScopePtr
    GetSpanWithParentContext(
      const std::string& tracer_name, const std::string& span_name,
      const opentelemetry::trace::SpanContext& parent_context);

  // NOTE: the next four functions are only needed for postgres spans,
  //    which are not using the C++ API and thus need to be managed manually.
  //    Consider keeping this functionality in pggate directly if we don't expect to need it elsewhere.
  //    Also, it can only be used by single threaded application such as postgres backend,
  //    as it relies on a stack to manage span relationships.
  static size_t StartSpan(const std::string& tracer_name, const std::string& span_name);
  static size_t StartSpanWithTraceParent(const std::string& tracer_name, const std::string& span_name, const std::string& trace_parent);
  static void EndSpan(size_t span_id);
  static void ClearAllSpans();
  static void SetAttribute(size_t span_id, const std::string& key, const opentelemetry::common::AttributeValue &value);
  static bool IsTracingEnabled();

private:
  static inline std::vector<SpanWithScope> span_stack_;
  static inline std::atomic<bool> tracing_enabled_{false};
  static inline std::thread::id main_thread_id_;

  static opentelemetry::trace::SpanContext ParseTraceParent(const std::string& traceparent);
  static opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>
    GetTracer(const std::string& name);
};

} // namespace common
} // namespace yb
