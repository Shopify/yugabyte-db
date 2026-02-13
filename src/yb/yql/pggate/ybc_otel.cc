#include "yb/common/otel.h"

#include "yb/yql/pggate/ybc_otel.h"

void YBCOtelInit() {
    yb::common::OpenTelemetry::Init("yb-pg-backend");
}

void YBCOtelShutdown() {
    yb::common::OpenTelemetry::Shutdown();
}

size_t YBCOtelStartSpan(const char* span_name) {
    return yb::common::OpenTelemetry::StartSpan("postgres-backend", span_name);
}

size_t YBCOtelStartSpanWithTraceParent(const char* span_name, const char* traceparent) {
    return yb::common::OpenTelemetry::StartSpanWithTraceParent("postgres-backend", span_name, traceparent);
}

void YBCOtelEndSpan(size_t span_id) {
    yb::common::OpenTelemetry::EndSpan(span_id);
}

void YBCOtelClearAllSpans() {
    yb::common::OpenTelemetry::ClearAllSpans();
}

void YBCOtelSetAttributeStr(size_t span_id, const char* key, const char* value) {
    yb::common::OpenTelemetry::SetAttribute(span_id, key, value);
}

void YBCOtelSetAttributeInt64(size_t span_id, const char* key, int64_t value) {
    yb::common::OpenTelemetry::SetAttribute(span_id, key, value);
}

void YBCOtelSetAttributeInt32(size_t span_id, const char* key, int32_t value) {
    yb::common::OpenTelemetry::SetAttribute(span_id, key, value);
}

void YBCOtelSetAttributeBool(size_t span_id, const char* key, bool value) {
    yb::common::OpenTelemetry::SetAttribute(span_id, key, value);
}

bool YBCOtelIsTracingEnabled() {
    return yb::common::OpenTelemetry::IsTracingEnabled();
}

