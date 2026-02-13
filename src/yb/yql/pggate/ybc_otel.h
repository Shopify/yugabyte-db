#pragma once
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

void YBCOtelInit();
void YBCOtelShutdown();

size_t YBCOtelStartSpan(const char* span_name);
size_t YBCOtelStartSpanWithTraceParent(const char* span_name, const char* traceparent);
void YBCOtelEndSpan(size_t span_id);
void YBCOtelClearAllSpans();
void YBCOtelSetAttributeStr(size_t span_id, const char* key, const char* value);
void YBCOtelSetAttributeInt64(size_t span_id, const char* key, int64_t value);
void YBCOtelSetAttributeInt32(size_t span_id, const char* key, int32_t value);
void YBCOtelSetAttributeBool(size_t span_id, const char* key, bool value);
bool YBCOtelIsTracingEnabled();

#ifdef __cplusplus
}  // extern "C"
#endif
