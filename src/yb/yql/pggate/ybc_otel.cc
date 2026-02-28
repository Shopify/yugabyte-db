#include "yb/common/otel.h"

#include "yb/yql/pggate/ybc_otel.h"


/* Buffer to store the current traceparent for OTEL tracing */
#define YB_TRACEPARENT_LEN 56
static char yb_current_traceparent[YB_TRACEPARENT_LEN] = {0};

/*
* Validate and parse W3C traceparent format:
* 2-hex-digit version, 32-hex-digit trace-id, 16-hex-digit span-id, 2-hex-digit flags
* Format: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
*
* Returns: length of valid traceparent (55 chars), or 0 if invalid
*
* Query comments follow the sqlcommenter format with key='value' pairs, e.g.:
*   application='myapp',traceparent='00-abc...-01'
* wrapped in a SQL block comment.
*/
static size_t
YBCValidateTraceparent(const char* str) {
	const char* pos = str;
	int i;

	// Version must be "00" (only supported version)
	if (pos[0] != '0' || pos[1] != '0')
		return 0;
	pos += 2;

	// Separator
	if (*pos != '-')
		return 0;
	pos++;

	// Trace ID: 32 hex digits
	for (i = 0; i < 32; i++) {
		if (!isxdigit((unsigned char)*pos))
			return 0;
		pos++;
	}

	// Separator
	if (*pos != '-')
		return 0;
	pos++;

	// Parent/Span ID: 16 hex digits
	for (i = 0; i < 16; i++) {
		if (!isxdigit((unsigned char)*pos))
			return 0;
		pos++;
	}

	// Separator
	if (*pos != '-')
		return 0;
	pos++;

	// Trace flags: 2 hex digits
	for (i = 0; i < 2; i++) {
		if (!isxdigit((unsigned char)*pos))
			return 0;
		pos++;
	}

	// Total: 2 + 1 + 32 + 1 + 16 + 1 + 2 = 55 characters
	return pos - str;
}

static void
YBCReadTraceParentFromQuery(const char *query_string)
{
	if (!YBCOtelIsTracingEnabled()) {
		return;
	}

	// Clear current traceparent before parsing a new one
	yb_current_traceparent[0] = '\0';
	if (query_string == NULL) {
		return;
	}

	const char* pos = query_string;

	// Look for comment start
	while ((pos = strstr(pos, "/*")) != NULL) {
		pos += 2;  // Skip past "/*"

		const char* comment_end = strstr(pos, "*/");
		if (comment_end == NULL) {
			return;  // Malformed comment
		}

		// Parse comma-separated key='value' pairs within this comment (sqlcommenter format)
		while (pos < comment_end) {
			// Skip whitespace
			while (*pos && isspace((unsigned char)*pos))
				pos++;

			// Check for "traceparent=" (sqlcommenter format: key='value')
			if (strncmp(pos, "traceparent=", 12) == 0) {
				pos += 12;

				// Skip whitespace after equals sign
				while (*pos && isspace((unsigned char)*pos))
					pos++;

				// Require opening single quote
				if (*pos != '\'')
					break;
				pos++;

				const char* value_start = pos;

				// Validate traceparent value
				size_t len = YBCValidateTraceparent(value_start);
				if (len == YB_TRACEPARENT_LEN - 1 && value_start[len] == '\'') {
					memcpy(yb_current_traceparent, value_start, len);
					yb_current_traceparent[len] = '\0';
					return;
				}
				// Invalid format or missing closing quote, continue searching
			}

			// Find next comma or end of comment
			const char* next_comma = strchr(pos, ',');
			if (next_comma == NULL || next_comma >= comment_end) {
				break;  // No more pairs in this comment
			}
			pos = next_comma + 1;  // Skip past comma
		}

		// Move past this comment to search for next one
		pos = comment_end + 2;
	}
}


void YBCOtelInit() {
    yb::common::OpenTelemetry::Init("yb-pg-backend");
}

void YBCOtelShutdown() {
    yb::common::OpenTelemetry::Shutdown();
}

size_t YBCOtelStartSpan(const char* span_name, const char* query_string) {
    if (!YBCOtelIsTracingEnabled()) {
        return 0;
    }
    size_t span_id = 0;
    if (query_string != nullptr && yb::common::OpenTelemetry::GetLastSpanId() == 0) {
        YBCReadTraceParentFromQuery(query_string);
        span_id = yb::common::OpenTelemetry::StartSpanWithTraceParent("postgres-backend", span_name, yb_current_traceparent);
    } else {
        span_id = yb::common::OpenTelemetry::StartSpan("postgres-backend", span_name);
    }
    if (query_string != nullptr) {
        YBCOtelSetAttributeStr(span_id, "query", query_string);
    }
    return span_id;
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

