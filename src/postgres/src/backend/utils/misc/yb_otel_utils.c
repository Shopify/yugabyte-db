#include "postgres.h"
#include "storage/ipc.h"

#include "yb_otel_utils.h"

void
YbSetUpOtel(void)
{
	YBCOtelInit();
	on_proc_exit(YBCOtelShutdown, 0);
}

/* Buffer to store the current traceparent for OTEL tracing */
#define YB_TRACEPARENT_LEN 56
static char yb_current_traceparent[YB_TRACEPARENT_LEN] = {0};

/*
* Validate and parse W3C traceparent format:
* 2-hex-digit version, 32-hex-digit trace-id, 16-hex-digit span-id, 2-hex-digit flags
* Format: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
*
* Returns: length of valid traceparent (55 chars), or 0 if invalid
*/
static size_t
YbValidateTraceparent(const char* str) {
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


void
YbReadTraceParentFromQuery(const char *query_string)
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

		// Skip whitespace
		while (*pos && isspace((unsigned char)*pos))
			pos++;

		// Check for "traceparent"
		if (strncmp(pos, "traceparent:", 12) != 0) {
			// Not a traceparent comment, keep searching
			continue;
		}
		pos += 12;

		// Skip whitespace
		while (*pos && (isspace((unsigned char)*pos)))
			pos++;

		const char* value_start = pos;

		// Found complete traceparent value and return its length.
		size_t len = YbValidateTraceparent(value_start);
		if (len != YB_TRACEPARENT_LEN - 1) {
			// Invalid traceparent format, return without setting it.
			return;
		}
		pos += len;

		// If we reached the end of the string right after the traceparent value,
		// then do not bother setting it. The string is malformed.
		if (*pos == '\0') {
			return;
		}

		memcpy(yb_current_traceparent, value_start, len);
		yb_current_traceparent[len] = '\0';
		return;
	}
}

const char *
YbGetCurrentTraceparent(void)
{
	return yb_current_traceparent;
}
