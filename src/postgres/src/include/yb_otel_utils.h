#ifndef YB_OTEL_UTILS_H
#define YB_OTEL_UTILS_H

#include "yb/yql/pggate/ybc_otel.h"

extern void YbSetUpOtel(void);
extern void YbReadTraceParentFromQuery(const char *query_string);
extern const char* YbGetCurrentTraceparent(void);

#endif							/* YB_OTEL_UTILS_H */