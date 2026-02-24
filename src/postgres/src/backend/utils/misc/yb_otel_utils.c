#include "postgres.h"
#include "storage/ipc.h"

#include "yb_otel_utils.h"

void
YbSetUpOtel(void)
{
	YBCOtelInit();
	on_proc_exit(YBCOtelShutdown, 0);
}
