/*-------------------------------------------------------------------------
 *
 * yb_pipeline_exec.c
 *	  Parallel pipeline execution engine for YugabyteDB YSQL.
 *
 *	  Implements buffering of extended query protocol Execute messages,
 *	  scheduling them into parallel groups via dependency analysis, and
 *	  executing each group.  Currently groups are executed sequentially;
 *	  the scheduling infrastructure enables future async RPC overlap.
 *
 * Copyright (c) YugabyteDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * src/backend/tcop/yb_pipeline_exec.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "miscadmin.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tcop/yb_pipeline.h"
#include "utils/memutils.h"
#include "utils/portal.h"

#include "pg_yb_utils.h"

#define YB_PIPELINE_INIT_CAPACITY 16

/* ----------------------------------------------------------------
 *		Pipeline lifecycle
 * ----------------------------------------------------------------
 */

YbPipeline *
YbPipelineCreate(void)
{
	YbPipeline *pipeline = (YbPipeline *) palloc0(sizeof(YbPipeline));
	pipeline->capacity = YB_PIPELINE_INIT_CAPACITY;
	pipeline->entries = (YbPipelineEntry *) palloc0(
		sizeof(YbPipelineEntry) * pipeline->capacity);
	pipeline->num_entries = 0;
	pipeline->num_groups = 0;
	return pipeline;
}

void
YbPipelineAddEntry(YbPipeline *pipeline, const char *portal_name,
				   long max_rows)
{
	if (pipeline->num_entries >= pipeline->capacity)
	{
		pipeline->capacity *= 2;
		pipeline->entries = (YbPipelineEntry *) repalloc(
			pipeline->entries,
			sizeof(YbPipelineEntry) * pipeline->capacity);
	}

	YbPipelineEntry *entry = &pipeline->entries[pipeline->num_entries];
	memset(entry, 0, sizeof(YbPipelineEntry));
	entry->portal_name = pstrdup(portal_name);
	entry->max_rows = max_rows;
	entry->group_id = -1;
	pipeline->num_entries++;
}

void
YbPipelineReset(YbPipeline *pipeline)
{
	for (int i = 0; i < pipeline->num_entries; i++)
	{
		YbPipelineEntry *entry = &pipeline->entries[i];
		list_free(entry->read_rels);
		list_free(entry->write_rels);
	}
	pipeline->num_entries = 0;
	pipeline->num_groups = 0;
}

/* ----------------------------------------------------------------
 *		Pipeline execution
 * ----------------------------------------------------------------
 */

/*
 * YbPipelineExecute
 *
 * Execute all entries in the pipeline.  Entries are first analyzed for
 * dependencies and assigned to execution groups.  Within each group,
 * entries execute sequentially using the standard executor path.
 *
 * Entries across different groups are separated by a dependency barrier:
 * all entries in group N must complete before group N+1 begins.
 *
 * The dependency analysis and grouping infrastructure is designed to
 * support future truly parallel execution (via pggate deferred flush),
 * where independent entries in the same group would have their RPCs
 * overlapped.
 *
 * This function is called from the Sync handler in PostgresMain when
 * yb_enable_pipeline_parallelism is enabled and >1 Execute messages
 * were buffered.
 */
void
YbPipelineExecute(YbPipeline *pipeline)
{
	int		group;

	/* Analyze dependencies and build execution schedule. */
	YbPipelineSchedule(pipeline);

	elog(DEBUG1, "YB pipeline: %d entries in %d groups",
		 pipeline->num_entries, pipeline->num_groups);

	/*
	 * Mark this as a batched execution so that existing YB optimizations
	 * (e.g., deferred flush, retry suppression) apply.
	 */
	YbSetIsBatchedExecution(true);

	PG_TRY();
	{
		for (group = 0; group < pipeline->num_groups; group++)
		{
			for (int i = 0; i < pipeline->num_entries; i++)
			{
				YbPipelineEntry *entry = &pipeline->entries[i];

				if (entry->group_id != group)
					continue;

				CHECK_FOR_INTERRUPTS();

				yb_exec_execute_message_for_pipeline(entry->portal_name,
													 entry->max_rows);
			}
		}
	}
	PG_CATCH();
	{
		YbSetIsBatchedExecution(false);
		PG_RE_THROW();
	}
	PG_END_TRY();

	YbSetIsBatchedExecution(false);
}
