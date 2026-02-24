/*-------------------------------------------------------------------------
 *
 * yb_pipeline.h
 *	  Parallel pipeline execution for YugabyteDB YSQL.
 *
 *	  Provides dependency analysis and parallel execution of independent
 *	  queries within an extended query protocol pipeline.
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
 * src/include/tcop/yb_pipeline.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef YB_PIPELINE_H
#define YB_PIPELINE_H

#include "nodes/plannodes.h"
#include "utils/portal.h"

/*
 * YbPipelineEntry represents a single Execute message buffered during
 * pipeline collection.  Populated incrementally: portal_name and max_rows
 * are set at collection time; the remaining fields are populated during
 * dependency analysis.
 */
typedef struct YbPipelineEntry
{
	const char *portal_name;
	long		max_rows;

	/* Resolved at analysis time */
	Portal		portal;
	PlannedStmt *pstmt;			/* primary PlannedStmt from portal */
	CmdType		cmd_type;		/* CMD_SELECT, CMD_INSERT, etc. */
	List	   *read_rels;		/* OIDs of tables read (as Oid list) */
	List	   *write_rels;		/* OIDs of tables written */
	bool		is_upsert;		/* ON CONFLICT present */
	bool		is_txn_control;	/* BEGIN/COMMIT/ROLLBACK/SAVEPOINT */

	/* Execution state */
	bool		completed;
	int			group_id;		/* execution group (0-based) */
} YbPipelineEntry;

/*
 * YbPipeline holds the full set of buffered pipeline entries and the
 * computed execution schedule.
 */
typedef struct YbPipeline
{
	YbPipelineEntry *entries;
	int			num_entries;
	int			capacity;
	int			num_groups;		/* number of execution groups after scheduling */
} YbPipeline;

/* Pipeline lifecycle */
extern YbPipeline *YbPipelineCreate(void);
extern void YbPipelineAddEntry(YbPipeline *pipeline, const char *portal_name,
							   long max_rows);
extern void YbPipelineReset(YbPipeline *pipeline);

/* Dependency analysis */
extern void YbPipelineExtractAccessInfo(YbPipelineEntry *entry);
extern bool YbPipelineConflicts(const YbPipelineEntry *a,
								const YbPipelineEntry *b);
extern void YbPipelineSchedule(YbPipeline *pipeline);

/* Parallel execution */
extern void YbPipelineExecute(YbPipeline *pipeline);

#endif							/* YB_PIPELINE_H */
