/*-------------------------------------------------------------------------
 *
 * yb_pipeline_analyze.c
 *	  Dependency analysis for parallel pipeline execution.
 *
 *	  Extracts table-level read/write access information from PlannedStmt
 *	  metadata and determines which pipeline entries conflict. Produces an
 *	  execution schedule that groups non-conflicting entries for parallel
 *	  dispatch.
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
 * src/backend/tcop/yb_pipeline_analyze.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "tcop/yb_pipeline.h"
#include "utils/portal.h"

#include "pg_yb_utils.h"

/*
 * Local implementation of transaction statement detection.
 * Mirrors the static IsTransactionStmtList in postgres.c.
 */
static bool
yb_is_transaction_stmt_list(List *pstmts)
{
	if (list_length(pstmts) == 1)
	{
		PlannedStmt *pstmt = linitial_node(PlannedStmt, pstmts);

		if (pstmt->commandType == CMD_UTILITY &&
			IsA(pstmt->utilityStmt, TransactionStmt))
			return true;
	}
	return false;
}

/*
 * Walk the plan tree to find ModifyTable nodes and check for ON CONFLICT.
 */
static bool
yb_plan_has_upsert(Plan *plan)
{
	if (plan == NULL)
		return false;

	if (IsA(plan, ModifyTable))
	{
		ModifyTable *mt = (ModifyTable *) plan;
		if (mt->onConflictAction != ONCONFLICT_NONE)
			return true;
	}

	if (yb_plan_has_upsert(plan->lefttree))
		return true;
	if (yb_plan_has_upsert(plan->righttree))
		return true;

	return false;
}

/*
 * YbPipelineExtractAccessInfo
 *
 * Populate a pipeline entry's access metadata (read_rels, write_rels,
 * is_upsert, is_txn_control) from the portal's PlannedStmt.
 */
void
YbPipelineExtractAccessInfo(YbPipelineEntry *entry)
{
	Portal		portal;
	PlannedStmt *pstmt;
	ListCell   *lc;
	Bitmapset  *write_rtindexes = NULL;

	entry->read_rels = NIL;
	entry->write_rels = NIL;
	entry->is_upsert = false;
	entry->is_txn_control = false;

	portal = GetPortalByName(entry->portal_name);
	if (!PortalIsValid(portal))
		return;

	entry->portal = portal;

	if (portal->stmts == NIL)
		return;

	/* Check for transaction control statements. */
	if (yb_is_transaction_stmt_list(portal->stmts))
	{
		entry->is_txn_control = true;
		return;
	}

	pstmt = PortalGetPrimaryStmt(portal);
	entry->pstmt = pstmt;
	entry->cmd_type = pstmt->commandType;

	/* Utility statements are pipeline barriers. */
	if (pstmt->commandType == CMD_UTILITY)
	{
		entry->is_txn_control = true;
		return;
	}

	/*
	 * Build a bitmapset of rtable indexes that are write targets so we can
	 * distinguish read relations from write relations.
	 */
	foreach(lc, pstmt->resultRelations)
	{
		int		rti = lfirst_int(lc);
		write_rtindexes = bms_add_member(write_rtindexes, rti);
	}

	/*
	 * Walk the range table.  Each entry with a valid relid is either a read
	 * relation (not in resultRelations) or a write relation.
	 */
	{
		int		rti = 0;
		foreach(lc, pstmt->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
			rti++;

			if (rte->rtekind != RTE_RELATION)
				continue;

			if (bms_is_member(rti, write_rtindexes))
				entry->write_rels = lappend_oid(entry->write_rels, rte->relid);
			else
				entry->read_rels = lappend_oid(entry->read_rels, rte->relid);
		}
	}

	/* Check for ON CONFLICT (UPSERT). */
	if (pstmt->commandType == CMD_INSERT && pstmt->planTree)
		entry->is_upsert = yb_plan_has_upsert(pstmt->planTree);

	bms_free(write_rtindexes);
}

/*
 * oid_lists_overlap
 *
 * Returns true if any OID appears in both lists.
 */
static bool
oid_lists_overlap(List *a, List *b)
{
	ListCell   *lca;

	foreach(lca, a)
	{
		Oid		oid_a = lfirst_oid(lca);
		ListCell *lcb;

		foreach(lcb, b)
		{
			if (oid_a == lfirst_oid(lcb))
				return true;
		}
	}
	return false;
}

/*
 * YbPipelineConflicts
 *
 * Determine whether two pipeline entries conflict and must therefore be
 * executed sequentially.  Conflict rules:
 *
 *   READ  vs READ  (any tables)          -> no conflict
 *   READ  vs WRITE (different tables)    -> no conflict
 *   READ  vs WRITE (same table)          -> conflict
 *   WRITE vs READ  (same table)          -> conflict
 *   WRITE vs WRITE (different tables)    -> no conflict
 *   WRITE vs WRITE (same table)          -> conflict
 *   UPSERT vs any write (same table)     -> conflict
 *
 * Transaction-control entries always conflict with everything.
 */
bool
YbPipelineConflicts(const YbPipelineEntry *a, const YbPipelineEntry *b)
{
	/* Transaction control commands are always barriers. */
	if (a->is_txn_control || b->is_txn_control)
		return true;

	/*
	 * Two pure reads never conflict.  A "pure read" has no write_rels and is
	 * not an upsert.
	 */
	bool a_writes = (a->write_rels != NIL);
	bool b_writes = (b->write_rels != NIL);

	if (!a_writes && !b_writes)
		return false;

	/* WRITE vs WRITE on the same table. */
	if (a_writes && b_writes && oid_lists_overlap(a->write_rels, b->write_rels))
		return true;

	/* UPSERT is conservatively treated as conflicting with any same-table write. */
	if (a->is_upsert && b_writes && oid_lists_overlap(a->write_rels, b->write_rels))
		return true;
	if (b->is_upsert && a_writes && oid_lists_overlap(b->write_rels, a->write_rels))
		return true;

	/*
	 * READ vs WRITE on the same table: the read might need to see the write
	 * (read-after-write) or the write ordering might matter (write-after-read).
	 */
	if (a_writes && oid_lists_overlap(a->write_rels, b->read_rels))
		return true;
	if (b_writes && oid_lists_overlap(b->write_rels, a->read_rels))
		return true;

	return false;
}

/*
 * YbPipelineSchedule
 *
 * Assign each pipeline entry to an execution group using a sliding-window
 * approach.  Entries are processed in pipeline order.  Each entry is checked
 * against all entries in currently in-flight groups; if there is no conflict,
 * it joins the current group.  If there is a conflict, a new group is started
 * after the conflicting entry's group.
 *
 * The max_parallel GUC limits how many entries can be in a single group.
 */
void
YbPipelineSchedule(YbPipeline *pipeline)
{
	int		max_parallel = yb_pipeline_max_parallel_queries;
	int		current_group = 0;
	int		current_group_size = 0;
	int		i, j;

	if (pipeline->num_entries == 0)
	{
		pipeline->num_groups = 0;
		return;
	}

	for (i = 0; i < pipeline->num_entries; i++)
		YbPipelineExtractAccessInfo(&pipeline->entries[i]);

	/* First entry always goes in group 0. */
	pipeline->entries[0].group_id = 0;
	current_group_size = 1;

	for (i = 1; i < pipeline->num_entries; i++)
	{
		YbPipelineEntry *entry = &pipeline->entries[i];
		int		required_group = current_group;
		bool	conflict_found = false;

		/*
		 * Check against all previous entries that are in the current group
		 * or any group that hasn't completed yet (i.e., from current_group
		 * backward to the earliest still-relevant group).
		 */
		for (j = 0; j < i; j++)
		{
			YbPipelineEntry *prev = &pipeline->entries[j];

			if (YbPipelineConflicts(entry, prev))
			{
				int		needed = prev->group_id + 1;
				if (needed > required_group)
					required_group = needed;
				conflict_found = true;
			}
		}

		if (required_group > current_group)
		{
			current_group = required_group;
			current_group_size = 0;
		}
		else if (conflict_found)
		{
			/* Conflict with something in earlier group, but we're already past it. */
		}

		/* Enforce max parallel queries per group. */
		if (current_group_size >= max_parallel)
		{
			current_group++;
			current_group_size = 0;
		}

		entry->group_id = current_group;
		current_group_size++;
	}

	pipeline->num_groups = current_group + 1;
}
