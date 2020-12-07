/*-------------------------------------------------------------------------
 *
 * statistics.c
 *    Commands for STATISTICS statements.
 *
 *    We currently support replicating statistics definitions on the
 *    coordinator in all the worker nodes in the form of
 *
 *    CREATE STATISTICS ... queries.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_type.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/commands.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/namespace_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_transaction.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"

static List * GetExplicitStatisticsIdList(Oid relationId);
static void EnsureSequentialModeForStatisticsDDL(void);


/*
 * PreprocessCreateStatisticsStmt is called during the planning phase for
 * CREATE STATISTICS.
 */
List *
PreprocessCreateStatisticsStmt(Node *node, const char *queryString)
{
	CreateStatsStmt *stmt = castNode(CreateStatsStmt, node);

	DDLJob *ddlJob = palloc0(sizeof(DDLJob));
	RangeVar *relation = (RangeVar *) linitial(stmt->relations);
	Oid relationId = RangeVarGetRelid(relation, AccessExclusiveLock, false);

	if (!IsCitusTable(relationId))
	{
		return NIL;
	}

	EnsureCoordinator();

	QualifyTreeNode((Node *) stmt);

	char *ddlCommand = DeparseTreeNode((Node *) stmt);

	EnsureSequentialModeForStatisticsDDL();

	ddlJob->targetRelationId = RangeVarGetRelid(relation, AccessExclusiveLock, false);
	ddlJob->concurrentIndexCmd = false;
	ddlJob->startNewTransaction = false;
	ddlJob->commandString = ddlCommand;
	ddlJob->taskList = DDLTaskList(relationId, ddlCommand);

	List *ddlJobs = list_make1(ddlJob);

	return ddlJobs;
}


/*
 * GetExplicitStatisticsCommandList returns the list of DDL commands to create
 * statistics that are explicitly created for the table with relationId. See
 * comment of GetExplicitStatisticsIdList function.
 */
List *
GetExplicitStatisticsCommandList(Oid relationId)
{
	List *createStatisticsCommandList = NIL;

	PushOverrideEmptySearchPath(CurrentMemoryContext);

	List *statisticsIdList = GetExplicitStatisticsIdList(relationId);

	Oid statisticsId = InvalidOid;
	foreach_oid(statisticsId, statisticsIdList)
	{
		char *createStatisticsCommand = pg_get_statisticsobj_worker(statisticsId, true);

		createStatisticsCommandList = lappend(
			createStatisticsCommandList,
			makeTableDDLCommandString(createStatisticsCommand));
	}

	/* revert back to original search_path */
	PopOverrideSearchPath();

	return createStatisticsCommandList;
}


/*
 * GetExplicitStatisticsIdList returns a list of OIDs corresponding to the statistics
 * that are explicitly created on the relation with relationId. That means,
 * this function discards internal statistics implicitly created by postgres.
 */
static List *
GetExplicitStatisticsIdList(Oid relationId)
{
	List *statisticsIdList = NIL;

	Relation pgStatistics = table_open(StatisticExtRelationId, AccessShareLock);

	int scanKeyCount = 1;
	ScanKeyData scanKey[1];

	ScanKeyInit(&scanKey[0], Anum_pg_statistic_ext_stxrelid,
				BTEqualStrategyNumber, F_OIDEQ, relationId);

	bool useIndex = true;
	SysScanDesc scanDescriptor = systable_beginscan(pgStatistics,
													StatisticExtRelidIndexId,
													useIndex, NULL, scanKeyCount,
													scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Oid statisticsId;
#if PG_VERSION_NUM >= PG_VERSION_12
		FormData_pg_statistic_ext *statisticsForm =
			(FormData_pg_statistic_ext *) GETSTRUCT(heapTuple);
		statisticsId = statisticsForm->oid;
#else
		statisticsId = HeapTupleGetOid(heapTuple);
#endif
		statisticsIdList = lappend_oid(statisticsIdList, statisticsId);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(pgStatistics, NoLock);

	return statisticsIdList;
}


/*
 * EnsureSequentialModeForSchemaDDL makes sure that the current transaction is already in
 * sequential mode, or can still safely be put in sequential mode, it errors if that is
 * not possible. The error contains information for the user to retry the transaction with
 * sequential mode set from the begining.
 *
 * Copy-pasted from type.c
 */
static void
EnsureSequentialModeForStatisticsDDL(void)
{
	if (!IsTransactionBlock())
	{
		/* we do not need to switch to sequential mode if we are not in a transaction */
		return;
	}

	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot create or modify statistics because there was a "
							   "parallel operation on a distributed table in the "
							   "transaction"),
						errdetail("When creating a statistics, Citus needs to "
								  "perform all operations over a single connection per "
								  "node to ensure consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail("Statistics is created. To make sure subsequent "
							   "commands see the stats correctly we need to make sure to"
							   " use only one connection for all future commands")));
	SetLocalMultiShardModifyModeToSequential();
}
