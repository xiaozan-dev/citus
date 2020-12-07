/*-------------------------------------------------------------------------
 *
 * qualify_statistics_stmt.c
 *	  Functions specialized in fully qualifying all statistics statements.
 *    These functions are dispatched from qualify.c
 *
 *	  Goal would be that the deparser functions for these statements can
 *	  serialize the statement without any external lookups.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

void
QualifyCreateStatisticsStmt(Node *node)
{
	CreateStatsStmt *stmt = castNode(CreateStatsStmt, node);

	RangeVar *relation = (RangeVar *) linitial(stmt->relations);

	if (relation->schemaname == NULL)
	{
		Oid tableOid = RelnameGetRelid(relation->relname);
		Oid schemaOid = get_rel_namespace(tableOid);
		relation->schemaname = get_namespace_name(schemaOid);
	}

	if (list_length(stmt->defnames) == 1)
	{
		StringInfoData schemaName;
		initStringInfo(&schemaName);

		/* if no schema name is set, use search_path */
		List *searchPath = fetch_search_path(false);

		if (searchPath == NIL)
		{
			appendStringInfo(&schemaName, "public");
		}
		else
		{
			Oid schemaId = linitial_oid(searchPath);
			char *fetchedNamespaceName = get_namespace_name(schemaId);
			appendStringInfo(&schemaName, "%s", fetchedNamespaceName);
		}

		Value *statName = linitial(stmt->defnames);
		stmt->defnames = list_make2(makeString(pstrdup(schemaName.data)), statName);
	}
}
