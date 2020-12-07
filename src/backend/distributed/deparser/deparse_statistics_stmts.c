/*-------------------------------------------------------------------------
 *
 * deparse_statistics_stmts.c
 *	  All routines to deparse statistics statements.
 *	  This file contains all entry points specific for statistics statement deparsing
 *    as well as functions that are currently only used for deparsing of the statistics
 *    statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "distributed/relay_utility.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "utils/builtins.h"

static void AppendCreateStatisticsStmt(StringInfo buf, CreateStatsStmt *stmt);
static void AppendStatisticsName(StringInfo buf, CreateStatsStmt *stmt);
static void AppendStatTypes(StringInfo buf, CreateStatsStmt *stmt);
static void AppendColumnNames(StringInfo buf, CreateStatsStmt *stmt);
static void AppendTableName(StringInfo buf, CreateStatsStmt *stmt);

char *
DeparseCreateStatisticsStmt(Node *node)
{
	CreateStatsStmt *stmt = castNode(CreateStatsStmt, node);

	StringInfoData str;
	initStringInfo(&str);

	AppendCreateStatisticsStmt(&str, stmt);

	return str.data;
}


static void
AppendCreateStatisticsStmt(StringInfo buf, CreateStatsStmt *stmt)
{
	appendStringInfo(buf, "CREATE STATISTICS ");

	appendStringInfo(buf, "%s", stmt->if_not_exists ? "IF NOT EXISTS " : "");

	AppendStatisticsName(buf, stmt);

	AppendStatTypes(buf, stmt);

	appendStringInfo(buf, " ON ");

	AppendColumnNames(buf, stmt);

	appendStringInfo(buf, " FROM ");

	AppendTableName(buf, stmt);

	appendStringInfo(buf, ";");
}


static void
AppendStatisticsName(StringInfo buf, CreateStatsStmt *stmt)
{
	Value *schemaNameVal = (Value *) linitial(stmt->defnames);
	char *schemaName = strVal(schemaNameVal);

	Value *statNameVal = (Value *) lsecond(stmt->defnames);
	char *statName = strVal(statNameVal);

	appendStringInfo(buf, "%s.%s", quote_identifier(schemaName), quote_identifier(
						 statName));
}


static void
AppendStatTypes(StringInfo buf, CreateStatsStmt *stmt)
{
	if (list_length(stmt->stat_types) == 0)
	{
		return;
	}

	appendStringInfo(buf, " (");

	ListCell *cell = NULL;
	foreach(cell, stmt->stat_types)
	{
		Value *statType = (Value *) lfirst(cell);

		appendStringInfoString(buf, strVal(statType));

		if (cell != list_tail(stmt->stat_types))
		{
			appendStringInfo(buf, ", ");
		}
	}

	appendStringInfo(buf, ")");
}


static void
AppendColumnNames(StringInfo buf, CreateStatsStmt *stmt)
{
	ListCell *cell = NULL;
	foreach(cell, stmt->exprs)
	{
		Node *node = (Node *) lfirst(cell);
		Assert(IsA(node, ColumnRef));

		ColumnRef *column = (ColumnRef *) node;
		Assert(list_length(column->fields) == 1);

		char *columnName = strVal((Value *) linitial(column->fields));

		appendStringInfoString(buf, columnName);

		if (cell != list_tail(stmt->exprs))
		{
			appendStringInfo(buf, ", ");
		}
	}
}


static void
AppendTableName(StringInfo buf, CreateStatsStmt *stmt)
{
	/* statistics' can be created with only one relation */
	Assert(list_length(stmt->relations));
	RangeVar *relation = (RangeVar *) linitial(stmt->relations);
	char *relationName = relation->relname;
	char *schemaName = relation->schemaname;

	appendStringInfo(buf, "%s.%s", quote_identifier(schemaName), quote_identifier(
						 relationName));
}
