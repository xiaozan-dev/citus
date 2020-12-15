/*-------------------------------------------------------------------------
 *
 * create_table_utils.c
 *
 * <Some words here>
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_constraint.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/commands.h"
#include "distributed/foreign_key_relationship.h"
#include "distributed/listutils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/worker_protocol.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


static void LockRelationsWithLockMode(List *relationIdList, LOCKMODE lockMode);
static void DropRelationFKeys(Oid relationId);
static List * GetRelationDropFkeyCommands(Oid relationId);
static char * GetRelationFkeyCascadeDropCommand(Oid relationId, Oid fkeyId);


void
CreateTableCascade(Oid relationId, LOCKMODE relLockMode, ConvertTableFunc convertTableFunc)
{
	/* we don't want foreign key graph to change */
	LockRelationOid(ConstraintRelationId, ExclusiveLock);

	/* including itself */
	List *fkeyConnectedRelations = GetFkeyConnectedRelations(relationId);

	LockRelationsWithLockMode(fkeyConnectedRelations, relLockMode);

	List *allFKeyCreationCommands = NIL;

	Oid graphRelId;
	foreach_oid(graphRelId, fkeyConnectedRelations)
	{
		/* store them before dropping */
		List *relFKeyCreationCommands = GetReferencingForeignConstaintCommands(graphRelId);
		allFKeyCreationCommands = list_concat(allFKeyCreationCommands, relFKeyCreationCommands);

		DropRelationFKeys(graphRelId);
	}

	/* dropped all foreign keys, execute convertTableFunc for each table in subgraph */
	foreach_oid(graphRelId, fkeyConnectedRelations)
	{
		/* don't cascade anymore */
		convertTableFunc(graphRelId, false);
	}

	/* now recreate foreign keys on citus local tables */
	ExecuteAndLogDDLCommandList(allFKeyCreationCommands);
}


static void
LockRelationsWithLockMode(List *relationIdList, LOCKMODE lockMode)
{
	/* first sort them to prevent dead-locks */
	relationIdList = SortList(relationIdList, CompareOids);

	Oid relationId;
	foreach_oid(relationId, relationIdList)
	{
		LockRelationOid(relationId, lockMode);
	}
}


static void
DropRelationFKeys(Oid relationId)
{
	List *commands = GetRelationDropFkeyCommands(relationId);
	ExecuteAndLogDDLCommandList(commands);
}


static List *
GetRelationDropFkeyCommands(Oid relationId)
{
	List *commands = NIL;

	Oid fkeyId;
	List *relFkeyIds = GetForeignKeyOids(relationId, INCLUDE_REFERENCING_CONSTRAINTS);
	foreach_oid(fkeyId, relFkeyIds)
	{
		char *command = GetRelationFkeyCascadeDropCommand(relationId, fkeyId);
		commands = lappend(commands, command);
	}

	return commands;
}


static char *
GetRelationFkeyCascadeDropCommand(Oid relationId, Oid fkeyId)
{
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);

	char *constraintName = get_constraint_name(fkeyId);
	const char *quotedConstraintName = quote_identifier(constraintName);

	StringInfo dropFkeyCascadeCommand = makeStringInfo();
	appendStringInfo(dropFkeyCascadeCommand, "ALTER TABLE %s DROP CONSTRAINT %s CASCADE;",
					 qualifiedRelationName, quotedConstraintName);

	return dropFkeyCascadeCommand->data;
}


/*
 * ExecuteAndLogDDLCommandList takes a list of ddl commands and calls
 * ExecuteAndLogDDLCommand function for each of them.
 */
void
ExecuteAndLogDDLCommandList(List *ddlCommandList)
{
	char *ddlCommand = NULL;
	foreach_ptr(ddlCommand, ddlCommandList)
	{
		ExecuteAndLogDDLCommand(ddlCommand);
	}
}


/*
 * ExecuteAndLogDDLCommand takes a ddl command and logs it in DEBUG4 log level.
 * Then, parses and executes it via CitusProcessUtility.
 */
void
ExecuteAndLogDDLCommand(const char *commandString)
{
	ereport(DEBUG4, (errmsg("executing \"%s\"", commandString)));

	Node *parseTree = ParseTreeNode(commandString);
	CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
						NULL, None_Receiver, NULL);
}
