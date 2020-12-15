#include "isolation_mx_common.include.spec"

setup
{
  CREATE OR REPLACE FUNCTION trigger_metadata_sync()
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus';

  CREATE TABLE deadlock_detection_test (user_id int UNIQUE, some_val int);
  INSERT INTO deadlock_detection_test SELECT i, i FROM generate_series(1,7) i;
  SELECT create_distributed_table('deadlock_detection_test', 'user_id');

  CREATE TABLE t2(a int);
  SELECT create_distributed_table('t2', 'a');
}

teardown
{
  DROP FUNCTION trigger_metadata_sync();
  DROP TABLE deadlock_detection_test;
  DROP TABLE t2;
  SET citus.shard_replication_factor = 1;
}

session "s1"

step "enable-deadlock-detection"
{
  ALTER SYSTEM SET citus.distributed_deadlock_detection_factor TO 1.1;
}

step "disable-deadlock-detection"
{
  ALTER SYSTEM SET citus.distributed_deadlock_detection_factor TO -1;
}

step "reload-conf"
{
    SELECT pg_reload_conf();
}

step "s1-begin"
{
  BEGIN;
}

step "s1-update-1"
{
  UPDATE deadlock_detection_test SET some_val = 1 WHERE user_id = 1;
}

step "s1-update-2"
{
  UPDATE deadlock_detection_test SET some_val = 1 WHERE user_id = 2;
}

step "s1-commit"
{
  COMMIT;
}

session "s2"

step "s2-start-session-level-connection"
{
	SELECT start_session_level_connection_to_node('localhost', 57638);
}

step "s2-begin-on-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s2-update-1-on-worker"
{
  SELECT run_commands_on_session_level_connection_to_node('UPDATE deadlock_detection_test SET some_val = 2 WHERE user_id = 1');
}

step "s2-update-2-on-worker"
{
  SELECT run_commands_on_session_level_connection_to_node('UPDATE deadlock_detection_test SET some_val = 2 WHERE user_id = 2');
}

step "s2-truncate-on-worker"
{
    SELECT run_commands_on_session_level_connection_to_node('TRUNCATE t2');
}

step "s2-commit-on-worker"
{
  SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

session "s3"

step "s3-begin"
{
  BEGIN;
}

step "s3-invalidate-metadata-and-resync"
{
    update pg_dist_node SET metadatasynced = false;
    SELECT trigger_metadata_sync();
    SELECT pg_sleep(1);
}

step "s3-commit"
{
  COMMIT;
}

permutation "enable-deadlock-detection" "reload-conf" "s2-start-session-level-connection" "s1-begin" "s1-update-1" "s2-begin-on-worker" "s2-update-2-on-worker" "s2-truncate-on-worker" "s3-invalidate-metadata-and-resync" "s2-update-1-on-worker" "s1-update-2" "s1-commit" "s2-commit-on-worker" "disable-deadlock-detection" "reload-conf"
