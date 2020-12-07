CREATE SCHEMA "statistics'test";

SET search_path TO "statistics'test";
SET citus.next_shard_id TO 980000;
SET client_min_messages TO WARNING;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

-- test create statistics propagation
CREATE TABLE test_stats (
    a   int,
    b   int
);

SELECT create_distributed_table('test_stats', 'a');

CREATE STATISTICS s1 (dependencies) ON a, b FROM test_stats;

-- test for distributing an already existing statistics
CREATE TABLE test_stats2 (
    a   int,
    b   int
);

CREATE STATISTICS s2 (dependencies) ON a, b FROM test_stats;

SELECT create_distributed_table('test_stats2', 'a');

DROP SCHEMA "statistics'test" CASCADE;
