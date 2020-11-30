-- citus--9.5-1--10.0-1

-- bump version to 10.0-1

#include "udfs/citus_finish_pg_upgrade/10.0-1.sql"

#include "../../columnar/sql/columnar--9.5-1--10.0-1.sql"

CREATE OR REPLACE FUNCTION shard_size(nodename text, nodeport int, logicalrelid regclass, shardId bigint)
RETURNS bigint AS $$
DECLARE shard_size BIGINT DEFAULT NULL;
BEGIN
    BEGIN
        SELECT result INTO shard_size 
         FROM master_run_on_worker(ARRAY[nodename], ARRAY[nodeport], ARRAY[CONCAT('SELECT pg_total_relation_size(', $one$'$one$  , logicalrelid, '_', shardId, $two$ '$two$, ')' )], false);
    EXCEPTION WHEN OTHERS THEN
        RETURN 0;
    END;
RETURN shard_size;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE VIEW citus_shards AS
SELECT
     pg_dist_shard.shardid, 
     pg_dist_shard.logicalrelid AS table_name,
     CASE WHEN partkey IS NOT NULL THEN 'distributed' WHEN repmodel = 't' THEN 'reference' ELSE 'local' END AS citus_table_type,
     colocationid AS colocation_id,
     pg_dist_node.nodename,
     pg_dist_node.nodeport,
     pg_size_pretty(shard_size(pg_dist_node.nodename, pg_dist_node.nodeport, pg_dist_shard.logicalrelid,  pg_dist_shard.shardid)) as shard_size,
     shard_size(pg_dist_node.nodename, pg_dist_node.nodeport, pg_dist_shard.logicalrelid,  pg_dist_shard.shardid) as shard_size_bytes
FROM
   pg_dist_shard 
JOIN 
   pg_dist_placement
ON 
   pg_dist_shard.shardid = pg_dist_placement.shardid   
JOIN
   pg_dist_node
ON
   pg_dist_placement.groupid = pg_dist_node.groupid      
JOIN
   pg_dist_partition         
ON
   pg_dist_partition.logicalrelid = pg_dist_shard.logicalrelid
;   