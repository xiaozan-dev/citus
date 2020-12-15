-- citus--9.5-1--10.0-1

DROP FUNCTION IF EXISTS pg_catalog.citus_total_relation_size(regclass);

#include "udfs/citus_total_relation_size/10.0-1.sql"
#include "udfs/citus_tables/10.0-1.sql"
#include "udfs/citus_finish_pg_upgrade/10.0-1.sql"
#include "udfs/debug1_print_fkey_connected_relations/10.0-1.sql"
#include "udfs/create_citus_local_table/10.0-1.sql"
#include "udfs/undistribute_table/10.0-1.sql"

#include "../../columnar/sql/columnar--9.5-1--10.0-1.sql"
