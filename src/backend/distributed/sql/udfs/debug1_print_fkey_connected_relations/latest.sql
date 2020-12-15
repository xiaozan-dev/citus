CREATE OR REPLACE FUNCTION pg_catalog.debug1_print_fkey_connected_relations(table_name regclass)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$debug1_print_fkey_connected_relations$$;
COMMENT ON FUNCTION pg_catalog.debug1_print_fkey_connected_relations(table_name regclass)
	IS 'debug fkey graph for relation';
