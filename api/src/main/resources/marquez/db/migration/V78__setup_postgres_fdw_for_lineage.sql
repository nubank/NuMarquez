-- Enable postgres_fdw extension
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Example federated server setup (customize for your environment)
-- CREATE SERVER your_remote_server
-- FOREIGN DATA WRAPPER postgres_fdw
-- OPTIONS (host 'remote-host', port '5432', dbname 'remote_marquez');

-- CREATE USER MAPPING FOR CURRENT_USER
-- SERVER your_remote_server  
-- OPTIONS (user 'remote_user', password 'remote_password');

-- Create foreign table template (uncomment and customize)
-- CREATE FOREIGN TABLE remote_column_lineage (
--   output_dataset_version_uuid UUID,
--   output_dataset_field_uuid UUID,
--   input_dataset_version_uuid UUID,
--   input_dataset_field_uuid UUID,
--   transformation_description TEXT,
--   transformation_type TEXT,
--   created_at TIMESTAMPTZ,
--   updated_at TIMESTAMPTZ
-- ) SERVER your_remote_server
-- OPTIONS (schema_name 'public', table_name 'column_lineage');