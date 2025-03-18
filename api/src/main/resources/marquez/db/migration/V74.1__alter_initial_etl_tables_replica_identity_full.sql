-- Implementing the replica identity full for the initial ETL tables
-- It's required to support streaming CDC
ALTER TABLE column_lineage REPLICA IDENTITY FULL;
ALTER TABLE lineage_events REPLICA IDENTITY FULL;
ALTER TABLE datasets REPLICA IDENTITY FULL;
ALTER TABLE dataset_versions REPLICA IDENTITY FULL;
ALTER TABLE dataset_fields REPLICA IDENTITY FULL;
ALTER TABLE jobs REPLICA IDENTITY FULL;
ALTER TABLE job_versions REPLICA IDENTITY FULL;
ALTER TABLE job_facets REPLICA IDENTITY FULL;
ALTER TABLE job_versions_io_mapping REPLICA IDENTITY FULL;
ALTER TABLE job_versions_io_mapping_inputs REPLICA IDENTITY FULL;
ALTER TABLE job_versions_io_mapping_outputs REPLICA IDENTITY FULL;