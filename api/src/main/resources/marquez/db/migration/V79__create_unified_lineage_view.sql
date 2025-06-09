-- Create unified view combining local and federated lineage
CREATE OR REPLACE VIEW unified_column_lineage AS
SELECT 
  output_dataset_version_uuid,
  output_dataset_field_uuid,
  input_dataset_version_uuid,
  input_dataset_field_uuid,
  transformation_description,
  transformation_type,
  created_at,
  updated_at,
  'local' as lineage_source
FROM tmp_column_lineage_latest  -- Use the optimized temp table
-- Add UNION ALL clauses for federated sources when ready
;