-- Drop the function if it exists (with explicit schema)
DROP FUNCTION IF EXISTS public.refresh_tmp_column_lineage_latest();

-- Create the function with explicit schema
CREATE OR REPLACE FUNCTION public.refresh_tmp_column_lineage_latest()
RETURNS void AS $$
DECLARE
    temp_table_name text := 'tmp_column_lineage_latest_new';
BEGIN
    -- Create a new temporary table with the same structure
    CREATE TEMP TABLE IF NOT EXISTS tmp_column_lineage_latest_new (LIKE public.tmp_column_lineage_latest INCLUDING ALL);
    
    -- Insert fresh data into the new table
    INSERT INTO tmp_column_lineage_latest_new
    SELECT DISTINCT ON (output_dataset_field_uuid, input_dataset_field_uuid) 
        output_dataset_version_uuid,
        output_dataset_field_uuid,
        input_dataset_version_uuid,
        input_dataset_field_uuid,
        transformation_description,
        transformation_type,
        created_at,
        updated_at
    FROM column_lineage
    ORDER BY output_dataset_field_uuid, input_dataset_field_uuid, updated_at DESC;
    
    -- Analyze the new table
    ANALYZE tmp_column_lineage_latest_new;
    
    -- Swap the tables (this is atomic)
    DROP TABLE IF EXISTS public.tmp_column_lineage_latest;
    ALTER TABLE tmp_column_lineage_latest_new RENAME TO tmp_column_lineage_latest;
    ALTER TABLE tmp_column_lineage_latest SET SCHEMA public;
    
    -- Recreate indexes
    CREATE INDEX idx_tmp_column_lineage_latest_output_field 
        ON public.tmp_column_lineage_latest(output_dataset_field_uuid);
    CREATE INDEX idx_tmp_column_lineage_latest_input_field 
        ON public.tmp_column_lineage_latest(input_dataset_field_uuid);
    CREATE INDEX idx_tmp_column_lineage_latest_updated_at 
        ON public.tmp_column_lineage_latest(updated_at);
END;
$$ LANGUAGE plpgsql;

-- Verify the function was created
DO $$ 
DECLARE
    func_exists boolean;
BEGIN
    -- Check if the function exists
    SELECT EXISTS (
        SELECT 1 
        FROM pg_proc 
        WHERE proname = 'refresh_tmp_column_lineage_latest' 
        AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
    ) INTO func_exists;
    
    -- If function doesn't exist, raise an exception
    IF NOT func_exists THEN
        RAISE EXCEPTION 'Function refresh_tmp_column_lineage_latest was not created successfully';
    END IF;
END $$; 