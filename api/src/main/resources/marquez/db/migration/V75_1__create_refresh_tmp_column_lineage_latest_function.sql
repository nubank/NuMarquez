-- Drop the function if it exists to ensure clean state
DROP FUNCTION IF EXISTS refresh_tmp_column_lineage_latest();

-- Create function to refresh the temporary table
CREATE OR REPLACE FUNCTION refresh_tmp_column_lineage_latest()
RETURNS void AS $$
BEGIN
    -- Create a new table with the same structure in public schema
    DROP TABLE IF EXISTS public.tmp_column_lineage_latest_swap;
    CREATE TABLE public.tmp_column_lineage_latest_swap (LIKE public.tmp_column_lineage_latest INCLUDING ALL);
    
    -- Insert fresh data into the swap table
    INSERT INTO public.tmp_column_lineage_latest_swap
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
    
    -- Analyze the swap table
    ANALYZE public.tmp_column_lineage_latest_swap;
    
    -- Swap the tables (this is atomic)
    DROP TABLE IF EXISTS public.tmp_column_lineage_latest;
    ALTER TABLE public.tmp_column_lineage_latest_swap RENAME TO public.tmp_column_lineage_latest;
    
    -- Recreate indexes
    CREATE INDEX idx_tmp_column_lineage_latest_output_field 
        ON public.tmp_column_lineage_latest(output_dataset_field_uuid);
    CREATE INDEX idx_tmp_column_lineage_latest_input_field 
        ON public.tmp_column_lineage_latest(input_dataset_field_uuid);
    CREATE INDEX idx_tmp_column_lineage_latest_updated_at 
        ON public.tmp_column_lineage_latest(updated_at);
EXCEPTION
    WHEN OTHERS THEN
        -- Log the error and rethrow
        RAISE LOG 'Error in refresh_tmp_column_lineage_latest: %', SQLERRM;
        RAISE;
END;
$$ LANGUAGE plpgsql; 