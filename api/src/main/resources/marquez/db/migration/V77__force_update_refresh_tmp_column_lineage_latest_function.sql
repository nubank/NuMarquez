-- First, drop the function with CASCADE to ensure it's completely removed
DROP FUNCTION IF EXISTS public.refresh_tmp_column_lineage_latest() CASCADE;

-- Create the function with a different approach
DO $$ 
BEGIN
    -- Drop the function if it exists
    DROP FUNCTION IF EXISTS public.refresh_tmp_column_lineage_latest();
    
    -- Create the function using EXECUTE to ensure it's created in a new transaction
    EXECUTE $func$
    CREATE OR REPLACE FUNCTION public.refresh_tmp_column_lineage_latest()
    RETURNS void AS $body$
    BEGIN
        -- Create a new table with the same structure in public schema
        DROP TABLE IF EXISTS public.tmp_column_lineage_latest_new;
        CREATE TABLE public.tmp_column_lineage_latest_new (LIKE public.tmp_column_lineage_latest INCLUDING ALL);
        
        -- Insert fresh data into the new table
        INSERT INTO public.tmp_column_lineage_latest_new
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
        ANALYZE public.tmp_column_lineage_latest_new;
        
        -- Swap the tables (this is atomic)
        DROP TABLE IF EXISTS public.tmp_column_lineage_latest;
        ALTER TABLE public.tmp_column_lineage_latest_new RENAME TO tmp_column_lineage_latest;
        
        -- Recreate indexes
        CREATE INDEX idx_tmp_column_lineage_latest_output_field 
            ON public.tmp_column_lineage_latest(output_dataset_field_uuid);
        CREATE INDEX idx_tmp_column_lineage_latest_input_field 
            ON public.tmp_column_lineage_latest(input_dataset_field_uuid);
        CREATE INDEX idx_tmp_column_lineage_latest_updated_at 
            ON public.tmp_column_lineage_latest(updated_at);
    END;
    $body$ LANGUAGE plpgsql;
    $func$;
END $$;

-- Verify the function was created correctly
DO $$ 
DECLARE
    func_def text;
BEGIN
    -- Get the function definition
    SELECT pg_get_functiondef(oid) INTO func_def
    FROM pg_proc 
    WHERE proname = 'refresh_tmp_column_lineage_latest' 
    AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public');
    
    -- Check if the function contains 'CREATE TEMP TABLE'
    IF func_def LIKE '%CREATE TEMP TABLE%' THEN
        RAISE EXCEPTION 'Function still contains temporary table creation';
    END IF;
    
    -- Check if the function contains 'SET SCHEMA public'
    IF func_def LIKE '%SET SCHEMA public%' THEN
        RAISE EXCEPTION 'Function still contains schema change';
    END IF;
END $$; 