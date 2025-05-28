-- First, let's check if we can create a simple table to test permissions
CREATE TABLE IF NOT EXISTS public.migration_test (
    id serial PRIMARY KEY,
    test_column text
);

-- Now let's check if the function exists and log it
DO $$ 
DECLARE
    func_exists boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1 
        FROM pg_proc 
        WHERE proname = 'refresh_tmp_column_lineage_latest' 
        AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
    ) INTO func_exists;
    
    -- Insert the result into our test table
    INSERT INTO public.migration_test (test_column) 
    VALUES ('Function exists: ' || func_exists::text);
END $$;

-- Try to create a very simple function
CREATE OR REPLACE FUNCTION public.test_function()
RETURNS text AS $$
BEGIN
    RETURN 'test';
END;
$$ LANGUAGE plpgsql;

-- Log that we created the test function
INSERT INTO public.migration_test (test_column) 
VALUES ('Test function created successfully');

-- Clean up
DROP TABLE IF EXISTS public.migration_test;
DROP FUNCTION IF EXISTS public.test_function();

-- Start a new transaction
BEGIN;

-- First, just try to create a very simple function
CREATE OR REPLACE FUNCTION public.test_simple_function()
RETURNS text AS $$
BEGIN
    RETURN 'test';
END;
$$ LANGUAGE plpgsql;

-- If that succeeds, commit the transaction
COMMIT;

-- Start a new transaction for the next step
BEGIN;

-- Now try to drop the existing function
DROP FUNCTION IF EXISTS public.refresh_tmp_column_lineage_latest();

-- If that succeeds, commit
COMMIT;

-- Start a new transaction for the final step
BEGIN;

-- Create the actual function
CREATE OR REPLACE FUNCTION public.refresh_tmp_column_lineage_latest()
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
END;
$$ LANGUAGE plpgsql;

-- Clean up test function
DROP FUNCTION IF EXISTS public.test_simple_function();

-- Commit the final transaction
COMMIT; 