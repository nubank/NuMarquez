-- Update the refresh function with improved error handling
DO $$ 
DECLARE
    v_function_exists boolean;
BEGIN
    -- Log the start of the migration
    RAISE LOG 'Starting V76 migration: update refresh_tmp_column_lineage_latest function';
    
    -- Check if the function exists and log the result
    SELECT EXISTS (
        SELECT 1 
        FROM pg_proc 
        WHERE proname = 'refresh_tmp_column_lineage_latest' 
        AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
    ) INTO v_function_exists;
    
    RAISE LOG 'Function exists check result: %', v_function_exists;
    
    IF v_function_exists THEN
        RAISE LOG 'Dropping existing function refresh_tmp_column_lineage_latest';
        -- Drop the existing function with explicit schema
        DROP FUNCTION IF EXISTS public.refresh_tmp_column_lineage_latest();
        RAISE LOG 'Successfully dropped existing function';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE LOG 'Error in DO block: %', SQLERRM;
        RAISE;
END $$;

-- Create the updated function
DO $$ 
BEGIN
    RAISE LOG 'Creating new refresh_tmp_column_lineage_latest function';
    
    EXECUTE $func$
    CREATE OR REPLACE FUNCTION public.refresh_tmp_column_lineage_latest()
    RETURNS void AS $body$
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
    $body$ LANGUAGE plpgsql;
    $func$;
    
    RAISE LOG 'Successfully created new function';
EXCEPTION
    WHEN OTHERS THEN
        RAISE LOG 'Error creating function: %', SQLERRM;
        RAISE;
END $$; 