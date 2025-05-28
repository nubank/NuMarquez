-- Create temporary table for latest column lineage
DO $$ 
BEGIN
    -- Drop the table if it exists to ensure clean state
    DROP TABLE IF EXISTS public.tmp_column_lineage_latest;
    
    -- Create the table
    CREATE TABLE public.tmp_column_lineage_latest (
        output_dataset_version_uuid UUID NOT NULL,
        output_dataset_field_uuid UUID NOT NULL,
        input_dataset_version_uuid UUID NOT NULL,
        input_dataset_field_uuid UUID NOT NULL,
        transformation_description TEXT,
        transformation_type TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    );

    -- Create indexes for better query performance
    CREATE INDEX idx_tmp_column_lineage_latest_output_field 
        ON public.tmp_column_lineage_latest(output_dataset_field_uuid);
    CREATE INDEX idx_tmp_column_lineage_latest_input_field 
        ON public.tmp_column_lineage_latest(input_dataset_field_uuid);
    CREATE INDEX idx_tmp_column_lineage_latest_updated_at 
        ON public.tmp_column_lineage_latest(updated_at);
END $$;