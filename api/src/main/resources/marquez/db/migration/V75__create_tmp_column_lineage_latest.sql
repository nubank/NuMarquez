-- Create temporary table for latest column lineage
CREATE TABLE IF NOT EXISTS public.tmp_column_lineage_latest (
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
CREATE INDEX IF NOT EXISTS idx_tmp_column_lineage_latest_output_field 
    ON public.tmp_column_lineage_latest(output_dataset_field_uuid);
CREATE INDEX IF NOT EXISTS idx_tmp_column_lineage_latest_input_field 
    ON public.tmp_column_lineage_latest(input_dataset_field_uuid);
CREATE INDEX IF NOT EXISTS idx_tmp_column_lineage_latest_updated_at 
    ON public.tmp_column_lineage_latest(updated_at);

-- Create function to refresh the temporary table
CREATE OR REPLACE FUNCTION refresh_tmp_column_lineage_latest()
RETURNS void AS $$
BEGIN
    -- Truncate the table first
    TRUNCATE TABLE public.tmp_column_lineage_latest;
    
    -- Insert fresh data
    INSERT INTO public.tmp_column_lineage_latest
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
    ORDER BY output_dataset_field_uuid, input_dataset_field_uuid, updated_at DESC, updated_at;
    
    -- Analyze the table to update statistics
    ANALYZE public.tmp_column_lineage_latest;
END;
$$ LANGUAGE plpgsql;

-- Initial population of the table
SELECT refresh_tmp_column_lineage_latest(); 