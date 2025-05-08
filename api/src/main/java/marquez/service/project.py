import base64

def recursive_logic_calculation(x, job_name, depth):
    loop_uuid = base64.b64encode(job_name.encode()).decode()[:10]  # Shorten the loop_uuid
    print(f"calcuating lineage for: {job_name}")

    for i in range(1, x + 1):
        print(f"loop: {i}")
        if i == 1:
            logic_calculation = spark.sql(
                f"""
                WITH upstream_list_{i} AS (
                    SELECT
                        io_type,
                        dataset_uuid,
                        dataset_version_uuid,
                        dataset_name
                    FROM
                        experiment_0
                    WHERE
                        io_type = 'INPUT'
                    AND
                        job_name = '{job_name}'
                )
                SELECT
                    A.job_uuid,
                    A.job_version_uuid,
                    A.job_name
                FROM
                    filtered_jobs_io_mapping A
                INNER JOIN
                    upstream_list_{i} B
                ON
                    A.io_type = 'OUTPUT' AND A.dataset_uuid = B.dataset_uuid
                ORDER BY
                    A.job_uuid, A.io_type
                """
            )
        elif i % 2 == 0:
            logic_calculation = spark.sql(
                f"""
                WITH upstream_list_{i} AS (
                    SELECT
                        *
                    FROM
                        filtered_jobs_io_mapping
                    WHERE
                        io_type = 'INPUT'
                    AND
                        job_version_uuid in (
                            SELECT job_version_uuid FROM experiment_{loop_uuid}_{i-1}_upstream_logic_calculation
                        )
                )
                SELECT
                    '{job_name}' as central_node,
                    *,
                    {depth} as depth,
                    dataset_name as in_edge,
                    'dataset' as in_edge_type,
                    job_name as out_edge,
                    'job' as out_edge_type
                FROM
                    upstream_list_{i}
                ORDER BY
                    out_edge, in_edge
                """
            )
            depth += 1
        else:
            logic_calculation = spark.sql(
                f"""
                    WITH upstream_list_{i-1} AS (
                    SELECT
                        io_type,
                        dataset_uuid,
                        dataset_version_uuid,
                        dataset_name
                    FROM
                        experiment_{loop_uuid}_{i-1}_upstream_logic_calculation
                    WHERE
                        io_type = 'INPUT'
                )
                SELECT
                    A.job_uuid,
                    A.job_version_uuid,
                    A.job_name
                FROM
                    filtered_jobs_io_mapping A
                INNER JOIN
                    upstream_list_{i-1} B
                ON
                    A.io_type = 'OUTPUT' AND A.dataset_uuid = B.dataset_uuid
                ORDER BY
                    A.job_uuid, A.io_type
                """
            )
        
        logic_calculation.createOrReplaceTempView(f"experiment_{loop_uuid}_{i}_upstream_logic_calculation")
        # display(logic_calculation.count())
        # display(logic_calculation.limit(1000))
        # logic_calculation.cache()

    experiment_0_upstream_logic_calculation_final = spark.sql(
        f"""
        SELECT '{job_name}' as central_node,
        * 
        FROM 
            experiment_0 WHERE io_type = "INPUT" AND job_name = '{job_name}'
        UNION
        SELECT * FROM experiment_{loop_uuid}_2_upstream_logic_calculation
        UNION
        SELECT * FROM experiment_{loop_uuid}_4_upstream_logic_calculation
        UNION
        SELECT * FROM experiment_{loop_uuid}_6_upstream_logic_calculation
        ORDER BY
            depth, out_edge, in_edge
        """
    )

    return experiment_0_upstream_logic_calculation_final