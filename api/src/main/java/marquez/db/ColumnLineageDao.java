/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static org.jdbi.v3.sqlobject.customizer.BindList.EmptyHandling.NULL_STRING;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import marquez.db.mappers.ColumnLineageNodeDataMapper;
import marquez.db.mappers.ColumnLineageRowMapper;
import marquez.db.models.ColumnLineageNodeData;
import marquez.db.models.ColumnLineageRow;
import org.apache.commons.lang3.tuple.Pair;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.BindBeanList;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

@RegisterRowMapper(ColumnLineageRowMapper.class)
@RegisterRowMapper(ColumnLineageNodeDataMapper.class)
public interface ColumnLineageDao extends BaseDao {

  default List<ColumnLineageRow> upsertColumnLineageRow(
      UUID outputDatasetVersionUuid,
      UUID outputDatasetFieldUuid,
      List<Pair<UUID, UUID>> inputs,
      String transformationDescription,
      String transformationType,
      Instant now) {

    if (inputs.isEmpty()) {
      return Collections.emptyList();
    }

    doUpsertColumnLineageRow(
        inputs.stream()
            .map(
                input ->
                    new ColumnLineageRow(
                        outputDatasetVersionUuid,
                        outputDatasetFieldUuid,
                        input.getLeft(), // input_dataset_version_uuid
                        input.getRight(), // input_dataset_field_uuid
                        transformationDescription,
                        transformationType,
                        now,
                        now))
            .collect(Collectors.toList()));
    return findColumnLineageByDatasetVersionColumnAndOutputDatasetField(
        outputDatasetVersionUuid, outputDatasetFieldUuid);
  }

  @SqlQuery(
      "SELECT * FROM column_lineage WHERE output_dataset_version_uuid = :datasetVersionUuid AND output_dataset_field_uuid = :outputDatasetFieldUuid")
  List<ColumnLineageRow> findColumnLineageByDatasetVersionColumnAndOutputDatasetField(
      UUID datasetVersionUuid, UUID outputDatasetFieldUuid);

  @SqlUpdate(
      """
          INSERT INTO column_lineage (
          output_dataset_version_uuid,
          output_dataset_field_uuid,
          input_dataset_version_uuid,
          input_dataset_field_uuid,
          transformation_description,
          transformation_type,
          created_at,
          updated_at
          ) VALUES <values>
          ON CONFLICT (output_dataset_version_uuid, output_dataset_field_uuid, input_dataset_version_uuid, input_dataset_field_uuid)
          DO UPDATE SET
          transformation_description = EXCLUDED.transformation_description,
          transformation_type = EXCLUDED.transformation_type,
          updated_at = EXCLUDED.updated_at
          """)
  void doUpsertColumnLineageRow(
      @BindBeanList(
              propertyNames = {
                "outputDatasetVersionUuid",
                "outputDatasetFieldUuid",
                "inputDatasetVersionUuid",
                "inputDatasetFieldUuid",
                "transformationDescription",
                "transformationType",
                "createdAt",
                "updatedAt"
              },
              value = "values")
          List<ColumnLineageRow> rows);

    @SqlQuery(
      """
          WITH RECURSIVE
            dataset_fields_view AS (
              SELECT d.namespace_name as namespace_name, d.name as dataset_name, df.name as field_name, df.type, df.uuid, d.namespace_uuid
              FROM dataset_fields df
              INNER JOIN datasets_view d ON d.uuid = df.dataset_uuid
            ),
            column_lineage_recursive AS (
              (
                SELECT
                  *,
                  0 as depth,
                  false as is_cycle,
                  ARRAY[ROW(output_dataset_field_uuid, input_dataset_field_uuid)] as path -- path and is_cycle mechanism as describe here https://www.postgresql.org/docs/current/queries-with.html (CYCLE clause not available in postgresql 12)
                FROM tmp_column_lineage_latest
                WHERE output_dataset_field_uuid IN (<datasetFieldUuids>)
                  AND created_at <= :createdAtUntil
              )
              UNION ALL
              SELECT
                adjacent_node.output_dataset_version_uuid,
                adjacent_node.output_dataset_field_uuid,
                adjacent_node.input_dataset_version_uuid,
                adjacent_node.input_dataset_field_uuid,
                adjacent_node.transformation_description,
                adjacent_node.transformation_type,
                adjacent_node.created_at,
                adjacent_node.updated_at,
                node.depth + 1 as depth,
                ROW(adjacent_node.input_dataset_field_uuid, adjacent_node.output_dataset_field_uuid) = ANY(path) as is_cycle,
                path || ROW(adjacent_node.input_dataset_field_uuid, adjacent_node.output_dataset_field_uuid) as path
              FROM tmp_column_lineage_latest adjacent_node, column_lineage_recursive node
              WHERE (
                (node.input_dataset_field_uuid = adjacent_node.output_dataset_field_uuid) --upstream lineage
                OR (:withDownstream AND adjacent_node.input_dataset_field_uuid = node.output_dataset_field_uuid) --optional downstream lineage
              )
              AND node.depth < :depth - 1 -- fetching single row means fetching single edge which is size 1
              AND NOT is_cycle
              AND adjacent_node.created_at <= :createdAtUntil
            )
            SELECT
                output_fields.namespace_name,
                output_fields.dataset_name,
                output_fields.field_name,
                output_fields.type,
                ARRAY_AGG(DISTINCT ARRAY[
                  input_fields.namespace_name,
                  input_fields.dataset_name,
                  CAST(clr.input_dataset_version_uuid AS VARCHAR),
                  input_fields.field_name,
                  clr.transformation_description,
                  clr.transformation_type
                ]) AS inputFields,
                ARRAY_AGG(DISTINCT ARRAY[
                  output_fields.namespace_name,
                  output_fields.dataset_name,
                  CAST(clr.output_dataset_version_uuid AS VARCHAR),
                  output_fields.field_name,
                  clr.transformation_description,
                  clr.transformation_type
                ]) FILTER (WHERE clr.output_dataset_field_uuid IS NOT NULL) AS outputFields,
                clr.output_dataset_version_uuid as dataset_version_uuid
            FROM column_lineage_recursive clr
            INNER JOIN dataset_fields_view output_fields ON clr.output_dataset_field_uuid = output_fields.uuid -- hidden datasets will be filtered
            INNER JOIN dataset_symlinks ds_output ON ds_output.namespace_uuid = output_fields.namespace_uuid AND ds_output.name = output_fields.dataset_name
            LEFT JOIN dataset_fields_view input_fields ON clr.input_dataset_field_uuid = input_fields.uuid
            INNER JOIN dataset_symlinks ds_input ON ds_input.namespace_uuid = input_fields.namespace_uuid AND ds_input.name = input_fields.dataset_name
            WHERE NOT clr.is_cycle AND ds_output.is_primary is true AND ds_input.is_primary
            GROUP BY
                output_fields.namespace_name,
                output_fields.dataset_name,
                output_fields.field_name,
                output_fields.type,
                clr.output_dataset_version_uuid
          """)
  Set<ColumnLineageNodeData> getLineage(
      int depth,
      @BindList(onEmpty = NULL_STRING) List<UUID> datasetFieldUuids,
      boolean withDownstream,
      Instant createdAtUntil);

  @SqlQuery(
      """
        WITH selected_column_lineage AS (
          SELECT cl.*, dv.namespace_uuid
          FROM tmp_column_lineage_latest cl
          JOIN dataset_fields df ON df.uuid = cl.output_dataset_field_uuid
          JOIN datasets_view dv ON dv.uuid = df.dataset_uuid
          WHERE ARRAY[<values>]::DATASET_NAME[] && dv.dataset_symlinks -- array of string pairs is cast onto array of DATASET_NAME types to be checked if it has non-empty intersection with dataset symlinks
        ),
        dataset_fields_view AS (
            SELECT
                d.namespace_name AS namespace_name,
                d.dataset_name AS dataset_name,
                df.name AS field_name,
                df.type,
                df.uuid
            FROM dataset_fields df
            INNER JOIN (
                SELECT DISTINCT dataset_uuid, namespace_name, dataset_name
                FROM dataset_versions
                WHERE uuid IN (
                    SELECT DISTINCT output_dataset_version_uuid
                    FROM selected_column_lineage
                    UNION
                    SELECT DISTINCT input_dataset_version_uuid
                    FROM selected_column_lineage
                )
            ) d ON d.dataset_uuid = df.dataset_uuid
        )
        SELECT
          output_fields.namespace_name,
          output_fields.dataset_name,
          output_fields.field_name,
          output_fields.type,
          ARRAY_AGG(DISTINCT ARRAY[
            input_fields.namespace_name,
            input_fields.dataset_name,
            CAST(c.input_dataset_version_uuid AS VARCHAR),
            input_fields.field_name,
            c.transformation_description,
            c.transformation_type
          ]) AS inputFields,
          ARRAY_AGG(DISTINCT ARRAY[
            output_fields.namespace_name,
            output_fields.dataset_name,
            CAST(c.output_dataset_version_uuid AS VARCHAR),
            output_fields.field_name,
            c.transformation_description,
            c.transformation_type
          ]) FILTER (WHERE c.output_dataset_field_uuid IS NOT NULL) AS outputFields,
          null as dataset_version_uuid
        FROM selected_column_lineage c
        INNER JOIN dataset_fields_view output_fields ON c.output_dataset_field_uuid = output_fields.uuid
        INNER JOIN dataset_symlinks ds ON ds.namespace_uuid = c.namespace_uuid and ds.name=output_fields.dataset_name
        LEFT JOIN dataset_fields_view input_fields ON c.input_dataset_field_uuid = input_fields.uuid
        WHERE ds.is_primary is true
        GROUP BY
          output_fields.namespace_name,
          output_fields.dataset_name,
          output_fields.field_name,
          output_fields.type
      """)
  /**
   * Each dataset is identified by a pair of strings (namespace and name). A query returns column
   * lineage for multiple datasets, that's why a list of pairs is expected as an argument. "left"
   * and "right" properties correspond to Java Pair class properties defined to bind query template
   * with values
   */
  Set<ColumnLineageNodeData> getLineageRowsForDatasets(
      @BindBeanList(
              propertyNames = {"left", "right"},
              value = "values")
          List<Pair<String, String>> datasets);

  /**
   * Fetch upstream lineage nodes that directly produce data TO the input dataset fields.
   * This returns fields that are direct producers of the given fields.
   * Only follows INPUT edges to the given fields (direct upstream producers).
   *
   * @param datasetFieldUuids The UUIDs of the dataset fields to get upstream producers for
   * @param createdAtUntil The point in time to get lineage for
   * @return Set of ColumnLineageNodeData representing the direct upstream producer fields
   */
  @SqlQuery("""
    WITH dataset_fields_view AS (
        SELECT 
            d.namespace_name as namespace_name, 
            d.name as dataset_name, 
            df.name as field_name, 
            df.type, 
            df.uuid, 
            d.namespace_uuid
        FROM dataset_fields df
        INNER JOIN datasets_view d ON d.uuid = df.dataset_uuid
    ),
    -- Find upstream producer fields: fields that produce data TO our target fields
    upstream_producers AS (
        SELECT DISTINCT
            input_fields.namespace_name,
            input_fields.dataset_name,
            input_fields.field_name,
            input_fields.type,
            ARRAY[]::text[][] AS inputFields,
            ARRAY_AGG(DISTINCT ARRAY[
                output_fields.namespace_name,
                output_fields.dataset_name,
                CAST(cl.output_dataset_version_uuid AS VARCHAR),
                output_fields.field_name,
                cl.transformation_description,
                cl.transformation_type
            ]) AS outputFields,
            cl.input_dataset_version_uuid as dataset_version_uuid
        FROM tmp_column_lineage_latest cl
        INNER JOIN dataset_fields_view input_fields 
            ON cl.input_dataset_field_uuid = input_fields.uuid
        INNER JOIN dataset_symlinks ds_input 
            ON ds_input.namespace_uuid = input_fields.namespace_uuid 
            AND ds_input.name = input_fields.dataset_name
        INNER JOIN dataset_fields_view output_fields 
            ON cl.output_dataset_field_uuid = output_fields.uuid
        INNER JOIN dataset_symlinks ds_output 
            ON ds_output.namespace_uuid = output_fields.namespace_uuid 
            AND ds_output.name = output_fields.dataset_name
        WHERE cl.output_dataset_field_uuid IN (<datasetFieldUuids>)
            AND cl.created_at <= :createdAtUntil
            AND ds_input.is_primary is true
            AND ds_output.is_primary is true
        GROUP BY
            input_fields.namespace_name,
            input_fields.dataset_name,
            input_fields.field_name,
            input_fields.type,
            cl.input_dataset_version_uuid
    )
    SELECT * FROM upstream_producers
  """)
  Set<ColumnLineageNodeData> getUpstreamColumnLineage(
      @BindList(onEmpty = NULL_STRING) List<UUID> datasetFieldUuids,
      Instant createdAtUntil);

  /**
   * Fetch downstream lineage nodes that directly consume data FROM the input dataset fields.
   * This returns fields that are direct consumers of the given fields.
   * Only follows OUTPUT edges from the given fields (direct downstream consumers).
   *
   * @param datasetFieldUuids The UUIDs of the dataset fields to get downstream consumers for
   * @param createdAtUntil The point in time to get lineage for
   * @return Set of ColumnLineageNodeData representing the direct downstream consumer fields
   */
  @SqlQuery("""
    WITH dataset_fields_view AS (
        SELECT 
            d.namespace_name as namespace_name, 
            d.name as dataset_name, 
            df.name as field_name, 
            df.type, 
            df.uuid, 
            d.namespace_uuid
        FROM dataset_fields df
        INNER JOIN datasets_view d ON d.uuid = df.dataset_uuid
    ),
    -- Find downstream consumer fields: fields that consume data FROM our target fields
    downstream_consumers AS (
        SELECT DISTINCT
            output_fields.namespace_name,
            output_fields.dataset_name,
            output_fields.field_name,
            output_fields.type,
            ARRAY_AGG(DISTINCT ARRAY[
                input_fields.namespace_name,
                input_fields.dataset_name,
                CAST(cl.input_dataset_version_uuid AS VARCHAR),
                input_fields.field_name,
                cl.transformation_description,
                cl.transformation_type
            ]) AS inputFields,
            ARRAY[]::text[][] AS outputFields,
            cl.output_dataset_version_uuid as dataset_version_uuid
        FROM tmp_column_lineage_latest cl
        INNER JOIN dataset_fields_view output_fields 
            ON cl.output_dataset_field_uuid = output_fields.uuid
        INNER JOIN dataset_symlinks ds_output 
            ON ds_output.namespace_uuid = output_fields.namespace_uuid 
            AND ds_output.name = output_fields.dataset_name
        INNER JOIN dataset_fields_view input_fields 
            ON cl.input_dataset_field_uuid = input_fields.uuid
        INNER JOIN dataset_symlinks ds_input 
            ON ds_input.namespace_uuid = input_fields.namespace_uuid 
            AND ds_input.name = input_fields.dataset_name
        WHERE cl.input_dataset_field_uuid IN (<datasetFieldUuids>)
            AND cl.created_at <= :createdAtUntil
            AND ds_output.is_primary is true
            AND ds_input.is_primary is true
        GROUP BY
            output_fields.namespace_name,
            output_fields.dataset_name,
            output_fields.field_name,
            output_fields.type,
            cl.output_dataset_version_uuid
    )
    SELECT * FROM downstream_consumers
  """)
  Set<ColumnLineageNodeData> getDownstreamColumnLineage(
      @BindList(onEmpty = NULL_STRING) List<UUID> datasetFieldUuids,
      Instant createdAtUntil);

  /**
   * Fetch all of the column lineage nodes that are directly connected to the input dataset fields.
   * This returns a single layer of lineage using column lineage as edges. Fields that have
   * no input or output lineage will have no results.
   *
   * @param datasetFieldUuids The UUIDs of the dataset fields to get lineage for
   * @param withDownstream Whether to include downstream lineage
   * @param createdAtUntil The point in time to get lineage for
   * @return Set of ColumnLineageNodeData representing the direct lineage
   */
  Set<ColumnLineageNodeData> getDirectColumnLineage(
      @BindList(onEmpty = NULL_STRING) List<UUID> datasetFieldUuids,
      boolean withDownstream,
      Instant createdAtUntil);

  /**
   * Execute a federated lineage query across multiple postgres-fdw connected databases.
   * This method allows querying lineage data from multiple federated sources in a single query.
   * Implementation should use Jdbi Handle to execute dynamic SQL.
   */
  default Set<ColumnLineageNodeData> executeFederatedLineageQuery(String federatedQuery) {
    return getHandle().createQuery(federatedQuery)
        .mapTo(ColumnLineageNodeData.class)
        .set();
  }

  /**
   * Execute a custom optimized lineage query that leverages postgres-fdw capabilities.
   * This method supports complex recursive queries that can span federated databases.
   * Implementation should use Jdbi Handle to execute dynamic SQL.
   */
  default Set<ColumnLineageNodeData> executeCustomLineageQuery(String customQuery) {
    return getHandle().createQuery(customQuery)
        .mapTo(ColumnLineageNodeData.class)
        .set();
  }

  /**
   * Execute a single optimized recursive query for column lineage using unified_column_lineage view.
   * This leverages postgres-fdw by using a view that combines local and federated lineage data.
   *
   * @param datasetFieldUuids Starting field UUIDs
   * @param depth Maximum traversal depth
   * @param withDownstream Whether to include downstream lineage
   * @param createdAtUntil Point in time for lineage
   * @return Set of all discovered lineage nodes
   */
  @SqlQuery("""
      WITH RECURSIVE lineage_traversal AS (
        -- Base case: starting fields from unified view (includes federated data)
        SELECT 
          output_dataset_field_uuid,
          input_dataset_field_uuid,
          transformation_description,
          transformation_type,
          0 as depth,
          ARRAY[output_dataset_field_uuid] as visited_path,
          lineage_source
        FROM unified_column_lineage 
        WHERE output_dataset_field_uuid IN (<datasetFieldUuids>)
          AND created_at <= :createdAtUntil
        
        UNION ALL
        
        -- Recursive case: traverse lineage across all sources
        SELECT 
          ucl.output_dataset_field_uuid,
          ucl.input_dataset_field_uuid,
          ucl.transformation_description,
          ucl.transformation_type,
          lt.depth + 1,
          lt.visited_path || ucl.output_dataset_field_uuid,
          ucl.lineage_source
        FROM unified_column_lineage ucl
        JOIN lineage_traversal lt ON (
          (:withDownstream AND ucl.input_dataset_field_uuid = lt.output_dataset_field_uuid) OR
          (ucl.output_dataset_field_uuid = lt.input_dataset_field_uuid)
        )
        WHERE lt.depth < :depth - 1
          AND NOT ucl.output_dataset_field_uuid = ANY(lt.visited_path)
          AND ucl.created_at <= :createdAtUntil
      ),
      dataset_fields_view AS (
        SELECT 
          d.namespace_name as namespace_name, 
          d.name as dataset_name, 
          df.name as field_name, 
          df.type, 
          df.uuid, 
          d.namespace_uuid
        FROM dataset_fields df
        INNER JOIN datasets_view d ON d.uuid = df.dataset_uuid
      )
      SELECT DISTINCT
        output_fields.namespace_name,
        output_fields.dataset_name,
        output_fields.field_name,
        output_fields.type,
        ARRAY_AGG(DISTINCT ARRAY[
          input_fields.namespace_name,
          input_fields.dataset_name,
          CAST(lt.input_dataset_field_uuid AS VARCHAR),
          input_fields.field_name,
          lt.transformation_description,
          lt.transformation_type
        ]) FILTER (WHERE input_fields.uuid IS NOT NULL) AS inputFields,
        ARRAY[]::text[][] AS outputFields,
        NULL::uuid as dataset_version_uuid
      FROM lineage_traversal lt
      JOIN dataset_fields_view output_fields ON lt.output_dataset_field_uuid = output_fields.uuid
      LEFT JOIN dataset_fields_view input_fields ON lt.input_dataset_field_uuid = input_fields.uuid
      GROUP BY
        output_fields.namespace_name,
        output_fields.dataset_name,
        output_fields.field_name,
        output_fields.type
      """)
  Set<ColumnLineageNodeData> getOptimizedLineageWithFederation(
      @BindList(onEmpty = NULL_STRING) List<UUID> datasetFieldUuids,
      int depth,
      boolean withDownstream,
      Instant createdAtUntil);
}
