/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import com.google.common.collect.ImmutableSortedSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet; 
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import marquez.common.models.DatasetFieldId;
import marquez.common.models.DatasetFieldVersionId;
import marquez.common.models.DatasetId;
import marquez.common.models.DatasetVersionId;
import marquez.common.models.JobId;
import marquez.common.models.JobVersionId;
import marquez.db.ColumnLineageDao;
import marquez.db.DatasetFieldDao;
import marquez.db.models.ColumnLineageNodeData;
import marquez.db.models.InputFieldNodeData;
import marquez.service.models.ColumnLineage;
import marquez.service.models.ColumnLineageInputField;
import marquez.service.models.Dataset;
import marquez.service.models.Edge;
import marquez.service.models.Lineage;
import marquez.service.models.Node;
import marquez.service.models.NodeId;
import marquez.service.models.NodeType;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class ColumnLineageService extends DelegatingDaos.DelegatingColumnLineageDao {
  private final DatasetFieldDao datasetFieldDao;
  private final FederatedLineageConfig federatedConfig;

  public ColumnLineageService(ColumnLineageDao dao, DatasetFieldDao datasetFieldDao) {
    this(dao, datasetFieldDao, null);
  }

  public ColumnLineageService(ColumnLineageDao dao, DatasetFieldDao datasetFieldDao, 
                             FederatedLineageConfig federatedConfig) {
    super(dao);
    this.datasetFieldDao = datasetFieldDao;
    this.federatedConfig = federatedConfig;
  }

  /**
   * Enhanced lineage method that automatically uses the best available approach:
   * 1. Optimized federated lineage (if configured and unified_column_lineage view exists)
   * 2. Single optimized recursive query
   * 3. Fallback to iterative approach
   */
  public Lineage lineage(NodeId nodeId, int depth, boolean withDownstream) {
    ColumnNodes columnNodes = getColumnNodes(nodeId);
    if (columnNodes.nodeIds.isEmpty()) {
        throw new NodeIdNotFoundException("Could not find node");
    }

    Set<ColumnLineageNodeData> allLineageData;

    try {
      // Try optimized federated approach first if configured
      if (federatedConfig != null && federatedConfig.isEnabled() && federatedConfig.isValid()) {
        log.debug("Using optimized federated lineage for node: {}", nodeId);
        allLineageData = super.getOptimizedLineageWithFederation(
            new ArrayList<>(columnNodes.nodeIds), depth, withDownstream, columnNodes.createdAtUntil);
        
        if (!allLineageData.isEmpty()) {
          log.debug("Successfully retrieved {} nodes using federated lineage", allLineageData.size());
          return toLineage(allLineageData, nodeId.hasVersion());
        }
      }
    } catch (Exception e) {
      log.warn("Federated lineage query failed, falling back to standard approach: {}", e.getMessage());
    }

    try {
      // Try single optimized query approach
      log.debug("Using optimized single-query lineage for node: {}", nodeId);
      allLineageData = executeOptimizedLineageQuery(
          new HashSet<>(columnNodes.nodeIds), depth, withDownstream, columnNodes.createdAtUntil);
      
      if (!allLineageData.isEmpty()) {
        log.debug("Successfully retrieved {} nodes using optimized query", allLineageData.size());
        return toLineage(allLineageData, nodeId.hasVersion());
      }
    } catch (Exception e) {
      log.warn("Optimized lineage query failed, falling back to iterative approach: {}", e.getMessage());
    }

    // Fallback to original iterative approach
    log.debug("Using iterative lineage approach for node: {}", nodeId);
    Set<ColumnLineageNodeData> upstreamLineage = fetchDirectColumnLineage(
        new HashSet<>(columnNodes.nodeIds), depth, true, columnNodes.createdAtUntil);
    
    allLineageData = new HashSet<>(upstreamLineage);
    
    if (withDownstream) {
        Set<ColumnLineageNodeData> downstreamLineage = fetchDirectColumnLineage(
            new HashSet<>(columnNodes.nodeIds), depth, false, columnNodes.createdAtUntil);
        allLineageData.addAll(downstreamLineage);
    }

    log.debug("Completed iterative lineage traversal with {} total nodes", allLineageData.size());
    return toLineage(allLineageData, nodeId.hasVersion());
  }

  /**
   * Fetch column lineage in a specific direction up to the specified depth.
   * Similar to LineageService.fetchDirectJobs method.
   *
   * @param initialFieldUuids The initial set of field UUIDs to start from
   * @param maxDepth Maximum depth to traverse
   * @param isUpstream True for upstream lineage, false for downstream
   * @param createdAtUntil Point in time for lineage
   * @return Set of all discovered column lineage nodes
   */
  private Set<ColumnLineageNodeData> fetchDirectColumnLineage(
      Set<UUID> initialFieldUuids, int maxDepth, boolean isUpstream, Instant createdAtUntil) {
    
    Map<UUID, ColumnLineageNodeData> allNodesMap = new HashMap<>();
    Set<UUID> processedFields = new HashSet<>();
    Set<UUID> currentLevelFields = new HashSet<>(initialFieldUuids);

    for (int currentDepth = 0; currentDepth < maxDepth && !currentLevelFields.isEmpty(); currentDepth++) {
        log.debug("Processing depth {} for {} lineage with {} fields", 
                 currentDepth, isUpstream ? "upstream" : "downstream", currentLevelFields.size());

        // Mark current fields as processed
        processedFields.addAll(currentLevelFields);

        // Get direct lineage for current level fields in the specified direction
        Set<ColumnLineageNodeData> directLineage;
        if (isUpstream) {
            directLineage = super.getUpstreamColumnLineage(new ArrayList<>(currentLevelFields), createdAtUntil);
        } else {
            directLineage = super.getDownstreamColumnLineage(new ArrayList<>(currentLevelFields), createdAtUntil);
        }
        
        log.debug("Found {} lineage nodes at depth {}", directLineage.size(), currentDepth);

        // Collect discovered nodes and prepare next level
        Set<UUID> nextLevelFields = new HashSet<>();
        for (ColumnLineageNodeData node : directLineage) {
            // Add node to results if not already present
            UUID nodeFieldUuid = datasetFieldDao.findUuid(
                node.getNamespace(), node.getDataset(), node.getField())
                .orElse(null);
            
            if (nodeFieldUuid != null && !allNodesMap.containsKey(nodeFieldUuid)) {
                allNodesMap.put(nodeFieldUuid, node);
                
                // Add to next level if not processed
                if (!processedFields.contains(nodeFieldUuid)) {
                    nextLevelFields.add(nodeFieldUuid);
                }
            }
        }
        
        log.debug("Found {} fields for next depth level", nextLevelFields.size());
        
        // Update current level for next iteration
        currentLevelFields = nextLevelFields;
    }

    return new HashSet<>(allNodesMap.values());
  }

  /**
   * Enhanced lineage method that leverages postgres-fdw for cross-database lineage.
   * This method can query lineage data across multiple federated databases.
   */
  public Lineage federatedLineage(NodeId nodeId, int depth, boolean withDownstream, List<String> federatedSources) {
    // 1. Get initial column nodes
    ColumnNodes columnNodes = getColumnNodes(nodeId);
    if (columnNodes.nodeIds.isEmpty()) {
        throw new NodeIdNotFoundException("Could not find node");
    }

    // 2. Fetch lineage using federated queries that can span multiple databases
    Set<ColumnLineageNodeData> allLineageData = fetchFederatedColumnLineage(
        new HashSet<>(columnNodes.nodeIds), depth, withDownstream, columnNodes.createdAtUntil, federatedSources);

    log.debug("Completed federated lineage traversal with {} total nodes", allLineageData.size());

    // 3. Build and return the lineage graph
    return toLineage(allLineageData, nodeId.hasVersion());
  }

  /**
   * Fetch column lineage across federated databases using postgres-fdw.
   * This method can execute a single query that spans multiple database instances.
   */
  private Set<ColumnLineageNodeData> fetchFederatedColumnLineage(
      Set<UUID> initialFieldUuids, int maxDepth, boolean withDownstream, 
      Instant createdAtUntil, List<String> federatedSources) {
    
    // Build a query that uses UNION ALL to combine lineage from multiple sources
    StringBuilder federatedQuery = new StringBuilder();
    federatedQuery.append("WITH federated_lineage AS (");
    
    // Add local lineage
    federatedQuery.append(buildLocalLineageQuery(initialFieldUuids, maxDepth, withDownstream, createdAtUntil));
    
    // Add federated sources
    for (String source : federatedSources) {
      federatedQuery.append(" UNION ALL ");
      federatedQuery.append(buildFederatedSourceQuery(source, initialFieldUuids, maxDepth, withDownstream, createdAtUntil));
    }
    
    federatedQuery.append(") SELECT * FROM federated_lineage");
    
    // Execute the federated query through DAO
    try {
      log.debug("Executing federated lineage query across {} sources", federatedSources.size());
      return super.executeFederatedLineageQuery(federatedQuery.toString());
    } catch (Exception e) {
      log.warn("Federated query execution failed: {}", e.getMessage());
      // Fallback to local lineage only
      return fetchDirectColumnLineage(initialFieldUuids, maxDepth, true, createdAtUntil);
    }
  }

  private String buildLocalLineageQuery(Set<UUID> fieldUuids, int depth, boolean withDownstream, Instant createdAt) {
    return String.format("""
        SELECT 
          df.namespace_name, df.dataset_name, df.field_name, df.type,
          cl.transformation_description, cl.transformation_type,
          'local' as source_origin
        FROM column_lineage_recursive cl
        JOIN dataset_fields_view df ON cl.output_dataset_field_uuid = df.uuid
        WHERE cl.output_dataset_field_uuid IN (%s) 
        AND cl.created_at <= '%s'
        """, 
        fieldUuids.stream().map(UUID::toString).collect(Collectors.joining("','")), 
        createdAt.toString());
  }

  private String buildFederatedSourceQuery(String source, Set<UUID> fieldUuids, int depth, boolean withDownstream, Instant createdAt) {
    return String.format("""
        SELECT 
          df.namespace_name, df.dataset_name, df.field_name, df.type,
          cl.transformation_description, cl.transformation_type,
          '%s' as source_origin
        FROM %s.column_lineage_recursive cl
        JOIN %s.dataset_fields_view df ON cl.output_dataset_field_uuid = df.uuid
        WHERE cl.output_dataset_field_uuid IN (%s)
        AND cl.created_at <= '%s'
        """, 
        source, source, source,
        fieldUuids.stream().map(UUID::toString).collect(Collectors.joining("','")), 
        createdAt.toString());
  }

  private Lineage toLineage(Set<ColumnLineageNodeData> lineageNodeData, boolean includeVersion) {
    Set<Node> nodes = new LinkedHashSet<>();
    
    // Build mapping for all unique nodes first
    Map<NodeId, ColumnLineageNodeData> allNodeData = new HashMap<>();
    Map<NodeId, Set<NodeId>> fieldInputToField = new HashMap<>();
    Map<NodeId, Set<NodeId>> fieldOutputToField = new HashMap<>();

    // Collect all nodes and build directional mappings
    for (ColumnLineageNodeData nodeData : lineageNodeData) {
        NodeId nodeId = toNodeId(nodeData, includeVersion);
        allNodeData.put(nodeId, nodeData);

        // Build reverse mappings for field relationships
        for (InputFieldNodeData input : nodeData.getInputFields()) {
            NodeId inputNodeId = toNodeId(input, includeVersion);
            // Track that this field has the input as a producer
            fieldInputToField.computeIfAbsent(nodeId, k -> new HashSet<>()).add(inputNodeId);
            // Track that the input field has this field as a consumer
            fieldOutputToField.computeIfAbsent(inputNodeId, k -> new HashSet<>()).add(nodeId);
            
            // Add input node data if not present
            if (!allNodeData.containsKey(inputNodeId)) {
                allNodeData.put(inputNodeId, new ColumnLineageNodeData(input));
            }
        }

        for (InputFieldNodeData output : nodeData.getOutputFields()) {
            NodeId outputNodeId = toNodeId(output, includeVersion);
            // Track that this field has the output as a consumer
            fieldOutputToField.computeIfAbsent(nodeId, k -> new HashSet<>()).add(outputNodeId);
            // Track that the output field has this field as a producer
            fieldInputToField.computeIfAbsent(outputNodeId, k -> new HashSet<>()).add(nodeId);
            
            // Add output node data if not present
            if (!allNodeData.containsKey(outputNodeId)) {
                allNodeData.put(outputNodeId, new ColumnLineageNodeData(output));
            }
        }
    }

    // Create nodes with proper directional edges
    for (Map.Entry<NodeId, ColumnLineageNodeData> entry : allNodeData.entrySet()) {
        NodeId nodeId = entry.getKey();
        ColumnLineageNodeData nodeData = entry.getValue();
        
        // Build inEdges from fields that produce data TO this field
        Set<Edge> inEdges = buildFieldEdges(fieldInputToField.get(nodeId), nodeId);
        
        // Build outEdges from this field TO fields that consume data from it
        Set<Edge> outEdges = buildFieldEdges(nodeId, fieldOutputToField.get(nodeId));
        
        Node fieldNode = new Node(
            nodeId,
            NodeType.DATASET_FIELD,
            nodeData,
            inEdges.isEmpty() ? null : inEdges,
            outEdges.isEmpty() ? null : outEdges);
        nodes.add(fieldNode);
    }

    return new Lineage(ImmutableSortedSet.copyOf(nodes));
  }

  private Set<Edge> buildFieldEdges(NodeId from, Set<NodeId> toNodes) {
    if (toNodes == null || toNodes.isEmpty()) {
        return Collections.emptySet();
    }
    return toNodes.stream()
        .map(to -> new Edge(from, to))
        .collect(Collectors.toSet());
  }

  private Set<Edge> buildFieldEdges(Set<NodeId> fromNodes, NodeId to) {
    if (fromNodes == null || fromNodes.isEmpty()) {
        return Collections.emptySet();
    }
    return fromNodes.stream()
        .map(from -> new Edge(from, to))
        .collect(Collectors.toSet());
  }

  private static NodeId toNodeId(ColumnLineageNodeData node, boolean includeVersion) {
    if (!includeVersion) {
      return NodeId.of(DatasetFieldId.of(node.getNamespace(), node.getDataset(), node.getField()));
    } else {
      return NodeId.of(
          DatasetFieldVersionId.of(
              node.getNamespace(), node.getDataset(), node.getField(), node.getDatasetVersion()));
    }
  }

  private static NodeId toNodeId(InputFieldNodeData node, boolean includeVersion) {
    if (!includeVersion) {
      return NodeId.of(DatasetFieldId.of(node.getNamespace(), node.getDataset(), node.getField()));
    } else {
      return NodeId.of(
          DatasetFieldVersionId.of(
              node.getNamespace(), node.getDataset(), node.getField(), node.getDatasetVersion()));
    }
  }

  private ColumnNodes getColumnNodes(NodeId nodeId) {
    if (nodeId.isDatasetFieldVersionType()) {
      return getColumnNodes(nodeId.asDatasetFieldVersionId());
    } else if (nodeId.isDatasetVersionType()) {
      return getColumnNodes(nodeId.asDatasetVersionId());
    } else if (nodeId.isJobVersionType()) {
      return getColumnNodes(nodeId.asJobVersionId());
    } else if (nodeId.isDatasetType()) {
      return getColumnNodes(nodeId.asDatasetId());
    } else if (nodeId.isDatasetFieldType()) {
      return getColumnNodes(nodeId.asDatasetFieldId());
    } else if (nodeId.isJobType()) {
      return getColumnNodes(nodeId.asJobId());
    }
    throw new UnsupportedOperationException("Unsupported NodeId: " + nodeId);
  }

  private ColumnNodes getColumnNodes(DatasetVersionId datasetVersionId) {
    List<Pair<UUID, Instant>> fieldsWithInstant =
        datasetFieldDao.findDatasetVersionFieldsUuids(datasetVersionId.getVersion());
    return new ColumnNodes(
        fieldsWithInstant.stream().map(pair -> pair.getValue()).findAny().orElse(Instant.now()),
        fieldsWithInstant.stream().map(pair -> pair.getKey()).collect(Collectors.toList()));
  }

  private ColumnNodes getColumnNodes(DatasetFieldVersionId datasetFieldVersionId) {
    List<Pair<UUID, Instant>> fieldsWithInstant =
        datasetFieldDao.findDatasetVersionFieldsUuids(
            datasetFieldVersionId.getFieldName().getValue(), datasetFieldVersionId.getVersion());
    return new ColumnNodes(
        fieldsWithInstant.stream().map(pair -> pair.getValue()).findAny().orElse(Instant.now()),
        fieldsWithInstant.stream().map(pair -> pair.getKey()).collect(Collectors.toList()));
  }

  private ColumnNodes getColumnNodes(JobVersionId jobVersionId) {
    List<Pair<UUID, Instant>> fieldsWithInstant =
        datasetFieldDao.findFieldsUuidsByJobVersion(jobVersionId.getVersion());
    return new ColumnNodes(
        fieldsWithInstant.stream().map(pair -> pair.getValue()).findAny().orElse(Instant.now()),
        fieldsWithInstant.stream().map(pair -> pair.getKey()).collect(Collectors.toList()));
  }

  private ColumnNodes getColumnNodes(DatasetId datasetId) {
    return new ColumnNodes(
        Instant.now(),
        datasetFieldDao.findDatasetFieldsUuids(
            datasetId.getNamespace().getValue(), datasetId.getName().getValue()));
  }

  private ColumnNodes getColumnNodes(DatasetFieldId datasetFieldId) {
    ColumnNodes columnNodes = new ColumnNodes(Instant.now(), new ArrayList<>());
    datasetFieldDao
        .findUuid(
            datasetFieldId.getDatasetId().getNamespace().getValue(),
            datasetFieldId.getDatasetId().getName().getValue(),
            datasetFieldId.getFieldName().getValue())
        .ifPresent(uuid -> columnNodes.nodeIds.add(uuid));
    return columnNodes;
  }

  private ColumnNodes getColumnNodes(JobId jobId) {
    return new ColumnNodes(
        Instant.now(),
        datasetFieldDao.findFieldsUuidsByJob(
            jobId.getNamespace().getValue(), jobId.getName().getValue()));
  }

  public void enrichWithColumnLineage(List<Dataset> datasets) {
    if (datasets.isEmpty()) {
      return;
    }

    Set<ColumnLineageNodeData> lineageRowsForDatasets =
        getLineageRowsForDatasets(
            datasets.stream()
                .map(d -> Pair.of(d.getNamespace().getValue(), d.getName().getValue()))
                .collect(Collectors.toList()));

    Map<Dataset, List<ColumnLineage>> datasetLineage = new HashMap<>();
    lineageRowsForDatasets.stream()
        .forEach(
            nodeData -> {
              Dataset dataset =
                  datasets.stream()
                      .filter(d -> d.getNamespace().getValue().equals(nodeData.getNamespace()))
                      .filter(d -> d.getName().getValue().equals(nodeData.getDataset()))
                      .findAny()
                      .get();

              if (!datasetLineage.containsKey(dataset)) {
                datasetLineage.put(dataset, new LinkedList<>());
              }
              datasetLineage
                  .get(dataset)
                  .add(
                      ColumnLineage.builder()
                          .name(nodeData.getField())
                          .inputFields(
                              nodeData.getInputFields().stream()
                                  .map(
                                      f ->
                                          new ColumnLineageInputField(
                                              f.getNamespace(),
                                              f.getDataset(),
                                              f.getField(),
                                              f.getTransformationDescription(),
                                              f.getTransformationType()))
                                  .collect(Collectors.toList()))
                          .build());
            });

    datasets.stream()
        .filter(dataset -> datasetLineage.containsKey(dataset))
        .forEach(dataset -> dataset.setColumnLineage(datasetLineage.get(dataset)));
  }

  /**
   * Optimized lineage method that uses a single recursive query instead of multiple iterations.
   * This leverages postgres-fdw's ability to push computation to remote servers.
   */
  public Lineage optimizedLineage(NodeId nodeId, int depth, boolean withDownstream) {
    ColumnNodes columnNodes = getColumnNodes(nodeId);
    if (columnNodes.nodeIds.isEmpty()) {
        throw new NodeIdNotFoundException("Could not find node");
    }

    // Execute a single optimized query that handles all depth traversal in the database
    Set<ColumnLineageNodeData> allLineageData = executeOptimizedLineageQuery(
        new HashSet<>(columnNodes.nodeIds), depth, withDownstream, columnNodes.createdAtUntil);

    log.debug("Completed optimized lineage traversal with {} total nodes", allLineageData.size());
    return toLineage(allLineageData, nodeId.hasVersion());
  }

  /**
   * Execute a single database query that handles all lineage traversal.
   * This reduces network round trips and allows postgres-fdw to optimize query execution.
   */
  private Set<ColumnLineageNodeData> executeOptimizedLineageQuery(
      Set<UUID> initialFieldUuids, int maxDepth, boolean withDownstream, Instant createdAtUntil) {
    
    String optimizedQuery = buildOptimizedLineageQuery(initialFieldUuids, maxDepth, withDownstream, createdAtUntil);
    
    try {
      log.debug("Executing optimized lineage query for {} fields", initialFieldUuids.size());
      return super.executeCustomLineageQuery(optimizedQuery);
    } catch (Exception e) {
      log.warn("Custom query execution failed: {}", e.getMessage());
      // Fallback to iterative approach
      return fetchDirectColumnLineage(initialFieldUuids, maxDepth, true, createdAtUntil);
    }
  }

  private String buildOptimizedLineageQuery(Set<UUID> fieldUuids, int depth, boolean withDownstream, Instant createdAt) {
    return String.format("""
        WITH RECURSIVE 
          -- Include federated tables if available
          all_column_lineage AS (
            -- Local lineage
            SELECT * FROM column_lineage WHERE created_at <= '%s'
            %s
          ),
          lineage_traversal AS (
            -- Base case: starting fields
            SELECT 
              output_dataset_field_uuid,
              input_dataset_field_uuid,
              transformation_description,
              transformation_type,
              0 as depth,
              ARRAY[output_dataset_field_uuid] as visited_path
            FROM all_column_lineage 
            WHERE output_dataset_field_uuid IN (%s)
            
            UNION ALL
            
            -- Recursive case: traverse lineage
            SELECT 
              cl.output_dataset_field_uuid,
              cl.input_dataset_field_uuid,
              cl.transformation_description,
              cl.transformation_type,
              lt.depth + 1,
              lt.visited_path || cl.output_dataset_field_uuid
            FROM all_column_lineage cl
            JOIN lineage_traversal lt ON (
              %s -- Direction conditions
            )
            WHERE lt.depth < %d
              AND NOT cl.output_dataset_field_uuid = ANY(lt.visited_path) -- Cycle detection
          )
        SELECT DISTINCT
          df.namespace_name,
          df.dataset_name, 
          df.field_name,
          df.type,
          lt.transformation_description,
          lt.transformation_type
        FROM lineage_traversal lt
        JOIN dataset_fields_view df ON lt.output_dataset_field_uuid = df.uuid
        """, 
        createdAt.toString(),
        buildFederatedUnionClauses(), // Add federated sources
        fieldUuids.stream().map(u -> "'" + u.toString() + "'").collect(Collectors.joining(",")),
        buildDirectionConditions(withDownstream),
        depth - 1);
  }

  private String buildFederatedUnionClauses() {
    if (federatedConfig == null || !federatedConfig.isEnabled() || 
        federatedConfig.getForeignTableMappings() == null) {
      return "";
    }
    
    StringBuilder unions = new StringBuilder();
    for (FederatedLineageConfig.ForeignTableMapping mapping : federatedConfig.getForeignTableMappings()) {
      unions.append(String.format("""
          UNION ALL 
          SELECT * FROM %s 
          """, mapping.getLocalTableName()));
    }
    return unions.toString();
  }

  private String buildDirectionConditions(boolean withDownstream) {
    if (withDownstream) {
      return """
          (cl.output_dataset_field_uuid = lt.input_dataset_field_uuid  -- upstream
           OR cl.input_dataset_field_uuid = lt.output_dataset_field_uuid) -- downstream
          """;
    } else {
      return "cl.output_dataset_field_uuid = lt.input_dataset_field_uuid"; // upstream only
    }
  }

  private record ColumnNodes(Instant createdAtUntil, List<UUID> nodeIds) {}
}
 