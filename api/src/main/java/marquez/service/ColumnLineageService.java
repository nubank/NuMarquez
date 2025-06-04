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
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class ColumnLineageService extends DelegatingDaos.DelegatingColumnLineageDao {
  private final DatasetFieldDao datasetFieldDao;

  public ColumnLineageService(ColumnLineageDao dao, DatasetFieldDao datasetFieldDao) {
    super(dao);
    this.datasetFieldDao = datasetFieldDao;
  }

  public Lineage lineage(NodeId nodeId, int depth, boolean withDownstream) {
    // 1. Get initial column nodes
    ColumnNodes columnNodes = getColumnNodes(nodeId);
    if (columnNodes.nodeIds.isEmpty()) {
        throw new NodeIdNotFoundException("Could not find node");
    }

    // 2. Initialize collections for lineage collection
    Set<ColumnLineageNodeData> allLineageData = new HashSet<>();
    Set<UUID> processedFields = new HashSet<>();
    Set<UUID> currentLevelFields = new HashSet<>(columnNodes.nodeIds);

    // 3. Process each depth level using BFS
    for (int currentDepth = 0; currentDepth < depth && !currentLevelFields.isEmpty(); currentDepth++) {
        log.debug("Processing depth {} for column lineage", currentDepth);

        // Get direct lineage for current level fields
        Set<ColumnLineageNodeData> directLineage = 
            getDirectColumnLineage(new ArrayList<>(currentLevelFields), withDownstream, columnNodes.createdAtUntil);
        
        // Add to results
        allLineageData.addAll(directLineage);
        
        // Mark current fields as processed
        processedFields.addAll(currentLevelFields);
        
        // Prepare next level fields
        Set<UUID> nextLevelFields = new HashSet<>();
        
        // Process each node in the direct lineage
        for (ColumnLineageNodeData node : directLineage) {
            // Handle upstream lineage (always included)
            for (InputFieldNodeData input : node.getInputFields()) {
                UUID fieldUuid = datasetFieldDao.findUuid(
                    input.getNamespace(), 
                    input.getDataset(), 
                    input.getField())
                    .orElse(null);
                if (fieldUuid != null && !processedFields.contains(fieldUuid)) {
                    nextLevelFields.add(fieldUuid);
                }
            }
            
            // Handle downstream lineage (only if requested)
            if (withDownstream) {
                for (InputFieldNodeData output : node.getOutputFields()) {
                    UUID fieldUuid = datasetFieldDao.findUuid(
                        output.getNamespace(), 
                        output.getDataset(), 
                        output.getField())
                        .orElse(null);
                    if (fieldUuid != null && !processedFields.contains(fieldUuid)) {
                        nextLevelFields.add(fieldUuid);
                    }
                }
            }
        }
        
        // Update current level for next iteration
        currentLevelFields = nextLevelFields;
    }

    // 4. Build and return the lineage graph
    return toLineage(allLineageData, nodeId.hasVersion());
  }

  private Lineage toLineage(Set<ColumnLineageNodeData> lineageNodeData, boolean includeVersion) {
    Map<NodeId, Node.Builder> graphNodes = new HashMap<>();
    Map<NodeId, Set<NodeId>> inEdges = new HashMap<>();
    Map<NodeId, Set<NodeId>> outEdges = new HashMap<>();

    // Create nodes and build edge mappings
    for (ColumnLineageNodeData nodeData : lineageNodeData) {
        NodeId nodeId = toNodeId(nodeData, includeVersion);
        
        // Create or get node builder
        graphNodes.computeIfAbsent(
            nodeId,
            id -> Node.datasetField().data(nodeData).id(id)
        );

        // Process input fields (upstream)
        for (InputFieldNodeData input : nodeData.getInputFields()) {
            NodeId inputNodeId = toNodeId(input, includeVersion);
            
            // Create input node if it doesn't exist
            graphNodes.computeIfAbsent(
                inputNodeId,
                id -> Node.datasetField()
                    .id(id)
                    .data(new ColumnLineageNodeData(input))
            );

            // Add edges
            inEdges.computeIfAbsent(nodeId, k -> new HashSet<>()).add(inputNodeId);
            outEdges.computeIfAbsent(inputNodeId, k -> new HashSet<>()).add(nodeId);
        }

        // Process output fields (downstream)
        for (InputFieldNodeData output : nodeData.getOutputFields()) {
            NodeId outputNodeId = toNodeId(output, includeVersion);
            
            // Create output node if it doesn't exist
            graphNodes.computeIfAbsent(
                outputNodeId,
                id -> Node.datasetField()
                    .id(id)
                    .data(new ColumnLineageNodeData(output))
            );

            // Add edges
            outEdges.computeIfAbsent(nodeId, k -> new HashSet<>()).add(outputNodeId);
            inEdges.computeIfAbsent(outputNodeId, k -> new HashSet<>()).add(nodeId);
        }
    }

    // Build final nodes with edges
    Set<Node> nodes = graphNodes.entrySet().stream()
        .map(entry -> {
            NodeId nodeId = entry.getKey();
            Node.Builder builder = entry.getValue();
            
            // Add in edges
            Set<NodeId> nodeInEdges = inEdges.getOrDefault(nodeId, Collections.emptySet());
            if (!nodeInEdges.isEmpty()) {
                builder.inEdges(nodeInEdges.stream()
                    .map(inNodeId -> new Edge(nodeId, inNodeId))
                    .collect(Collectors.toSet()));
            }
            
            // Add out edges
            Set<NodeId> nodeOutEdges = outEdges.getOrDefault(nodeId, Collections.emptySet());
            if (!nodeOutEdges.isEmpty()) {
                builder.outEdges(nodeOutEdges.stream()
                    .map(outNodeId -> new Edge(nodeId, outNodeId))
                    .collect(Collectors.toSet()));
            }
            
            return builder.build();
        })
        .collect(Collectors.toSet());

    return new Lineage(ImmutableSortedSet.copyOf(nodes));
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

  private record ColumnNodes(Instant createdAtUntil, List<UUID> nodeIds) {}
}
 