/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.service.models.LineageKindsModels.*;
import marquez.service.models.Lineage;
import marquez.service.models.Node;
import marquez.service.models.NodeId;

@Slf4j
public class LineageKindsService {

  private final LineageService lineageService;

  public LineageKindsService(@NonNull LineageService lineageService) {
    this.lineageService = lineageService;
  }

  /**
   * Convert traditional Marquez lineage to LineageGraph kind format
   */
  public LineageGraphKind convertToLineageGraphKind(
      @NonNull String nodeIdValue, 
      int depth,
      boolean includeMetadata) throws NodeIdNotFoundException {
    
    log.debug("Converting lineage to kinds format for nodeId: {}, depth: {}", nodeIdValue, depth);
    
    // 1. Get traditional lineage using existing directLineage service
    NodeId nodeId = NodeId.of(nodeIdValue);
    Lineage traditionalLineage = lineageService.directLineage(nodeId, depth);
    
    // 2. Convert to kinds format
    return convertToLineageGraphKind(traditionalLineage, nodeId, depth, includeMetadata);
  }

  /**
   * Get a LineageGraph kind by user-friendly name
   */
  public LineageGraphKind getLineageGraphByName(
      @NonNull String name, 
      int depth) throws NodeIdNotFoundException {
    
    log.debug("Getting lineage graph by name: {}, depth: {}", name, depth);
    
    NodeId nodeId = deriveNodeIdFromName(name);
    Lineage traditionalLineage = lineageService.directLineage(nodeId, depth);
    
    return convertToLineageGraphKind(traditionalLineage, nodeId, depth, true);
  }

  /**
   * List LineageGraph kinds with optional filtering
   */
  public LineageGraphKindList listLineageGraphKinds(String labelSelector, int limit) {
    log.debug("Listing lineage graphs with labelSelector: {}, limit: {}", labelSelector, limit);
    
    // This is a simplified implementation
    // In practice, you'd query your metadata store based on labels
    List<LineageGraphKind> items = findLineageGraphsByLabels(labelSelector, limit);
    
    return LineageGraphKindList.builder()
        .apiVersion("graphs/v1alpha1")
        .kind("LineageGraphList")
        .metadata(ListMetadata.builder()
            .totalCount(items.size())
            .build())
        .items(items)
        .build();
  }

  /**
   * Convert traditional Marquez lineage to LineageGraph kind
   */
  private LineageGraphKind convertToLineageGraphKind(
      Lineage traditionalLineage, 
      NodeId centralNodeId, 
      int depth,
      boolean includeMetadata) {
    
    // Extract central node information
    CentralNodeInfo centralNode = extractCentralNodeInfo(traditionalLineage, centralNodeId);
    
    // Build metadata
    KindMetadata.KindMetadataBuilder metadataBuilder = KindMetadata.builder()
        .name(generateLineageGraphName(centralNodeId))
        .graphDepth(depth)
        .centralNode(centralNode)
        .createdAt(Instant.now());
    
    if (includeMetadata) {
      // Add governance-related labels and annotations
      metadataBuilder
          .labels(Map.of(
              "data-domain", centralNode.getDataGovernance().getDataDomain(),
              "data-subdomain", centralNode.getDataGovernance().getDataSubdomain(),
              "geo", centralNode.getDataGovernance().getGeo(),
              "source-system", centralNode.getSourceSystem() != null ? centralNode.getSourceSystem() : "unknown"
          ))
          .annotations(Map.of(
              "marquez.source-endpoint", "/api/v1/lineage/direct",
              "conversion.timestamp", Instant.now().toString(),
              "conversion.depth", String.valueOf(depth)
          ));
    }
    
    // Convert nodes
    List<DataObjectNodeSpec> nodes = traditionalLineage.getGraph().stream()
        .map(this::convertToDataObjectNodeSpec)
        .collect(Collectors.toList());
    
    return LineageGraphKind.builder()
        .apiVersion("graphs/v1alpha1")
        .kind("LineageGraph")
        .metadata(metadataBuilder.build())
        .spec(LineageGraphSpec.builder()
            .nodes(nodes)
            .build())
        .build();
  }

  /**
   * Convert a traditional graph node to DataObjectNodeSpec
   */
  private DataObjectNodeSpec convertToDataObjectNodeSpec(Node traditionalNode) {
    return DataObjectNodeSpec.builder()
        .nurn(generateNuRN(traditionalNode))
        .name(traditionalNode.getId().getValue())
        .type(inferDataObjectType(traditionalNode))
        .sourceSystem(extractSourceSystem(traditionalNode))
        .dataGovernance(extractDataGovernance(traditionalNode))
        .distanceFromTheCenter(calculateDistance(traditionalNode))
        .inEdges(extractInEdges(traditionalNode))
        .outEdges(extractOutEdges(traditionalNode))
        .description(extractDescription(traditionalNode))
        .version(extractVersion(traditionalNode))
        .build();
  }

  // Helper methods for data extraction and transformation
  private String generateNuRN(Node node) {
    String nodeValue = node.getId().getValue();
    return "nurn:nu:data:metapod:" + nodeValue.replace(":", "/");
  }

  private String inferDataObjectType(Node node) {
    if (node.getId().getValue().contains("stream")) return "stream";
    if (node.getId().getValue().contains("notebook")) return "notebook";
    return "dataset"; // default
  }

  private DataGovernance extractDataGovernance(Node node) {
    return DataGovernance.builder()
        .geo("DATA") // default or extracted from metadata
        .dataDomain("data") // extracted from node metadata
        .dataSubdomain("lineage") // extracted from node metadata
        .build();
  }

  private List<String> extractInEdges(Node node) {
    return node.getInEdges().stream()
        .map(edge -> edge.getOrigin().getValue())
        .collect(Collectors.toList());
  }

  private List<String> extractOutEdges(Node node) {
    return node.getOutEdges().stream()
        .map(edge -> edge.getDestination().getValue())
        .collect(Collectors.toList());
  }

  private String extractSourceSystem(Node node) {
    return "Marquez";
  }

  private Integer calculateDistance(Node node) {
    // Calculate distance from center node
    // For now, return 0 (would need proper graph traversal)
    return 0;
  }

  private String extractDescription(Node node) {
    return "Converted from Marquez node: " + node.getId().getValue();
  }

  private String extractVersion(Node node) {
    return "v1.0";
  }

  private CentralNodeInfo extractCentralNodeInfo(Lineage lineage, NodeId centralNodeId) {
    String centralIdValue = centralNodeId.getValue();
    
    // Extract namespace and name from nodeId (format: "dataset:namespace:name")
    String[] parts = centralIdValue.split(":", 3);
    String namespace = parts.length > 1 ? parts[1] : "default";
    String name = parts.length > 2 ? parts[2] : centralIdValue;
    
    return CentralNodeInfo.builder()
        .dataGovernance(DataGovernance.builder()
            .geo("DATA")
            .dataDomain(namespace)
            .dataSubdomain("datasets")
            .build())
        .nurn("nurn:nu:data:metapod:" + centralIdValue.replace(":", "/"))
        .name(namespace + "/" + name)
        .type("dataset")
        .sourceSystem("Marquez")
        .build();
  }

  private String generateLineageGraphName(NodeId nodeId) {
    return nodeId.getValue().replace(":", "-") + "-lineage";
  }

  private NodeId deriveNodeIdFromName(String name) {
    // Convert lineage graph name back to NodeId
    String nodeIdValue = name.replace("-lineage", "").replace("-", ":");
    return NodeId.of(nodeIdValue);
  }

  private List<LineageGraphKind> findLineageGraphsByLabels(String labelSelector, int limit) {
    // Implementation would query your metadata store
    // For now, return empty list
    return List.of();
  }
} 