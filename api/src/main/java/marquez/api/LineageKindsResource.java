/*
 * Example implementation of Data Lineage Kinds API endpoints
 * Add these methods to your OpenLineageResource.java or create a separate resource class
 */

package marquez.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import marquez.service.ServiceFactory;
import marquez.service.NodeIdNotFoundException;
import marquez.service.models.NodeId;
import marquez.service.models.Lineage;

@Slf4j
@Path("/api/graphs/v1alpha1")
public class LineageKindsResource extends BaseResource {

  public LineageKindsResource(@NonNull final ServiceFactory serviceFactory) {
    super(serviceFactory);
  }

  /**
   * Convert traditional lineage to kinds format
   * This is the main testing endpoint that transforms getDirectLineage output
   */
  @Timed
  @ResponseMetered
  @ExceptionMetered
  @POST
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  @Path("/convert/traditional-to-kinds")
  public Response convertTraditionalToKinds(@Valid @NotNull ConversionRequest request) {
    try {
      log.info("Converting lineage to kinds format for nodeId: {}", request.getNodeId());
      
      // 1. Get traditional lineage using existing service
      NodeId nodeId = NodeId.of(request.getNodeId());
      Lineage traditionalLineage = lineageService.directLineage(nodeId, request.getDepth());
      
      // 2. Convert to kinds format
      LineageGraphKind lineageGraphKind = convertToLineageGraphKind(
          traditionalLineage, 
          nodeId, 
          request.getDepth(),
          request.isIncludeMetadata()
      );
      
      // 3. Create conversion response
      ConversionResponse response = ConversionResponse.builder()
          .traditional(traditionalLineage)
          .kinds(lineageGraphKind)
          .conversionMetadata(ConversionMetadata.builder()
              .timestamp(Instant.now())
              .sourceEndpoint("/api/v1/lineage/direct")
              .nodesProcessed(traditionalLineage.getGraph().size())
              .build())
          .build();
      
      return Response.ok(response).build();
      
    } catch (Exception e) {
      log.error("Failed to convert lineage to kinds format", e);
      return Response.status(500)
          .entity(Map.of("error", "Conversion failed", "message", e.getMessage()))
          .build();
    }
  }

  /**
   * Get a LineageGraph kind by name
   */
  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Produces(APPLICATION_JSON)
  @Path("/lineage-graphs/{name}")
  public Response getLineageGraphKind(
      @PathParam("name") @NotNull String name,
      @QueryParam("depth") @DefaultValue("20") int depth) {
    
    try {
      // For this example, we'll derive the nodeId from the name
      // In a real implementation, you might have a mapping service
      NodeId nodeId = deriveNodeIdFromName(name);
      
      Lineage traditionalLineage = lineageService.directLineage(nodeId, depth);
      LineageGraphKind lineageGraphKind = convertToLineageGraphKind(
          traditionalLineage, nodeId, depth, true);
      
      return Response.ok(lineageGraphKind).build();
      
    } catch (NodeIdNotFoundException e) {
      return Response.status(404)
          .entity(Map.of("error", "LineageGraph not found", "name", name))
          .build();
    }
  }

  /**
   * List LineageGraph kinds with optional filtering
   */
  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Produces(APPLICATION_JSON)
  @Path("/lineage-graphs")
  public Response listLineageGraphKinds(
      @QueryParam("labelSelector") String labelSelector,
      @QueryParam("limit") @DefaultValue("50") @Min(1) int limit) {
    
    try {
      // This is a simplified implementation
      // In practice, you'd query your metadata store based on labels
      List<LineageGraphKind> items = findLineageGraphsByLabels(labelSelector, limit);
      
      LineageGraphKindList response = LineageGraphKindList.builder()
          .apiVersion("graphs/v1alpha1")
          .kind("LineageGraphList")
          .metadata(ListMetadata.builder()
              .totalCount(items.size())
              .build())
          .items(items)
          .build();
      
      return Response.ok(response).build();
      
    } catch (Exception e) {
      log.error("Failed to list LineageGraph kinds", e);
      return Response.status(500).build();
    }
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
  private DataObjectNodeSpec convertToDataObjectNodeSpec(marquez.service.models.Node traditionalNode) {
    // This is a simplified conversion
    // You'll need to adapt based on your actual Node structure
    
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
  private String generateNuRN(marquez.service.models.Node node) {
    // Generate NuRN based on your organization's conventions
    String nodeValue = node.getId().getValue();
    return "nurn:nu:data:metapod:" + nodeValue.replace(":", "/");
  }

  private String inferDataObjectType(marquez.service.models.Node node) {
    // Infer type based on node characteristics
    if (node.getId().getValue().contains("stream")) return "stream";
    if (node.getId().getValue().contains("notebook")) return "notebook";
    return "dataset"; // default
  }

  private DataGovernance extractDataGovernance(marquez.service.models.Node node) {
    // Extract governance info from node data
    // This would depend on your data model
    return DataGovernance.builder()
        .geo("DATA") // default or extracted from metadata
        .dataDomain("data") // extracted from node metadata
        .dataSubdomain("lineage") // extracted from node metadata
        .build();
  }

  private List<String> extractInEdges(marquez.service.models.Node node) {
    // Convert traditional edges to string references
    return node.getInEdges().stream()
        .map(edge -> edge.getOrigin().getValue())
        .collect(Collectors.toList());
  }

  private List<String> extractOutEdges(marquez.service.models.Node node) {
    // Convert traditional edges to string references
    return node.getOutEdges().stream()
        .map(edge -> edge.getDestination().getValue())
        .collect(Collectors.toList());
  }

  // Additional helper methods for data extraction
  private String extractSourceSystem(marquez.service.models.Node node) {
    // Extract source system from node data
    // For now, return a default value
    return "Marquez";
  }

  private Integer calculateDistance(marquez.service.models.Node node) {
    // Calculate distance from center node
    // For now, return 0 (would need proper graph traversal)
    return 0;
  }

  private String extractDescription(marquez.service.models.Node node) {
    // Extract description from node data
    return "Converted from Marquez node: " + node.getId().getValue();
  }

  private String extractVersion(marquez.service.models.Node node) {
    // Extract version from node data
    return "v1.0";
  }

  private CentralNodeInfo extractCentralNodeInfo(Lineage lineage, NodeId centralNodeId) {
    // Find the central node in the graph
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
    // This is a simplified implementation
    String nodeIdValue = name.replace("-lineage", "").replace("-", ":");
    return NodeId.of(nodeIdValue);
  }

  private List<LineageGraphKind> findLineageGraphsByLabels(String labelSelector, int limit) {
    // Implementation would query your metadata store
    // For now, return empty list
    return List.of();
  }

  // Data classes for the kinds API
  @Value
  @lombok.Builder
  public static class ConversionRequest {
    @JsonProperty("nodeId")
    @NotNull
    String nodeId;
    
    @JsonProperty("depth")
    int depth = 20;
    
    @JsonProperty("targetKind")
    String targetKind = "LineageGraph";
    
    @JsonProperty("includeMetadata")
    boolean includeMetadata = true;
  }

  @Value
  @lombok.Builder
  public static class ConversionResponse {
    @JsonProperty("traditional")
    Lineage traditional;
    
    @JsonProperty("kinds")
    LineageGraphKind kinds;
    
    @JsonProperty("conversionMetadata")
    ConversionMetadata conversionMetadata;
  }

  @Value
  @lombok.Builder
  public static class ConversionMetadata {
    @JsonProperty("timestamp")
    Instant timestamp;
    
    @JsonProperty("sourceEndpoint")
    String sourceEndpoint;
    
    @JsonProperty("nodesProcessed")
    Integer nodesProcessed;
  }

  @Value
  @lombok.Builder
  public static class LineageGraphKind {
    @JsonProperty("apiVersion")
    String apiVersion;
    
    @JsonProperty("kind")
    String kind;
    
    @JsonProperty("metadata")
    KindMetadata metadata;
    
    @JsonProperty("spec")
    LineageGraphSpec spec;
  }

  @Value
  @lombok.Builder
  public static class KindMetadata {
    @JsonProperty("name")
    String name;
    
    @JsonProperty("graphDepth")
    Integer graphDepth;
    
    @JsonProperty("centralNode") 
    CentralNodeInfo centralNode;
    
    @JsonProperty("labels")
    Map<String, String> labels;
    
    @JsonProperty("annotations")
    Map<String, String> annotations;
    
    @JsonProperty("createdAt")
    Instant createdAt;
  }

  @Value
  @lombok.Builder
  public static class LineageGraphSpec {
    @JsonProperty("nodes")
    List<DataObjectNodeSpec> nodes;
  }

  @Value
  @lombok.Builder
  public static class DataObjectNodeSpec {
    @JsonProperty("nurn")
    String nurn;
    
    @JsonProperty("name")
    String name;
    
    @JsonProperty("type")
    String type;
    
    @JsonProperty("sourceSystem")
    String sourceSystem;
    
    @JsonProperty("dataGovernance")
    DataGovernance dataGovernance;
    
    @JsonProperty("distanceFromTheCenter")
    Integer distanceFromTheCenter;
    
    @JsonProperty("inEdges")
    List<String> inEdges;
    
    @JsonProperty("outEdges")
    List<String> outEdges;
    
    @JsonProperty("description")
    String description;
    
    @JsonProperty("version")
    String version;
  }

  @Value
  @lombok.Builder
  public static class DataGovernance {
    @JsonProperty("geo")
    String geo;
    
    @JsonProperty("dataDomain")
    String dataDomain;
    
    @JsonProperty("dataSubdomain") 
    String dataSubdomain;
  }

  @Value
  @lombok.Builder
  public static class CentralNodeInfo {
    @JsonProperty("dataGovernance")
    DataGovernance dataGovernance;
    
    @JsonProperty("nurn")
    String nurn;
    
    @JsonProperty("name")
    String name;
    
    @JsonProperty("type")
    String type;
    
    @JsonProperty("sourceSystem")
    String sourceSystem;
  }

  @Value
  @lombok.Builder
  public static class LineageGraphKindList {
    @JsonProperty("apiVersion")
    String apiVersion;
    
    @JsonProperty("kind")
    String kind;
    
    @JsonProperty("metadata")
    ListMetadata metadata;
    
    @JsonProperty("items")
    List<LineageGraphKind> items;
  }

  @Value
  @lombok.Builder
  public static class ListMetadata {
    @JsonProperty("totalCount")
    Integer totalCount;
  }
} 