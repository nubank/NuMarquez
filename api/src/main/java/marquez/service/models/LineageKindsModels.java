/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;
import lombok.Value;

/**
 * Container class for all LineageKinds-related model classes.
 * These models represent the "kinds" format for lineage data.
 */
public class LineageKindsModels {

  @Value
  @lombok.Builder
  @lombok.extern.jackson.Jacksonized
  public static class ConversionRequest {
    @JsonProperty("nodeId")
    @NotNull
    String nodeId;
    
    @JsonProperty("depth")
    @lombok.Builder.Default
    int depth = 20;
    
    @JsonProperty("targetKind")
    @lombok.Builder.Default
    String targetKind = "LineageGraph";
    
    @JsonProperty("includeMetadata")
    @lombok.Builder.Default
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