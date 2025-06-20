/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.service.LineageKindsService;
import marquez.service.NodeIdNotFoundException;
import marquez.service.ServiceFactory;
import marquez.service.models.LineageKindsModels.LineageGraphKind;
import marquez.service.models.LineageKindsModels.LineageGraphKindList;

@Slf4j
@Path("/api/v1/alpha")
public class LineageKindsResource extends BaseResource {

  private final LineageKindsService lineageKindsService;

  public LineageKindsResource(@NonNull final ServiceFactory serviceFactory) {
    super(serviceFactory);
    this.lineageKindsService = serviceFactory.getLineageKindsService();
  }

  /**
   * Get lineage kinds using GET with path parameters Example: GET
   * /api/graphs/v1alpha1/kinds/dataset%3Abrazil-operations.smart-efficiency%3Adataset.ae-training-case-andre-brito?depth=2
   */
  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Produces(APPLICATION_JSON)
  @Path("/kinds/{nodeId}")
  public Response getLineageKinds(
      @PathParam("nodeId") @NotNull String nodeId,
      @QueryParam("depth") @DefaultValue("1") int depth,
      @QueryParam("includeMetadata") @DefaultValue("true") boolean includeMetadata) {

    try {
      // URL decode the nodeId since it contains special characters like colons
      String decodedNodeId;
      try {
        decodedNodeId = URLDecoder.decode(nodeId, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        log.error("Failed to decode nodeId: {}", nodeId, e);
        return Response.status(400)
            .entity(Map.of("error", "Invalid nodeId encoding", "nodeId", nodeId))
            .build();
      }

      log.info("Getting lineage kinds for nodeId: {}", decodedNodeId);

      // Delegate to service
      LineageGraphKind lineageGraphKind =
          lineageKindsService.convertToLineageGraphKind(decodedNodeId, depth, includeMetadata);

      return Response.ok(lineageGraphKind).build();

    } catch (NodeIdNotFoundException e) {
      log.warn("NodeId not found: {}", nodeId);
      return Response.status(404)
          .entity(Map.of("error", "Node not found", "nodeId", nodeId))
          .build();
    } catch (Exception e) {
      log.error("Failed to get lineage kinds for nodeId: {}", nodeId, e);
      return Response.status(500)
          .entity(Map.of("error", "Conversion failed", "message", e.getMessage()))
          .build();
    }
  }

  /**
   * Alternative endpoint using query parameter instead of path parameter Example: GET
   * /api/graphs/v1alpha1/kinds?nodeId=dataset:ai-private-banking.common:dataset.nusignals-stability-index-daily-categorization-time&depth=2
   */
  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Produces(APPLICATION_JSON)
  @Path("/kinds")
  public Response getLineageKindsByQuery(
      @QueryParam("nodeId") @NotNull String nodeId,
      @QueryParam("depth") @DefaultValue("1") int depth,
      @QueryParam("includeMetadata") @DefaultValue("true") boolean includeMetadata) {

    try {
      log.info("Getting lineage kinds for nodeId: {}", nodeId);

      // No need to URL decode since it's a query parameter
      LineageGraphKind lineageGraphKind =
          lineageKindsService.convertToLineageGraphKind(nodeId, depth, includeMetadata);

      return Response.ok(lineageGraphKind).build();

    } catch (NodeIdNotFoundException e) {
      log.warn("NodeId not found: {}", nodeId);
      return Response.status(404)
          .entity(Map.of("error", "Node not found", "nodeId", nodeId))
          .build();
    } catch (Exception e) {
      log.error("Failed to get lineage kinds for nodeId: {}", nodeId, e);
      return Response.status(500)
          .entity(Map.of("error", "Conversion failed", "message", e.getMessage()))
          .build();
    }
  }

  /** Get a LineageGraph kind by name */
  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Produces(APPLICATION_JSON)
  @Path("/lineage-graphs/{name}")
  public Response getLineageGraphKind(
      @PathParam("name") @NotNull String name, @QueryParam("depth") @DefaultValue("20") int depth) {

    try {
      log.info("Getting lineage graph by name: {}", name);

      LineageGraphKind lineageGraphKind = lineageKindsService.getLineageGraphByName(name, depth);

      return Response.ok(lineageGraphKind).build();

    } catch (NodeIdNotFoundException e) {
      log.warn("LineageGraph not found: {}", name);
      return Response.status(404)
          .entity(Map.of("error", "LineageGraph not found", "name", name))
          .build();
    } catch (Exception e) {
      log.error("Failed to get lineage graph by name: {}", name, e);
      return Response.status(500)
          .entity(Map.of("error", "Failed to get lineage graph", "message", e.getMessage()))
          .build();
    }
  }

  /** List LineageGraph kinds with optional filtering */
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
      log.info("Listing lineage graphs with labelSelector: {}, limit: {}", labelSelector, limit);

      LineageGraphKindList response =
          lineageKindsService.listLineageGraphKinds(labelSelector, limit);

      return Response.ok(response).build();

    } catch (Exception e) {
      log.error("Failed to list LineageGraph kinds", e);
      return Response.status(500)
          .entity(Map.of("error", "Failed to list lineage graphs", "message", e.getMessage()))
          .build();
    }
  }
}
