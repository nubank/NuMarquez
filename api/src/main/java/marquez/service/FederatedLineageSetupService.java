/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import marquez.db.ColumnLineageDao;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.statement.StatementException;

/**
 * Service for setting up and monitoring postgres-fdw federated lineage connections.
 * This helps manage the lifecycle of federated database connections and monitors their health.
 */
@Slf4j
public class FederatedLineageSetupService {
  
  private final ColumnLineageDao columnLineageDao;
  private final FederatedLineageConfig config;

  public FederatedLineageSetupService(ColumnLineageDao columnLineageDao, FederatedLineageConfig config) {
    this.columnLineageDao = columnLineageDao;
    this.config = config;
  }

  /**
   * Setup all federated servers and foreign tables based on configuration.
   * This should be called during application startup if federated lineage is enabled.
   */
  public void setupFederatedLineage() {
    if (!config.isEnabled() || !config.isValid()) {
      log.info("Federated lineage is disabled or invalid configuration");
      return;
    }

    log.info("Setting up federated lineage with {} servers", config.getFederatedServers().size());

    try (Handle handle = columnLineageDao.getHandle()) {
      // Create servers and user mappings
      String serverSQL = config.generateServerCreationSQL();
      log.debug("Executing server creation SQL: {}", serverSQL);
      handle.execute(serverSQL);

      // Create foreign tables
      String foreignTableSQL = config.generateForeignTableSQL();
      log.debug("Executing foreign table creation SQL: {}", foreignTableSQL);
      handle.execute(foreignTableSQL);

      // Create unified view
      String viewSQL = config.generateUnifiedLineageViewSQL();
      log.debug("Executing unified view creation SQL: {}", viewSQL);
      handle.execute(viewSQL);

      log.info("Successfully set up federated lineage infrastructure");

    } catch (Exception e) {
      log.error("Failed to setup federated lineage: {}", e.getMessage(), e);
      throw new RuntimeException("Federated lineage setup failed", e);
    }
  }

  /**
   * Test connectivity to all configured federated servers.
   * Returns a health report for monitoring purposes.
   */
  public FederatedHealthReport checkFederatedHealth() {
    if (!config.isEnabled()) {
      return FederatedHealthReport.builder()
          .enabled(false)
          .allServersHealthy(true)
          .checkTime(Instant.now())
          .build();
    }

    List<ServerHealthStatus> serverStatuses = new ArrayList<>();

    for (FederatedLineageConfig.FederatedServer server : config.getFederatedServers().values()) {
      ServerHealthStatus status = testServerConnection(server);
      serverStatuses.add(status);
    }

    return FederatedHealthReport.builder()
        .enabled(true)
        .checkTime(Instant.now())
        .configValid(config.isValid())
        .serverStatuses(serverStatuses)
        .allServersHealthy(serverStatuses.stream().allMatch(ServerHealthStatus::isHealthy))
        .build();
  }

  /**
   * Test connection to a specific federated server by executing a simple query.
   */
  private ServerHealthStatus testServerConnection(FederatedLineageConfig.FederatedServer server) {
    String testQuery = String.format(
        "SELECT 1 FROM %s.pg_stat_activity LIMIT 1", 
        server.getServerName());

    try (Handle handle = columnLineageDao.getHandle()) {
      long startTime = System.currentTimeMillis();
      
      handle.createQuery(testQuery)
          .mapTo(Integer.class)
          .findFirst();
      
      long responseTime = System.currentTimeMillis() - startTime;

      return ServerHealthStatus.builder()
          .serverName(server.getServerName())
          .healthy(true)
          .responseTimeMs(responseTime)
          .build();

    } catch (StatementException e) {
      log.warn("Health check failed for server {}: {}", server.getServerName(), e.getMessage());
      
      return ServerHealthStatus.builder()
          .serverName(server.getServerName())
          .healthy(false)
          .errorMessage(e.getMessage())
          .build();
    } catch (Exception e) {
      log.error("Unexpected error during health check for server {}: {}", 
                server.getServerName(), e.getMessage());
      
      return ServerHealthStatus.builder()
          .serverName(server.getServerName())
          .healthy(false)
          .errorMessage("Unexpected error: " + e.getMessage())
          .build();
    }
  }

  /**
   * Cleanup federated resources. Should be called during application shutdown.
   */
  public void cleanupFederatedLineage() {
    if (!config.isEnabled()) {
      return;
    }

    log.info("Cleaning up federated lineage resources");

    try (Handle handle = columnLineageDao.getHandle()) {
      // Drop foreign tables
      for (FederatedLineageConfig.ForeignTableMapping mapping : config.getForeignTableMappings()) {
        try {
          handle.execute("DROP FOREIGN TABLE IF EXISTS " + mapping.getLocalTableName());
        } catch (Exception e) {
          log.warn("Failed to drop foreign table {}: {}", mapping.getLocalTableName(), e.getMessage());
        }
      }

      // Drop servers (this will also drop user mappings)
      for (FederatedLineageConfig.FederatedServer server : config.getFederatedServers().values()) {
        try {
          handle.execute("DROP SERVER IF EXISTS " + server.getServerName() + " CASCADE");
        } catch (Exception e) {
          log.warn("Failed to drop server {}: {}", server.getServerName(), e.getMessage());
        }
      }

      log.info("Federated lineage cleanup completed");

    } catch (Exception e) {
      log.error("Error during federated lineage cleanup: {}", e.getMessage());
    }
  }

  /**
   * Get performance metrics for federated queries.
   */
  public FederatedPerformanceMetrics getPerformanceMetrics() {
    if (!config.isEnabled()) {
      return FederatedPerformanceMetrics.disabled();
    }

    try (Handle handle = columnLineageDao.getHandle()) {
      // Query pg_stat_statements for federated query performance
      String metricsQuery = """
          SELECT 
            query,
            calls,
            total_exec_time,
            mean_exec_time,
            max_exec_time,
            rows
          FROM pg_stat_statements 
          WHERE query LIKE '%unified_column_lineage%' 
             OR query LIKE '%foreign%'
          ORDER BY total_exec_time DESC
          LIMIT 10
          """;

      return handle.createQuery(metricsQuery)
          .map((rs, ctx) -> FederatedPerformanceMetrics.builder()
              .enabled(true)
              .totalCalls(rs.getLong("calls"))
              .avgExecutionTimeMs(rs.getDouble("mean_exec_time"))
              .maxExecutionTimeMs(rs.getDouble("max_exec_time"))
              .totalRows(rs.getLong("rows"))
              .build())
          .findFirst()
          .orElse(FederatedPerformanceMetrics.noData());

    } catch (Exception e) {
      log.warn("Failed to get performance metrics: {}", e.getMessage());
      return FederatedPerformanceMetrics.error(e.getMessage());
    }
  }

  @Data
  @Builder
  public static class FederatedHealthReport {
    private final boolean enabled;
    private final boolean configValid;
    private final Instant checkTime;
    private final List<ServerHealthStatus> serverStatuses;
    private final boolean allServersHealthy;
  }

  @Data
  @Builder
  public static class ServerHealthStatus {
    private final String serverName;
    private final boolean healthy;
    private final Long responseTimeMs;
    private final String errorMessage;
  }

  public static class FederatedPerformanceMetrics {
    private final boolean enabled;
    private final Long totalCalls;
    private final Double avgExecutionTimeMs;
    private final Double maxExecutionTimeMs;
    private final Long totalRows;
    private final String errorMessage;

    public static Builder builder() {
      return new Builder();
    }

    public static FederatedPerformanceMetrics disabled() {
      return builder().enabled(false).build();
    }

    public static FederatedPerformanceMetrics noData() {
      return builder().enabled(true).totalCalls(0L).build();
    }

    public static FederatedPerformanceMetrics error(String errorMessage) {
      return builder().enabled(true).errorMessage(errorMessage).build();
    }

    public static class Builder {
      private boolean enabled = true;
      private Long totalCalls;
      private Double avgExecutionTimeMs;
      private Double maxExecutionTimeMs;
      private Long totalRows;
      private String errorMessage;

      public Builder enabled(boolean enabled) { this.enabled = enabled; return this; }
      public Builder totalCalls(Long totalCalls) { this.totalCalls = totalCalls; return this; }
      public Builder avgExecutionTimeMs(Double avgExecutionTimeMs) { this.avgExecutionTimeMs = avgExecutionTimeMs; return this; }
      public Builder maxExecutionTimeMs(Double maxExecutionTimeMs) { this.maxExecutionTimeMs = maxExecutionTimeMs; return this; }
      public Builder totalRows(Long totalRows) { this.totalRows = totalRows; return this; }
      public Builder errorMessage(String errorMessage) { this.errorMessage = errorMessage; return this; }

      public FederatedPerformanceMetrics build() {
        return new FederatedPerformanceMetrics(enabled, totalCalls, avgExecutionTimeMs, maxExecutionTimeMs, totalRows, errorMessage);
      }
    }

    private FederatedPerformanceMetrics(boolean enabled, Long totalCalls, Double avgExecutionTimeMs, 
                                        Double maxExecutionTimeMs, Long totalRows, String errorMessage) {
      this.enabled = enabled;
      this.totalCalls = totalCalls;
      this.avgExecutionTimeMs = avgExecutionTimeMs;
      this.maxExecutionTimeMs = maxExecutionTimeMs;
      this.totalRows = totalRows;
      this.errorMessage = errorMessage;
    }

    // Getters
    public boolean isEnabled() { return enabled; }
    public Long getTotalCalls() { return totalCalls; }
    public Double getAvgExecutionTimeMs() { return avgExecutionTimeMs; }
    public Double getMaxExecutionTimeMs() { return maxExecutionTimeMs; }
    public Long getTotalRows() { return totalRows; }
    public String getErrorMessage() { return errorMessage; }
  }
} 