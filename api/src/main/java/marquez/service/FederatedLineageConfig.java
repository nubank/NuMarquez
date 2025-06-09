/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Configuration class for managing postgres-fdw federated lineage connections.
 * This helps set up and manage foreign data wrappers for cross-database lineage queries.
 */
@Slf4j
@Data
@Builder
public class FederatedLineageConfig {
  
  /**
   * Map of federated server names to their connection details
   */
  private Map<String, FederatedServer> federatedServers;
  
  /**
   * List of foreign table mappings for lineage data
   */
  private List<ForeignTableMapping> foreignTableMappings;
  
  /**
   * Whether to enable federated lineage queries
   */
  @Builder.Default
  private boolean enabled = false;
  
  /**
   * Maximum number of federated servers to query in parallel
   */
  @Builder.Default
  private int maxParallelConnections = 5;

  @Data
  @Builder
  public static class FederatedServer {
    private String serverName;
    private String hostName;
    private int port;
    private String databaseName;
    private String username;
    private String password;
    private Map<String, String> options;
  }

  @Data
  @Builder
  public static class ForeignTableMapping {
    private String localTableName;
    private String foreignServerName;
    private String foreignTableName;
    private String foreignSchemaName;
  }

  /**
   * Generate SQL statements to create federated server connections
   */
  public String generateServerCreationSQL() {
    StringBuilder sql = new StringBuilder();
    
    sql.append("-- Create postgres_fdw extension if not exists\n");
    sql.append("CREATE EXTENSION IF NOT EXISTS postgres_fdw;\n\n");
    
    for (FederatedServer server : federatedServers.values()) {
      sql.append(String.format("""
          -- Create server: %s
          CREATE SERVER IF NOT EXISTS %s
          FOREIGN DATA WRAPPER postgres_fdw
          OPTIONS (host '%s', port '%d', dbname '%s');
          
          -- Create user mapping
          CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
          SERVER %s
          OPTIONS (user '%s', password '%s');
          
          """, 
          server.getServerName(),
          server.getServerName(),
          server.getHostName(),
          server.getPort(),
          server.getDatabaseName(),
          server.getServerName(),
          server.getUsername(),
          server.getPassword()));
    }
    
    return sql.toString();
  }

  /**
   * Generate SQL to create foreign tables for lineage data
   */
  public String generateForeignTableSQL() {
    StringBuilder sql = new StringBuilder();
    
    for (ForeignTableMapping mapping : foreignTableMappings) {
      sql.append(String.format("""
          -- Create foreign table for lineage data
          CREATE FOREIGN TABLE IF NOT EXISTS %s (
            output_dataset_version_uuid UUID,
            output_dataset_field_uuid UUID,
            input_dataset_version_uuid UUID,
            input_dataset_field_uuid UUID,
            transformation_description TEXT,
            transformation_type TEXT,
            created_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ
          )
          SERVER %s
          OPTIONS (schema_name '%s', table_name '%s');
          
          """,
          mapping.getLocalTableName(),
          mapping.getForeignServerName(),
          mapping.getForeignSchemaName(),
          mapping.getForeignTableName()));
    }
    
    return sql.toString();
  }

  /**
   * Generate a unified view that combines local and federated lineage data
   */
  public String generateUnifiedLineageViewSQL() {
    StringBuilder sql = new StringBuilder();
    
    sql.append("""
        -- Create unified lineage view combining local and federated data
        CREATE OR REPLACE VIEW unified_column_lineage AS
        SELECT 
          output_dataset_version_uuid,
          output_dataset_field_uuid,
          input_dataset_version_uuid,
          input_dataset_field_uuid,
          transformation_description,
          transformation_type,
          created_at,
          updated_at,
          'local' as lineage_source
        FROM column_lineage
        """);
    
    for (ForeignTableMapping mapping : foreignTableMappings) {
      sql.append(String.format("""
          
          UNION ALL
          
          SELECT 
            output_dataset_version_uuid,
            output_dataset_field_uuid,
            input_dataset_version_uuid,
            input_dataset_field_uuid,
            transformation_description,
            transformation_type,
            created_at,
            updated_at,
            '%s' as lineage_source
          FROM %s
          """,
          mapping.getForeignServerName(),
          mapping.getLocalTableName()));
    }
    
    sql.append(";\n");
    return sql.toString();
  }

  /**
   * Validate the federated configuration
   */
  public boolean isValid() {
    if (!enabled) {
      return true; // Valid if disabled
    }
    
    if (federatedServers == null || federatedServers.isEmpty()) {
      log.warn("No federated servers configured");
      return false;
    }
    
    if (foreignTableMappings == null || foreignTableMappings.isEmpty()) {
      log.warn("No foreign table mappings configured");
      return false;
    }
    
    return true;
  }
} 