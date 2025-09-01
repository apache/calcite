/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.rest.RESTCatalog;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages Iceberg catalog instances and table loading.
 */
public class IcebergCatalogManager {
  private static final Map<String, Catalog> CATALOG_CACHE = new HashMap<>();

  /**
   * Loads a table from the specified catalog configuration.
   *
   * @param config The configuration containing catalog details
   * @param tablePath The table path or identifier
   * @return The loaded Iceberg table
   */
  public static Table loadTable(Map<String, Object> config, String tablePath) {
    // Check if this is a direct file path (starts with / or contains file://)
    if (tablePath.startsWith("/") || tablePath.startsWith("file://") || tablePath.contains("warehouse")) {
      // Direct path loading - load table directly from filesystem
      try {
        Configuration hadoopConf = new Configuration();
        return new HadoopTables(hadoopConf).load(tablePath);
      } catch (Exception e) {
        throw new RuntimeException("Failed to load table from path: " + tablePath, e);
      }
    }

    String catalogType = (String) config.get("catalog");
    if (catalogType == null) {
      catalogType = "hadoop";
    }

    Catalog catalog = getCatalog(catalogType, config);

    // Parse table identifier
    TableIdentifier tableId = parseTableIdentifier(tablePath, config);

    return catalog.loadTable(tableId);
  }

  /**
   * Gets or creates a catalog instance for use by storage provider.
   *
   * @param catalogType The type of catalog (hadoop, hive, rest)
   * @param config The configuration
   * @return The catalog instance
   */
  public static synchronized Catalog getCatalogForProvider(String catalogType, Map<String, Object> config) {
    return getCatalog(catalogType, config);
  }

  /**
   * Gets or creates a catalog instance.
   *
   * @param catalogType The type of catalog (hadoop, hive, rest)
   * @param config The configuration
   * @return The catalog instance
   */
  private static synchronized Catalog getCatalog(String catalogType, Map<String, Object> config) {
    String cacheKey = catalogType + ":" + config.hashCode();

    if (CATALOG_CACHE.containsKey(cacheKey)) {
      return CATALOG_CACHE.get(cacheKey);
    }

    Catalog catalog;
    switch (catalogType.toLowerCase()) {
      case "hadoop":
        catalog = createHadoopCatalog(config);
        break;
      case "hive":
        throw new UnsupportedOperationException("Hive catalog support not yet implemented");
        // catalog = createHiveCatalog(config);
        // break;
      case "rest":
        catalog = createRestCatalog(config);
        break;
      default:
        throw new IllegalArgumentException("Unknown catalog type: " + catalogType);
    }

    CATALOG_CACHE.put(cacheKey, catalog);
    return catalog;
  }

  /**
   * Creates a Hadoop catalog.
   *
   * @param config The configuration
   * @return The Hadoop catalog
   */
  private static HadoopCatalog createHadoopCatalog(Map<String, Object> config) {
    String warehouse = (String) config.get("warehouse");
    if (warehouse == null) {
      warehouse = (String) config.get("warehousePath");
    }
    if (warehouse == null) {
      throw new IllegalArgumentException("Hadoop catalog requires 'warehouse' or 'warehousePath' configuration");
    }

    Configuration hadoopConf = new Configuration();

    // Apply Hadoop configuration if provided
    @SuppressWarnings("unchecked")
    Map<String, String> hadoopConfig = (Map<String, String>) config.get("hadoopConfig");
    if (hadoopConfig != null) {
      for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
        hadoopConf.set(entry.getKey(), entry.getValue());
      }
    }

    return new HadoopCatalog(hadoopConf, warehouse);
  }

  /**
   * Creates a Hive catalog.
   *
   * @param config The configuration
   * @return The Hive catalog
   */
  /*
  private static HiveCatalog createHiveCatalog(Map<String, Object> config) {
    HiveCatalog catalog = new HiveCatalog();

    // Set catalog name
    String catalogName = (String) config.getOrDefault("catalogName", "hive");
    Map<String, String> properties = new HashMap<>();
    properties.put("catalog-name", catalogName);

    // Set URI if provided
    String uri = (String) config.get("uri");
    if (uri != null) {
      properties.put("uri", uri);
    }

    // Set warehouse if provided
    String warehouse = (String) config.get("warehouse");
    if (warehouse != null) {
      properties.put("warehouse", warehouse);
    }

    // Apply additional Hive configuration
    @SuppressWarnings("unchecked")
    Map<String, String> hiveConfig = (Map<String, String>) config.get("hiveConfig");
    if (hiveConfig != null) {
      properties.putAll(hiveConfig);
    }

    Configuration hadoopConf = new Configuration();
    @SuppressWarnings("unchecked")
    Map<String, String> hadoopConfig = (Map<String, String>) config.get("hadoopConfig");
    if (hadoopConfig != null) {
      for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
        hadoopConf.set(entry.getKey(), entry.getValue());
      }
    }

    catalog.setConf(hadoopConf);
    catalog.initialize(catalogName, properties);

    return catalog;
  }

  /**
   * Creates a REST catalog.
   *
   * @param config The configuration
   * @return The REST catalog
   */
  private static RESTCatalog createRestCatalog(Map<String, Object> config) {
    RESTCatalog catalog = new RESTCatalog();

    Map<String, String> properties = new HashMap<>();

    // Set URI (required)
    String uri = (String) config.get("uri");
    if (uri == null) {
      throw new IllegalArgumentException("REST catalog requires 'uri' configuration");
    }
    properties.put("uri", uri);

    // Set warehouse if provided
    String warehouse = (String) config.get("warehouse");
    if (warehouse != null) {
      properties.put("warehouse", warehouse);
    }

    // Set authentication if provided
    String token = (String) config.get("token");
    if (token != null) {
      properties.put("token", token);
    }

    String credential = (String) config.get("credential");
    if (credential != null) {
      properties.put("credential", credential);
    }

    // Apply additional REST configuration
    @SuppressWarnings("unchecked")
    Map<String, String> restConfig = (Map<String, String>) config.get("restConfig");
    if (restConfig != null) {
      properties.putAll(restConfig);
    }

    Configuration hadoopConf = new Configuration();
    @SuppressWarnings("unchecked")
    Map<String, String> hadoopConfig = (Map<String, String>) config.get("hadoopConfig");
    if (hadoopConfig != null) {
      for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
        hadoopConf.set(entry.getKey(), entry.getValue());
      }
    }

    catalog.setConf(hadoopConf);
    catalog.initialize("rest", properties);

    return catalog;
  }

  /**
   * Parses a table identifier from a path string.
   *
   * @param tablePath The table path
   * @param config The configuration
   * @return The table identifier
   */
  private static TableIdentifier parseTableIdentifier(String tablePath, Map<String, Object> config) {
    // Check if namespace is provided in config
    String namespace = (String) config.get("namespace");

    if (tablePath.contains(".")) {
      // Path includes namespace (e.g., "namespace.table")
      String[] parts = tablePath.split("\\.", 2);
      return TableIdentifier.of(parts[0], parts[1]);
    } else if (namespace != null) {
      // Use namespace from config
      return TableIdentifier.of(namespace, tablePath);
    } else {
      // No namespace (single-level)
      return TableIdentifier.of(tablePath);
    }
  }

  /**
   * Clears the catalog cache.
   */
  public static synchronized void clearCache() {
    CATALOG_CACHE.clear();
  }
}
