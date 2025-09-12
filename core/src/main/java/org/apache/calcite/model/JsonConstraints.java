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
package org.apache.calcite.model;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelReferentialConstraintImpl;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Core constraint metadata support for Calcite tables.
 * Allows defining primary keys and foreign keys through model file configuration,
 * providing metadata hints for query optimization and documentation.
 *
 * <p>These constraints serve as metadata only and are not enforced by the underlying
 * storage engines. They help with query optimization, JDBC metadata, and logical
 * data model documentation.
 */
public class JsonConstraints {

  /**
   * Creates a Statistic object from constraint configuration.
   *
   * @param constraintsMap The constraints configuration map from model file
   * @param columnNames The ordered list of column names in the table
   * @param rowCount Optional row count estimate
   * @return A Statistic with the configured constraints
   */
  @SuppressWarnings("unchecked")
  public static Statistic fromConstraintsMap(
      @Nullable Map<String, Object> constraintsMap,
      List<String> columnNames,
      @Nullable Double rowCount) {
    return fromConstraintsMap(constraintsMap, columnNames, rowCount, null, null);
  }

  /**
   * Creates a Statistic object from constraint configuration with qualified name.
   *
   * @param constraintsMap The constraints configuration map from model file
   * @param columnNames The ordered list of column names in the table
   * @param rowCount Optional row count estimate
   * @param schemaName The schema name this table belongs to
   * @param tableName The table name
   * @return A Statistic with the configured constraints
   */
  @SuppressWarnings("unchecked")
  public static Statistic fromConstraintsMap(
      @Nullable Map<String, Object> constraintsMap,
      List<String> columnNames,
      @Nullable Double rowCount,
      @Nullable String schemaName,
      @Nullable String tableName) {

    if (constraintsMap == null) {
      return Statistics.UNKNOWN;
    }

    // If column names are not available yet (lazy evaluation), extract them from constraints
    List<String> effectiveColumnNames = columnNames;
    if (columnNames == null || columnNames.isEmpty()) {
      effectiveColumnNames = extractColumnNamesFromConstraints(constraintsMap);
    }

    // Parse primary keys
    List<ImmutableBitSet> keys = parsePrimaryKeys(constraintsMap, effectiveColumnNames);

    // Parse foreign keys
    List<RelReferentialConstraint> foreignKeys = parseForeignKeys(constraintsMap, effectiveColumnNames, schemaName, tableName);

    // Parse collations (optional)
    List<RelCollation> collations = parseCollations(constraintsMap, columnNames);

    // Create and return the statistic
    return Statistics.of(rowCount, keys, foreignKeys, collations);
  }

  /**
   * Extracts all column names referenced in constraints.
   * This is used as a fallback when column names are not yet available (lazy evaluation).
   *
   * @param constraints The constraints configuration map
   * @return List of unique column names from all constraints
   */
  @SuppressWarnings("unchecked")
  private static List<String> extractColumnNamesFromConstraints(Map<String, Object> constraints) {
    List<String> allColumns = new ArrayList<>();
    
    // Extract from primary key
    Object primaryKey = constraints.get("primaryKey");
    if (primaryKey instanceof List) {
      allColumns.addAll((List<String>) primaryKey);
    }
    
    // Extract from unique keys
    List<List<String>> uniqueKeys = (List<List<String>>) constraints.get("uniqueKeys");
    if (uniqueKeys != null) {
      for (List<String> keyColumns : uniqueKeys) {
        allColumns.addAll(keyColumns);
      }
    }
    
    // Extract from foreign keys
    List<Map<String, Object>> foreignKeys = (List<Map<String, Object>>) constraints.get("foreignKeys");
    if (foreignKeys != null) {
      for (Map<String, Object> fk : foreignKeys) {
        List<String> columns = (List<String>) fk.get("columns");
        if (columns != null) {
          allColumns.addAll(columns);
        }
      }
    }
    
    // Return unique columns in order
    return new ArrayList<>(new java.util.LinkedHashSet<>(allColumns));
  }

  /**
   * Parses primary key definitions from configuration.
   *
   * @param constraints The constraints configuration map
   * @param columnNames The ordered list of column names
   * @return List of primary keys as bit sets
   */
  @SuppressWarnings("unchecked")
  private static List<ImmutableBitSet> parsePrimaryKeys(
      Map<String, Object> constraints,
      List<String> columnNames) {

    List<ImmutableBitSet> keys = new ArrayList<>();

    // Primary key can be a single column list or multiple keys
    Object primaryKey = constraints.get("primaryKey");
    if (primaryKey instanceof List) {
      List<String> keyColumns = (List<String>) primaryKey;
      ImmutableBitSet.Builder keyBuilder = ImmutableBitSet.builder();
      for (String col : keyColumns) {
        int index = columnNames.indexOf(col);
        if (index >= 0) {
          keyBuilder.set(index);
        }
      }
      ImmutableBitSet key = keyBuilder.build();
      if (!key.isEmpty()) {
        keys.add(key);
      }
    }

    // Also support multiple unique keys
    List<List<String>> uniqueKeys = (List<List<String>>) constraints.get("uniqueKeys");
    if (uniqueKeys != null) {
      for (List<String> keyColumns : uniqueKeys) {
        ImmutableBitSet.Builder keyBuilder = ImmutableBitSet.builder();
        for (String col : keyColumns) {
          int index = columnNames.indexOf(col);
          if (index >= 0) {
            keyBuilder.set(index);
          }
        }
        ImmutableBitSet key = keyBuilder.build();
        if (!key.isEmpty()) {
          keys.add(key);
        }
      }
    }

    return keys;
  }

  /**
   * Parses foreign key definitions from configuration.
   *
   * @param constraints The constraints configuration map
   * @param columnNames The ordered list of column names
   * @param schemaName The schema name this table belongs to
   * @param tableName The table name
   * @return List of foreign key constraints
   */
  @SuppressWarnings("unchecked")
  private static List<RelReferentialConstraint> parseForeignKeys(
      Map<String, Object> constraints,
      List<String> columnNames,
      @Nullable String schemaName,
      @Nullable String tableName) {

    List<RelReferentialConstraint> foreignKeys = new ArrayList<>();

    List<Map<String, Object>> fkList = 
        (List<Map<String, Object>>) constraints.get("foreignKeys");
    if (fkList == null) {
      return foreignKeys;
    }

    for (Map<String, Object> fkDef : fkList) {
      // Extract foreign key definition
      List<String> sourceColumns = (List<String>) fkDef.get("columns");
      List<String> targetTable = (List<String>) fkDef.get("targetTable");
      List<String> targetColumns = (List<String>) fkDef.get("targetColumns");

      if (sourceColumns == null || targetTable == null || targetColumns == null) {
        continue; // Skip invalid FK definition
      }

      // Build column pairs
      List<IntPair> columnPairs = new ArrayList<>();
      for (int i = 0; i < sourceColumns.size() && i < targetColumns.size(); i++) {
        int sourceIndex = columnNames.indexOf(sourceColumns.get(i));
        int targetIndex = i; // Target column ordinal in target table
        if (sourceIndex >= 0) {
          columnPairs.add(IntPair.of(sourceIndex, targetIndex));
        }
      }

      if (!columnPairs.isEmpty()) {
        // Source table name should be provided in the config
        List<String> sourceTable = (List<String>) fkDef.get("sourceTable");
        if (sourceTable == null) {
          // If not specified, use the provided schema and table names
          if (schemaName != null && tableName != null) {
            sourceTable = List.of(schemaName, tableName);
          } else {
            // Fallback to empty list if names not provided
            sourceTable = new ArrayList<>();
          }
        }
        
        // If target table doesn't have a schema and we have a schema name, prepend it
        // But only if the target table is a single name
        List<String> resolvedTargetTable = targetTable;
        if (targetTable.size() == 1 && schemaName != null) {
          // Only table name provided, prepend the schema
          resolvedTargetTable = List.of(schemaName, targetTable.get(0));
        }

        RelReferentialConstraint fk = RelReferentialConstraintImpl.of(
            sourceTable, resolvedTargetTable, columnPairs);
        foreignKeys.add(fk);
      }
    }

    return foreignKeys;
  }

  /**
   * Parses collation definitions from configuration.
   *
   * @param constraints The constraints configuration map
   * @param columnNames The ordered list of column names
   * @return List of collations (currently returns null as not implemented)
   */
  private static @Nullable List<RelCollation> parseCollations(
      Map<String, Object> constraints,
      List<String> columnNames) {
    // Collations could be added in the future if needed
    // For now, return null to indicate no specific collations
    return null;
  }

  /**
   * Creates a simple primary key constraint configuration.
   *
   * @param keyColumns The columns that form the primary key
   * @return A configuration map with the primary key
   */
  public static Map<String, Object> primaryKey(String... keyColumns) {
    return Map.of("primaryKey", List.of(keyColumns));
  }

  /**
   * Creates a foreign key constraint configuration.
   *
   * @param columns The source columns
   * @param targetSchema The target schema name
   * @param targetTable The target table name
   * @param targetColumns The target columns
   * @return A configuration map with the foreign key
   */
  public static Map<String, Object> foreignKey(
      List<String> columns,
      String targetSchema,
      String targetTable,
      List<String> targetColumns) {
    return Map.of(
        "columns", columns,
        "targetTable", List.of(targetSchema, targetTable),
        "targetColumns", targetColumns
    );
  }
}