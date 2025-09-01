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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.adapter.file.execution.linq4j.CsvEnumerator;
import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Base class for table that reads CSV files.
 *
 * <p>Copied from {@code CsvFilterableTable} in demo CSV adapter,
 * with more advanced features.
 */
public abstract class CsvTable extends AbstractTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(CsvTable.class);
  protected final Source source;
  protected final @Nullable RelProtoDataType protoRowType;
  public final String columnCasing;
  private @Nullable RelDataType rowType;
  private @Nullable List<RelDataType> fieldTypes;
  protected final CsvTypeInferrer.TypeInferenceConfig typeInferenceConfig;

  /** Creates a CsvTable. */
  public CsvTable(Source source, @Nullable RelProtoDataType protoRowType) {
    this(source, protoRowType, "UNCHANGED");
  }

  /** Creates a CsvTable with column casing. */
  public CsvTable(Source source, @Nullable RelProtoDataType protoRowType, String columnCasing) {
    this(source, protoRowType, columnCasing, CsvTypeInferrer.TypeInferenceConfig.disabled());
  }

  /** Creates a CsvTable with column casing and type inference. */
  public CsvTable(Source source, @Nullable RelProtoDataType protoRowType, String columnCasing,
      CsvTypeInferrer.TypeInferenceConfig typeInferenceConfig) {
    this.source = source;
    this.protoRowType = protoRowType;
    this.columnCasing = columnCasing;
    this.typeInferenceConfig = typeInferenceConfig;
  }

  /** Returns the type inference configuration. */
  public CsvTypeInferrer.TypeInferenceConfig getTypeInferenceConfig() {
    return typeInferenceConfig;
  }

  /**
   * Create a default type inference configuration for consistent processing.
   * This ensures we always use the same inference logic even when type inference is disabled.
   */
  private CsvTypeInferrer.TypeInferenceConfig createDefaultInferenceConfig() {
    return new CsvTypeInferrer.TypeInferenceConfig(
        false, // enabled = false (but we'll still run the inference for consistency)
        1.0,   // samplingRate = 1.0 (check all rows)
        100,   // maxSampleRows = 100
        0.95,  // confidenceThreshold = 0.95
        true,  // makeAllNullable = true (important for consistent nullability)
        false, // inferDates = false (avoid timestamp parsing issues when forcing VARCHAR)
        false, // inferTimes = false (avoid timestamp parsing issues when forcing VARCHAR)
        false, // inferTimestamps = false (avoid timestamp parsing issues when forcing VARCHAR)
        0.0);  // nullThreshold = 0.0
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    if (rowType == null) {
      LOGGER.debug("Deducing row type for source: {}", source.path());
      try {
        // First get the basic row type with column names
        rowType =
            CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, source,
                null, isStream(), columnCasing);

        LOGGER.debug("CSV table {}: typeInferenceConfig={}, enabled={}",
            source.path(),
            typeInferenceConfig != null ? "present" : "null",
            typeInferenceConfig != null ? typeInferenceConfig.isEnabled() : "N/A");

        // If type inference is enabled, refine the column types
        if (typeInferenceConfig != null && typeInferenceConfig.isEnabled() && !isStream()) {
          try {
            LOGGER.info("Applying type inference to CSV table: {}", source.path());
            List<CsvTypeInferrer.ColumnTypeInfo> inferredTypes =
                CsvTypeInferrer.inferTypes(source, typeInferenceConfig, columnCasing);
            LOGGER.info("Inferred {} column types", inferredTypes.size());

            if (!inferredTypes.isEmpty()) {
              // Log all inferred types for debugging
              LOGGER.info("=== TYPE INFERENCE RESULTS ===");
              for (int i = 0; i < inferredTypes.size(); i++) {
                CsvTypeInferrer.ColumnTypeInfo typeInfo = inferredTypes.get(i);
                LOGGER.info("Column {}: {} -> {} (nullable={}, confidence={})",
                    i, typeInfo.columnName, typeInfo.inferredType, typeInfo.nullable, typeInfo.confidence);
              }
              LOGGER.info("=== END TYPE INFERENCE RESULTS ===");

              // Build a new row type with inferred types
              RelDataTypeFactory.Builder builder = typeFactory.builder();
              List<String> fieldNames = rowType.getFieldNames();

              for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                RelDataType fieldType;

                if (i < inferredTypes.size()) {
                  CsvTypeInferrer.ColumnTypeInfo typeInfo = inferredTypes.get(i);
                  LOGGER.info("Applying type to column {}: {} -> {}",
                      fieldName, typeInfo.inferredType, typeInfo.inferredType);
                  fieldType = typeFactory.createSqlType(typeInfo.inferredType);
                  fieldType = typeFactory.createTypeWithNullability(fieldType, typeInfo.nullable);
                } else {
                  // Fallback to VARCHAR for any extra columns
                  LOGGER.warn("Column {} has no inferred type, falling back to VARCHAR", fieldName);
                  fieldType = typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR);
                  fieldType = typeFactory.createTypeWithNullability(fieldType, true);
                }

                builder.add(fieldName, fieldType);
              }

              rowType = builder.build();
              LOGGER.debug("Applied type inference, row type now has {} fields", rowType.getFieldCount());
            }
          } catch (Exception e) {
            LOGGER.warn("Type inference failed, falling back to all VARCHAR: {}", e.getMessage());
            // Keep the original rowType with all VARCHAR columns
          }
        }

        LOGGER.debug("Final row type with {} fields", rowType.getFieldCount());
      } catch (Exception e) {
        LOGGER.error("ERROR deducing row type: {}", e.getMessage(), e);
        throw e;
      }
    }
    return rowType;
  }

  /** Returns the field types of this CSV table. */
  public List<RelDataType> getFieldTypes(RelDataTypeFactory typeFactory) {
    if (fieldTypes == null) {
      // If type inference is disabled or this is a stream, use the original deduction
      if (typeInferenceConfig == null || !typeInferenceConfig.isEnabled() || isStream()) {
        fieldTypes = new ArrayList<>();
        CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, source,
            fieldTypes, isStream(), columnCasing);
      } else {
        // Get the row type which includes inferred types
        RelDataType rowType = getRowType(typeFactory);
        fieldTypes = rowType.getFieldList().stream()
            .map(field -> field.getType())
            .collect(Collectors.toList());
      }
    }
    return fieldTypes;
  }

  /** Returns whether the table represents a stream. */
  protected boolean isStream() {
    return false;
  }

  /** Various degrees of table "intelligence". */
  public enum Flavor {
    SCANNABLE, FILTERABLE, TRANSLATABLE
  }
}
