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
package org.apache.calcite.adapter.splunk;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

/**
 * Factory for creating custom Splunk tables with user-defined schemas.
 * Used when tables are explicitly defined in JSON rather than
 * auto-generated from CIM models.
 */
public class SplunkTableFactory implements TableFactory<SplunkTable> {

  @Override
  public SplunkTable create(SchemaPlus schema, String name,
                           @Nullable Map<String, Object> operand,
                           @Nullable RelDataType rowType) {

    // Handle nullable rowType parameter
    if (rowType == null) {
      throw new IllegalArgumentException("rowType cannot be null for SplunkTable");
    }

    // Ensure we always have an _extra field for unmapped data
    RelDataType enhancedRowType = ensureExtraField(rowType);

    return new SplunkTable(enhancedRowType);
  }

  /**
   * Ensures the table schema includes an "_extra" field for capturing
   * unmapped Splunk fields as JSON. If already present, returns the
   * original type unchanged.
   */
  private RelDataType ensureExtraField(RelDataType originalType) {
    if (originalType.getFieldNames().contains("_extra")) {
      return originalType; // Already has _extra field
    }

    // Add _extra field for overflow data
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = typeFactory.builder();

    // Add all original fields
    for (RelDataTypeField field : originalType.getFieldList()) {
      builder.add(field.getName(), field.getType());
    }

    // Add _extra field for unmapped fields
    RelDataType stringType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    builder.add("_extra", stringType);

    return builder.build();
  }
}
