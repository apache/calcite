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
package org.apache.calcite.adapter.csvnextgen;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Universal data batch interface that can represent data in both
 * row-based and columnar formats for performance comparison.
 */
public class DataBatch implements AutoCloseable {
  private final List<String[]> rows;
  private final RelDataType rowType;
  private VectorSchemaRoot arrowBatch;
  private BufferAllocator allocator;
  private boolean arrowCreated = false;

  public DataBatch(List<String[]> rows, RelDataType rowType) {
    this.rows = rows;
    this.rowType = rowType;
  }

  public int getRowCount() {
    return rows.size();
  }

  public int getColumnCount() {
    return rowType.getFieldCount();
  }

  public Object getValue(int row, int column) {
    return rows.get(row)[column];
  }

  /**
   * Returns data as row iterator for Linq4j processing.
   */
  public Iterator<Object[]> asRows() {
    return new Iterator<Object[]>() {
      private int index = 0;

      @Override public boolean hasNext() {
        return index < rows.size();
      }

      @Override public Object[] next() {
        String[] stringRow = rows.get(index++);
        Object[] objectRow = new Object[stringRow.length];

        // Convert strings to appropriate types based on schema
        for (int i = 0; i < stringRow.length && i < rowType.getFieldCount(); i++) {
          objectRow[i] =
              convertValue(stringRow[i], rowType.getFieldList().get(i).getType().getSqlTypeName());
        }

        return objectRow;
      }
    };
  }

  /**
   * Returns data as Arrow VectorSchemaRoot for columnar processing.
   */
  public VectorSchemaRoot asArrowBatch() {
    if (!arrowCreated) {
      createArrowBatch();
    }
    return arrowBatch;
  }

  private void createArrowBatch() {
    allocator = new RootAllocator();

    // Create Arrow schema from RelDataType
    List<Field> fields = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      ArrowType arrowType = convertSqlTypeToArrow(field.getType().getSqlTypeName());
      fields.add(
          new Field(field.getName(),
          new FieldType(true, arrowType, null), null));
    }
    Schema schema = new Schema(fields);

    // Create vectors
    arrowBatch = VectorSchemaRoot.create(schema, allocator);
    arrowBatch.allocateNew();

    // Populate vectors
    for (int col = 0; col < getColumnCount(); col++) {
      FieldVector vector = arrowBatch.getVector(col);
      SqlTypeName sqlType = rowType.getFieldList().get(col).getType().getSqlTypeName();

      populateVector(vector, col, sqlType);
    }

    arrowBatch.setRowCount(getRowCount());
    arrowCreated = true;
  }

  private void populateVector(FieldVector vector, int columnIndex, SqlTypeName sqlType) {
    for (int row = 0; row < getRowCount(); row++) {
      String value = rows.get(row)[columnIndex];

      if (value == null || value.isEmpty()) {
        vector.setNull(row);
        continue;
      }

      try {
        switch (sqlType) {
        case INTEGER:
          ((IntVector) vector).set(row, Integer.parseInt(value));
          break;
        case BIGINT:
          ((IntVector) vector).set(row, Integer.parseInt(value)); // Simplified
          break;
        case DOUBLE:
        case DECIMAL:
          ((Float8Vector) vector).set(row, Double.parseDouble(value));
          break;
        default:
          ((VarCharVector) vector).set(row,
              value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
          break;
        }
      } catch (NumberFormatException e) {
        // Fall back to string for parsing errors
        if (vector instanceof VarCharVector) {
          ((VarCharVector) vector).set(row,
              value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } else {
          vector.setNull(row);
        }
      }
    }
    vector.setValueCount(getRowCount());
  }

  private ArrowType convertSqlTypeToArrow(SqlTypeName sqlType) {
    switch (sqlType) {
    case INTEGER:
    case BIGINT:
      return new ArrowType.Int(32, true);
    case DOUBLE:
    case DECIMAL:
      return new ArrowType.FloatingPoint(
          org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
    case BOOLEAN:
      return new ArrowType.Bool();
    default:
      return new ArrowType.Utf8();
    }
  }

  private Object convertValue(String value, SqlTypeName type) {
    if (value == null || value.isEmpty()) {
      return null;
    }

    try {
      switch (type) {
      case INTEGER:
        return Integer.parseInt(value);
      case BIGINT:
        return Long.parseLong(value);
      case DOUBLE:
      case DECIMAL:
        return Double.parseDouble(value);
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      default:
        return value;
      }
    } catch (NumberFormatException e) {
      return value; // Fall back to string
    }
  }

  @Override public void close() {
    if (arrowBatch != null) {
      arrowBatch.close();
      arrowBatch = null;
    }
    if (allocator != null) {
      allocator.close();
      allocator = null;
    }
  }
}
