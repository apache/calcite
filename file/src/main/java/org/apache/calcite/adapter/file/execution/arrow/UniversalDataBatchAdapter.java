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
package org.apache.calcite.adapter.file.execution.arrow;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Universal adapter for converting row-based file data to Arrow DataBatch format.
 *
 * <p>This adapter works with any file format that can produce Object[] rows:
 * <ul>
 *   <li>CSV files via CsvEnumerator</li>
 *   <li>JSON files via JsonEnumerator</li>
 *   <li>YAML files via JsonEnumerator</li>
 *   <li>TSV files via CsvEnumerator</li>
 * </ul>
 *
 * <p>Note: JsonEnumerator is imported from org.apache.calcite.adapter.file.json package.
 */
public class UniversalDataBatchAdapter {
  private static final RootAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

  private UniversalDataBatchAdapter() {
    // Utility class should not be instantiated
  }

  /**
   * Converts row-based data to Arrow VectorSchemaRoot format.
   */
  public static VectorSchemaRoot convertToArrowBatch(
      Iterator<Object[]> rowIterator,
      RelDataType rowType,
      int batchSize) {

    // Create Arrow schema from Calcite RelDataType
    Schema arrowSchema = createArrowSchema(rowType);
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, ALLOCATOR);
    vectorSchemaRoot.allocateNew();

    int rowCount = 0;
    List<Object[]> rows = new ArrayList<>();

    // Collect rows for this batch
    while (rowIterator.hasNext() && rowCount < batchSize) {
      rows.add(rowIterator.next());
      rowCount++;
    }

    // Populate Arrow vectors
    populateVectors(vectorSchemaRoot, rows, rowType);
    vectorSchemaRoot.setRowCount(rowCount);

    return vectorSchemaRoot;
  }

  /**
   * Creates Arrow schema from Calcite RelDataType.
   */
  private static Schema createArrowSchema(RelDataType rowType) {
    List<Field> fields = new ArrayList<>();

    for (RelDataTypeField field : rowType.getFieldList()) {
      ArrowType arrowType = convertSqlTypeToArrow(field.getType().getSqlTypeName());
      FieldType fieldType = new FieldType(field.getType().isNullable(), arrowType, null);
      fields.add(new Field(field.getName(), fieldType, null));
    }

    return new Schema(fields);
  }

  /**
   * Converts Calcite SQL types to Arrow types.
   */
  private static ArrowType convertSqlTypeToArrow(SqlTypeName sqlType) {
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

  /**
   * Populates Arrow vectors with row data.
   */
  private static void populateVectors(VectorSchemaRoot vectorSchemaRoot,
      List<Object[]> rows, RelDataType rowType) {

    List<RelDataTypeField> fields = rowType.getFieldList();

    for (int col = 0; col < fields.size(); col++) {
      RelDataTypeField field = fields.get(col);
      SqlTypeName sqlType = field.getType().getSqlTypeName();

      for (int row = 0; row < rows.size(); row++) {
        Object[] rowData = rows.get(row);
        Object value = col < rowData.length ? rowData[col] : null;

        setVectorValue(vectorSchemaRoot, col, row, value, sqlType);
      }
    }
  }

  /**
   * Sets a value in the appropriate Arrow vector.
   */
  private static void setVectorValue(VectorSchemaRoot vectorSchemaRoot,
      int col, int row, Object value, SqlTypeName sqlType) {

    if (value == null) {
      vectorSchemaRoot.getVector(col).setNull(row);
      return;
    }

    try {
      switch (sqlType) {
      case INTEGER:
        int intValue;
        if (value instanceof Integer) {
          intValue = (Integer) value;
        } else if (value instanceof Number) {
          intValue = ((Number) value).intValue();
        } else {
          intValue = Integer.parseInt(value.toString());
        }
        ((IntVector) vectorSchemaRoot.getVector(col)).set(row, intValue);
        break;
      case BIGINT:
        long longValue;
        if (value instanceof Long) {
          longValue = (Long) value;
        } else if (value instanceof Number) {
          longValue = ((Number) value).longValue();
        } else {
          longValue = Long.parseLong(value.toString());
        }
        ((BigIntVector) vectorSchemaRoot.getVector(col)).set(row, longValue);
        break;
      case DOUBLE:
      case DECIMAL:
        double doubleValue;
        if (value instanceof Double) {
          doubleValue = (Double) value;
        } else if (value instanceof Number) {
          doubleValue = ((Number) value).doubleValue();
        } else {
          doubleValue = Double.parseDouble(value.toString());
        }
        ((Float8Vector) vectorSchemaRoot.getVector(col)).set(row, doubleValue);
        break;
      case BOOLEAN:
        boolean boolValue;
        if (value instanceof Boolean) {
          boolValue = (Boolean) value;
        } else {
          boolValue = Boolean.parseBoolean(value.toString());
        }
        ((BitVector) vectorSchemaRoot.getVector(col)).set(row, boolValue ? 1 : 0);
        break;
      default:
        ((VarCharVector) vectorSchemaRoot.getVector(col)).set(row,
            value.toString().getBytes(StandardCharsets.UTF_8));
        break;
      }
    } catch (NumberFormatException e) {
      // Fall back to string for parsing errors
      ((VarCharVector) vectorSchemaRoot.getVector(col)).set(row,
          value.toString().getBytes(StandardCharsets.UTF_8));
    }
  }

  /**
   * Converts Arrow VectorSchemaRoot back to Object[] rows.
   */
  public static Iterator<Object[]> convertFromArrowBatch(VectorSchemaRoot vectorSchemaRoot) {
    List<Object[]> rows = new ArrayList<>();
    int rowCount = vectorSchemaRoot.getRowCount();
    int fieldCount = vectorSchemaRoot.getFieldVectors().size();

    for (int row = 0; row < rowCount; row++) {
      Object[] rowData = new Object[fieldCount];
      for (int col = 0; col < fieldCount; col++) {
        rowData[col] = getVectorValue(vectorSchemaRoot, col, row);
      }
      rows.add(rowData);
    }

    return rows.iterator();
  }

  /**
   * Gets a value from an Arrow vector.
   */
  private static Object getVectorValue(VectorSchemaRoot vectorSchemaRoot, int col, int row) {
    org.apache.arrow.vector.FieldVector vector = vectorSchemaRoot.getVector(col);

    if (vector.isNull(row)) {
      return null;
    }

    if (vector instanceof IntVector) {
      return ((IntVector) vector).get(row);
    } else if (vector instanceof BigIntVector) {
      return ((BigIntVector) vector).get(row);
    } else if (vector instanceof Float8Vector) {
      return ((Float8Vector) vector).get(row);
    } else if (vector instanceof BitVector) {
      return ((BitVector) vector).get(row) == 1;
    } else if (vector instanceof VarCharVector) {
      byte[] bytes = ((VarCharVector) vector).get(row);
      if (bytes == null) {
        return null;
      }
      // Convert bytes to string using UTF-8 charset
      return StandardCharsets.UTF_8.decode(java.nio.ByteBuffer.wrap(bytes)).toString();
    }

    return vector.getObject(row);
  }
}
