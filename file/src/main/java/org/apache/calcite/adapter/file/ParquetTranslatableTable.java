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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Table based on a Parquet file that implements TranslatableTable.
 * This enables native Parquet operations with proper predicate pushdown.
 */
public class ParquetTranslatableTable extends AbstractTable implements TranslatableTable {

  private final File parquetFile;
  private RelDataType rowType;

  public ParquetTranslatableTable(File parquetFile) {
    this.parquetFile = parquetFile;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      rowType = deriveRowType(typeFactory);
    }
    return rowType;
  }

  private RelDataType deriveRowType(RelDataTypeFactory typeFactory) {
    try {
      Path hadoopPath = new Path(parquetFile.getAbsolutePath());
      Configuration conf = new Configuration();

      // Read Parquet schema
      @SuppressWarnings("deprecation")
      ParquetMetadata metadata = ParquetFileReader.readFooter(conf, hadoopPath);
      MessageType messageType = metadata.getFileMetaData().getSchema();

      // Convert Parquet schema to Calcite schema
      List<String> names = new ArrayList<>();
      List<RelDataType> types = new ArrayList<>();
      List<Type> parquetFields = messageType.getFields();

      for (Type field : parquetFields) {
        // Unsanitize the field name to get back the original column name
        String originalName = ParquetConversionUtil.unsanitizeAvroName(field.getName());
        names.add(originalName);

        // Handle DECIMAL type specially to preserve precision and scale
        LogicalTypeAnnotation logicalType = field.getLogicalTypeAnnotation();
        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
          LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
              (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType;
          int precision = decimalType.getPrecision();
          int scale = decimalType.getScale();
          types.add(typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale));
        } else {
          SqlTypeName sqlType = convertParquetTypeToSql(field);
          // Special handling for TIMESTAMP to ensure it's treated as local time
          if (sqlType == SqlTypeName.TIMESTAMP) {
            // Create TIMESTAMP with precision 3 (milliseconds) to match our data
            // This helps ensure consistent timestamp handling
            types.add(typeFactory.createSqlType(sqlType, 3));
          } else {
            types.add(typeFactory.createSqlType(sqlType));
          }
        }
      }

      return typeFactory.createStructType(Pair.zip(names, types));

    } catch (IOException e) {
      throw new RuntimeException("Failed to read Parquet schema", e);
    }
  }

  private SqlTypeName convertParquetTypeToSql(Type parquetType) {
    // Check for logical types first using new API
    LogicalTypeAnnotation logicalType = parquetType.getLogicalTypeAnnotation();


    if (logicalType != null) {
      if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
        return SqlTypeName.DECIMAL;
      } else if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
        return SqlTypeName.DATE;
      } else if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
        return SqlTypeName.TIME;
      } else if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
        // For timezone-naive timestamps from CSV, we want TIMESTAMP WITHOUT TIME ZONE
        // This prevents Calcite from applying timezone conversions during SQL processing
        return SqlTypeName.TIMESTAMP;
      }
    }

    // Handle primitive types
    switch (parquetType.asPrimitiveType().getPrimitiveTypeName()) {
    case INT32:
      return SqlTypeName.INTEGER;
    case INT64:
      return SqlTypeName.BIGINT;
    case FLOAT:
      return SqlTypeName.FLOAT;
    case DOUBLE:
      return SqlTypeName.DOUBLE;
    case BOOLEAN:
      return SqlTypeName.BOOLEAN;
    case BINARY:
    default:
      return SqlTypeName.VARCHAR;
    }
  }


  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return new ParquetTableScan(context.getCluster(), relOptTable, this, relOptTable.getRowType());
  }

  /**
   * Enumerator that reads from Parquet files.
   */
  private class ParquetReaderEnumerator implements Enumerator<Object[]> {
    private final AtomicBoolean cancelFlag;
    private ParquetReader<GenericRecord> reader;
    private GenericRecord current;
    private Object[] currentRow;
    private boolean finished = false;

    ParquetReaderEnumerator(AtomicBoolean cancelFlag) {
      this.cancelFlag = cancelFlag;
      initReader();
    }

    private void initReader() {
      try {
        Path hadoopPath = new Path(parquetFile.getAbsolutePath());
        Configuration conf = new Configuration();

        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> tempReader =
            AvroParquetReader.<GenericRecord>builder(hadoopPath)
            .withConf(conf)
            .build();
        reader = tempReader;

      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize Parquet reader", e);
      }
    }

    @Override public Object[] current() {
      return currentRow;
    }

    @Override public boolean moveNext() {
      if (finished || cancelFlag.get()) {
        return false;
      }

      try {
        while (true) {
          current = reader.read();
          if (current == null) {
            finished = true;
            return false;
          }

          // Convert GenericRecord to Object[]
          int fieldCount = current.getSchema().getFields().size();
          currentRow = new Object[fieldCount];
          boolean shouldSkipRow = false;

          for (int i = 0; i < fieldCount; i++) {
            Object value = current.get(i);

            // Convert based on logical type
            Schema.Field field = current.getSchema().getFields().get(i);
            Schema fieldSchema = field.schema();

            // Handle union types (nullable fields)
            if (fieldSchema.getType() == Schema.Type.UNION) {
              for (Schema unionType : fieldSchema.getTypes()) {
                if (unionType.getType() != Schema.Type.NULL) {
                  fieldSchema = unionType;
                  break;
                }
              }
            }

            LogicalType logicalType = fieldSchema.getLogicalType();
            if (logicalType != null) {
              String logicalTypeName = logicalType.getName();
              if ("date".equals(logicalTypeName)) {
                // Keep as Integer to match LINQ4J engine behavior
              } else if ("time-millis".equals(logicalTypeName)) {
                // Keep as integer (milliseconds since midnight) for Calcite compatibility
                // Skip rows with null TIME values to avoid GROUP BY issues
                if (value == null) {
                  shouldSkipRow = true;
                  break;
                }
              } else if ("timestamp-millis".equals(logicalTypeName)) {
                if (value instanceof Long) {
                  long milliseconds = (Long) value;
                  value = new LocalTimestamp(milliseconds);
                }
              }
            } else if (value != null && value.getClass().getName().equals("org.apache.avro.util.Utf8")) {
              // Convert Avro UTF8 to String
              value = value.toString();
            }

            currentRow[i] = value;
          }

          // Skip this row if it has null TIME values
          if (shouldSkipRow) {
            continue; // Read next row
          }

          return true; // Return this row
        }
      } catch (IOException e) {
        throw new RuntimeException("Error reading Parquet file", e);
      }
    }

    @Override public void reset() {
      close();
      finished = false;
      initReader();
    }

    @Override public void close() {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // Ignore
        }
        reader = null;
      }
    }
  }

  /**
   * Relational expression representing a scan of a Parquet file with native predicate pushdown.
   */
  public static class ParquetTableScan extends TableScan implements EnumerableRel {
    private final ParquetTranslatableTable parquetTable;
    private final RelDataType projectRowType;

    public ParquetTableScan(RelOptCluster cluster, RelOptTable table,
        ParquetTranslatableTable parquetTable, RelDataType projectRowType) {
      super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), ImmutableList.of(), table);
      this.parquetTable = parquetTable;
      this.projectRowType = projectRowType;
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new ParquetTableScan(getCluster(), table, parquetTable, projectRowType);
    }

    @Override public RelDataType deriveRowType() {
      return projectRowType != null ? projectRowType : table.getRowType();
    }

    @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      PhysType physType =
          PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());

      String path = parquetTable.parquetFile.getAbsolutePath();
      Expression parquetFileExpression = Expressions.constant(path);

      // Generate enumerable expression that uses filtered Parquet factory
      BlockBuilder builder = new BlockBuilder();

      // Use filtered enumerable that skips null TIME values at Parquet level
      Expression enumerable =
          builder.append("enumerable",
          Expressions.call(
              ParquetEnumerableFactory.class,
              "enumerableWithTimeFiltering",
              parquetFileExpression));

      builder.add(Expressions.return_(null, enumerable));

      return implementor.result(physType, builder.toBlock());
    }
  }
}
