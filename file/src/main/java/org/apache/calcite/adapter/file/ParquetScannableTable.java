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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Table that reads data from Parquet files using the ScannableTable interface.
 * This avoids the complexity of the Arrow adapter and works directly with
 * the file adapter's execution model.
 */
public class ParquetScannableTable extends AbstractTable implements ScannableTable {

  private final File parquetFile;
  private RelDataType rowType;

  public ParquetScannableTable(File parquetFile) {
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
        SqlTypeName sqlType = convertParquetTypeToSql(field);
        names.add(field.getName());
        types.add(typeFactory.createSqlType(sqlType));
      }

      return typeFactory.createStructType(Pair.zip(names, types));

    } catch (IOException e) {
      throw new RuntimeException("Failed to read Parquet schema", e);
    }
  }

  private SqlTypeName convertParquetTypeToSql(Type parquetType) {
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

  @Override public Enumerable<Object[]> scan(DataContext root) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new ParquetReaderEnumerator(cancelFlag);
      }
    };
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
        current = reader.read();
        if (current == null) {
          finished = true;
          return false;
        }

        // Convert GenericRecord to Object[]
        int fieldCount = current.getSchema().getFields().size();
        currentRow = new Object[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
          currentRow[i] = current.get(i);
        }

        return true;
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
}
