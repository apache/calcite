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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;

import com.opencsv.CSVReader;

/**
 * Simple table implementation for CsvNextGen adapter.
 * Extends AbstractTable to inherit standard interface implementations.
 */
public class CsvNextGenTableSimple extends AbstractTable
    implements ScannableTable, TranslatableTable {

  private final Source source;
  private final RelProtoDataType protoRowType;
  private final String engineType;
  private final int batchSize;
  private final boolean header;

  public CsvNextGenTableSimple(Source source, RelProtoDataType protoRowType,
      String engineType, int batchSize, boolean header) {
    this.source = source;
    this.protoRowType = protoRowType;
    this.engineType = engineType;
    this.batchSize = batchSize;
    this.header = header;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    // Auto-detect schema by reading first few rows
    return autoDetectSchema(typeFactory);
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new CsvEnumerator(source, header);
      }
    };
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    // For now, just return a simple table scan
    // In the future, this would use the execution engine
    return new org.apache.calcite.adapter.enumerable.EnumerableTableScan(
        context.getCluster(),
        context.getCluster().traitSetOf(
            org.apache.calcite.adapter.enumerable.EnumerableConvention.INSTANCE),
        relOptTable,
        Object[].class);
  }

  /**
   * Auto-detects schema by reading the first few rows.
   */
  private RelDataType autoDetectSchema(RelDataTypeFactory typeFactory) {
    try (CSVReader reader = openCsv(source)) {
      String[] firstRow = reader.readNext();
      if (firstRow == null) {
        return typeFactory.builder().build();
      }

      RelDataTypeFactory.Builder builder = typeFactory.builder();
      if (header) {
        // Use first row as field names
        for (String fieldName : firstRow) {
          builder.add(fieldName, SqlTypeName.VARCHAR);
        }
      } else {
        // Generate field names
        for (int i = 0; i < firstRow.length; i++) {
          builder.add("FIELD" + i, SqlTypeName.VARCHAR);
        }
      }

      return builder.build();
    } catch (Exception e) {
      throw new RuntimeException("Error detecting schema", e);
    }
  }

  /**
   * Opens a CSV reader, handling TSV files.
   */
  private static CSVReader openCsv(Source source) throws java.io.IOException {
    if (source.path().endsWith(".tsv")) {
      com.opencsv.CSVParserBuilder parserBuilder = new com.opencsv.CSVParserBuilder()
          .withSeparator('\t');
      return new com.opencsv.CSVReaderBuilder(source.reader())
          .withCSVParser(parserBuilder.build())
          .build();
    }
    return new CSVReader(source.reader());
  }

  /**
   * Enumerator that reads CSV data row by row.
   */
  static class CsvEnumerator implements Enumerator<Object[]> {
    private final CSVReader csvReader;
    private final boolean hasHeader;
    private Object[] current;
    private boolean started = false;

    CsvEnumerator(Source source, boolean hasHeader) {
      this.hasHeader = hasHeader;
      try {
        this.csvReader = openCsv(source);
        if (hasHeader) {
          csvReader.readNext(); // Skip header
        }
      } catch (Exception e) {
        throw new RuntimeException("Error opening CSV file", e);
      }
    }

    @Override public Object[] current() {
      return current;
    }

    @Override public boolean moveNext() {
      try {
        String[] row = csvReader.readNext();
        if (row == null) {
          return false;
        }

        // Convert strings to objects
        current = new Object[row.length];
        System.arraycopy(row, 0, current, 0, row.length);
        return true;
      } catch (Exception e) {
        throw new RuntimeException("Error reading CSV file", e);
      }
    }

    @Override public void reset() {
      throw new UnsupportedOperationException();
    }

    @Override public void close() {
      try {
        csvReader.close();
      } catch (Exception e) {
        throw new RuntimeException("Error closing CSV file", e);
      }
    }
  }
}
