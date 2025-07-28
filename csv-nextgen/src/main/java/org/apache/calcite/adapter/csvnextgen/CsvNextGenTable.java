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
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Table representing a CSV file with configurable execution engines.
 *
 * <p>This table can use different execution engines:
 * <ul>
 *   <li><b>LINQ4J</b>: Traditional row-by-row processing</li>
 *   <li><b>ARROW</b>: Arrow-based columnar processing with enumerable interface</li>
 *   <li><b>VECTORIZED</b>: Optimized vectorized Arrow processing</li>
 * </ul>
 */
public class CsvNextGenTable extends AbstractTable implements ScannableTable, TranslatableTable {
  private final Source source;
  private final boolean hasHeader;
  private final CsvNextGenSchemaFactory.ExecutionEngineType executionEngine;
  private final int batchSize;
  private final AtomicBoolean cancelFlag = new AtomicBoolean(false);

  // Lazy initialization of schema and execution engines
  private volatile RelDataType rowType;
  private volatile Linq4jExecutionEngine linq4jEngine;
  private volatile ArrowExecutionEngine arrowEngine;
  private volatile VectorizedArrowExecutionEngine vectorizedEngine;

  public CsvNextGenTable(Source source, boolean hasHeader,
      CsvNextGenSchemaFactory.ExecutionEngineType executionEngine, int batchSize) {
    this.source = source;
    this.hasHeader = hasHeader;
    this.executionEngine = executionEngine;
    this.batchSize = batchSize;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      rowType = deduceRowType((JavaTypeFactory) typeFactory);
    }
    return rowType;
  }

  /**
   * Scans the table using the configured execution engine.
   */
  @Override public Enumerable<Object[]> scan(DataContext root) {
    switch (executionEngine) {
    case LINQ4J:
      return getLinq4jEngine().scan(source, getRowType(root.getTypeFactory()), hasHeader);
    case ARROW:
      return getArrowEngine().scan(source, getRowType(root.getTypeFactory()), hasHeader);
    case VECTORIZED:
      return getVectorizedEngine().scan(source, getRowType(root.getTypeFactory()), hasHeader);
    default:
      throw new IllegalStateException("Unknown execution engine: " + executionEngine);
    }
  }

  /**
   * Translates to a table scan for query optimization.
   */
  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return new CsvNextGenTableScan(context.getCluster(), relOptTable, this);
  }

  /**
   * Deduces the row type by reading the first few rows of the CSV file.
   */
  private RelDataType deduceRowType(JavaTypeFactory typeFactory) {
    try (CSVReader csvReader = openCsv()) {
      String[] header = null;
      String[] firstRow = null;

      // Read header if present
      if (hasHeader) {
        header = csvReader.readNext();
        if (header == null) {
          throw new RuntimeException("CSV file is empty");
        }
      }

      // Read first data row for type inference
      firstRow = csvReader.readNext();
      if (firstRow == null) {
        throw new RuntimeException("CSV file has no data rows");
      }

      // If no header, generate column names
      if (header == null) {
        header = new String[firstRow.length];
        for (int i = 0; i < firstRow.length; i++) {
          header[i] = "COLUMN" + (i + 1);
        }
      }

      // Build row type
      RelDataTypeFactory.Builder builder = typeFactory.builder();
      for (int i = 0; i < header.length; i++) {
        String columnName = header[i].trim();
        if (columnName.isEmpty()) {
          columnName = "COLUMN" + (i + 1);
        }

        // Infer type from first row value
        SqlTypeName sqlType = inferSqlType(i < firstRow.length ? firstRow[i] : "");
        builder.add(columnName, sqlType);
      }

      return builder.build();

    } catch (Exception e) {
      throw new RuntimeException("Error reading CSV file to deduce schema: " + source, e);
    }
  }

  /**
   * Infers SQL type from a string value.
   */
  private SqlTypeName inferSqlType(String value) {
    if (value == null || value.trim().isEmpty()) {
      return SqlTypeName.VARCHAR; // Default to VARCHAR for null/empty values
    }

    value = value.trim();

    // Try integer
    try {
      Integer.parseInt(value);
      return SqlTypeName.INTEGER;
    } catch (NumberFormatException ignored) {
      // Continue to next type
    }

    // Try double
    try {
      Double.parseDouble(value);
      return SqlTypeName.DOUBLE;
    } catch (NumberFormatException ignored) {
      // Continue to next type
    }

    // Try boolean
    if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
      return SqlTypeName.BOOLEAN;
    }

    // Default to VARCHAR
    return SqlTypeName.VARCHAR;
  }

  /**
   * Opens a CSV reader, handling TSV files.
   */
  private CSVReader openCsv() throws IOException {
    if (source.path().endsWith(".tsv")) {
      CSVParserBuilder parserBuilder = new CSVParserBuilder()
          .withSeparator('\t');
      return new CSVReaderBuilder(source.reader())
          .withCSVParser(parserBuilder.build())
          .build();
    }
    return new CSVReader(source.reader());
  }

  // Lazy initialization of execution engines

  private Linq4jExecutionEngine getLinq4jEngine() {
    if (linq4jEngine == null) {
      synchronized (this) {
        if (linq4jEngine == null) {
          linq4jEngine = new Linq4jExecutionEngine(batchSize);
        }
      }
    }
    return linq4jEngine;
  }

  private ArrowExecutionEngine getArrowEngine() {
    if (arrowEngine == null) {
      synchronized (this) {
        if (arrowEngine == null) {
          arrowEngine = new ArrowExecutionEngine(batchSize);
        }
      }
    }
    return arrowEngine;
  }

  private VectorizedArrowExecutionEngine getVectorizedEngine() {
    if (vectorizedEngine == null) {
      synchronized (this) {
        if (vectorizedEngine == null) {
          vectorizedEngine = new VectorizedArrowExecutionEngine(batchSize);
        }
      }
    }
    return vectorizedEngine;
  }

  /**
   * Gets the source file.
   */
  public Source getSource() {
    return source;
  }

  /**
   * Gets whether the file has a header row.
   */
  public boolean hasHeader() {
    return hasHeader;
  }

  /**
   * Gets the execution engine type.
   */
  public CsvNextGenSchemaFactory.ExecutionEngineType getExecutionEngine() {
    return executionEngine;
  }

  /**
   * Gets the batch size.
   */
  public int getBatchSize() {
    return batchSize;
  }

  /**
   * Sets the cancel flag for long-running operations.
   */
  public void cancel() {
    cancelFlag.set(true);
  }

  /**
   * Checks if the operation was cancelled.
   */
  public boolean isCancelled() {
    return cancelFlag.get();
  }
}
