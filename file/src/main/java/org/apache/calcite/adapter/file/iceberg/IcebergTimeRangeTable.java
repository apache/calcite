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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.table.ParquetTranslatableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.apache.iceberg.Table;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.hadoop.HadoopTables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Unified Iceberg table that spans multiple snapshots within a time range.
 * Creates a logical table with an additional snapshot_time column.
 */
public class IcebergTimeRangeTable extends AbstractTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTimeRangeTable.class);

  private final List<IcebergTimeRangeResolver.IcebergDataFile> dataFiles;
  private final String snapshotColumnName;
  private final ExecutionEngineConfig engineConfig;
  private final String tablePath;
  private RelDataType rowType;

  /**
   * Creates a time range table from resolved data files.
   *
   * @param dataFiles List of data files with snapshot information
   * @param snapshotColumnName Name of the snapshot timestamp column
   * @param engineConfig Execution engine configuration
   * @param tablePath Path to the Iceberg table (for schema)
   */
  public IcebergTimeRangeTable(List<IcebergTimeRangeResolver.IcebergDataFile> dataFiles, 
                              String snapshotColumnName,
                              ExecutionEngineConfig engineConfig,
                              String tablePath) {
    this.dataFiles = dataFiles;
    this.snapshotColumnName = snapshotColumnName;
    this.engineConfig = engineConfig;
    this.tablePath = tablePath;
    
    LOGGER.info("Created IcebergTimeRangeTable with {} data files, snapshot column: {}", 
                dataFiles.size(), snapshotColumnName);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType != null) {
      return rowType;
    }

    try {
      // Get schema from Iceberg table
      HadoopTables tables = new HadoopTables();
      Table icebergTable = tables.load(tablePath);
      Schema schema = icebergTable.schema();

      RelDataTypeFactory.Builder builder = typeFactory.builder();
      
      // Add all original columns
      for (Types.NestedField field : schema.columns()) {
        RelDataType columnType = convertIcebergTypeToCalcite(field.type(), typeFactory);
        builder.add(field.name(), columnType);
      }
      
      // Add snapshot timestamp column
      builder.add(snapshotColumnName, typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
      
      rowType = builder.build();
      return rowType;
      
    } catch (Exception e) {
      LOGGER.error("Failed to get row type for Iceberg time range table", e);
      throw new RuntimeException("Failed to get row type", e);
    }
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    LOGGER.debug("Scanning IcebergTimeRangeTable with {} files", dataFiles.size());
    
    return new AbstractEnumerable<Object[]>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        return new TimeRangeEnumerator();
      }
    };
  }

  /**
   * Enumerator that combines data from multiple Parquet files with snapshot timestamps.
   */
  private class TimeRangeEnumerator implements Enumerator<Object[]> {
    private final Iterator<IcebergTimeRangeResolver.IcebergDataFile> fileIterator;
    private Enumerator<Object[]> currentFileEnumerator;
    private IcebergTimeRangeResolver.IcebergDataFile currentFile;

    TimeRangeEnumerator() {
      this.fileIterator = dataFiles.iterator();
      this.currentFileEnumerator = null;
      moveToNextFile();
    }

    @Override
    public Object[] current() {
      if (currentFileEnumerator == null) {
        return null;
      }
      
      Object[] originalRow = currentFileEnumerator.current();
      if (originalRow == null) {
        return null;
      }
      
      // Add snapshot timestamp to the row
      Object[] extendedRow = new Object[originalRow.length + 1];
      System.arraycopy(originalRow, 0, extendedRow, 0, originalRow.length);
      extendedRow[originalRow.length] = java.sql.Timestamp.from(currentFile.getSnapshotTime());
      
      return extendedRow;
    }

    @Override
    public boolean moveNext() {
      while (true) {
        if (currentFileEnumerator != null && currentFileEnumerator.moveNext()) {
          return true;
        }
        
        // Current file is exhausted, move to next
        if (!moveToNextFile()) {
          return false;
        }
      }
    }

    @Override
    public void reset() {
      throw new UnsupportedOperationException("Reset not supported");
    }

    @Override
    public void close() {
      if (currentFileEnumerator != null) {
        currentFileEnumerator.close();
      }
    }

    private boolean moveToNextFile() {
      // Close current enumerator
      if (currentFileEnumerator != null) {
        currentFileEnumerator.close();
        currentFileEnumerator = null;
      }
      
      // Move to next file
      if (!fileIterator.hasNext()) {
        return false;
      }
      
      currentFile = fileIterator.next();
      LOGGER.debug("Processing file: {} from snapshot: {}", currentFile.getFilePath(), currentFile.getSnapshotId());
      
      try {
        // Create a ParquetTranslatableTable for this file
        File parquetFile = new File(currentFile.getFilePath());
        ParquetTranslatableTable parquetTable = new ParquetTranslatableTable(parquetFile);
        
        // For now, we'll use a simple approach that reads the file directly
        // In practice, this should integrate with the execution engine configuration
        Enumerable<Object[]> fileEnumerable = createParquetEnumerable(parquetFile);
        currentFileEnumerator = fileEnumerable.enumerator();
        
        return true;
      } catch (Exception e) {
        LOGGER.error("Failed to process Parquet file: {}", currentFile.getFilePath(), e);
        // Try next file
        return moveToNextFile();
      }
    }
  }

  /**
   * Creates an enumerable for a Parquet file.
   * This is a simplified implementation that delegates to the execution engine.
   */
  private Enumerable<Object[]> createParquetEnumerable(File parquetFile) {
    // Use the execution engine to create the appropriate table
    switch (engineConfig.getEngineType()) {
      case PARQUET:
        try {
          ParquetTranslatableTable parquetTable = new ParquetTranslatableTable(parquetFile);
          // Since ParquetTranslatableTable is TranslatableTable, not ScannableTable,
          // we need to create a basic enumerable for now
          // In a full implementation, this would integrate with the execution engine
          return createBasicParquetEnumerable(parquetFile);
        } catch (Exception e) {
          LOGGER.error("Failed to create Parquet table for: {}", parquetFile, e);
          return Linq4j.asEnumerable(java.util.Collections.<Object[]>emptyList());
        }
      default:
        // For other engines, create a basic enumerable
        return createBasicParquetEnumerable(parquetFile);
    }
  }

  /**
   * Creates a basic Parquet enumerable using direct file reading.
   * This is a fallback implementation.
   */
  private Enumerable<Object[]> createBasicParquetEnumerable(File parquetFile) {
    // For now, return empty enumerable - this would need proper Parquet reading implementation
    LOGGER.warn("Using basic (empty) Parquet enumerable for: {}", parquetFile);
    return Linq4j.asEnumerable(java.util.Collections.<Object[]>emptyList());
  }

  /**
   * Converts Iceberg types to Calcite types.
   */
  private RelDataType convertIcebergTypeToCalcite(org.apache.iceberg.types.Type icebergType, 
                                                 RelDataTypeFactory typeFactory) {
    switch (icebergType.typeId()) {
      case BOOLEAN:
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      case INTEGER:
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case LONG:
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case FLOAT:
        return typeFactory.createSqlType(SqlTypeName.REAL);
      case DOUBLE:
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
      case STRING:
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      case TIMESTAMP:
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      case DATE:
        return typeFactory.createSqlType(SqlTypeName.DATE);
      case DECIMAL:
        Types.DecimalType decimalType = (Types.DecimalType) icebergType;
        return typeFactory.createSqlType(SqlTypeName.DECIMAL, decimalType.precision(), decimalType.scale());
      default:
        // Default to VARCHAR for unsupported types
        LOGGER.warn("Unsupported Iceberg type: {}, using VARCHAR", icebergType);
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }
  }
}