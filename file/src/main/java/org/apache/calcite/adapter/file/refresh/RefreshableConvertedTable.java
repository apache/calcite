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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.adapter.file.converters.SafeExcelToJsonConverter;
import org.apache.calcite.adapter.file.converters.HtmlToJsonConverter;
import org.apache.calcite.adapter.file.converters.XmlToJsonConverter;
import org.apache.calcite.adapter.file.table.JsonScannableTable;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;

/**
 * A refreshable table that monitors a source file and regenerates converted files
 * when the source changes. This handles conversion pipelines like:
 * - Excel → JSON → Table
 * - HTML → JSON → Table  
 * - XML → JSON → Table
 * - Any custom conversion pipeline
 * 
 * When the source file changes, it re-runs the conversion and updates the table.
 */
public class RefreshableConvertedTable extends AbstractRefreshableTable 
    implements TranslatableTable, ScannableTable {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(RefreshableConvertedTable.class);
  
  /** Function that converts source file to intermediate format (e.g., Excel to JSON) */
  private final Function<File, File> conversionFunction;
  
  /** The original source file to monitor */
  private final File sourceFile;
  
  /** The converted file (e.g., JSON file generated from Excel) */
  private volatile File convertedFile;
  
  /** The table name */
  private final String tableName;
  
  /** Whether to convert to Parquet */
  private final boolean needsParquet;
  
  /** Cache directory for Parquet files */
  private final File cacheDir;
  
  /** Schema name for Parquet conversion */
  private final String schemaName;
  
  /** Parent schema for Parquet conversion */
  private final SchemaPlus parentSchema;
  
  /** The current delegate table */
  private volatile Table delegateTable;
  
  /** Parquet file if using Parquet engine */
  private volatile File parquetFile;
  
  /**
   * Creates a refreshable converted table.
   * 
   * @param sourceFile The source file to monitor (e.g., Excel file)
   * @param conversionFunction Function to convert source to intermediate format
   * @param tableName The table name
   * @param refreshInterval How often to check for changes
   * @param needsParquet Whether to convert to Parquet
   * @param cacheDir Cache directory for conversions
   * @param schemaName Schema name
   * @param parentSchema Parent schema
   */
  public RefreshableConvertedTable(File sourceFile, 
      Function<File, File> conversionFunction,
      String tableName,
      Duration refreshInterval,
      boolean needsParquet,
      File cacheDir,
      String schemaName,
      SchemaPlus parentSchema) {
    super(sourceFile.getAbsolutePath(), refreshInterval);
    this.sourceFile = sourceFile;
    this.conversionFunction = conversionFunction;
    this.tableName = tableName;
    this.needsParquet = needsParquet;
    this.cacheDir = cacheDir;
    this.schemaName = schemaName;
    this.parentSchema = parentSchema;
    
    // Initial conversion
    updateConvertedTable();
  }
  
  /**
   * Factory method for Excel files.
   */
  public static RefreshableConvertedTable forExcel(File excelFile, String sheetName,
      Duration refreshInterval, boolean needsParquet, File cacheDir, 
      String schemaName, SchemaPlus parentSchema) {
    
    Function<File, File> excelConverter = sourceFile -> {
      try {
        // Convert Excel to JSON (this creates files like filename__sheetname.json)
        SafeExcelToJsonConverter.convertIfNeeded(sourceFile, true);
        
        // Find the generated JSON file for this sheet
        String baseName = sourceFile.getName();
        if (baseName.contains(".")) {
          baseName = baseName.substring(0, baseName.lastIndexOf('.'));
        }
        
        File jsonFile = new File(sourceFile.getParentFile(), baseName + "__" + sheetName + ".json");
        if (!jsonFile.exists()) {
          // Try without double underscore (some converters might use single)
          jsonFile = new File(sourceFile.getParentFile(), baseName + "_" + sheetName + ".json");
        }
        
        return jsonFile;
      } catch (Exception e) {
        throw new RuntimeException("Failed to convert Excel file: " + sourceFile, e);
      }
    };
    
    return new RefreshableConvertedTable(excelFile, excelConverter, sheetName,
        refreshInterval, needsParquet, cacheDir, schemaName, parentSchema);
  }
  
  /**
   * Factory method for HTML files.
   */
  public static RefreshableConvertedTable forHtml(File htmlFile, String tableName,
      Duration refreshInterval, boolean needsParquet, File cacheDir,
      String schemaName, SchemaPlus parentSchema) {
    
    Function<File, File> htmlConverter = sourceFile -> {
      try {
        // Convert HTML to JSON
        File outputDir = sourceFile.getParentFile();
        List<File> jsonFiles = HtmlToJsonConverter.convert(sourceFile, outputDir);
        // Return the first file if any were created
        if (!jsonFiles.isEmpty()) {
          return jsonFiles.get(0);
        } else {
          throw new RuntimeException("No JSON files created from HTML file: " + sourceFile);
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to convert HTML file: " + sourceFile, e);
      }
    };
    
    return new RefreshableConvertedTable(htmlFile, htmlConverter, tableName,
        refreshInterval, needsParquet, cacheDir, schemaName, parentSchema);
  }
  
  /**
   * Factory method for XML files.
   */
  public static RefreshableConvertedTable forXml(File xmlFile, String tableName,
      Duration refreshInterval, boolean needsParquet, File cacheDir,
      String schemaName, SchemaPlus parentSchema) {
    
    Function<File, File> xmlConverter = sourceFile -> {
      try {
        // Convert XML to JSON
        File outputDir = sourceFile.getParentFile();
        List<File> jsonFiles = XmlToJsonConverter.convert(sourceFile, outputDir);
        // Return the first file if any were created
        if (!jsonFiles.isEmpty()) {
          return jsonFiles.get(0);
        } else {
          throw new RuntimeException("No JSON files created from XML file: " + sourceFile);
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to convert XML file: " + sourceFile, e);
      }
    };
    
    return new RefreshableConvertedTable(xmlFile, xmlConverter, tableName,
        refreshInterval, needsParquet, cacheDir, schemaName, parentSchema);
  }
  
  @Override
  protected void doRefresh() {
    // Check if source file has been modified
    if (isFileModified(sourceFile)) {
      LOGGER.debug("Source file {} has been modified, regenerating converted table", 
          sourceFile.getAbsolutePath());
      
      try {
        // Re-run conversion
        updateConvertedTable();
        
        // Update our tracking of when the source file was last seen
        updateLastModified(sourceFile);
        
        LOGGER.info("Updated converted table {} from {}", tableName, sourceFile.getName());
      } catch (Exception e) {
        LOGGER.error("Failed to update converted table {}: {}", 
            tableName, e.getMessage(), e);
      }
    }
  }
  
  private void updateConvertedTable() {
    try {
      // Run the conversion function to get the intermediate file
      this.convertedFile = conversionFunction.apply(sourceFile);
      
      if (convertedFile == null || !convertedFile.exists()) {
        throw new IllegalStateException("Conversion failed - no output file generated");
      }
      
      // Create source from converted file
      Source convertedSource = Sources.of(convertedFile);
      
      // Create table from converted file
      Table jsonTable = new JsonScannableTable(convertedSource);
      
      // For now, always use JSON table directly
      // TODO: Add Parquet support when ParquetConversionUtil is available
      this.delegateTable = jsonTable;
    } catch (Exception e) {
      LOGGER.error("Failed to convert source file {}: {}", 
          sourceFile.getAbsolutePath(), e.getMessage(), e);
      // Keep existing delegate table if refresh fails
      if (this.delegateTable == null) {
        // Fallback to empty table
        this.delegateTable = new JsonScannableTable(Sources.of(sourceFile));
      }
    }
  }
  
  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    checkRefresh();
    return delegateTable.getRowType(typeFactory);
  }
  
  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    checkRefresh();
    if (delegateTable instanceof ScannableTable) {
      return ((ScannableTable) delegateTable).scan(root);
    }
    throw new UnsupportedOperationException("Delegate table is not scannable");
  }
  
  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    checkRefresh();
    if (delegateTable instanceof TranslatableTable) {
      return ((TranslatableTable) delegateTable).toRel(context, relOptTable);
    }
    throw new UnsupportedOperationException("Delegate table is not translatable");
  }

  @Override
  public RefreshBehavior getRefreshBehavior() {
    return RefreshBehavior.SINGLE_FILE;
  }

  /**
   * Checks if the source file needs refresh and performs conversion if needed.
   */
  private void checkRefresh() {
    // Check if source file has been modified
    if (sourceFile.exists() && isFileModified(sourceFile)) {
      // Re-run conversion
      try {
        convertedFile = conversionFunction.apply(sourceFile);
        if (convertedFile != null && convertedFile.exists()) {
          // Create new delegate table
          Source newSource = Sources.of(convertedFile);
          delegateTable = createTableFromSource(newSource);
          updateLastModified(sourceFile);
          LOGGER.info("Refreshed converted table from source: {}", sourceFile.getName());
        }
      } catch (Exception e) {
        LOGGER.error("Failed to refresh converted table from source: {}", sourceFile.getName(), e);
      }
    }
  }

  /**
   * Creates a table from the given source file.
   */
  private Table createTableFromSource(Source source) throws Exception {
    // For now, just use JSON table directly regardless of needsParquet setting
    // TODO: Add Parquet support when ParquetConversionUtil is available
    return new JsonScannableTable(source, null);
  }
}