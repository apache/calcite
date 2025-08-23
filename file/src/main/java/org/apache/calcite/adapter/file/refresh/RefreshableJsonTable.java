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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.execution.linq4j.JsonEnumerator;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.converters.HtmlToJsonConverter;
import org.apache.calcite.adapter.file.converters.MultiTableExcelToJsonConverter;
import org.apache.calcite.adapter.file.converters.XmlToJsonConverter;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Refreshable JSON table that re-reads the source file when modified.
 * Also monitors original source files (HTML, Excel, XML) and re-converts them when changed.
 */
public class RefreshableJsonTable extends AbstractRefreshableTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RefreshableJsonTable.class);
  
  private final Source source;
  private final String columnNameCasing;
  private @Nullable RelDataType rowType;
  private @Nullable List<Object> dataList;
  private @Nullable ConversionMetadata conversionMetadata;

  public RefreshableJsonTable(Source source, String tableName, @Nullable Duration refreshInterval) {
    this(source, tableName, refreshInterval, "UNCHANGED");
  }
  
  public RefreshableJsonTable(Source source, String tableName, @Nullable Duration refreshInterval, String columnNameCasing) {
    super(tableName, refreshInterval);
    this.source = source;
    this.columnNameCasing = columnNameCasing;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      rowType = JsonEnumerator.deduceRowType(typeFactory, source, (Map<String, Object>) null, columnNameCasing).getRelDataType();
    }
    return rowType;
  }

  @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
    // Check and refresh if needed before scanning
    refresh();

    return new AbstractEnumerable<@Nullable Object[]>() {
      @Override public Enumerator<@Nullable Object[]> enumerator() {
        JavaTypeFactory typeFactory = root.getTypeFactory();
        return new JsonEnumerator(getDataList(typeFactory));
      }
    };
  }

  private List<Object> getDataList(RelDataTypeFactory typeFactory) {
    if (dataList == null) {
      JsonEnumerator.JsonDataConverter jsonDataConverter =
          JsonEnumerator.deduceRowType(typeFactory, source, (Map<String, Object>) null, columnNameCasing);
      dataList = jsonDataConverter.getDataList();
    }
    return dataList;
  }

  @Override protected void doRefresh() {
    File jsonFile = source.file();
    if (jsonFile == null) {
      return;
    }
    
    // Initialize conversion metadata if not already done
    if (conversionMetadata == null) {
      try {
        conversionMetadata = new ConversionMetadata(jsonFile.getParentFile());
      } catch (Exception e) {
        LOGGER.debug("Could not initialize conversion metadata: {}", e.getMessage());
      }
    }
    
    // Check if original source file needs re-conversion
    boolean needsReconversion = false;
    File originalSource = null;
    
    if (conversionMetadata != null) {
      try {
        originalSource = conversionMetadata.findOriginalSource(jsonFile);
        if (originalSource != null && originalSource.exists()) {
          // Check if original source has been modified
          if (isFileModified(originalSource)) {
            needsReconversion = true;
            LOGGER.debug("Original source {} has been modified, triggering re-conversion", 
                originalSource.getName());
          }
        }
      } catch (Exception e) {
        LOGGER.debug("Error checking original source: {}", e.getMessage());
      }
    }
    
    // If original source changed, re-convert it
    if (needsReconversion && originalSource != null) {
      try {
        String fileName = originalSource.getName().toLowerCase();
        File outputDir = jsonFile.getParentFile();
        
        // Check if this is a JSONPath extraction
        ConversionMetadata.ConversionRecord record = conversionMetadata.getConversionRecord(jsonFile);
        if (record != null && record.getConversionType() != null 
            && record.getConversionType().startsWith("JSONPATH_EXTRACTION")) {
          // Extract the JSONPath from the conversion type
          String jsonPath = record.getConversionType().substring("JSONPATH_EXTRACTION:".length());
          
          // Re-run the JSONPath extraction
          org.apache.calcite.adapter.file.converters.JsonPathConverter.extract(
              originalSource, jsonFile, jsonPath, outputDir.getParentFile());
          LOGGER.debug("Re-extracted JSONPath {} from {} to {}", 
              jsonPath, originalSource.getName(), jsonFile.getName());
        } else if (fileName.endsWith(".html") || fileName.endsWith(".htm")) {
          // Re-convert HTML to JSON
          List<File> newJsonFiles = HtmlToJsonConverter.convert(originalSource, outputDir, columnNameCasing, outputDir.getParentFile());
          LOGGER.debug("Re-converted HTML file {} to {} JSON files", 
              originalSource.getName(), newJsonFiles.size());
        } else if (fileName.endsWith(".xlsx") || fileName.endsWith(".xls")) {
          // Re-convert Excel to JSON
          // Use the columnNameCasing configured for this table
          MultiTableExcelToJsonConverter.convertFileToJson(originalSource, outputDir, true, columnNameCasing, columnNameCasing, outputDir.getParentFile());
          LOGGER.debug("Re-converted Excel file {}", originalSource.getName());
        } else if (fileName.endsWith(".xml")) {
          // Re-convert XML to JSON
          List<File> newJsonFiles = XmlToJsonConverter.convert(originalSource, outputDir, columnNameCasing, outputDir.getParentFile());
          LOGGER.debug("Re-converted XML file {} to {} JSON files", 
              originalSource.getName(), newJsonFiles.size());
        }
        
        // Update the last modified time of the original source
        updateLastModified(originalSource);
        
        // Clear cached data to force re-read of new JSON
        dataList = null;
        rowType = null;
      } catch (IOException e) {
        LOGGER.warn("Failed to re-convert original source {}: {}", 
            originalSource.getName(), e.getMessage());
      }
    }
    
    // Also check if JSON file itself has been modified directly
    if (isFileModified(jsonFile)) {
      // Clear cached data to force re-read
      dataList = null;
      rowType = null;
      updateLastModified(jsonFile);
    }
  }

  @Override public RefreshBehavior getRefreshBehavior() {
    return RefreshBehavior.SINGLE_FILE;
  }

  @Override public String toString() {
    return "RefreshableJsonTable(" + tableName + ")";
  }
}
