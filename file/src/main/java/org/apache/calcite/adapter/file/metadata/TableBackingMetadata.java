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
package org.apache.calcite.adapter.file.metadata;

import java.io.File;

/**
 * Comprehensive metadata for tracking table backing files.
 * Each table has:
 * - originalSource: The original source file (e.g., HTML, Excel, CSV)
 * - generatedSource: Intermediate generated file (e.g., JSON from HTML conversion)
 * - cached: Optional cached file (e.g., Parquet for DuckDB/PARQUET engines)
 */
public class TableBackingMetadata {
  private final String tableName;
  private File originalSource;
  private File generatedSource;
  private File cached;
  
  public TableBackingMetadata(String tableName) {
    this.tableName = tableName;
  }
  
  public String getTableName() {
    return tableName;
  }
  
  public File getOriginalSource() {
    return originalSource;
  }
  
  public void setOriginalSource(File originalSource) {
    this.originalSource = originalSource;
  }
  
  public File getGeneratedSource() {
    return generatedSource;
  }
  
  public void setGeneratedSource(File generatedSource) {
    this.generatedSource = generatedSource;
  }
  
  public File getCached() {
    return cached;
  }
  
  public void setCached(File cached) {
    this.cached = cached;
  }
  
  /**
   * Get the appropriate backing file based on engine requirements.
   * For DuckDB/PARQUET engines, returns cached file if available.
   * Otherwise returns generatedSource or originalSource.
   */
  public File getBackingFile(boolean requiresCached) {
    if (requiresCached && cached != null) {
      return cached;
    }
    if (generatedSource != null) {
      return generatedSource;
    }
    return originalSource;
  }
  
  @Override
  public String toString() {
    return "TableBackingMetadata{" +
        "tableName='" + tableName + '\'' +
        ", originalSource=" + originalSource +
        ", generatedSource=" + generatedSource +
        ", cached=" + cached +
        '}';
  }
}