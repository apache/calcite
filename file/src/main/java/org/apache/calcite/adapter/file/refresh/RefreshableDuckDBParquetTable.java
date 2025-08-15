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
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.time.Duration;

/**
 * Refreshable Parquet table that implements ScannableTable.
 * Works with both DuckDB and PARQUET execution engines.
 */
public class RefreshableDuckDBParquetTable extends RefreshableParquetCacheTable 
    implements ScannableTable {
  
  public RefreshableDuckDBParquetTable(Source source, File initialParquetFile, 
      File cacheDir, @Nullable Duration refreshInterval, boolean typeInferenceEnabled,
      String columnNameCasing, @Nullable RelProtoDataType protoRowType,
      ExecutionEngineConfig.ExecutionEngineType engineType,
      @Nullable SchemaPlus parentSchema, String fileSchemaName) {
    super(source, initialParquetFile, cacheDir, refreshInterval, typeInferenceEnabled,
          columnNameCasing, protoRowType, engineType,
          parentSchema, fileSchemaName);
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    // Always check for refresh before scanning - this handles query plan caching
    refresh();
    
    // Both DuckDB and PARQUET delegates should implement ScannableTable now
    if (delegateTable instanceof ScannableTable) {
      return ((ScannableTable) delegateTable).scan(root);
    } else {
      throw new RuntimeException("Delegate table does not support scanning: " + delegateTable.getClass());
    }
  }
}