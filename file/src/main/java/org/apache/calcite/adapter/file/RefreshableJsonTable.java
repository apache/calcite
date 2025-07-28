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
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.time.Duration;
import java.util.List;

/**
 * Refreshable JSON table that re-reads the source file when modified.
 */
public class RefreshableJsonTable extends AbstractRefreshableTable implements ScannableTable {
  private final Source source;
  private @Nullable RelDataType rowType;
  private @Nullable List<Object> dataList;

  public RefreshableJsonTable(Source source, String tableName, @Nullable Duration refreshInterval) {
    super(tableName, refreshInterval);
    this.source = source;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      rowType = JsonEnumerator.deduceRowType(typeFactory, source).getRelDataType();
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
          JsonEnumerator.deduceRowType(typeFactory, source);
      dataList = jsonDataConverter.getDataList();
    }
    return dataList;
  }

  @Override protected void doRefresh() {
    // Check if file has been modified
    File file = source.file();
    if (file != null && isFileModified(file)) {
      // Clear cached data to force re-read
      dataList = null;
      rowType = null;
      updateLastModified(file);
    }
  }

  @Override public RefreshBehavior getRefreshBehavior() {
    return RefreshBehavior.SINGLE_FILE;
  }

  @Override public String toString() {
    return "RefreshableJsonTable(" + tableName + ")";
  }
}
