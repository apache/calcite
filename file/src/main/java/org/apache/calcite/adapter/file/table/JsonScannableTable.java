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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.execution.linq4j.JsonEnumerator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Table based on a JSON file.
 *
 * <p>It implements the {@link ScannableTable} interface, so Calcite gets
 * data by calling the {@link #scan(DataContext)} method.
 */
public class JsonScannableTable extends JsonTable
    implements ScannableTable {
  /**
   * Creates a JsonScannableTable.
   */
  public JsonScannableTable(Source source) {
    super(source);
  }

  /**
   * Creates a JsonScannableTable with options.
   */
  public JsonScannableTable(Source source, Map<String, Object> options) {
    super(source, options);
  }
  
  /**
   * Creates a JsonScannableTable with options and column casing.
   */
  public JsonScannableTable(Source source, Map<String, Object> options, String columnNameCasing) {
    super(source, options, columnNameCasing);
  }

  @Override public String toString() {
    return "JsonScannableTable";
  }

  @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
    return new AbstractEnumerable<@Nullable Object[]>() {
      @Override public Enumerator<@Nullable Object[]> enumerator() {
        JavaTypeFactory typeFactory = root.getTypeFactory();
        List<Object> dataList = getDataList(typeFactory);
        System.out.println("JsonScannableTable.scan: dataList size = " + dataList.size());
        if (!dataList.isEmpty()) {
          System.out.println("JsonScannableTable.scan: first item = " + dataList.get(0));
        }
        return new JsonEnumerator(dataList);
      }
    };
  }
}
