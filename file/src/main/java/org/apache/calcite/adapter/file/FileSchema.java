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

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * Schema mapped onto a set of URLs / HTML tables. Each table in the schema
 * is an HTML table on a URL.
 */
class FileSchema extends AbstractSchema {
  private ImmutableList<Map<String, Object>> tables;

  /**
   * Creates an HTML tables schema.
   *
   * @param parentSchema Parent schema
   * @param name Schema name
   * @param tables List containing HTML table identifiers
   */
  FileSchema(SchemaPlus parentSchema, String name,
      List<Map<String, Object>> tables) {
    this.tables = ImmutableList.copyOf(tables);
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    for (Map<String, Object> tableDef : this.tables) {
      String tableName = (String) tableDef.get("name");

      try {
        FileTable table = new FileTable(tableDef, null);
        builder.put(tableName, table);
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println("Unable to instantiate table for: " + tableName);
      }
    }

    return builder.build();
  }
}

// End FileSchema.java
