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
package org.apache.calcite.jdbc;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

import java.sql.SQLException;
import java.util.Map;

import static org.apache.calcite.jdbc.CalciteMetaImpl.MetaColumn;
import static org.apache.calcite.jdbc.CalciteMetaImpl.MetaTable;

/** Schema that contains metadata tables such as "TABLES" and "COLUMNS". */
class MetadataSchema extends AbstractSchema {
  private static final Map<String, Table> TABLE_MAP =
      ImmutableMap.of(
          "COLUMNS",
          new CalciteMetaImpl.MetadataTable<MetaColumn>(MetaColumn.class) {
            public Enumerator<MetaColumn> enumerator(
                final CalciteMetaImpl meta) {
              final String catalog;
              try {
                catalog = meta.getConnection().getCatalog();
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
              return meta.tables(catalog)
                  .selectMany(meta::columns).enumerator();
            }
          },
          "TABLES",
          new CalciteMetaImpl.MetadataTable<MetaTable>(MetaTable.class) {
            public Enumerator<MetaTable> enumerator(CalciteMetaImpl meta) {
              final String catalog;
              try {
                catalog = meta.getConnection().getCatalog();
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
              return meta.tables(catalog).enumerator();
            }
          });

  public static final Schema INSTANCE = new MetadataSchema();

  /** Creates the data dictionary, also called the information schema. It is a
   * schema called "metadata" that contains tables "TABLES", "COLUMNS" etc. */
  private MetadataSchema() {}

  @Override protected Map<String, Table> getTableMap() {
    return TABLE_MAP;
  }
}

// End MetadataSchema.java
