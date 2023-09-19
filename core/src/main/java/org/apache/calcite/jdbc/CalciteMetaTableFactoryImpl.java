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

import org.apache.calcite.jdbc.CalciteMetaImpl.CalciteMetaTable;
import org.apache.calcite.schema.Table;

/** Default implementation of CalciteMetaTableFactory. */
public class CalciteMetaTableFactoryImpl implements
    CalciteMetaTableFactory {

  public static final CalciteMetaTableFactoryImpl INSTANCE = new CalciteMetaTableFactoryImpl();
  public CalciteMetaTableFactoryImpl() {}

  protected static final String[] COLUMN_NAMES =
      new String[] {"TABLE_CAT",
          "TABLE_SCHEM",
          "TABLE_NAME",
          "TABLE_TYPE",
          "REMARKS",
          "TYPE_CAT",
          "TYPE_SCHEM",
          "TYPE_NAME",
          "SELF_REFERENCING_COL_NAME",
          "REF_GENERATION"};

  @Override public CalciteMetaTable newMetaTable(
      Table table,
      String tableCat,
      String tableSchem,
      String tableName) {
    return new CalciteMetaTable(table, tableCat, tableSchem, tableName);
  }

  @Override public String[] getColumnNames() {
    return COLUMN_NAMES;
  }

  @Override public Class<?> getMetaTableClass() {
    return CalciteMetaTable.class;
  }
}
