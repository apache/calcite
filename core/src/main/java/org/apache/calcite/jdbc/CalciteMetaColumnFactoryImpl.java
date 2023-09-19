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

import org.apache.calcite.avatica.MetaImpl.MetaColumn;
import org.apache.calcite.schema.Table;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Default implementation of CalciteMetaColumnFactoryImpl. */
public class CalciteMetaColumnFactoryImpl implements
    CalciteMetaColumnFactory {

  public static final CalciteMetaColumnFactoryImpl INSTANCE = new CalciteMetaColumnFactoryImpl();
  public CalciteMetaColumnFactoryImpl() {}

  protected static final String[] COLUMN_NAMES =
      new String[] {
          "TABLE_CAT",
          "TABLE_SCHEM",
          "TABLE_NAME",
          "COLUMN_NAME",
          "DATA_TYPE",
          "TYPE_NAME",
          "COLUMN_SIZE",
          "BUFFER_LENGTH",
          "DECIMAL_DIGITS",
          "NUM_PREC_RADIX",
          "NULLABLE",
          "REMARKS",
          "COLUMN_DEF",
          "SQL_DATA_TYPE",
          "SQL_DATETIME_SUB",
          "CHAR_OCTET_LENGTH",
          "ORDINAL_POSITION",
          "IS_NULLABLE",
          "SCOPE_CATALOG",
          "SCOPE_SCHEMA",
          "SCOPE_TABLE",
          "SOURCE_DATA_TYPE",
          "IS_AUTOINCREMENT",
          "IS_GENERATEDCOLUMN"};

  @Override public MetaColumn newMetaColumn(
      Table table,
      String tableCat,
      String tableSchem,
      String tableName,
      String columnName,
      int dataType,
      String typeName,
      Integer columnSize,
      @Nullable Integer decimalDigits,
      Integer numPrecRadix,
      int nullable,
      Integer charOctetLength,
      int ordinalPosition,
      String isNullable) {
    return new MetaColumn(
        tableCat,
        tableSchem,
        tableName,
        columnName,
        dataType,
        typeName,
        columnSize,
        decimalDigits,
        numPrecRadix,
        nullable,
        charOctetLength,
        ordinalPosition,
        isNullable);
  }

  @Override public String[] getColumnNames() {
    return COLUMN_NAMES;
  }

  @Override public Class<?> getMetaColumnClass() {
    return MetaColumn.class;
  }
}
