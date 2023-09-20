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

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/** Factory for creating MetaColumns for getColumns(). */
public interface CalciteMetaColumnFactory {
  /** Instantiates a MetaColumn. */
  MetaColumn newMetaColumn(
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
      String isNullable);

  /** Returns the list of expected column names.
   *  The default implementation returns the columns described in the JDBC specification.
   * */
  default List<String> getColumnNames() {
    return JDBC_STANDARD_COLUMNS;
  }

  /** Returns the type of object created. Must be a subclass of MetaColumn. */
  Class<? extends MetaColumn> getMetaColumnClass();

  List<String> JDBC_STANDARD_COLUMNS =
      ImmutableList.of("TABLE_CAT",
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
      "IS_GENERATEDCOLUMN");
}
