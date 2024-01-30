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
package org.apache.calcite.adapter.innodb;

import org.apache.calcite.sql.type.SqlTypeName;

import com.alibaba.innodb.java.reader.column.ColumnType;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Mappings from <code>innodb-java-reader</code> column types
 * to corresponding SQL types.
 */
public class ColumnTypeToSqlTypeConversionRules {
  //~ Static fields/initializers ---------------------------------------------

  private static final ColumnTypeToSqlTypeConversionRules INSTANCE =
      new ColumnTypeToSqlTypeConversionRules();

  //~ Instance fields --------------------------------------------------------

  private final Map<String, SqlTypeName> rules =
      ImmutableMap.<String, SqlTypeName>builder()
          .put(ColumnType.TINYINT, SqlTypeName.TINYINT)
          .put(ColumnType.SMALLINT, SqlTypeName.SMALLINT)
          .put(ColumnType.MEDIUMINT, SqlTypeName.INTEGER)
          .put(ColumnType.INT, SqlTypeName.INTEGER)
          .put(ColumnType.BIGINT, SqlTypeName.BIGINT)
          .put(ColumnType.UNSIGNED_TINYINT, SqlTypeName.TINYINT)
          .put(ColumnType.UNSIGNED_SMALLINT, SqlTypeName.SMALLINT)
          .put(ColumnType.UNSIGNED_MEDIUMINT, SqlTypeName.INTEGER)
          .put(ColumnType.UNSIGNED_INT, SqlTypeName.INTEGER)
          .put(ColumnType.UNSIGNED_BIGINT, SqlTypeName.BIGINT)

          .put(ColumnType.FLOAT, SqlTypeName.REAL)
          .put(ColumnType.REAL, SqlTypeName.REAL)
          .put(ColumnType.DOUBLE, SqlTypeName.DOUBLE)
          .put(ColumnType.DECIMAL, SqlTypeName.DECIMAL)
          .put(ColumnType.NUMERIC, SqlTypeName.DECIMAL)

          .put(ColumnType.BOOL, SqlTypeName.BOOLEAN)
          .put(ColumnType.BOOLEAN, SqlTypeName.BOOLEAN)

          .put(ColumnType.CHAR, SqlTypeName.CHAR)
          .put(ColumnType.VARCHAR, SqlTypeName.VARCHAR)
          .put(ColumnType.BINARY, SqlTypeName.BINARY)
          .put(ColumnType.VARBINARY, SqlTypeName.VARBINARY)
          .put(ColumnType.TINYBLOB, SqlTypeName.VARBINARY)
          .put(ColumnType.MEDIUMBLOB, SqlTypeName.VARBINARY)
          .put(ColumnType.BLOB, SqlTypeName.VARBINARY)
          .put(ColumnType.LONGBLOB, SqlTypeName.VARBINARY)
          .put(ColumnType.TINYTEXT, SqlTypeName.VARCHAR)
          .put(ColumnType.MEDIUMTEXT, SqlTypeName.VARCHAR)
          .put(ColumnType.TEXT, SqlTypeName.VARCHAR)
          .put(ColumnType.LONGTEXT, SqlTypeName.VARCHAR)

          .put(ColumnType.YEAR, SqlTypeName.SMALLINT)
          .put(ColumnType.TIME, SqlTypeName.TIME)
          .put(ColumnType.DATE, SqlTypeName.DATE)
          .put(ColumnType.DATETIME, SqlTypeName.TIMESTAMP)
          .put(ColumnType.TIMESTAMP, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)

          .put(ColumnType.ENUM, SqlTypeName.VARCHAR)
          .put(ColumnType.SET, SqlTypeName.VARCHAR)
          .put(ColumnType.BIT, SqlTypeName.VARBINARY)

          .build();

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the
   * {@link org.apache.calcite.util.Glossary#SINGLETON_PATTERN singleton}
   * instance.
   */
  public static ColumnTypeToSqlTypeConversionRules instance() {
    return INSTANCE;
  }

  /**
   * Returns a corresponding {@link SqlTypeName} for a given InnoDB type name.
   *
   * @param name the column type name to lookup
   * @return a corresponding SqlTypeName if found, ANY otherwise
   */
  public SqlTypeName lookup(String name) {
    return rules.getOrDefault(name, SqlTypeName.ANY);
  }
}
