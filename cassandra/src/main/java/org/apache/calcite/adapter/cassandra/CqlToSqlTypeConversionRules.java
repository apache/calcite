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
package org.apache.calcite.adapter.cassandra;

import org.apache.calcite.sql.type.SqlTypeName;

import com.datastax.driver.core.DataType;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * CqlToSqlTypeConversionRules defines mappings from CQL types to
 * corresponding SQL types.
 */
public class CqlToSqlTypeConversionRules {
  //~ Static fields/initializers ---------------------------------------------

  private static final CqlToSqlTypeConversionRules INSTANCE =
      new CqlToSqlTypeConversionRules();

  //~ Instance fields --------------------------------------------------------

  private final Map<DataType.Name, SqlTypeName> rules =
      ImmutableMap.<DataType.Name, SqlTypeName>builder()
          .put(DataType.Name.UUID, SqlTypeName.CHAR)
          .put(DataType.Name.TIMEUUID, SqlTypeName.CHAR)

          .put(DataType.Name.ASCII, SqlTypeName.VARCHAR)
          .put(DataType.Name.TEXT, SqlTypeName.VARCHAR)
          .put(DataType.Name.VARCHAR, SqlTypeName.VARCHAR)

          .put(DataType.Name.INT, SqlTypeName.INTEGER)
          .put(DataType.Name.VARINT, SqlTypeName.INTEGER)
          .put(DataType.Name.BIGINT, SqlTypeName.BIGINT)
          .put(DataType.Name.TINYINT, SqlTypeName.TINYINT)
          .put(DataType.Name.SMALLINT, SqlTypeName.SMALLINT)

          .put(DataType.Name.DOUBLE, SqlTypeName.DOUBLE)
          .put(DataType.Name.FLOAT, SqlTypeName.REAL)
          .put(DataType.Name.DECIMAL, SqlTypeName.DOUBLE)

          .put(DataType.Name.BLOB, SqlTypeName.VARBINARY)

          .put(DataType.Name.BOOLEAN, SqlTypeName.BOOLEAN)

          .put(DataType.Name.COUNTER, SqlTypeName.BIGINT)

          // number of nanoseconds since midnight
          .put(DataType.Name.TIME, SqlTypeName.BIGINT)
          .put(DataType.Name.DATE, SqlTypeName.DATE)
          .put(DataType.Name.TIMESTAMP, SqlTypeName.TIMESTAMP)

          .put(DataType.Name.MAP, SqlTypeName.MAP)
          .put(DataType.Name.LIST, SqlTypeName.ARRAY)
          .put(DataType.Name.SET, SqlTypeName.MULTISET)
          .put(DataType.Name.TUPLE, SqlTypeName.STRUCTURED)
          .build();

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the
   * {@link org.apache.calcite.util.Glossary#SINGLETON_PATTERN singleton}
   * instance.
   */
  public static CqlToSqlTypeConversionRules instance() {
    return INSTANCE;
  }

  /**
   * Returns a corresponding {@link SqlTypeName} for a given CQL type name.
   *
   * @param name the CQL type name to lookup
   * @return a corresponding SqlTypeName if found, ANY otherwise
   */
  public SqlTypeName lookup(DataType.Name name) {
    return rules.getOrDefault(name, SqlTypeName.ANY);
  }
}
