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

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
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

  private final Map<DataType, SqlTypeName> rules =
      ImmutableMap.<DataType, SqlTypeName>builder()
          .put(DataTypes.UUID, SqlTypeName.CHAR)
          .put(DataTypes.TIMEUUID, SqlTypeName.CHAR)

          .put(DataTypes.ASCII, SqlTypeName.VARCHAR)
          .put(DataTypes.TEXT, SqlTypeName.VARCHAR)

          .put(DataTypes.INT, SqlTypeName.INTEGER)
          .put(DataTypes.VARINT, SqlTypeName.INTEGER)
          .put(DataTypes.BIGINT, SqlTypeName.BIGINT)
          .put(DataTypes.TINYINT, SqlTypeName.TINYINT)
          .put(DataTypes.SMALLINT, SqlTypeName.SMALLINT)

          .put(DataTypes.DOUBLE, SqlTypeName.DOUBLE)
          .put(DataTypes.FLOAT, SqlTypeName.REAL)
          .put(DataTypes.DECIMAL, SqlTypeName.DOUBLE)

          .put(DataTypes.BLOB, SqlTypeName.VARBINARY)

          .put(DataTypes.BOOLEAN, SqlTypeName.BOOLEAN)

          .put(DataTypes.COUNTER, SqlTypeName.BIGINT)

          // number of nanoseconds since midnight
          .put(DataTypes.TIME, SqlTypeName.BIGINT)
          .put(DataTypes.DATE, SqlTypeName.DATE)
          .put(DataTypes.TIMESTAMP, SqlTypeName.TIMESTAMP)
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
  public SqlTypeName lookup(DataType name) {
    return rules.getOrDefault(name, SqlTypeName.ANY);
  }
}
