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
package org.apache.calcite.sql;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Defines the name of the types which can occur as a type argument
 * in a JDBC <code>{fn CONVERT(value, type)}</code> function.
 * (This function has similar functionality to {@code CAST}, and is not to be
 * confused with the SQL standard
 * {@link org.apache.calcite.sql.fun.SqlConvertFunction CONVERT} function.)
 *
 * @see SqlJdbcFunctionCall
 */
public enum SqlJdbcDataTypeName {
  SQL_CHAR(SqlTypeName.CHAR),
  SQL_VARCHAR(SqlTypeName.VARCHAR),
  SQL_DATE(SqlTypeName.DATE),
  SQL_TIME(SqlTypeName.TIME),
  SQL_TIME_WITH_LOCAL_TIME_ZONE(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE),
  SQL_TIMESTAMP(SqlTypeName.TIMESTAMP),
  SQL_TIMESTAMP_WITH_LOCAL_TIME_ZONE(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
  SQL_DECIMAL(SqlTypeName.DECIMAL),
  SQL_NUMERIC(SqlTypeName.DECIMAL),
  SQL_BOOLEAN(SqlTypeName.BOOLEAN),
  SQL_INTEGER(SqlTypeName.INTEGER),
  SQL_BINARY(SqlTypeName.BINARY),
  SQL_VARBINARY(SqlTypeName.VARBINARY),
  SQL_TINYINT(SqlTypeName.TINYINT),
  SQL_SMALLINT(SqlTypeName.SMALLINT),
  SQL_BIGINT(SqlTypeName.BIGINT),
  SQL_REAL(SqlTypeName.REAL),
  SQL_DOUBLE(SqlTypeName.DOUBLE),
  SQL_FLOAT(SqlTypeName.FLOAT),
  SQL_INTERVAL_YEAR(TimeUnitRange.YEAR),
  SQL_INTERVAL_YEAR_TO_MONTH(TimeUnitRange.YEAR_TO_MONTH),
  SQL_INTERVAL_MONTH(TimeUnitRange.MONTH),
  SQL_INTERVAL_DAY(TimeUnitRange.DAY),
  SQL_INTERVAL_DAY_TO_HOUR(TimeUnitRange.DAY_TO_HOUR),
  SQL_INTERVAL_DAY_TO_MINUTE(TimeUnitRange.DAY_TO_MINUTE),
  SQL_INTERVAL_DAY_TO_SECOND(TimeUnitRange.DAY_TO_SECOND),
  SQL_INTERVAL_HOUR(TimeUnitRange.HOUR),
  SQL_INTERVAL_HOUR_TO_MINUTE(TimeUnitRange.HOUR_TO_MINUTE),
  SQL_INTERVAL_HOUR_TO_SECOND(TimeUnitRange.HOUR_TO_SECOND),
  SQL_INTERVAL_MINUTE(TimeUnitRange.MINUTE),
  SQL_INTERVAL_MINUTE_TO_SECOND(TimeUnitRange.MINUTE_TO_SECOND),
  SQL_INTERVAL_SECOND(TimeUnitRange.SECOND);

  private final TimeUnitRange range;
  private final SqlTypeName typeName;

  SqlJdbcDataTypeName(SqlTypeName typeName) {
    this(typeName, null);
  }

  SqlJdbcDataTypeName(TimeUnitRange range) {
    this(null, range);
  }

  SqlJdbcDataTypeName(SqlTypeName typeName, TimeUnitRange range) {
    assert (typeName == null) != (range == null);
    this.typeName = typeName;
    this.range = range;
  }

  /**
   * Creates a parse-tree node representing an occurrence of this keyword
   * at a particular position in the parsed text.
   */
  public SqlLiteral symbol(SqlParserPos pos) {
    return SqlLiteral.createSymbol(this, pos);
  }

  /** Creates a parse tree node for a type identifier of this name. */
  public SqlNode createDataType(SqlParserPos pos) {
    if (typeName != null) {
      assert range == null;
      return new SqlDataTypeSpec(new SqlBasicTypeNameSpec(typeName, pos), pos);
    } else {
      assert range != null;
      return new SqlIntervalQualifier(range.startUnit, range.endUnit, pos);
    }
  }
}

// End SqlJdbcDataTypeName.java
