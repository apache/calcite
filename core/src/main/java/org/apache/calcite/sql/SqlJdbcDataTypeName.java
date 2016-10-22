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
 * in a JDBC CONVERT function.
 */
public enum SqlJdbcDataTypeName {
  SQL_CHAR(SqlTypeName.CHAR),
  SQL_VARCHAR(SqlTypeName.VARCHAR),
  SQL_DATE(SqlTypeName.DATE),
  SQL_TIME(SqlTypeName.TIME),
  SQL_TIMESTAMP(SqlTypeName.TIMESTAMP),
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

  private final SqlNode dataType;

  private SqlJdbcDataTypeName(SqlTypeName typeName) {
    SqlIdentifier id = new SqlIdentifier(typeName.name(), SqlParserPos.ZERO);
    dataType = new SqlDataTypeSpec(id, -1, -1, null, null, SqlParserPos.ZERO);
  }

  private SqlJdbcDataTypeName(TimeUnitRange range) {
    dataType = new SqlIntervalQualifier(range.startUnit, range.endUnit, SqlParserPos.ZERO);
  }
  /**
   * Creates a parse-tree node representing an occurrence of this keyword
   * at a particular position in the parsed text.
   */
  public SqlLiteral symbol(SqlParserPos pos) {
    return SqlLiteral.createSymbol(this, pos);
  }

  public SqlNode createDataType(SqlParserPos pos) {
    // Cloning to not leak the original data type
    return dataType.clone(pos);
  }
}

// End SqlJdbcDataTypeName.java
