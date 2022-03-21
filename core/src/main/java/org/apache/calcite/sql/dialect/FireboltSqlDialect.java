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

package org.apache.calcite.sql.dialect;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A SqlDialect implementation for the Firebolt database.
 */
public class FireboltSqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.FIREBOLT)
      .withIdentifierQuoteString("\"")
      .withUnquotedCasing(Casing.TO_UPPER);

  public static final SqlDialect DEFAULT =
      new FireboltSqlDialect(DEFAULT_CONTEXT);

  /** Creates a FireboltSqlDialect. */
  public FireboltSqlDialect(Context context) { super(context); }

  /** Unparses offset/fetch using "LIMIT fetch OFFSET offset" syntax.
   *
   * @param writer   Object of SqlWriter class.
   * @param offset   Operator used to omit a specified number of rows from the beginning of the result set.
   * @param fetch    Operator used to specify the number of rows to be returned after the OFFSET.
   */
  @Override public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
      @Nullable SqlNode fetch) {
    unparseFetchUsingLimit(writer, offset, fetch);
  }

  /**
   * Returns whether the dialect supports character set names as part of a
   * data type, for instance {VARCHAR(30) CHARACTER SET `ISO-8859-1`}.
   *
   * @return boolean
   */
  @Override public boolean supportsCharSet() { return false; }

  /**
   * Returns whether the dialect supports aggregation functions.
   *
   * @param kind    Enum SqlKind that enumerates the possible types of {@link SqlNode}.
   * @return boolean
   */
  @Override public boolean supportsAggregateFunction(SqlKind kind) {
    switch (kind) {
    case ANY_VALUE:
//    case APPROX_PERCENTILE:
    case AVG:
//    case CHECKSUM:
    case COUNT:
    case MAX:
//    case MAX_BY:
//    case MEDIAN:
    case MIN:
//    case MIN_BY:
//    case NEST:
    case STDDEV_SAMP:
    case SUM:
      return true;
    default:
      break;
    }
    return false;
  }

  /**
   * Returns SqlNode for type in "cast(column as type)", which might be
   * different for Firebolt by type name, precision etc.
   *
   * Firebolt datatypes reference: https://docs.firebolt.io/general-reference/data-types.html
   *
   * If this method returns null, the cast will be omitted. In the default
   * implementation, this is the case for the NULL type, and therefore
   * {CAST(NULL AS <nulltype>)} is rendered as {NULL}.
   *
   * @param type      object of RelDataType class; SQL datatype of column.
   * @return SqlNode  casts the datatype of column received into datatype supported by Firebolt.
   */
  @Override public @Nullable SqlNode getCastSpec(RelDataType type) {
    String castSpec;
    switch (type.getSqlTypeName()) {
    case TINYINT:
    case SMALLINT:
      // Firebolt has no tinyint or smallint, so instead cast to INT
      // fall through
      castSpec = "INT";
      break;
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      // Firebolt has no TIME, TimeWithLocalTimezone and TimestampWithLocalTimezone
      // so instead cast all those to TIMESTAMP
      // fall through
      castSpec = "TIMESTAMP";
      break;
    case CHAR:
      // Firebolt has no CHAR, so instead cast to VARCHAR
      castSpec = "VARCHAR";
      break;
    case DECIMAL:
      // Firebolt has no DECIMAL, so instead cast to FLOAT
      castSpec = "FLOAT";
      break;
    case REAL:
      // Firebolt has no REAL, so instead cast to DOUBLE
      castSpec = "DOUBLE";
      break;
    default:
      return super.getCastSpec(type);
    }

    // returning a type specification representing a type.
    return new SqlDataTypeSpec(
        new SqlUserDefinedTypeNameSpec(castSpec, SqlParserPos.ZERO),
        SqlParserPos.ZERO);
  }
}
