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
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlWriter;

import java.util.Locale;

/**
 * A <code>SqlDialect</code> implementation for the Snowflake database.
 */
public class SnowflakeSqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.SNOWFLAKE)
      .withIdentifierQuoteString("\"")
      .withUnquotedCasing(Casing.TO_UPPER);

  public static final SqlDialect DEFAULT =
      new SnowflakeSqlDialect(DEFAULT_CONTEXT);

  /** Creates a SnowflakeSqlDialect. */
  public SnowflakeSqlDialect(Context context) {
    super(context);
  }

  @Override public boolean supportsApproxCountDistinct() {
    return true;
  }

  @Override public void unparseSqlIntervalLiteral(SqlWriter writer,
      SqlIntervalLiteral literal, int leftPrec, int rightPrec) {
    SqlIntervalLiteral.IntervalValue interval =
        literal.getValueAs(SqlIntervalLiteral.IntervalValue.class);
    writer.keyword("INTERVAL");
    if (interval.getSign() == -1) {
      writer.print("-");
    }
    // Snowflake requires the interval qualifier to be included
    // in the literal definition and is not a keyword
    // https://docs.snowflake.com/en/sql-reference/data-types-datetime#interval-constants
    writer.print("'");
    writer.print(interval.getIntervalLiteral());
    writer.print(" ");
    unparseSqlIntervalQualifier(writer, interval.getIntervalQualifier(),
        RelDataTypeSystem.DEFAULT);
    writer.print("'");
  }

  @Override public void unparseSqlIntervalQualifier(
      SqlWriter writer, SqlIntervalQualifier qualifier, RelDataTypeSystem typeSystem) {
    final String start = validate(qualifier.timeUnitRange.startUnit).name();
    if (qualifier.timeUnitRange.endUnit == null) {
      // Snowflake requires the interval qualifier not be a keyword
      // https://docs.snowflake.com/en/sql-reference/data-types-datetime#interval-constants
      writer.print(start);
    } else {
      throw new RuntimeException("Range time unit is not supported for Snowflake.");
    }
  }

  private static TimeUnit validate(TimeUnit timeUnit) {
    switch (timeUnit) {
    case NANOSECOND:
    case MICROSECOND:
    case MILLISECOND:
    case SECOND:
    case MINUTE:
    case HOUR:
    case DAY:
    case WEEK:
    case MONTH:
    case QUARTER:
    case YEAR:
      return timeUnit;
    default:
      throw new RuntimeException(
          String.format(Locale.ROOT, "Time unit %s is not supported for Snowflake.", timeUnit));
    }
  }
}
