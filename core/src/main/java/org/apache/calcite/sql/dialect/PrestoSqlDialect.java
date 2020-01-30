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
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.google.common.base.Preconditions;

/**
 * A <code>SqlDialect</code> implementation for the Presto database.
 */
public class PrestoSqlDialect extends SqlDialect {
  public static final Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(DatabaseProduct.PRESTO)
      .withIdentifierQuoteString("\"")
      .withUnquotedCasing(Casing.UNCHANGED)
      .withNullCollation(NullCollation.LOW);

  public static final SqlDialect DEFAULT = new PrestoSqlDialect(DEFAULT_CONTEXT);

  private static final SqlSpecialOperator PRESTO_SUBSTRING =
      new SqlSpecialOperator("SUBSTR", SqlKind.OTHER_FUNCTION) {
        public void unparse(
            SqlWriter writer,
            SqlCall call,
            int leftPrec,
            int rightPrec) {
          writer.print(getName());
          final SqlWriter.Frame frame =
              writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
          for (SqlNode operand : call.getOperandList()) {
            writer.sep(",");
            operand.unparse(writer, 0, 0);
          }
          writer.endList(frame);
        }
      };

  /**
   * Creates a PrestoSqlDialect.
   */
  public PrestoSqlDialect(Context context) {
    super(context);
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public boolean requiresAliasForFromItems() {
    return true;
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, SqlNode offset,
      SqlNode fetch) {
    unparseUsingLimit(writer, offset, fetch);
  }

  /** Unparses offset/fetch using "OFFSET offset LIMIT fetch " syntax. */
  public void unparseUsingLimit(SqlWriter writer, SqlNode offset,
      SqlNode fetch) {
    Preconditions.checkArgument(fetch != null || offset != null);
    if (offset != null) {
      writer.newlineAndIndent();
      final SqlWriter.Frame offsetFrame =
          writer.startList(SqlWriter.FrameTypeEnum.OFFSET);
      writer.keyword("OFFSET");
      offset.unparse(writer, -1, -1);
      writer.endList(offsetFrame);
    }
    if (fetch != null) {
      writer.newlineAndIndent();
      final SqlWriter.Frame fetchFrame =
          writer.startList(SqlWriter.FrameTypeEnum.FETCH);
      writer.keyword("LIMIT");
      fetch.unparse(writer, -1, -1);
      writer.endList(fetchFrame);
    }
  }

  @Override public SqlNode emulateNullDirection(SqlNode node,
      boolean nullsFirst, boolean desc) {
    return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
  }

  @Override public boolean supportsAggregateFunction(SqlKind kind) {
    switch (kind) {
    case AVG:
    case COUNT:
    case CUBE:
    case SUM:
    case MIN:
    case MAX:
    case ROLLUP:
      return true;
    }
    return false;
  }

  @Override public boolean supportsGroupByWithCube() {
    return true;
  }

  @Override public boolean supportsNestedAggregations() {
    return false;
  }

  @Override public boolean supportsGroupByWithRollup() {
    return true;
  }

  @Override public CalendarPolicy getCalendarPolicy() {
    return CalendarPolicy.SHIFT;
  }

  @Override public SqlNode getCastSpec(RelDataType type) {
    return super.getCastSpec(type);
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
      PRESTO_SUBSTRING.unparse(writer, call, 0, 0);
    } else {
      switch (call.getKind()) {
      case FLOOR:
        if (call.operandCount() != 2) {
          super.unparseCall(writer, call, leftPrec, rightPrec);
          return;
        }

        final SqlLiteral timeUnitNode = call.operand(1);
        final TimeUnitRange timeUnit = timeUnitNode.getValueAs(TimeUnitRange.class);

        SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand(call, timeUnit.name(),
            timeUnitNode.getParserPosition());
        SqlFloorFunction.unparseDatetimeFunction(writer, call2, "DATE_TRUNC", false);
        break;

      default:
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
    }
  }


  @Override public void unparseSqlIntervalQualifier(SqlWriter writer,
      SqlIntervalQualifier qualifier, RelDataTypeSystem typeSystem) {

    //  Unit Value         | Expected Format
    // --------------------+-------------------------------------------
    //  MICROSECOND        | MICROSECONDS
    //  SECOND             | SECONDS
    //  MINUTE             | MINUTES
    //  HOUR               | HOURS
    //  DAY                | DAYS
    //  WEEK               | WEEKS
    //  MONTH              | MONTHS
    //  QUARTER            | QUARTERS
    //  YEAR               | YEARS
    //  MINUTE_SECOND      | 'MINUTES:SECONDS'
    //  HOUR_MINUTE        | 'HOURS:MINUTES'
    //  DAY_HOUR           | 'DAYS HOURS'
    //  YEAR_MONTH         | 'YEARS-MONTHS'
    //  MINUTE_MICROSECOND | 'MINUTES:SECONDS.MICROSECONDS'
    //  HOUR_MICROSECOND   | 'HOURS:MINUTES:SECONDS.MICROSECONDS'
    //  SECOND_MICROSECOND | 'SECONDS.MICROSECONDS'
    //  DAY_MINUTE         | 'DAYS HOURS:MINUTES'
    //  DAY_MICROSECOND    | 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS'
    //  DAY_SECOND         | 'DAYS HOURS:MINUTES:SECONDS'
    //  HOUR_SECOND        | 'HOURS:MINUTES:SECONDS'

    if (!qualifier.useDefaultFractionalSecondPrecision()) {
      throw new AssertionError("Fractional second precision is not supported now ");
    }

    final String start = validate(qualifier.timeUnitRange.startUnit).name();
    if (qualifier.timeUnitRange.startUnit == TimeUnit.SECOND
        || qualifier.timeUnitRange.endUnit == null) {
      writer.keyword(start);
    } else {
      writer.keyword(start + "_" + qualifier.timeUnitRange.endUnit.name());
    }
  }

  private TimeUnit validate(TimeUnit timeUnit) {
    switch (timeUnit) {
    case MICROSECOND:
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
      throw new AssertionError(" Time unit " + timeUnit + "is not supported now.");
    }
  }
}
