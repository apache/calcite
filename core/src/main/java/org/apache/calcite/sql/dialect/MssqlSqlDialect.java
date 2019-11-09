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

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * A <code>SqlDialect</code> implementation for the Microsoft SQL Server
 * database.
 */
public class MssqlSqlDialect extends SqlDialect {
  public static final Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.MSSQL)
      .withIdentifierQuoteString("[")
      .withCaseSensitive(false)
      .withNullCollation(NullCollation.LOW);

  public static final SqlDialect DEFAULT = new MssqlSqlDialect(DEFAULT_CONTEXT);

  private static final SqlFunction MSSQL_SUBSTRING =
      new SqlFunction("SUBSTRING", SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE_VARYING, null, null,
          SqlFunctionCategory.STRING);

  /** Whether to generate "SELECT TOP(fetch)" rather than
   * "SELECT ... FETCH NEXT fetch ROWS ONLY". */
  private final boolean top;

  /** Creates a MssqlSqlDialect. */
  public MssqlSqlDialect(Context context) {
    super(context);
    // MSSQL 2008 (version 10) and earlier only supports TOP
    // MSSQL 2012 (version 11) and higher supports OFFSET and FETCH
    top = context.databaseMajorVersion() < 11;
  }

  /** {@inheritDoc}
   *
   * <p>MSSQL does not support NULLS FIRST, so we emulate using CASE
   * expressions. For example,
   *
   * <blockquote>{@code ORDER BY x NULLS FIRST}</blockquote>
   *
   * <p>becomes
   *
   * <blockquote>
   *   {@code ORDER BY CASE WHEN x IS NULL THEN 0 ELSE 1 END, x}
   * </blockquote>
   */
  @Override public SqlNode emulateNullDirection(SqlNode node,
      boolean nullsFirst, boolean desc) {
    // Default ordering preserved
    if (nullCollation.isDefaultOrder(nullsFirst, desc)) {
      return null;
    }

    // Grouping node should preserve grouping, no emulation needed
    if (node.getKind() == SqlKind.GROUPING) {
      return node;
    }

    // Emulate nulls first/last with case ordering
    final SqlParserPos pos = SqlParserPos.ZERO;
    final SqlNodeList whenList =
        SqlNodeList.of(SqlStdOperatorTable.IS_NULL.createCall(pos, node));

    final SqlNode oneLiteral = SqlLiteral.createExactNumeric("1", pos);
    final SqlNode zeroLiteral = SqlLiteral.createExactNumeric("0", pos);

    if (nullsFirst) {
      // IS NULL THEN 0 ELSE 1 END
      return SqlStdOperatorTable.CASE.createCall(null, pos,
          null, whenList, SqlNodeList.of(zeroLiteral), oneLiteral);
    } else {
      // IS NULL THEN 1 ELSE 0 END
      return SqlStdOperatorTable.CASE.createCall(null, pos,
          null, whenList, SqlNodeList.of(oneLiteral), zeroLiteral);
    }
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, SqlNode offset,
      SqlNode fetch) {
    if (!top) {
      super.unparseOffsetFetch(writer, offset, fetch);
    }
  }

  @Override public void unparseTopN(SqlWriter writer, SqlNode offset,
      SqlNode fetch) {
    if (top) {
      // Per Microsoft:
      //   "For backward compatibility, the parentheses are optional in SELECT
      //   statements. We recommend that you always use parentheses for TOP in
      //   SELECT statements. Doing so provides consistency with its required
      //   use in INSERT, UPDATE, MERGE, and DELETE statements."
      //
      // Note that "fetch" is ignored.
      writer.keyword("TOP");
      writer.keyword("(");
      fetch.unparse(writer, -1, -1);
      writer.keyword(")");
    }
  }

  @Override public void unparseDateTimeLiteral(SqlWriter writer,
      SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
    writer.literal("'" + literal.toFormattedString() + "'");
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
      if (call.operandCount() != 3) {
        throw new IllegalArgumentException("MSSQL SUBSTRING requires FROM and FOR arguments");
      }
      SqlUtil.unparseFunctionSyntax(MSSQL_SUBSTRING, writer, call);
    } else {
      switch (call.getKind()) {
      case FLOOR:
        if (call.operandCount() != 2) {
          super.unparseCall(writer, call, leftPrec, rightPrec);
          return;
        }
        unparseFloor(writer, call);
        break;

      default:
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
    }
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  /**
   * Unparses datetime floor for Microsoft SQL Server.
   * There is no TRUNC function, so simulate this using calls to CONVERT.
   *
   * @param writer Writer
   * @param call Call
   */
  private void unparseFloor(SqlWriter writer, SqlCall call) {
    SqlLiteral node = call.operand(1);
    TimeUnitRange unit = (TimeUnitRange) node.getValue();

    switch (unit) {
    case YEAR:
      unparseFloorWithUnit(writer, call, 4, "-01-01");
      break;
    case MONTH:
      unparseFloorWithUnit(writer, call, 7, "-01");
      break;
    case WEEK:
      writer.print("CONVERT(DATETIME, CONVERT(VARCHAR(10), "
          + "DATEADD(day, - (6 + DATEPART(weekday, ");
      call.operand(0).unparse(writer, 0, 0);
      writer.print(")) % 7, ");
      call.operand(0).unparse(writer, 0, 0);
      writer.print("), 126))");
      break;
    case DAY:
      unparseFloorWithUnit(writer, call, 10, "");
      break;
    case HOUR:
      unparseFloorWithUnit(writer, call, 13, ":00:00");
      break;
    case MINUTE:
      unparseFloorWithUnit(writer, call, 16, ":00");
      break;
    case SECOND:
      unparseFloorWithUnit(writer, call, 19, ":00");
      break;
    default:
      throw new IllegalArgumentException("MSSQL does not support FLOOR for time unit: "
          + unit);
    }
  }

  @Override public void unparseSqlDatetimeArithmetic(SqlWriter writer,
      SqlCall call, SqlKind sqlKind, int leftPrec, int rightPrec) {

    final SqlWriter.Frame frame = writer.startFunCall("DATEADD");
    SqlNode operand = call.operand(1);
    if (operand instanceof SqlIntervalLiteral) {
      //There is no DATESUB method available, so change the sign.
      unparseSqlIntervalLiteralMssql(
          writer, (SqlIntervalLiteral) operand, sqlKind == SqlKind.MINUS ? -1 : 1);
    } else {
      operand.unparse(writer, leftPrec, rightPrec);
    }
    writer.sep(",", true);

    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.endList(frame);
  }

  @Override public void unparseSqlIntervalQualifier(SqlWriter writer,
      SqlIntervalQualifier qualifier, RelDataTypeSystem typeSystem) {
    switch (qualifier.timeUnitRange) {
    case YEAR:
    case QUARTER:
    case MONTH:
    case WEEK:
    case DAY:
    case HOUR:
    case MINUTE:
    case SECOND:
    case MILLISECOND:
    case MICROSECOND:
      final String timeUnit = qualifier.timeUnitRange.startUnit.name();
      writer.keyword(timeUnit);
      break;
    default:
      throw new AssertionError("Unsupported type: " + qualifier.timeUnitRange);
    }

    if (null != qualifier.timeUnitRange.endUnit) {
      throw new AssertionError("End unit is not supported now: "
          + qualifier.timeUnitRange.endUnit);
    }
  }

  @Override public void unparseSqlIntervalLiteral(
      SqlWriter writer, SqlIntervalLiteral literal, int leftPrec, int rightPrec) {
    unparseSqlIntervalLiteralMssql(writer, literal, 1);
  }

  private void unparseSqlIntervalLiteralMssql(
      SqlWriter writer, SqlIntervalLiteral literal, int sign) {
    final SqlIntervalLiteral.IntervalValue interval =
        (SqlIntervalLiteral.IntervalValue) literal.getValue();
    unparseSqlIntervalQualifier(writer, interval.getIntervalQualifier(),
        RelDataTypeSystem.DEFAULT);
    writer.sep(",", true);
    if (interval.getSign() * sign == -1) {
      writer.print("-");
    }
    writer.literal(literal.getValue().toString());
  }

  private void unparseFloorWithUnit(SqlWriter writer, SqlCall call, int charLen,
      String offset) {
    writer.print("CONVERT");
    SqlWriter.Frame frame = writer.startList("(", ")");
    writer.print("DATETIME, CONVERT(VARCHAR(" + charLen + "), ");
    call.operand(0).unparse(writer, 0, 0);
    writer.print(", 126)");

    if (offset.length() > 0) {
      writer.print("+'" + offset + "'");
    }
    writer.endList(frame);
  }
}

// End MssqlSqlDialect.java
