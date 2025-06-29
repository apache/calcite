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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Locale;

/**
 * A <code>SqlDialect</code> implementation for the MySQL database.
 */
public class MysqlSqlDialect extends SqlDialect {

  /** MySQL type system. */
  public static final RelDataTypeSystem MYSQL_TYPE_SYSTEM =
      new RelDataTypeSystemImpl() {
        @Override public int getMaxPrecision(SqlTypeName typeName) {
          switch (typeName) {
          case CHAR:
            return 255;
          case VARCHAR:
            return 65535;
          case TIMESTAMP:
            return 6;
          case DECIMAL:
            return 65;
          case UBIGINT:
            return 20;
          default:
            return super.getMaxPrecision(typeName);
          }
        }

        // We can refer to document of MySQL 5.x and 8.x:
        // https://dev.mysql.com/doc/refman/8.4/en/precision-math-decimal-characteristics.html
        // https://dev.mysql.com/doc/refman/5.7/en/precision-math-decimal-characteristics.html
        @Override public int getMaxScale(SqlTypeName typeName) {
          switch (typeName) {
          case DECIMAL:
            return 30;
          default:
            return super.getMaxScale(typeName);
          }
        }

        @Override public int getDefaultPrecision(SqlTypeName typeName) {
          if (typeName == SqlTypeName.CHAR) {
            return RelDataType.PRECISION_NOT_SPECIFIED;
          }
          return super.getDefaultPrecision(typeName);
        }
      };

  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.MYSQL)
      .withIdentifierQuoteString("`")
      .withDataTypeSystem(MYSQL_TYPE_SYSTEM)
      .withUnquotedCasing(Casing.UNCHANGED)
      .withNullCollation(NullCollation.LOW);

  public static final SqlDialect DEFAULT = new MysqlSqlDialect(DEFAULT_CONTEXT);

  /** MySQL specific function. */
  public static final SqlFunction ISNULL_FUNCTION =
      SqlBasicFunction.create("ISNULL", ReturnTypes.BOOLEAN, OperandTypes.ANY);

  private final int majorVersion;

  /** Creates a MysqlSqlDialect. */
  public MysqlSqlDialect(Context context) {
    super(context);
    majorVersion = context.databaseMajorVersion();
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public boolean requiresAliasForFromItems() {
    return true;
  }

  @Override public boolean supportsAliasedValues() {
    // MySQL supports VALUES only in INSERT; not in a FROM clause
    return false;
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
      @Nullable SqlNode fetch) {
    unparseFetchUsingLimit(writer, offset, fetch);
  }

  @Override public @Nullable SqlNode emulateNullDirection(SqlNode node,
      boolean nullsFirst, boolean desc) {
    return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
  }

  @Override public boolean supportsAggregateFunction(SqlKind kind) {
    switch (kind) {
    case COUNT:
    case SUM:
    case SUM0:
    case MIN:
    case MAX:
    case SINGLE_VALUE:
      return true;
    case ROLLUP:
      // MySQL 5 does not support standard "GROUP BY ROLLUP(x, y)",
      // only the non-standard "GROUP BY x, y WITH ROLLUP".
      return majorVersion >= 8;
    default:
      break;
    }
    return false;
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

  @Override public @Nullable SqlNode getCastSpec(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case VARCHAR:
      // MySQL doesn't have a VARCHAR type, only CHAR.
      int vcMaxPrecision = this.getTypeSystem().getMaxPrecision(SqlTypeName.CHAR);
      int precision = type.getPrecision();
      if (vcMaxPrecision > 0 && precision > vcMaxPrecision) {
        precision = vcMaxPrecision;
      }
      return new SqlDataTypeSpec(
          new SqlBasicTypeNameSpec(SqlTypeName.CHAR, precision, SqlParserPos.ZERO),
          SqlParserPos.ZERO);
    case INTEGER:
    case BIGINT:
      return new SqlDataTypeSpec(
          new SqlAlienSystemTypeNameSpec(
              "SIGNED",
              type.getSqlTypeName(),
              SqlParserPos.ZERO),
          SqlParserPos.ZERO);
    case TIMESTAMP:
      return new SqlDataTypeSpec(
          new SqlAlienSystemTypeNameSpec(
              "DATETIME",
              type.getSqlTypeName(),
              SqlParserPos.ZERO),
          SqlParserPos.ZERO);
    default:
      break;
    }
    return super.getCastSpec(type);
  }

  @Override public SqlNode rewriteSingleValueExpr(SqlNode aggCall, RelDataType relDataType) {
    final SqlNode operand = ((SqlBasicCall) aggCall).operand(0);
    final SqlLiteral nullLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
    final SqlNode unionOperand =
        new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY,
            SqlNodeList.of(nullLiteral), null, null, null, null,
            SqlNodeList.EMPTY, null, null, null, null, SqlNodeList.EMPTY);
    // For MySQL, generate
    //   CASE COUNT(*)
    //   WHEN 0 THEN NULL
    //   WHEN 1 THEN MIN(<result>)
    //   ELSE (SELECT NULL UNION ALL SELECT NULL)
    //   END
    final SqlNode caseExpr =
        new SqlCase(SqlParserPos.ZERO,
            SqlStdOperatorTable.COUNT.createCall(SqlParserPos.ZERO,
                ImmutableList.of(SqlIdentifier.STAR)),
            SqlNodeList.of(
                SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)),
            SqlNodeList.of(
                nullLiteral,
                SqlStdOperatorTable.MIN.createCall(SqlParserPos.ZERO, operand)),
            SqlStdOperatorTable.SCALAR_QUERY.createCall(SqlParserPos.ZERO,
                SqlStdOperatorTable.UNION_ALL
                    .createCall(SqlParserPos.ZERO, unionOperand, unionOperand)));

    LOGGER.debug("SINGLE_VALUE rewritten into [{}]", caseExpr);

    return caseExpr;
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    switch (call.getKind()) {
    case POSITION:
      final SqlWriter.Frame frame = writer.startFunCall("INSTR");
      writer.sep(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(frame);
      break;
    case FLOOR:
      if (call.operandCount() != 2) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }

      unparseFloor(writer, call);
      break;

    case WITHIN_GROUP:
      final List<SqlNode> operands = call.getOperandList();
      if (operands.size() <= 0 || operands.get(0).getKind() != SqlKind.LISTAGG) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }
      unparseListAggCall(writer, (SqlCall) operands.get(0),
          operands.size() == 2 ? operands.get(1) : null, leftPrec, rightPrec);
      break;

    case LISTAGG:
      unparseListAggCall(writer, call, null, leftPrec, rightPrec);
      break;

    case EXTRACT:
      SqlLiteral node = call.operand(0);
      TimeUnitRange unit = node.getValueAs(TimeUnitRange.class);
      String funName;
      switch (unit) {
      case DOW:
        funName = "DAYOFWEEK";
        break;
      case DOY:
        funName = "DAYOFYEAR";
        break;
      default:
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }
      writer.print(funName);
      final SqlWriter.Frame extractFrame = writer.startList("(", ")");
      call.operand(1).unparse(writer, 0, 0);
      writer.endList(extractFrame);
      break;

    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  /**
   * Unparses LISTAGG for MySQL. This call is translated to GROUP_CONCAT.
   *
   * <p>For example:
   * <ul>
   * <li>source:
   *   <code>LISTAGG(DISTINCT c1, ',') WITHIN GROUP (ORDER BY c2, c3)</code>
   * <li>target:
   *   <code>GROUP_CONCAT(DISTINCT c1 ORDER BY c2, c3 SEPARATOR ',')</code>
   * </ul>
   *
   * @param writer Writer
   * @param call Call to LISTAGG
   * @param orderItemNode Elements of WITHIN_GROUP; null means no elements in
   *                     the WITHIN_GROUP
   * @param leftPrec Left precedence
   * @param rightPrec Right precedence
   */
  private static void unparseListAggCall(SqlWriter writer, SqlCall call,
      @Nullable SqlNode orderItemNode, int leftPrec, int rightPrec) {
    final List<SqlNode> listAggCallOperands = call.getOperandList();
    final boolean separatorExist = listAggCallOperands.size() == 2;

    final ImmutableList.Builder<SqlNode> newOperandListBuilder =
        ImmutableList.builder();
    newOperandListBuilder.add(listAggCallOperands.get(0));
    if (orderItemNode != null) {
      newOperandListBuilder.add(orderItemNode);
    }
    if (separatorExist) {
      newOperandListBuilder.add(
          SqlInternalOperators.SEPARATOR.createCall(SqlParserPos.ZERO,
              listAggCallOperands.get(1)));
    }
    final SqlCall call2 =
        SqlLibraryOperators.GROUP_CONCAT.createCall(
            call.getFunctionQuantifier(), call.getParserPosition(),
            newOperandListBuilder.build());
    call2.unparse(writer, leftPrec, rightPrec);
  }

  /**
   * Unparses datetime floor for MySQL. There is no TRUNC function, so simulate
   * this using calls to DATE_FORMAT.
   *
   * @param writer Writer
   * @param call Call
   */
  private static void unparseFloor(SqlWriter writer, SqlCall call) {
    SqlLiteral node = call.operand(1);
    TimeUnitRange unit = node.getValueAs(TimeUnitRange.class);

    if (unit == TimeUnitRange.WEEK) {
      writer.print("STR_TO_DATE");
      SqlWriter.Frame frame = writer.startList("(", ")");

      writer.print("DATE_FORMAT(");
      call.operand(0).unparse(writer, 0, 0);
      writer.print(", '%x%v-1'), '%x%v-%w'");
      writer.endList(frame);
      return;
    }

    String format;
    boolean needSubStr = false;
    int substringLength = 0;
    switch (unit) {
    case YEAR:
      format = "%Y-01-01";
      break;
    case MONTH:
      format = "%Y-%m-01";
      break;
    case DAY:
      format = "%Y-%m-%d";
      break;
    case HOUR:
      format = "%Y-%m-%d %H:00:00";
      break;
    case MINUTE:
      format = "%Y-%m-%d %H:%i:00";
      break;
    case SECOND:
      format = "%Y-%m-%d %H:%i:%s";
      break;
    case MILLISECOND:
      needSubStr = true;
      format = "%Y-%m-%d %H:%i:%s.%f";
      substringLength = 23;
      break;
    case MICROSECOND:
      needSubStr = true;
      format = "%Y-%m-%d %H:%i:%s.%f";
      substringLength = 26;
      break;
    default:
      throw new AssertionError("MYSQL does not support FLOOR for time unit: "
          + unit);
    }

    SqlWriter.Frame substringFrame = null;
    if (needSubStr) {
      writer.print("SUBSTRING");
      substringFrame = writer.startList("(", ")");
    }

    writer.print("DATE_FORMAT");
    SqlWriter.Frame frame = writer.startList("(", ")");
    call.operand(0).unparse(writer, 0, 0);
    writer.sep(",", true);
    writer.print("'" + format + "'");
    writer.endList(frame);

    if (needSubStr) {
      String substringFormat = String.format(Locale.ROOT, ", 1, %s", substringLength);
      writer.print(substringFormat);
      writer.endList(substringFrame);
    }
  }

  @Override public boolean supportsAggregateFunctionFilter() {
    return false;
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

  @Override public boolean supportsJoinType(JoinRelType joinType) {
    return joinType != JoinRelType.FULL;
  }

  private static TimeUnit validate(TimeUnit timeUnit) {
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
