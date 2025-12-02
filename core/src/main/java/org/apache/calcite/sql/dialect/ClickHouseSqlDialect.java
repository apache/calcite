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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDateLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTimeLiteral;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.RelToSqlConverterUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Locale;

import static org.apache.calcite.util.RelToSqlConverterUtil.unparseBoolLiteralToCondition;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlDialect</code> implementation for the ClickHouse database.
 */
public class ClickHouseSqlDialect extends SqlDialect {
  public static final RelDataTypeSystem TYPE_SYSTEM =
      new RelDataTypeSystemImpl() {
        @Override public int getMaxPrecision(SqlTypeName typeName) {
          switch (typeName) {
          case DECIMAL:
            return 76;
          default:
            return super.getMaxPrecision(typeName);
          }
        }

        @Override public int getMaxScale(SqlTypeName typeName) {
          switch (typeName) {
          case DECIMAL:
            return 76;
          default:
            return super.getMaxScale(typeName);
          }
        }

        @Override public int getMaxNumericScale() {
          return getMaxScale(SqlTypeName.DECIMAL);
        }
      };

  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.CLICKHOUSE)
      .withIdentifierQuoteString("`")
      .withNullCollation(NullCollation.LOW)
      .withDataTypeSystem(TYPE_SYSTEM);

  public static final SqlDialect DEFAULT = new ClickHouseSqlDialect(DEFAULT_CONTEXT);

  /** Creates a ClickHouseSqlDialect. */
  public ClickHouseSqlDialect(Context context) {
    super(context);
  }

  @Override public boolean supportsApproxCountDistinct() {
    return true;
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public boolean supportsNestedAggregations() {
    return false;
  }

  @Override public boolean supportsWindowFunctions() {
    return false;
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }

  @Override public CalendarPolicy getCalendarPolicy() {
    return CalendarPolicy.SHIFT;
  }

  @Override public RexNode prepareUnparse(RexNode arg) {
    return RelToSqlConverterUtil.unparseIsTrueOrFalse(arg);
  }

  @Override public @Nullable SqlNode getCastSpec(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    switch (typeName) {
    case CHAR:
      return createSqlDataTypeSpecByName(
          String.format(Locale.ROOT, "FixedString(%s)",
              type.getPrecision()), typeName, type.isNullable());
    case VARCHAR:
      return createSqlDataTypeSpecByName("String", typeName, type.isNullable());
    case TINYINT:
      return createSqlDataTypeSpecByName("Int8", typeName, type.isNullable());
    case SMALLINT:
      return createSqlDataTypeSpecByName("Int16", typeName, type.isNullable());
    case INTEGER:
      return createSqlDataTypeSpecByName("Int32", typeName, type.isNullable());
    case BIGINT:
      return createSqlDataTypeSpecByName("Int64", typeName, type.isNullable());
    case REAL:
      return createSqlDataTypeSpecByName("Float32", typeName, type.isNullable());
    case FLOAT:
    case DOUBLE:
      return createSqlDataTypeSpecByName("Float64", typeName, type.isNullable());
    case DATE:
      return createSqlDataTypeSpecByName("Date", typeName, type.isNullable());
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return createSqlDataTypeSpecByName("DateTime", typeName, type.isNullable());
    case MAP:
      return RelToSqlConverterUtil.getCastSpecClickHouseSqlMapType(this, type,
          SqlParserPos.ZERO);
    case ARRAY:
      return RelToSqlConverterUtil.getCastSpecClickHouseSqlArrayType(this, type,
          SqlParserPos.ZERO);
    case MULTISET:
      throw new UnsupportedOperationException("ClickHouse dialect does not support cast to "
          + type.getSqlTypeName());
    default:
      break;
    }

    return super.getCastSpec(type);
  }

  private static SqlDataTypeSpec createSqlDataTypeSpecByName(String typeAlias,
      SqlTypeName typeName, boolean isNullable) {
    if (isNullable) {
      typeAlias = "Nullable(" + typeAlias + ")";
    }
    String finalTypeAlias = typeAlias;
    SqlBasicTypeNameSpec spec = new SqlBasicTypeNameSpec(typeName, SqlParserPos.ZERO) {
      @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        // unparse as an identifier to ensure that type names are cased correctly
        writer.identifier(finalTypeAlias, true);
      }
    };
    return new SqlDataTypeSpec(spec, SqlParserPos.ZERO);
  }

  @Override public void unparseDateTimeLiteral(SqlWriter writer,
      SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
    String toFunc;
    if (literal instanceof SqlDateLiteral) {
      toFunc = "toDate";
    } else if (literal instanceof SqlTimestampLiteral) {
      toFunc = "toDateTime";
    } else if (literal instanceof SqlTimeLiteral) {
      toFunc = "toTime";
    } else {
      throw new RuntimeException("ClickHouse does not support DateTime literal: "
          + literal);
    }

    writer.literal(toFunc + "('" + literal.toFormattedString() + "')");
  }

  @Override public void unparseBoolLiteral(SqlWriter writer, SqlLiteral literal, int leftPrec,
      int rightPrec) {
    Boolean value = (Boolean) literal.getValue();
    if (value == null) {
      return;
    }
    unparseBoolLiteralToCondition(writer, value);
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
      @Nullable SqlNode fetch) {
    requireNonNull(fetch, "fetch");

    writer.newlineAndIndent();
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.FETCH);
    writer.keyword("LIMIT");

    if (offset != null) {
      offset.unparse(writer, -1, -1);
      writer.sep(",", true);
    }

    fetch.unparse(writer, -1, -1);
    writer.endList(frame);
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    if (call.getOperator() == SqlStdOperatorTable.APPROX_COUNT_DISTINCT) {
      RelToSqlConverterUtil.specialOperatorByName("UNIQ")
          .unparse(writer, call, 0, 0);
      return;
    }

    switch (call.getKind()) {
    case MAP_VALUE_CONSTRUCTOR:
      writer.print(call.getOperator().getName().toLowerCase(Locale.ROOT));
      final SqlWriter.Frame mapFrame = writer.startList("(", ")");
      for (int i = 0; i < call.operandCount(); i++) {
        writer.sep(",");
        call.operand(i).unparse(writer, leftPrec, rightPrec);
      }
      writer.endList(mapFrame);
      break;
    case ARRAY_VALUE_CONSTRUCTOR:
      writer.print(call.getOperator().getName().toLowerCase(Locale.ROOT));
      final SqlWriter.Frame arrayFrame = writer.startList("(", ")");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endList(arrayFrame);
      break;
    case POSITION:
      final SqlWriter.Frame f = writer.startFunCall("POSITION");
      writer.sep(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      if (call.operandCount() == 3) {
        writer.sep(",");
        call.operand(2).unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(f);
      break;
    case FLOOR:
      if (call.operandCount() != 2) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }
      unparseFloor(writer, call);
      break;
    case STARTS_WITH:
      writer.print("startsWith");
      final SqlWriter.Frame startList = writer.startList("(", ")");
      call.operand(0).unparse(writer, 0, 0);
      writer.sep(",");
      call.operand(1).unparse(writer, 0, 0);
      writer.endList(startList);
      break;
    case ENDS_WITH:
      writer.print("endsWith");
      final SqlWriter.Frame endsList = writer.startList("(", ")");
      call.operand(0).unparse(writer, 0, 0);
      writer.sep(",");
      call.operand(1).unparse(writer, 0, 0);
      writer.endList(endsList);
      break;
    case BITAND:
      writer.print("bitAnd");
      final SqlWriter.Frame bitAndList = writer.startList("(", ")");
      call.operand(0).unparse(writer, 0, 0);
      writer.sep(",");
      call.operand(1).unparse(writer, 0, 0);
      writer.endList(bitAndList);
      break;
    case BITOR:
      writer.print("bitOr");
      final SqlWriter.Frame bitOrList = writer.startList("(", ")");
      call.operand(0).unparse(writer, 0, 0);
      writer.sep(",");
      call.operand(1).unparse(writer, 0, 0);
      writer.endList(bitOrList);
      break;
    case BITXOR:
      writer.print("bitXor");
      final SqlWriter.Frame bitXorList = writer.startList("(", ")");
      call.operand(0).unparse(writer, 0, 0);
      writer.sep(",");
      call.operand(1).unparse(writer, 0, 0);
      writer.endList(bitXorList);
      break;
    case BITNOT:
      writer.print("bitNot");
      final SqlWriter.Frame bitNotList = writer.startList("(", ")");
      call.operand(0).unparse(writer, 0, 0);
      writer.endList(bitNotList);
      break;
    case COUNT:
      // CH returns NULL rather than 0 for COUNT(DISTINCT) of NULL values.
      // https://github.com/yandex/ClickHouse/issues/2494
      // Wrap the call in a CH specific coalesce (assumeNotNull).
      if (call.getFunctionQuantifier() != null
          && call.getFunctionQuantifier().toString().equals("DISTINCT")) {
        writer.print("assumeNotNull");
        SqlWriter.Frame frame = writer.startList("(", ")");
        super.unparseCall(writer, call, leftPrec, rightPrec);
        writer.endList(frame);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
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
      case WEEK:
        funName = "toWeek";
        break;
      default:
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }
      writer.print(funName);
      final SqlWriter.Frame frame = writer.startList("(", ")");
      call.operand(1).unparse(writer, 0, 0);
      writer.endList(frame);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  /**
   * Unparses datetime floor for ClickHouse.
   *
   * @param writer Writer
   * @param call Call
   */
  private static void unparseFloor(SqlWriter writer, SqlCall call) {
    final SqlLiteral timeUnitNode = call.operand(1);
    TimeUnitRange unit = timeUnitNode.getValueAs(TimeUnitRange.class);

    String funName;
    switch (unit) {
    case YEAR:
      funName = "toStartOfYear";
      break;
    case MONTH:
      funName = "toStartOfMonth";
      break;
    case WEEK:
      funName = "toMonday";
      break;
    case DAY:
      funName = "toDate";
      break;
    case HOUR:
      funName = "toStartOfHour";
      break;
    case MINUTE:
      funName = "toStartOfMinute";
      break;
    case SECOND:
      funName = "toStartOfSecond";
      break;
    case MILLISECOND:
      funName = "toStartOfMillisecond";
      break;
    case MICROSECOND:
      funName = "toStartOfMicrosecond";
      break;
    case NANOSECOND:
      funName = "toStartOfNanosecond";
      break;
    default:
      throw new RuntimeException("ClickHouse does not support FLOOR for time unit: "
          + unit);
    }

    writer.print(funName);
    SqlWriter.Frame frame = writer.startList("(", ")");
    call.operand(0).unparse(writer, 0, 0);
    writer.endList(frame);
  }
}
