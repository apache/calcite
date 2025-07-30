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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMapTypeNameSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.RelToSqlConverterUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.util.RelToSqlConverterUtil.unparseHiveTrim;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlDialect</code> implementation for the StarRocks database.
 */
public class StarRocksSqlDialect extends MysqlSqlDialect {

  /** StarRocks type system. */
  public static final RelDataTypeSystem STARROCKS_TYPE_SYSTEM =
      new RelDataTypeSystemImpl() {
        @Override public int getMaxPrecision(SqlTypeName typeName) {
          switch (typeName) {
          case CHAR:
            return 255;
          case VARCHAR:
            return 65533;
          case VARBINARY:
            return 1048576;
          default:
            return super.getMaxPrecision(typeName);
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
      .withDatabaseProduct(SqlDialect.DatabaseProduct.STARROCKS)
      .withIdentifierQuoteString("`")
      .withDataTypeSystem(STARROCKS_TYPE_SYSTEM)
      .withNullCollation(NullCollation.LOW);

  public static final SqlDialect DEFAULT = new StarRocksSqlDialect(DEFAULT_CONTEXT);

  /**
   * Creates a StarRocksSqlDialect.
   */
  public StarRocksSqlDialect(Context context) {
    super(context);
  }

  @Override public boolean supportsGroupByWithRollup() {
    return false;
  }

  @Override public boolean supportsTimestampPrecision() {
    return false;
  }

  @Override public boolean supportsApproxCountDistinct() {
    return true;
  }

  @Override public boolean supportsJoinType(JoinRelType joinType) {
    return true;
  }

  @Override public RexNode prepareUnparse(RexNode arg) {
    return RelToSqlConverterUtil.unparseIsTrueOrFalse(arg);
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getKind()) {
    case ARRAY_VALUE_CONSTRUCTOR:
      final SqlWriter.Frame arrayFrame = writer.startList("[", "]");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endList(arrayFrame);
      break;
    case MAP_VALUE_CONSTRUCTOR:
      writer.keyword(call.getOperator().getName());
      final SqlWriter.Frame mapFrame = writer.startList("{", "}");
      for (int i = 0; i < call.operandCount(); i++) {
        String sep = i % 2 == 0 ? "," : ":";
        writer.sep(sep);
        call.operand(i).unparse(writer, leftPrec, rightPrec);
      }
      writer.endList(mapFrame);
      break;
    case TRIM:
      unparseHiveTrim(writer, call, leftPrec, rightPrec);
      break;
    case EXTRACT:
      writer.print(call.getOperator().getName());
      final SqlWriter.Frame extractFrame = writer.startList("(", ")");
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
        funName = unit.name();
      }
      writer.print(funName);
      writer.print(" FROM ");
      String value = ((SqlLiteral) call.operand(1)).toValue();
      writer.print("'" + value + "'");
      writer.endList(extractFrame);
      break;
    case FLOOR:
      if (call.operandCount() != 2) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }
      final SqlLiteral timeUnitNode = call.operand(1);
      final TimeUnitRange timeUnit = timeUnitNode.getValueAs(TimeUnitRange.class);
      SqlCall newCall =
          SqlFloorFunction.replaceTimeUnitOperand(call, timeUnit.name(),
              timeUnitNode.getParserPosition());
      SqlFloorFunction.unparseDatetimeFunction(writer, newCall, "DATE_TRUNC", false);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
      break;
    }
  }

  @Override public @Nullable SqlNode getCastSpec(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case INTEGER:
      return new SqlDataTypeSpec(
          new SqlAlienSystemTypeNameSpec(
              "INT",
              type.getSqlTypeName(),
              SqlParserPos.ZERO),
          SqlParserPos.ZERO);
    case REAL:
      return new SqlDataTypeSpec(
          new SqlAlienSystemTypeNameSpec("FLOAT", type.getSqlTypeName(),
              SqlParserPos.ZERO),
          SqlParserPos.ZERO);
    case BIGINT:
      return new SqlDataTypeSpec(
          new SqlBasicTypeNameSpec(SqlTypeName.BIGINT, SqlParserPos.ZERO),
          SqlParserPos.ZERO);
    case TIMESTAMP:
      return new SqlDataTypeSpec(
          new SqlAlienSystemTypeNameSpec(
              "DATETIME",
              type.getSqlTypeName(),
              SqlParserPos.ZERO),
          SqlParserPos.ZERO);
    case VARCHAR:
      return new SqlDataTypeSpec(
          new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, type.getPrecision(), SqlParserPos.ZERO),
          SqlParserPos.ZERO);
    case MAP:
      MapSqlType mapSqlType = (MapSqlType) type;
      SqlDataTypeSpec keySpec = (SqlDataTypeSpec) getCastSpec(mapSqlType.getKeyType());
      SqlDataTypeSpec valueSpec =
          (SqlDataTypeSpec) getCastSpec(mapSqlType.getValueType());
      SqlDataTypeSpec nonNullKeySpec =
          requireNonNull(keySpec, "keySpec");
      SqlDataTypeSpec nonNullValueSpec =
          requireNonNull(valueSpec, "valueSpec");
      SqlMapTypeNameSpec sqlMapTypeNameSpec =
          new SqlMapTypeNameSpec(nonNullKeySpec, nonNullValueSpec, SqlParserPos.ZERO);
      return new SqlDataTypeSpec(sqlMapTypeNameSpec,
          SqlParserPos.ZERO);
    case ARRAY:
      return RelToSqlConverterUtil.getCastSpecAngleBracketArrayType(this, type,
          SqlParserPos.ZERO);
    case MULTISET:
      throw new UnsupportedOperationException("StarRocks dialect does not support cast to "
          + type.getSqlTypeName());
    default:
      break;
    }

    return super.getCastSpec(type);
  }

  @Override public void unparseDateTimeLiteral(SqlWriter writer,
      SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
    if (literal.getTypeName() == SqlTypeName.TIMESTAMP) {
      writer.literal("DATETIME '" + literal.toFormattedString() + "'");
    } else {
      super.unparseDateTimeLiteral(writer, literal, leftPrec, rightPrec);
    }
  }

}
