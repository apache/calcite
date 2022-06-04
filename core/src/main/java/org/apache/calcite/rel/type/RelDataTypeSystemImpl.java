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
package org.apache.calcite.rel.type;

import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorException;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/** Default implementation of
 * {@link org.apache.calcite.rel.type.RelDataTypeSystem},
 * providing parameters from the SQL standard.
 *
 * <p>To implement other type systems, create a derived class and override
 * values as needed.
 *
 * <table border='1'>
 *   <caption>Parameter values</caption>
 *   <tr><th>Parameter</th>         <th>Value</th></tr>
 *   <tr><td>MAX_NUMERIC_SCALE</td> <td>19</td></tr>
 * </table>
 */
public abstract class RelDataTypeSystemImpl implements RelDataTypeSystem {
  @Override public int getMaxScale(SqlTypeName typeName) {
    switch (typeName) {
    case DECIMAL:
      return getMaxNumericScale();
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return SqlTypeName.MAX_INTERVAL_FRACTIONAL_SECOND_PRECISION;
    default:
      return -1;
    }
  }

  @Override public int getDefaultPrecision(SqlTypeName typeName) {
    // Following BasicSqlType precision as the default
    switch (typeName) {
    case CHAR:
    case BINARY:
      return 1;
    case VARCHAR:
    case VARBINARY:
      return RelDataType.PRECISION_NOT_SPECIFIED;
    case DECIMAL:
      return getMaxNumericPrecision();
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return SqlTypeName.DEFAULT_INTERVAL_START_PRECISION;
    case BOOLEAN:
      return 1;
    case TINYINT:
      return 3;
    case SMALLINT:
      return 5;
    case INTEGER:
      return 10;
    case BIGINT:
      return 19;
    case REAL:
      return 7;
    case FLOAT:
    case DOUBLE:
      return 15;
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case DATE:
      return 0; // SQL99 part 2 section 6.1 syntax rule 30
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      // farrago supports only 0 (see
      // SqlTypeName.getDefaultPrecision), but it should be 6
      // (microseconds) per SQL99 part 2 section 6.1 syntax rule 30.
      return 0;
    default:
      return -1;
    }
  }

  @Override public int getMaxPrecision(SqlTypeName typeName) {
    switch (typeName) {
    case DECIMAL:
      return getMaxNumericPrecision();
    case VARCHAR:
    case CHAR:
      return 65536;
    case VARBINARY:
    case BINARY:
      return 65536;
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return SqlTypeName.MAX_DATETIME_PRECISION;
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return SqlTypeName.MAX_INTERVAL_START_PRECISION;
    default:
      return getDefaultPrecision(typeName);
    }
  }

  @Override public int getMaxNumericScale() {
    return 19;
  }

  @Override public int getMaxNumericPrecision() {
    return 19;
  }

  @Override public @Nullable String getLiteral(SqlTypeName typeName, boolean isPrefix) {
    switch (typeName) {
    case VARBINARY:
    case VARCHAR:
    case CHAR:
      return "'";
    case BINARY:
      return isPrefix ? "x'" : "'";
    case TIMESTAMP:
      return isPrefix ? "TIMESTAMP '" : "'";
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return isPrefix ? "TIMESTAMP WITH LOCAL TIME ZONE '" : "'";
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return isPrefix ? "INTERVAL '" : "' DAY";
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      return isPrefix ? "INTERVAL '" : "' YEAR TO MONTH";
    case TIME:
      return isPrefix ? "TIME '" : "'";
    case TIME_WITH_LOCAL_TIME_ZONE:
      return isPrefix ? "TIME WITH LOCAL TIME ZONE '" : "'";
    case DATE:
      return isPrefix ? "DATE '" : "'";
    case ARRAY:
      return isPrefix ? "(" : ")";
    default:
      return null;
    }
  }

  @Override public boolean isCaseSensitive(SqlTypeName typeName) {
    switch (typeName) {
    case CHAR:
    case VARCHAR:
      return true;
    default:
      return false;
    }
  }

  @Override public boolean isAutoincrement(SqlTypeName typeName) {
    return false;
  }

  @Override public int getNumTypeRadix(SqlTypeName typeName) {
    if (typeName.getFamily() == SqlTypeFamily.NUMERIC
        && getDefaultPrecision(typeName) != -1) {
      return 10;
    }
    return 0;
  }

  @Override public RelDataType deriveSumType(RelDataTypeFactory typeFactory,
      RelDataType argumentType) {
    if (argumentType instanceof BasicSqlType) {
      SqlTypeName typeName = argumentType.getSqlTypeName();
      if (typeName.allowsPrec()
          && argumentType.getPrecision() != RelDataType.PRECISION_NOT_SPECIFIED) {
        int precision = typeFactory.getTypeSystem().getMaxPrecision(typeName);
        if (typeName.allowsScale()) {
          argumentType = typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(typeName, precision, argumentType.getScale()),
              argumentType.isNullable());
        } else {
          argumentType = typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(typeName, precision), argumentType.isNullable());
        }
      }
    }
    return argumentType;
  }

  @Override public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory,
      RelDataType argumentType) {
    // AVG(x) → SUM(x) / COUNT(x)
    RelDataType sumX =
        getOperatorRelDataType(typeFactory, SqlStdOperatorTable.SUM, argumentType);
    RelDataType countX =
        getOperatorRelDataType(typeFactory, SqlStdOperatorTable.COUNT, argumentType);
    return getOperatorRelDataType(typeFactory, SqlStdOperatorTable.DIVIDE, sumX, countX);
  }

  @Override public RelDataType deriveCovarType(RelDataTypeFactory typeFactory, SqlKind sqlKind,
      RelDataType arg0Type, RelDataType arg1Type) {
    switch (sqlKind) {
    case REGR_SXX:
      // REGR_SXX(x, y) → REGR_COUNT(x, y) * VAR_POP(y)

      // REGR_COUNT(x, y)
      RelDataType regrCountTypeSXX =
          getOperatorRelDataType(typeFactory, SqlStdOperatorTable.REGR_COUNT, arg0Type, arg1Type);
      // VAR_POP(y)
      RelDataType varPopTypeY =
          getOperatorRelDataType(typeFactory, SqlStdOperatorTable.VAR_POP, arg1Type);
      // REGR_COUNT(x, y) * VAR_POP(y)
      return getOperatorRelDataType(
          typeFactory, SqlStdOperatorTable.MULTIPLY, regrCountTypeSXX, varPopTypeY);

    case REGR_SYY:
      // REGR_SYY(x, y) → REGR_COUNT(x, y) * VAR_POP(x)

      // REGR_COUNT(x, y)
      RelDataType regrCountTypeSYY =
          getOperatorRelDataType(typeFactory, SqlStdOperatorTable.REGR_COUNT, arg0Type, arg1Type);
      // VAR_POP(x)
      RelDataType varPopTypeX =
          getOperatorRelDataType(typeFactory, SqlStdOperatorTable.VAR_POP, arg0Type);
      // REGR_COUNT(x, y) * VAR_POP(y)
      return getOperatorRelDataType(
          typeFactory, SqlStdOperatorTable.MULTIPLY, regrCountTypeSYY, varPopTypeX);

    case COVAR_POP:
    case COVAR_SAMP:
      // COVAR_POP(x, y) → (SUM(x * y) - SUM(x) * SUM(y) / REGR_COUNT(x, y)) / REGR_COUNT(x, y)
      // COVAR_SAMP(x, y) → (SUM(x *  y) - SUM(x) * SUM(y) / REGR_COUNT(x, y)) /
      //                    CASE REGR_COUNT(x, y) WHEN 1 THEN NULL ELSE REGR_COUNT(x, y) - 1 END

      // (x *  y)
      RelDataType  multiplyXY =
          getOperatorRelDataType(typeFactory, SqlStdOperatorTable.MULTIPLY, arg0Type, arg1Type);
      // SUM(x *  y)
      RelDataType sumMultiplyXY =
          getOperatorRelDataType(typeFactory, SqlStdOperatorTable.SUM, multiplyXY);
      // SUM(x)
      RelDataType sumX = getOperatorRelDataType(typeFactory, SqlStdOperatorTable.SUM, arg0Type);
      // SUM(y)
      RelDataType sumY = getOperatorRelDataType(typeFactory, SqlStdOperatorTable.SUM, arg1Type);
      // SUM(x) * SUM(y)
      RelDataType multiplySumXSumY =
          getOperatorRelDataType(typeFactory, SqlStdOperatorTable.MULTIPLY, sumX, sumY);
      // REGR_COUNT(x, y)
      RelDataType regrCountXY =
          getOperatorRelDataType(typeFactory, SqlStdOperatorTable.REGR_COUNT, arg0Type, arg1Type);
      // SUM(x) * SUM(y) / REGR_COUNT(x, y)
      RelDataType divide = getOperatorRelDataType(
          typeFactory, SqlStdOperatorTable.DIVIDE, multiplySumXSumY, regrCountXY);
      // SUM(x *  y) - SUM(x) * SUM(y) / REGR_COUNT(x, y)
      RelDataType minus =
          getOperatorRelDataType(typeFactory, SqlStdOperatorTable.MINUS, sumMultiplyXY, divide);
      // (sum(x * y) - sum(x) * sum(y) / regr_count(x, y)) / regr_count(x, y)
      RelDataType relDataType =
          getOperatorRelDataType(typeFactory, SqlStdOperatorTable.DIVIDE, minus, regrCountXY);
      if (sqlKind == SqlKind.COVAR_POP) {
        return relDataType;
      }
      return typeFactory.createTypeWithNullability(relDataType, true);
    }
    return arg0Type;
  }

  @Override public RelDataType deriveFractionalRankType(RelDataTypeFactory typeFactory) {
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.DOUBLE), false);
  }

  @Override public RelDataType deriveRankType(RelDataTypeFactory typeFactory) {
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.BIGINT), false);
  }

  @Override public boolean isSchemaCaseSensitive() {
    return true;
  }

  @Override public boolean shouldConvertRaggedUnionTypesToVarying() {
    return false;
  }

  /**
   * Implementation of the {@link SqlOperatorBinding} interface.
   */
  public static class TypeCallBinding extends SqlOperatorBinding {
    private final List<RelDataType> operands;

    public TypeCallBinding(RelDataTypeFactory typeFactory,
        SqlOperator sqlOperator, List<RelDataType> operands) {
      super(typeFactory, sqlOperator);
      this.operands = operands;
    }

    @Override public int getOperandCount() {
      return operands.size();
    }

    @Override public RelDataType getOperandType(int ordinal) {
      return operands.get(ordinal);
    }

    @Override public CalciteException newError(
        Resources.ExInst<SqlValidatorException> e) {
      return SqlUtil.newContextException(SqlParserPos.ZERO, e);
    }
  }

  private RelDataType getOperatorRelDataType(RelDataTypeFactory typeFactory,
      SqlOperator sqlOperator, RelDataType... argumentTypes) {
    ImmutableList<RelDataType> operatorTypeList = ImmutableList.copyOf(argumentTypes);
    TypeCallBinding operatorBinding =
        new TypeCallBinding(typeFactory, sqlOperator, operatorTypeList);
    return sqlOperator.getReturnTypeInference().inferReturnType(operatorBinding);
  }
}
