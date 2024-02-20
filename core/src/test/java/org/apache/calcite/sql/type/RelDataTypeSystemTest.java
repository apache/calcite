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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorException;

import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the inference of return types using {@code RelDataTypeSystem}.
 */
class RelDataTypeSystemTest {

  /**
   * Custom type system class that overrides the default decimal plus type derivation and
   * overrides the max precision for timestamps.
   */
  private static final class CustomTypeSystem extends RelDataTypeSystemImpl {
    // Arbitrarily choose a different maximum timestamp precision from the default.
    private static final int CUSTOM_MAX_TIMESTAMP_PRECISION =
        SqlTypeName.MAX_DATETIME_PRECISION + 3;

    @Override public RelDataType deriveDecimalPlusType(RelDataTypeFactory typeFactory,
        RelDataType type1, RelDataType type2) {

      if (!SqlTypeUtil.isExactNumeric(type1)
          && !SqlTypeUtil.isExactNumeric(type2)) {
        return null;
      }
      if (!SqlTypeUtil.isDecimal(type1)
            || !SqlTypeUtil.isDecimal(type2)) {
        return null;
      }

      int resultScale = Math.max(type1.getScale(), type2.getScale());
      int resultPrecision =
          resultScale
              + Math.max(type1.getPrecision() - type1.getScale(),
                  type2.getPrecision() - type2.getScale())
              + 1;
      if (resultPrecision > 38) {
        int minScale = Math.min(resultScale, 6);
        int delta = resultPrecision - 38;
        resultPrecision = 38;
        resultScale = Math.max(resultScale - delta, minScale);
      }

      return typeFactory.createSqlType(SqlTypeName.DECIMAL, resultPrecision, resultScale);
    }

    @Override public RelDataType deriveDecimalMultiplyType(RelDataTypeFactory typeFactory,
        RelDataType type1, RelDataType type2) {

      if (!SqlTypeUtil.isExactNumeric(type1)
          && !SqlTypeUtil.isExactNumeric(type2)) {
        return null;
      }
      if (!SqlTypeUtil.isDecimal(type1)
            || !SqlTypeUtil.isDecimal(type2)) {
        return null;
      }

      return typeFactory.createSqlType(SqlTypeName.DECIMAL,
          type1.getPrecision() * type2.getPrecision(), type1.getScale() * type2.getScale());
    }

    @Override public RelDataType deriveDecimalDivideType(RelDataTypeFactory typeFactory,
        RelDataType type1, RelDataType type2) {

      if (!SqlTypeUtil.isExactNumeric(type1)
          && !SqlTypeUtil.isExactNumeric(type2)) {
        return null;
      }
      if (!SqlTypeUtil.isDecimal(type1)
            || !SqlTypeUtil.isDecimal(type2)) {
        return null;
      }

      return typeFactory.createSqlType(SqlTypeName.DECIMAL,
          Math.abs(type1.getPrecision() - type2.getPrecision()),
          Math.abs(type1.getScale() - type2.getScale()));
    }

    @Override public RelDataType deriveDecimalModType(RelDataTypeFactory typeFactory,
        RelDataType type1, RelDataType type2) {
      if (!SqlTypeUtil.isExactNumeric(type1)
          && !SqlTypeUtil.isExactNumeric(type2)) {
        return null;
      }
      if (!SqlTypeUtil.isDecimal(type1)
            || !SqlTypeUtil.isDecimal(type2)) {
        return null;
      }

      return type1;
    }

    @Override public int getMaxNumericPrecision() {
      return 38;
    }

    @Override public int getMaxPrecision(SqlTypeName typeName) {
      if (typeName == SqlTypeName.TIMESTAMP) {
        return CUSTOM_MAX_TIMESTAMP_PRECISION;
      }
      return super.getMaxPrecision(typeName);
    }
  }

  /** Test fixture with custom type factory. */
  static class Fixture extends SqlTypeFixture {
    final SqlTypeFactoryImpl customTypeFactory = new SqlTypeFactoryImpl(new CustomTypeSystem());
  }

  @Test void testDecimalAdditionReturnTypeInference() {
    final SqlTypeFactoryImpl f = new Fixture().typeFactory;
    RelDataType operand1 = f.createSqlType(SqlTypeName.DECIMAL, 10, 1);
    RelDataType operand2 = f.createSqlType(SqlTypeName.DECIMAL, 10, 2);

    RelDataType dataType =
        SqlStdOperatorTable.MINUS.inferReturnType(f,
            Lists.newArrayList(operand1, operand2));
    assertEquals(12, dataType.getPrecision());
    assertEquals(2, dataType.getScale());
  }

  @Test void testDecimalModReturnTypeInference() {
    final SqlTypeFactoryImpl f = new Fixture().typeFactory;
    RelDataType operand1 = f.createSqlType(SqlTypeName.DECIMAL, 10, 1);
    RelDataType operand2 = f.createSqlType(SqlTypeName.DECIMAL, 19, 2);

    RelDataType dataType = SqlStdOperatorTable.MOD.inferReturnType(f, Lists
            .newArrayList(operand1, operand2));
    assertEquals(11, dataType.getPrecision());
    assertEquals(2, dataType.getScale());
  }

  @Test void testDoubleModReturnTypeInference() {
    final SqlTypeFactoryImpl f = new Fixture().typeFactory;
    RelDataType operand1 = f.createSqlType(SqlTypeName.DOUBLE);
    RelDataType operand2 = f.createSqlType(SqlTypeName.DOUBLE);

    RelDataType dataType = SqlStdOperatorTable.MOD.inferReturnType(f, Lists
            .newArrayList(operand1, operand2));
    assertEquals(SqlTypeName.DOUBLE, dataType.getSqlTypeName());
  }

  /** Tests that LEAST_RESTRICTIVE considers a MEASURE's element type
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5869">[CALCITE-5869]
   * LEAST_RESTRICTIVE does not use MEASURE element type</a>. */
  @Test void testLeastRestrictiveUsesMeasureElementType() {
    final SqlTypeFactoryImpl f = new Fixture().typeFactory;
    RelDataType innerType = f.createSqlType(SqlTypeName.DOUBLE);
    RelDataType operand1 = f.createMeasureType(innerType);
    RelDataType operand2 = f.createSqlType(SqlTypeName.INTEGER);
    RelDataType dataType = SqlLibraryOperators.IFNULL
        .inferReturnType(f, Lists.newArrayList(operand1, operand2));
    assertThat(dataType, is(innerType));
  }

  @Test void testCustomDecimalPlusReturnTypeInference() {
    final SqlTypeFactoryImpl f = new Fixture().customTypeFactory;
    RelDataType operand1 = f.createSqlType(SqlTypeName.DECIMAL, 38, 10);
    RelDataType operand2 = f.createSqlType(SqlTypeName.DECIMAL, 38, 20);

    RelDataType dataType = SqlStdOperatorTable.PLUS.inferReturnType(f, Lists
            .newArrayList(operand1, operand2));
    assertEquals(SqlTypeName.DECIMAL, dataType.getSqlTypeName());
    assertEquals(38, dataType.getPrecision());
    assertEquals(9, dataType.getScale());
  }

  @Test void testCustomDecimalMultiplyReturnTypeInference() {
    final SqlTypeFactoryImpl f = new Fixture().customTypeFactory;
    RelDataType operand1 = f.createSqlType(SqlTypeName.DECIMAL, 2, 4);
    RelDataType operand2 = f.createSqlType(SqlTypeName.DECIMAL, 3, 5);

    RelDataType dataType = SqlStdOperatorTable.MULTIPLY.inferReturnType(f, Lists
            .newArrayList(operand1, operand2));
    assertEquals(SqlTypeName.DECIMAL, dataType.getSqlTypeName());
    assertEquals(6, dataType.getPrecision());
    assertEquals(20, dataType.getScale());
  }

  @Test void testCustomDecimalDivideReturnTypeInference() {
    final SqlTypeFactoryImpl f = new Fixture().customTypeFactory;
    RelDataType operand1 = f.createSqlType(SqlTypeName.DECIMAL, 28, 10);
    RelDataType operand2 = f.createSqlType(SqlTypeName.DECIMAL, 38, 20);

    RelDataType dataType = SqlStdOperatorTable.DIVIDE.inferReturnType(f, Lists
            .newArrayList(operand1, operand2));
    assertEquals(SqlTypeName.DECIMAL, dataType.getSqlTypeName());
    assertEquals(10, dataType.getPrecision());
    assertEquals(10, dataType.getScale());
  }

  @Test void testCustomDecimalModReturnTypeInference() {
    final SqlTypeFactoryImpl f = new Fixture().customTypeFactory;
    RelDataType operand1 = f.createSqlType(SqlTypeName.DECIMAL, 28, 10);
    RelDataType operand2 = f.createSqlType(SqlTypeName.DECIMAL, 38, 20);

    RelDataType dataType = SqlStdOperatorTable.MOD.inferReturnType(f, Lists
            .newArrayList(operand1, operand2));
    assertEquals(SqlTypeName.DECIMAL, dataType.getSqlTypeName());
    assertEquals(28, dataType.getPrecision());
    assertEquals(10, dataType.getScale());
  }

  /** Tests that when inferring the return type for a timestamp function that takes a precision,
   * the maximum precision as defined by the type system is used, rather than the default max
   * precision.
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6262">[CALCITE-6262]
   * CURRENT_TIMESTAMP(P) ignores DataTypeSystem#getMaxPrecision</a>. */
  @Test void testCustomMaxTimestampPrecisionTimeFunctionReturnTypeInference() {
    final SqlTypeFactoryImpl f = new Fixture().customTypeFactory;
    final SqlLiteral sqlOperand =
        SqlTypeName.INTEGER.createLiteral(
            String.valueOf(CustomTypeSystem.CUSTOM_MAX_TIMESTAMP_PRECISION), SqlParserPos.ZERO);

    final RelDataType dataType =
        SqlStdOperatorTable.LOCALTIMESTAMP.inferReturnType(
            new SqlOperatorBinding(f, SqlStdOperatorTable.LOCALTIMESTAMP) {
            @Override public int getOperandCount() {
              return 1;
            }

            @Override public RelDataType getOperandType(int ordinal) {
              return sqlOperand.createSqlType(f);
            }

            @Override public CalciteException newError(Resources.ExInst<SqlValidatorException> e) {
              return null;
            }

            @Override public <T> @Nullable T getOperandLiteralValue(int ordinal, Class<T> clazz) {
              return sqlOperand.getValueAs(clazz);
            }
          });
    assertEquals(SqlTypeName.TIMESTAMP, dataType.getSqlTypeName());
    assertEquals(CustomTypeSystem.CUSTOM_MAX_TIMESTAMP_PRECISION, dataType.getPrecision());
  }
}
