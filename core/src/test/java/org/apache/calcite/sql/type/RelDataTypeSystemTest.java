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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests return type inference using {@code RelDataTypeSystem}
 */
public class RelDataTypeSystemTest {

  private static final SqlTypeFixture TYPE_FIXTURE = new SqlTypeFixture();
  private static final SqlTypeFactoryImpl TYPE_FACTORY = TYPE_FIXTURE.typeFactory;

  /**
   * Custom type system class that overrides the default decimal plus type derivation.
   */
  private static final class CustomTypeSystem extends RelDataTypeSystemImpl {

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
      int resultPrecision = resultScale + Math.max(type1.getPrecision() - type1.getScale(),
              type2.getPrecision() - type2.getScale()) + 1;
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
  }

  private static final SqlTypeFactoryImpl CUSTOM_FACTORY = new SqlTypeFactoryImpl(new
          CustomTypeSystem());

  @Test
  public void testDecimalAdditionReturnTypeInference() {
    RelDataType operand1 = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 10, 1);
    RelDataType operand2 = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 10, 2);

    RelDataType dataType = SqlStdOperatorTable.MINUS.inferReturnType(TYPE_FACTORY,
            Lists.newArrayList(operand1, operand2));
    Assert.assertEquals(12, dataType.getPrecision());
    Assert.assertEquals(2, dataType.getScale());
  }

  @Test
  public void testDecimalModReturnTypeInference() {
    RelDataType operand1 = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 10, 1);
    RelDataType operand2 = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 19, 2);

    RelDataType dataType = SqlStdOperatorTable.MOD.inferReturnType(TYPE_FACTORY, Lists
            .newArrayList(operand1, operand2));
    Assert.assertEquals(19, dataType.getPrecision());
    Assert.assertEquals(2, dataType.getScale());
  }

  @Test
  public void testDoubleModReturnTypeInference() {
    RelDataType operand1 = TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE);
    RelDataType operand2 = TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE);

    RelDataType dataType = SqlStdOperatorTable.MOD.inferReturnType(TYPE_FACTORY, Lists
            .newArrayList(operand1, operand2));
    Assert.assertEquals(SqlTypeName.DOUBLE, dataType.getSqlTypeName());
  }

  @Test
  public void testCustomDecimalPlusReturnTypeInference() {
    RelDataType operand1 = CUSTOM_FACTORY.createSqlType(SqlTypeName.DECIMAL, 38, 10);
    RelDataType operand2 = CUSTOM_FACTORY.createSqlType(SqlTypeName.DECIMAL, 38, 20);

    RelDataType dataType = SqlStdOperatorTable.PLUS.inferReturnType(CUSTOM_FACTORY, Lists
            .newArrayList(operand1, operand2));
    Assert.assertEquals(SqlTypeName.DECIMAL, dataType.getSqlTypeName());
    Assert.assertEquals(38, dataType.getPrecision());
    Assert.assertEquals(9, dataType.getScale());
  }

  @Test
  public void testCustomDecimalMultiplyReturnTypeInference() {
    RelDataType operand1 = CUSTOM_FACTORY.createSqlType(SqlTypeName.DECIMAL, 2, 4);
    RelDataType operand2 = CUSTOM_FACTORY.createSqlType(SqlTypeName.DECIMAL, 3, 5);

    RelDataType dataType = SqlStdOperatorTable.MULTIPLY.inferReturnType(CUSTOM_FACTORY, Lists
            .newArrayList(operand1, operand2));
    Assert.assertEquals(SqlTypeName.DECIMAL, dataType.getSqlTypeName());
    Assert.assertEquals(6, dataType.getPrecision());
    Assert.assertEquals(20, dataType.getScale());
  }

  @Test
  public void testCustomDecimalDivideReturnTypeInference() {
    RelDataType operand1 = CUSTOM_FACTORY.createSqlType(SqlTypeName.DECIMAL, 28, 10);
    RelDataType operand2 = CUSTOM_FACTORY.createSqlType(SqlTypeName.DECIMAL, 38, 20);

    RelDataType dataType = SqlStdOperatorTable.DIVIDE.inferReturnType(CUSTOM_FACTORY, Lists
            .newArrayList(operand1, operand2));
    Assert.assertEquals(SqlTypeName.DECIMAL, dataType.getSqlTypeName());
    Assert.assertEquals(10, dataType.getPrecision());
    Assert.assertEquals(10, dataType.getScale());
  }

  @Test
  public void testCustomDecimalModReturnTypeInference() {
    RelDataType operand1 = CUSTOM_FACTORY.createSqlType(SqlTypeName.DECIMAL, 28, 10);
    RelDataType operand2 = CUSTOM_FACTORY.createSqlType(SqlTypeName.DECIMAL, 38, 20);

    RelDataType dataType = SqlStdOperatorTable.MOD.inferReturnType(CUSTOM_FACTORY, Lists
            .newArrayList(operand1, operand2));
    Assert.assertEquals(SqlTypeName.DECIMAL, dataType.getSqlTypeName());
    Assert.assertEquals(28, dataType.getPrecision());
    Assert.assertEquals(10, dataType.getScale());
  }
}

// End RelDataTypeSystemTest.java
