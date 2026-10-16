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

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.DecimalDivisionTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests for {@link DecimalDivisionTypeSystem}. */
class DecimalDivisionTypeSystemTest {
  @BeforeAll static void initializeDefaultTypeSystem() {
    // Initialize the default before constructing custom type systems in parallel.
    assertThat(RelDataTypeSystem.DEFAULT.getMaxPrecision(SqlTypeName.DECIMAL), is(19));
  }

  @Test void testDefaultLimits() {
    checkDivide(RelDataTypeSystem.DEFAULT, 6, 2, 6, 2, 15, 6);
    checkDivide(new DecimalDivisionTypeSystem(), 6, 2, 6, 2, 15, 9);
    checkDivide(new DecimalDivisionTypeSystem(), 5, 2, 10, 2, 18, 13);
    checkDivide(new DecimalDivisionTypeSystem(), 19, 2, 19, 2, 19, 6);
  }

  @Test void testPrecision38() {
    final RelDataTypeSystem typeSystem = new DecimalDivisionTypeSystem() {
      @Override public int getMaxPrecision(SqlTypeName typeName) {
        return typeName == SqlTypeName.DECIMAL ? 38 : super.getMaxPrecision(typeName);
      }

      @Override public int getMaxScale(SqlTypeName typeName) {
        return typeName == SqlTypeName.DECIMAL ? 38 : super.getMaxScale(typeName);
      }
    };
    checkDivide(typeSystem, 18, 6, 18, 6, 38, 20);
    checkDivide(typeSystem, 18, 0, 18, 0, 37, 19);
    checkDivide(typeSystem, 31, 0, 18, 0, 38, 7);
    checkDivide(typeSystem, 32, 0, 18, 0, 38, 6);
    checkDivide(typeSystem, 32, 0, 1, 0, 38, 6);
    checkDivide(typeSystem, 33, 0, 1, 0, 38, 6);
    checkDivide(typeSystem, 38, 20, 38, 20, 38, 6);
  }

  @Test void testNonDecimalOperands() {
    final RelDataTypeSystem typeSystem = new DecimalDivisionTypeSystem();
    final SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(typeSystem);
    final RelDataType decimal = factory.createSqlType(SqlTypeName.DECIMAL, 6, 2);
    final RelDataType integer = factory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType approximate = factory.createSqlType(SqlTypeName.DOUBLE);
    assertNull(typeSystem.deriveDecimalDivideType(factory, integer, integer));
    assertNull(typeSystem.deriveDecimalDivideType(factory, decimal, approximate));
    assertNull(typeSystem.deriveDecimalDivideType(factory, approximate, decimal));
    final RelDataType result =
        SqlStdOperatorTable.DIVIDE.inferReturnType(factory, ImmutableList.of(decimal, integer));
    assertThat(result, hasToString("DECIMAL(17, 13)"));
  }

  @Test void testJavaNumericOperand() {
    final JavaTypeFactoryImpl factory =
        new JavaTypeFactoryImpl(new DecimalDivisionTypeSystem());
    final RelDataType decimal = factory.createSqlType(SqlTypeName.DECIMAL, 6, 2);
    final RelDataType javaInteger = factory.createJavaType(int.class);
    final RelDataType sqlInteger = factory.createSqlType(SqlTypeName.INTEGER);
    for (boolean javaLeft : ImmutableList.of(false, true)) {
      final List<RelDataType> javaOperands = javaLeft
          ? ImmutableList.of(javaInteger, decimal) : ImmutableList.of(decimal, javaInteger);
      final List<RelDataType> sqlOperands = javaLeft
          ? ImmutableList.of(sqlInteger, decimal) : ImmutableList.of(decimal, sqlInteger);
      final RelDataType actual =
          SqlStdOperatorTable.DIVIDE.inferReturnType(factory, javaOperands);
      final RelDataType expected =
          SqlStdOperatorTable.DIVIDE.inferReturnType(factory, sqlOperands);
      assertThat(actual, is(expected));
    }
  }

  @Test void testConnectionProperty() throws SQLException {
    final Properties properties = new Properties();
    checkConnection(properties, 6);
    properties.setProperty(CalciteConnectionProperty.TYPE_SYSTEM.camelName(),
        DecimalDivisionTypeSystem.class.getName());
    checkConnection(properties, 9);
  }

  private static void checkConnection(Properties properties, int scale) throws SQLException {
    final String sql = "select cast(1 as decimal(6, 2)) / cast(3 as decimal(6, 2)) as q";
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", properties);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      assertThat(resultSet.getMetaData().getPrecision(1), is(15));
      assertThat(resultSet.getMetaData().getScale(1), is(scale));
      assertThat(resultSet.next(), is(true));
    }
  }

  private static void checkDivide(RelDataTypeSystem typeSystem,
      int p1, int s1, int p2, int s2, int precision, int scale) {
    final SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(typeSystem);
    final RelDataType left = factory.createSqlType(SqlTypeName.DECIMAL, p1, s1);
    final RelDataType right = factory.createSqlType(SqlTypeName.DECIMAL, p2, s2);
    final RelDataType result =
        SqlStdOperatorTable.DIVIDE.inferReturnType(factory, ImmutableList.of(left, right));
    assertThat(result.getPrecision(), is(precision));
    assertThat(result.getScale(), is(scale));
  }
}
