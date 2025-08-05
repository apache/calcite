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
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.SqlLibraryOperators;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the conversion of RelDataType to SqlDataTypeSpec specific to target dialect.
 */
class SqlDataTypeSpecTest {

  private static final RelDataTypeSystem TYPE_SYSTEM = RelDataTypeSystem.DEFAULT;

  private String getSqlDataTypeSpec(RelDataType dataType, SqlDialect dialect) {
    return Objects.requireNonNull(dialect.getCastSpec(dataType)).toString();
  }

  private String getSqlDataTypeSpecWithPrecisionAndScale(RelDataType dataType, SqlDialect dialect) {
    return Objects.requireNonNull(dialect.getCastSpecWithPrecisionAndScale(dataType)).toString();
  }

  @Test void testDecimalWithPrecisionAndScale() {
    RelDataType dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DECIMAL, 10, 1);
    SqlDialect dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    String dataTypeSpec = "NUMERIC";
    String dataTypeSpecPrecScale = "NUMERIC(10,1)";

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));
  }

  @Test void testDecimalWithPrecision() {
    RelDataType dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DECIMAL, 10);
    SqlDialect dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    String dataTypeSpec = "NUMERIC";
    String dataTypeSpecPrecScale = "NUMERIC(10)";

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));
  }

  @Test void testSubstrWithFourArgs() {
    SqlFunction sqlFunction = SqlLibraryOperators.SUBSTR_BIG_QUERY;

    Assertions.assertNotNull(sqlFunction.getOperandTypeChecker());
    ImmutableList<SqlTypeFamily> families =
        ((FamilyOperandTypeChecker) sqlFunction.getOperandTypeChecker()).families;
    int operandSize = families.size();
    assertEquals(4, operandSize);
    String dataTypeName = families.get(3).name();
    assertEquals("STRING", dataTypeName);
  }

  @Test void testDecimalWithoutPrecisionAndScale() {
    RelDataType dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DECIMAL);
    SqlDialect dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    String dataTypeSpec = "NUMERIC";
    String dataTypeSpecPrecScale = "NUMERIC";

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));
  }

  @Test void testDecimalWithPrecisionGreaterThan29() {
    RelDataType dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DECIMAL, 30);
    SqlDialect dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    String dataTypeSpec = "BIGNUMERIC";
    String dataTypeSpecPrecScale = "BIGNUMERIC(30)";

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));
  }

  @Test void testDecimalWithPrecisionAndScaleGreaterThan9() {
    RelDataType dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DECIMAL, 30, 10);
    SqlDialect dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    String dataTypeSpec = "BIGNUMERIC";
    String dataTypeSpecPrecScale = "BIGNUMERIC(30,10)";

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));
  }

  @Test void testDecimalWithoutPrecisionAndNegativeScale() {
    RelDataType dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DECIMAL, 39, -2);
    SqlDialect dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    String dataTypeSpec = "BIGNUMERIC";
    String dataTypeSpecPrecScale = "BIGNUMERIC";

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));
  }

  @Test void testDecimalWithBQMaxPrecisionAndMaxScale() {
    RelDataType dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DECIMAL, 38, 38);
    SqlDialect dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    String dataTypeSpecPrecScale = "BIGNUMERIC(38,38)";

    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));
  }

  @Test void testDecimalWithPrecisionAndMaxScale() {
    RelDataType dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DECIMAL, 38, 36);
    SqlDialect dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    String dataTypeSpecPrecScale = "BIGNUMERIC(38,36)";

    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));
  }

  @Test void testVarcharAndChar() {
    RelDataType dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR);
    RelDataType dataType1 = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.CHAR);
    SqlDialect dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    String dataTypeSpec = "STRING";
    String dataTypeSpecPrecScale = "STRING";

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType1, dialect));
    assertEquals(
        dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType1, dialect));
  }

  @Test void testVarcharAndCharWithPrecision() {
    RelDataType dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR, 1);
    RelDataType dataType1 = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.CHAR, 1);
    SqlDialect dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    String dataTypeSpec = "STRING";
    String dataTypeSpecPrecScale = "STRING(1)";

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType1, dialect));
    assertEquals(
        dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType1, dialect));
  }

  @Test void testVarbinaryAndBinary() {
    RelDataType dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARBINARY);
    RelDataType dataType1 = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.BINARY);
    SqlDialect dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    String dataTypeSpec = "BYTES";
    String dataTypeSpecPrecScale = "BYTES";

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType1, dialect));
    assertEquals(
        dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType1, dialect));
  }

  @Test void testVarbinaryAndBinaryWithPrecision() {
    RelDataType dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARBINARY, 1);
    RelDataType dataType1 = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.BINARY, 1);
    SqlDialect dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    String dataTypeSpec = "BYTES";
    String dataTypeSpecPrecScale = "BYTES(1)";

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType1, dialect));
    assertEquals(
        dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType1, dialect));
  }

  @Test void testForInterval() {
    RelDataType dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.INTERVAL_YEAR_MONTH);
    SqlDialect dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();
    String dataTypeSpec = "INTERVAL";
    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
  }

  @Test void testPrecisiononScaleBigNumericOnBQ() {
    RelDataType dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DECIMAL, 150, 38);
    SqlDialect dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    String dataTypeSpec = "BIGNUMERIC";
    String dataTypeSpecPrecScale = "BIGNUMERIC(76,38)";

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));

    dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DECIMAL, 30, 10);
    dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    dataTypeSpec = "BIGNUMERIC";
    dataTypeSpecPrecScale = "BIGNUMERIC(30,10)";

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));

    dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DECIMAL, 40, 40);
    dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    dataTypeSpec = "BIGNUMERIC";
    dataTypeSpecPrecScale = "BIGNUMERIC(38,38)";

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));

    dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DECIMAL, 0, 28);
    dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    dataTypeSpec = "BIGNUMERIC";
    dataTypeSpecPrecScale = "BIGNUMERIC(28,28)";

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));

    dataType = new BasicSqlType(TYPE_SYSTEM, SqlTypeName.DECIMAL, 76, 40);
    dialect = SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();

    dataTypeSpec = "BIGNUMERIC";
    dataTypeSpecPrecScale = "BIGNUMERIC(74,38)";

    assertEquals(dataTypeSpec, getSqlDataTypeSpec(dataType, dialect));
    assertEquals(dataTypeSpecPrecScale, getSqlDataTypeSpecWithPrecisionAndScale(dataType, dialect));
  }
}
