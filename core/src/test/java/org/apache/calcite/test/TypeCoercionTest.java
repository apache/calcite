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
package org.apache.calcite.test;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.implicit.AbstractTypeCoercion;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for implicit type coercion. see {@link TypeCoercion} doc
 * or <a href="https://docs.google.com/spreadsheets/d/1GhleX5h5W8-kJKh7NMJ4vtoE78pwfaZRJl88ULX_MgU/edit?usp=sharing">CalciteImplicitCasts</a>
 * for conversion details.
 */
class TypeCoercionTest {

  public static final Fixture DEFAULT_FIXTURE =
      Fixture.create(SqlTestFactory.INSTANCE);

  //~ Helper methods ---------------------------------------------------------

  public Fixture fixture() {
    return DEFAULT_FIXTURE;
  }

  public static SqlValidatorFixture sql(String sql) {
    return validatorFixture()
        .withSql(sql);
  }

  public static SqlValidatorFixture expr(String sql) {
    return validatorFixture()
        .withExpr(sql);
  }

  private static SqlValidatorFixture validatorFixture() {
    return SqlValidatorTestCase.FIXTURE
        .withCatalogReader(TCatalogReader::create);
  }

  private static ImmutableList<RelDataType> combine(
      List<RelDataType> list0,
      List<RelDataType> list1) {
    return ImmutableList.<RelDataType>builder()
        .addAll(list0)
        .addAll(list1)
        .build();
  }

  private static ImmutableList<RelDataType> combine(
      List<RelDataType> list0,
      List<RelDataType> list1,
      List<RelDataType> list2) {
    return ImmutableList.<RelDataType>builder()
        .addAll(list0)
        .addAll(list1)
        .addAll(list2)
        .build();
  }

  //~ Tests ------------------------------------------------------------------

  /**
   * Test case for {@link TypeCoercion#getTightestCommonType}.
   */
  @Test void testGetTightestCommonType() {
    // NULL
    final Fixture f = fixture();
    f.checkCommonType(f.nullType, f.nullType, f.nullType, true);
    // BOOLEAN
    f.checkCommonType(f.nullType, f.booleanType, f.booleanType, true);
    f.checkCommonType(f.booleanType, f.booleanType, f.booleanType, true);
    f.checkCommonType(f.intType, f.booleanType, null, true);
    f.checkCommonType(f.bigintType, f.booleanType, null, true);
    // INT
    f.checkCommonType(f.nullType, f.tinyintType, f.tinyintType, true);
    f.checkCommonType(f.nullType, f.intType, f.intType, true);
    f.checkCommonType(f.nullType, f.bigintType, f.bigintType, true);
    f.checkCommonType(f.smallintType, f.intType, f.intType, true);
    f.checkCommonType(f.smallintType, f.bigintType, f.bigintType, true);
    f.checkCommonType(f.intType, f.bigintType, f.bigintType, true);
    f.checkCommonType(f.bigintType, f.bigintType, f.bigintType, true);
    // FLOAT/DOUBLE
    f.checkCommonType(f.nullType, f.floatType, f.floatType, true);
    f.checkCommonType(f.nullType, f.doubleType, f.doubleType, true);
    // Use RelDataTypeFactory#leastRestrictive to find the common type; it's not
    // symmetric but it's ok because precision does not become lower.
    f.checkCommonType(f.floatType, f.doubleType, f.floatType, false);
    f.checkCommonType(f.floatType, f.floatType, f.floatType, true);
    f.checkCommonType(f.doubleType, f.doubleType, f.doubleType, true);
    // EXACT + FRACTIONAL
    f.checkCommonType(f.intType, f.floatType, f.floatType, true);
    f.checkCommonType(f.intType, f.doubleType, f.doubleType, true);
    f.checkCommonType(f.bigintType, f.floatType, f.floatType, true);
    f.checkCommonType(f.bigintType, f.doubleType, f.doubleType, true);
    // Fixed precision decimal
    RelDataType decimal54 =
        f.typeFactory.createSqlType(SqlTypeName.DECIMAL, 5, 4);
    RelDataType decimal71 =
        f.typeFactory.createSqlType(SqlTypeName.DECIMAL, 7, 1);
    f.checkCommonType(decimal54, decimal71, null, true);
    f.checkCommonType(decimal54, f.doubleType, null, true);
    f.checkCommonType(decimal54, f.intType, null, true);
    // CHAR/VARCHAR
    f.checkCommonType(f.nullType, f.charType, f.charType, true);
    f.checkCommonType(f.charType, f.varcharType, f.varcharType, true);
    f.checkCommonType(f.intType, f.charType, null, true);
    f.checkCommonType(f.doubleType, f.charType, null, true);
    // TIMESTAMP
    f.checkCommonType(f.nullType, f.timestampType, f.timestampType, true);
    f.checkCommonType(f.timestampType, f.timestampType, f.timestampType, true);
    f.checkCommonType(f.dateType, f.timestampType, f.timestampType, true);
    f.checkCommonType(f.intType, f.timestampType, null, true);
    f.checkCommonType(f.varcharType, f.timestampType, null, true);
    // STRUCT
    f.checkCommonType(f.nullType, f.mapType(f.intType, f.charType),
        f.mapType(f.intType, f.charType), true);
    f.checkCommonType(f.nullType, f.recordType(ImmutableList.of()),
        f.recordType(ImmutableList.of()), true);
    f.checkCommonType(f.charType, f.mapType(f.intType, f.charType), null, true);
    f.checkCommonType(f.arrayType(f.intType), f.recordType(ImmutableList.of()),
        null, true);

    f.checkCommonType(f.recordType("a", f.intType),
        f.recordType("b", f.intType), null, true);
    f.checkCommonType(f.recordType("a", f.intType),
        f.recordType("a", f.intType), f.recordType("a", f.intType), true);
    f.checkCommonType(f.recordType("a", f.arrayType(f.intType)),
        f.recordType("a", f.arrayType(f.intType)),
        f.recordType("a", f.arrayType(f.intType)), true);
  }

  /** Test case for {@link TypeCoercion#getWiderTypeForTwo}
   * and {@link TypeCoercion#getWiderTypeFor}. */
  @Test void testWiderTypeFor() {
    final Fixture f = fixture();
    // DECIMAL please see details in SqlTypeFactoryImpl#leastRestrictiveSqlType.
    f.checkWiderType(f.decimalType(5, 4), f.decimalType(7, 1),
        f.decimalType(10, 4), true, true);
    f.checkWiderType(f.decimalType(5, 4), f.doubleType, f.doubleType, true,
        true);
    f.checkWiderType(f.decimalType(5, 4), f.intType, f.decimalType(14, 4), true,
        true);
    f.checkWiderType(f.decimalType(5, 4), f.bigintType, f.decimalType(19, 0),
        true, true);
    // Array
    f.checkWiderType(f.arrayType(f.smallintType), f.arrayType(f.doubleType),
        f.arrayType(f.doubleType), true, true);
    f.checkWiderType(f.arrayType(f.timestampType), f.arrayType(f.varcharType),
        f.arrayType(f.varcharType), true, true);
    f.checkWiderType(f.arrayType(f.intType), f.arrayType(f.bigintType),
        f.arrayType(f.bigintType), true, true);
    // No string promotion
    f.checkWiderType(f.intType, f.charType, null, false, true);
    f.checkWiderType(f.timestampType, f.charType, null, false, true);
    f.checkWiderType(f.arrayType(f.bigintType), f.arrayType(f.charType), null,
        false, true);
    f.checkWiderType(f.arrayType(f.charType), f.arrayType(f.timestampType), null,
        false, true);
    // String promotion
    f.checkWiderType(f.intType, f.charType, f.varcharType, true, true);
    f.checkWiderType(f.timestampType, f.charType, f.varcharType, true, true);
    f.checkWiderType(f.arrayType(f.bigintType), f.arrayType(f.varcharType),
        f.arrayType(f.varcharType), true, true);
    f.checkWiderType(f.arrayType(f.charType), f.arrayType(f.timestampType),
        f.arrayType(f.varcharType), true, true);
  }

  /** Test set operations: UNION, INTERSECT, EXCEPT type coercion. */
  @Test void testSetOperations() {
    // union
    sql("select 1 from (values(true)) union select '2' from (values(true))")
        .type("RecordType(VARCHAR NOT NULL EXPR$0) NOT NULL");
    sql("select 1 from (values(true)) union select '2' from (values(true))"
        + "union select '3' from (values(true))")
        .type("RecordType(VARCHAR NOT NULL EXPR$0) NOT NULL");
    sql("select 1, '2' from (values(true, false)) union select '3', 4 from (values(true, false))")
        .type("RecordType(VARCHAR NOT NULL EXPR$0, VARCHAR NOT NULL EXPR$1) NOT NULL");
    sql("select '1' from (values(true)) union values 2")
        .type("RecordType(VARCHAR NOT NULL EXPR$0) NOT NULL");
    sql("select (select 1+2 from (values true)) tt from (values(true)) union values '2'")
        .type("RecordType(VARCHAR NOT NULL TT) NOT NULL");
    // union with star
    sql("select * from (values(1, '3')) union select * from (values('2', 4))")
        .type("RecordType(VARCHAR NOT NULL EXPR$0, VARCHAR NOT NULL EXPR$1) NOT NULL");
    sql("select 1 from (values(true)) union values (select '1' from (values (true)) as tt)")
        .type("RecordType(VARCHAR EXPR$0) NOT NULL");
    // union with func
    sql("select LOCALTIME from (values(true)) union values '1'")
        .type("RecordType(VARCHAR NOT NULL LOCALTIME) NOT NULL");
    sql("select t1_int, t1_decimal, t1_smallint, t1_double from t1 "
        + "union select t2_varchar20, t2_decimal, t2_float, t2_bigint from t2 "
        + "union select t1_varchar20, t1_decimal, t1_float, t1_double from t1 "
        + "union select t2_varchar20, t2_decimal, t2_smallint, t2_double from t2")
        .type("RecordType(VARCHAR NOT NULL T1_INT,"
            + " DECIMAL(19, 0) NOT NULL T1_DECIMAL,"
            + " FLOAT NOT NULL T1_SMALLINT,"
            + " DOUBLE NOT NULL T1_DOUBLE) NOT NULL");
    // (int) union (int) union (varchar(20))
    sql("select t1_int from t1 "
        + "union select t2_int from t2 "
        + "union select t1_varchar20 from t1")
        .columnType("VARCHAR NOT NULL");

    // (varchar(20)) union (int) union (int)
    sql("select t1_varchar20 from t1 "
        + "union select t2_int from t2 "
        + "union select t1_int from t1")
        .columnType("VARCHAR NOT NULL");

    // date union timestamp
    sql("select t1_date, t1_timestamp from t1\n"
        + "union select t2_timestamp, t2_date from t2")
        .type("RecordType(TIMESTAMP(0) NOT NULL T1_DATE,"
            + " TIMESTAMP(0) NOT NULL T1_TIMESTAMP) NOT NULL");

    // intersect
    sql("select t1_int, t1_decimal, t1_smallint, t1_double from t1 "
        + "intersect select t2_varchar20, t2_decimal, t2_float, t2_bigint from t2 ")
        .type("RecordType(VARCHAR NOT NULL T1_INT,"
            + " DECIMAL(19, 0) NOT NULL T1_DECIMAL,"
            + " FLOAT NOT NULL T1_SMALLINT,"
            + " DOUBLE NOT NULL T1_DOUBLE) NOT NULL");
    // except
    sql("select t1_int, t1_decimal, t1_smallint, t1_double from t1 "
        + "except select t2_varchar20, t2_decimal, t2_float, t2_bigint from t2 ")
        .type("RecordType(VARCHAR NOT NULL T1_INT,"
            + " DECIMAL(19, 0) NOT NULL T1_DECIMAL,"
            + " FLOAT NOT NULL T1_SMALLINT,"
            + " DOUBLE NOT NULL T1_DOUBLE) NOT NULL");
  }

  /** Test arithmetic expressions with string type arguments. */
  @Test void testArithmeticExpressionsWithStrings() {
    SqlValidatorFixture f = validatorFixture();
    // for null type in binary arithmetic.
    expr("1 + null").ok();
    expr("1 - null").ok();
    expr("1 / null").ok();
    expr("1 * null").ok();
    expr("MOD(1, null)").ok();

    sql("select 1+'2', 2-'3', 2*'3', 2/'3', MOD(4,'3') "
        + "from (values (true, true, true, true, true))")
        .type("RecordType(INTEGER NOT NULL EXPR$0, "
            + "INTEGER NOT NULL EXPR$1, "
            + "INTEGER NOT NULL EXPR$2, "
            + "INTEGER NOT NULL EXPR$3, "
            + "DECIMAL(19, 9) "
            + "NOT NULL EXPR$4) NOT NULL");
    expr("select abs(t1_varchar20) from t1").ok();
    expr("select sum(t1_varchar20) from t1").ok();
    expr("select avg(t1_varchar20) from t1").ok();

    f.setFor(SqlStdOperatorTable.STDDEV_POP);
    f.setFor(SqlStdOperatorTable.STDDEV_SAMP);
    expr("select STDDEV_POP(t1_varchar20) from t1").ok();
    expr("select STDDEV_SAMP(t1_varchar20) from t1").ok();
    expr("select -(t1_varchar20) from t1").ok();
    expr("select +(t1_varchar20) from t1").ok();
    f.setFor(SqlStdOperatorTable.VAR_POP);
    f.setFor(SqlStdOperatorTable.VAR_SAMP);
    expr("select VAR_POP(t1_varchar20) from t1").ok();
    expr("select VAR_SAMP(t1_varchar20) from t1").ok();
    // test divide with strings
    expr("'12.3'/5")
        .columnType("INTEGER NOT NULL");
    expr("'12.3'/cast(5 as bigint)")
        .columnType("BIGINT NOT NULL");
    expr("'12.3'/cast(5 as float)")
        .columnType("FLOAT NOT NULL");
    expr("'12.3'/cast(5 as double)")
        .columnType("DOUBLE NOT NULL");
    expr("'12.3'/5.1")
        .columnType("DECIMAL(19, 8) NOT NULL");
    expr("12.3/'5.1'")
        .columnType("DECIMAL(19, 8) NOT NULL");
    // test binary arithmetic with two strings.
    expr("'12.3' + '5'")
        .columnType("DECIMAL(19, 9) NOT NULL");
    expr("'12.3' - '5'")
        .columnType("DECIMAL(19, 9) NOT NULL");
    expr("'12.3' * '5'")
        .columnType("DECIMAL(19, 18) NOT NULL");
    expr("'12.3' / '5'")
        .columnType("DECIMAL(19, 0) NOT NULL");
  }

  /** Test cases for binary comparison expressions. */
  @Test void testBinaryComparisonCoercion() {
    expr("'2' = 3").columnType("BOOLEAN NOT NULL");
    expr("'2' > 3").columnType("BOOLEAN NOT NULL");
    expr("'2' >= 3").columnType("BOOLEAN NOT NULL");
    expr("'2' < 3").columnType("BOOLEAN NOT NULL");
    expr("'2' <= 3").columnType("BOOLEAN NOT NULL");
    expr("'2' is distinct from 3").columnType("BOOLEAN NOT NULL");
    expr("'2' is not distinct from 3").columnType("BOOLEAN NOT NULL");
    // NULL operand
    expr("'2' = null").columnType("BOOLEAN");
    expr("'2' > null").columnType("BOOLEAN");
    expr("'2' >= null").columnType("BOOLEAN");
    expr("'2' < null").columnType("BOOLEAN");
    expr("'2' <= null").columnType("BOOLEAN");
    expr("'2' is distinct from null").columnType("BOOLEAN NOT NULL");
    expr("'2' is not distinct from null").columnType("BOOLEAN NOT NULL");
    // BETWEEN operator
    expr("'2' between 1 and 3").columnType("BOOLEAN NOT NULL");
    expr("NULL between 1 and 3").columnType("BOOLEAN");
    sql("select '2019-09-23' between t1_date and t1_timestamp from t1")
        .columnType("BOOLEAN NOT NULL");
    sql("select t1_date between '2019-09-23' and t1_timestamp from t1")
        .columnType("BOOLEAN NOT NULL");
    sql("select cast('2019-09-23' as date) between t1_date and t1_timestamp from t1")
        .columnType("BOOLEAN NOT NULL");
    sql("select t1_date between cast('2019-09-23' as date) and t1_timestamp from t1")
        .columnType("BOOLEAN NOT NULL");
  }

  /** Test case for case when expression and COALESCE operator. */
  @Test void testCaseWhen() {
    // coalesce
    // double int float
    sql("select COALESCE(t1_double, t1_int, t1_float) from t1")
        .type("RecordType(DOUBLE NOT NULL EXPR$0) NOT NULL");
    // bigint int decimal
    sql("select COALESCE(t1_bigint, t1_int, t1_decimal) from t1")
        .type("RecordType(DECIMAL(19, 0) NOT NULL EXPR$0) NOT NULL");
    // null int
    sql("select COALESCE(null, t1_int) from t1")
        .type("RecordType(INTEGER EXPR$0) NOT NULL");
    // timestamp varchar
    sql("select COALESCE(t1_varchar20, t1_timestamp) from t1")
        .type("RecordType(VARCHAR NOT NULL EXPR$0) NOT NULL");
    // null float int
    sql("select COALESCE(null, t1_float, t1_int) from t1")
        .type("RecordType(FLOAT EXPR$0) NOT NULL");
    // null int decimal double
    sql("select COALESCE(null, t1_int, t1_decimal, t1_double) from t1")
        .type("RecordType(DOUBLE EXPR$0) NOT NULL");
    // null float double varchar
    sql("select COALESCE(null, t1_float, t1_double, t1_varchar20) from t1")
        .type("RecordType(VARCHAR EXPR$0) NOT NULL");
    // timestamp int varchar
    sql("select COALESCE(t1_timestamp, t1_int, t1_varchar20) from t1")
        .type("RecordType(TIMESTAMP(0) NOT NULL EXPR$0) NOT NULL");
    // timestamp date
    sql("select COALESCE(t1_timestamp, t1_date) from t1")
        .type("RecordType(TIMESTAMP(0) NOT NULL EXPR$0) NOT NULL");
    // date timestamp
    sql("select COALESCE(t1_timestamp, t1_date) from t1")
        .type("RecordType(TIMESTAMP(0) NOT NULL EXPR$0) NOT NULL");
    // null date timestamp
    sql("select COALESCE(t1_timestamp, t1_date) from t1")
        .type("RecordType(TIMESTAMP(0) NOT NULL EXPR$0) NOT NULL");

    // case when
    // smallint int char
    sql("select case "
        + "when 1 > 0 then t2_smallint "
        + "when 2 > 3 then t2_int "
        + "else t2_varchar20 end from t2")
        .type("RecordType(VARCHAR NOT NULL EXPR$0) NOT NULL");
    // boolean int char
    sql("select case "
        + "when 1 > 0 then t2_boolean "
        + "when 2 > 3 then t2_int "
        + "else t2_varchar20 end from t2")
        .type("RecordType(VARCHAR NOT NULL EXPR$0) NOT NULL");
    // float decimal
    sql("select case when 1 > 0 then t2_float else t2_decimal end from t2")
        .type("RecordType(DOUBLE NOT NULL EXPR$0) NOT NULL");
    // bigint decimal
    sql("select case when 1 > 0 then t2_bigint else t2_decimal end from t2")
        .type("RecordType(DECIMAL(19, 0) NOT NULL EXPR$0) NOT NULL");
    // date timestamp
    sql("select case when 1 > 0 then t2_date else t2_timestamp end from t2")
        .type("RecordType(TIMESTAMP(0) NOT NULL EXPR$0) NOT NULL");
  }

  /** Test for {@link AbstractTypeCoercion#implicitCast}. */
  @Test void testImplicitCasts() {
    final Fixture f = fixture();
    // TINYINT
    ImmutableList<RelDataType> charTypes = f.charTypes;
    RelDataType checkedType1 = f.typeFactory.createSqlType(SqlTypeName.TINYINT);
    f.checkShouldCast(checkedType1, combine(f.numericTypes, charTypes));
    f.shouldCast(checkedType1, SqlTypeFamily.DECIMAL,
        f.typeFactory.decimalOf(checkedType1));
    f.shouldCast(checkedType1, SqlTypeFamily.NUMERIC, checkedType1);
    f.shouldCast(checkedType1, SqlTypeFamily.INTEGER, checkedType1);
    f.shouldCast(checkedType1, SqlTypeFamily.EXACT_NUMERIC, checkedType1);
    f.shouldNotCast(checkedType1, SqlTypeFamily.APPROXIMATE_NUMERIC);

    // SMALLINT
    RelDataType checkedType2 = f.smallintType;
    f.checkShouldCast(checkedType2, combine(f.numericTypes, charTypes));
    f.shouldCast(checkedType2, SqlTypeFamily.DECIMAL,
        f.typeFactory.decimalOf(checkedType2));
    f.shouldCast(checkedType2, SqlTypeFamily.NUMERIC, checkedType2);
    f.shouldCast(checkedType2, SqlTypeFamily.INTEGER, checkedType2);
    f.shouldCast(checkedType2, SqlTypeFamily.EXACT_NUMERIC, checkedType2);
    f.shouldNotCast(checkedType2, SqlTypeFamily.APPROXIMATE_NUMERIC);

    // INT
    RelDataType checkedType3 = f.intType;
    f.checkShouldCast(checkedType3, combine(f.numericTypes, charTypes));
    f.shouldCast(checkedType3, SqlTypeFamily.DECIMAL,
        f.typeFactory.decimalOf(checkedType3));
    f.shouldCast(checkedType3, SqlTypeFamily.NUMERIC, checkedType3);
    f.shouldCast(checkedType3, SqlTypeFamily.INTEGER, checkedType3);
    f.shouldCast(checkedType3, SqlTypeFamily.EXACT_NUMERIC, checkedType3);
    f.shouldNotCast(checkedType3, SqlTypeFamily.APPROXIMATE_NUMERIC);

    // BIGINT
    RelDataType checkedType4 = f.bigintType;
    f.checkShouldCast(checkedType4, combine(f.numericTypes, charTypes));
    f.shouldCast(checkedType4, SqlTypeFamily.DECIMAL,
        f.typeFactory.decimalOf(checkedType4));
    f.shouldCast(checkedType4, SqlTypeFamily.NUMERIC, checkedType4);
    f.shouldCast(checkedType4, SqlTypeFamily.INTEGER, checkedType4);
    f.shouldCast(checkedType4, SqlTypeFamily.EXACT_NUMERIC, checkedType4);
    f.shouldNotCast(checkedType4, SqlTypeFamily.APPROXIMATE_NUMERIC);

    // FLOAT/REAL
    RelDataType checkedType5 = f.floatType;
    f.checkShouldCast(checkedType5, combine(f.numericTypes, charTypes));
    f.shouldCast(checkedType5, SqlTypeFamily.DECIMAL,
        f.typeFactory.decimalOf(checkedType5));
    f.shouldCast(checkedType5, SqlTypeFamily.NUMERIC, checkedType5);
    f.shouldNotCast(checkedType5, SqlTypeFamily.INTEGER);
    f.shouldCast(checkedType5, SqlTypeFamily.EXACT_NUMERIC,
        f.typeFactory.decimalOf(checkedType5));
    f.shouldCast(checkedType5, SqlTypeFamily.APPROXIMATE_NUMERIC, checkedType5);

    // DOUBLE
    RelDataType checkedType6 = f.doubleType;
    f.checkShouldCast(checkedType6, combine(f.numericTypes, charTypes));
    f.shouldCast(checkedType6, SqlTypeFamily.DECIMAL,
        f.typeFactory.decimalOf(checkedType6));
    f.shouldCast(checkedType6, SqlTypeFamily.NUMERIC, checkedType6);
    f.shouldNotCast(checkedType6, SqlTypeFamily.INTEGER);
    f.shouldCast(checkedType6, SqlTypeFamily.EXACT_NUMERIC,
        f.typeFactory.decimalOf(checkedType5));
    f.shouldCast(checkedType6, SqlTypeFamily.APPROXIMATE_NUMERIC, checkedType6);

    // DECIMAL(10, 2)
    RelDataType checkedType7 = f.decimalType(10, 2);
    f.checkShouldCast(checkedType7, combine(f.numericTypes, charTypes));
    f.shouldCast(checkedType7, SqlTypeFamily.DECIMAL,
        f.typeFactory.decimalOf(checkedType7));
    f.shouldCast(checkedType7, SqlTypeFamily.NUMERIC, checkedType7);
    f.shouldNotCast(checkedType7, SqlTypeFamily.INTEGER);
    f.shouldCast(checkedType7, SqlTypeFamily.EXACT_NUMERIC, checkedType7);
    f.shouldNotCast(checkedType7, SqlTypeFamily.APPROXIMATE_NUMERIC);

    // BINARY
    RelDataType checkedType8 = f.binaryType;
    f.checkShouldCast(checkedType8, combine(f.binaryTypes, charTypes));
    f.shouldNotCast(checkedType8, SqlTypeFamily.DECIMAL);
    f.shouldNotCast(checkedType8, SqlTypeFamily.NUMERIC);
    f.shouldNotCast(checkedType8, SqlTypeFamily.INTEGER);

    // BOOLEAN
    RelDataType checkedType9 = f.booleanType;
    f.checkShouldCast(checkedType9, combine(f.booleanTypes, charTypes));
    f.shouldNotCast(checkedType9, SqlTypeFamily.DECIMAL);
    f.shouldNotCast(checkedType9, SqlTypeFamily.NUMERIC);
    f.shouldNotCast(checkedType9, SqlTypeFamily.INTEGER);

    // CHARACTER
    RelDataType checkedType10 = f.varcharType;
    ImmutableList.Builder<RelDataType> builder = ImmutableList.builder();
    for (RelDataType type : f.atomicTypes) {
      if (!SqlTypeUtil.isBoolean(type)) {
        builder.add(type);
      }
    }
    builder.addAll(f.geometryTypes);
    f.checkShouldCast(checkedType10, builder.build());
    f.shouldCast(checkedType10, SqlTypeFamily.DECIMAL,
        SqlTypeUtil.getMaxPrecisionScaleDecimal(f.typeFactory));
    f.shouldCast(checkedType10, SqlTypeFamily.NUMERIC,
        SqlTypeUtil.getMaxPrecisionScaleDecimal(f.typeFactory));
    f.shouldNotCast(checkedType10, SqlTypeFamily.BOOLEAN);

    // DATE
    RelDataType checkedType11 = f.dateType;
    f.checkShouldCast(
        checkedType11,
        combine(ImmutableList.of(f.timestampType, checkedType11),
            charTypes));
    f.shouldNotCast(checkedType11, SqlTypeFamily.DECIMAL);
    f.shouldNotCast(checkedType11, SqlTypeFamily.NUMERIC);
    f.shouldNotCast(checkedType11, SqlTypeFamily.INTEGER);

    // TIME
    RelDataType checkedType12 = f.timeType;
    f.checkShouldCast(
        checkedType12,
        combine(ImmutableList.of(checkedType12), charTypes));
    f.shouldNotCast(checkedType12, SqlTypeFamily.DECIMAL);
    f.shouldNotCast(checkedType12, SqlTypeFamily.NUMERIC);
    f.shouldNotCast(checkedType12, SqlTypeFamily.INTEGER);

    // TIMESTAMP
    RelDataType checkedType13 = f.timestampType;
    f.checkShouldCast(
        checkedType13,
        combine(ImmutableList.of(f.dateType, checkedType13),
            charTypes));
    f.shouldNotCast(checkedType13, SqlTypeFamily.DECIMAL);
    f.shouldNotCast(checkedType13, SqlTypeFamily.NUMERIC);
    f.shouldNotCast(checkedType13, SqlTypeFamily.INTEGER);

    // NULL
    RelDataType checkedType14 = f.nullType;
    f.checkShouldCast(checkedType14, f.allTypes);
    f.shouldCast(checkedType14, SqlTypeFamily.DECIMAL, f.decimalType);
    f.shouldCast(checkedType14, SqlTypeFamily.NUMERIC, f.intType);

    // INTERVAL
    RelDataType checkedType15 = f.typeFactory.createSqlIntervalType(
        new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO));
    f.checkShouldCast(checkedType15, ImmutableList.of(checkedType15));
    f.shouldNotCast(checkedType15, SqlTypeFamily.DECIMAL);
    f.shouldNotCast(checkedType15, SqlTypeFamily.NUMERIC);
    f.shouldNotCast(checkedType15, SqlTypeFamily.INTEGER);
  }

  /** Test case for {@link TypeCoercion#builtinFunctionCoercion}. */
  @Test void testBuiltinFunctionCoercion() {
    // concat
    expr("'ab'||'cde'")
        .columnType("CHAR(5) NOT NULL");
    expr("null||'cde'")
        .columnType("VARCHAR");
    expr("1||'234'")
        .columnType("VARCHAR NOT NULL");
    expr("select ^'a'||t1_binary^ from t1")
        .fails("(?s).*Cannot apply.*");
    // smallint int double
    expr("select t1_smallint||t1_int||t1_double from t1")
        .columnType("VARCHAR");
    // boolean float smallint
    expr("select t1_boolean||t1_float||t1_smallint from t1")
        .columnType("VARCHAR");
    // decimal
    expr("select t1_decimal||t1_varchar20 from t1")
        .columnType("VARCHAR");
    // date timestamp
    expr("select t1_timestamp||t1_date from t1")
        .columnType("VARCHAR");
  }

  /** Test case for {@link TypeCoercion#querySourceCoercion}. */
  @Test void testQuerySourceCoercion() {
    final String expectRowType = "RecordType("
        + "VARCHAR(20) NOT NULL t1_varchar20, "
        + "SMALLINT NOT NULL t1_smallint, "
        + "INTEGER NOT NULL t1_int, "
        + "BIGINT NOT NULL t1_bigint, "
        + "FLOAT NOT NULL t1_float, "
        + "DOUBLE NOT NULL t1_double, "
        + "DECIMAL(19, 0) NOT NULL t1_decimal, "
        + "TIMESTAMP(0) NOT NULL t1_timestamp, "
        + "DATE NOT NULL t1_date, "
        + "BINARY(1) NOT NULL t1_binary, "
        + "BOOLEAN NOT NULL t1_boolean) NOT NULL";

    final String sql = "insert into t1 select t2_smallint, t2_int, t2_bigint, t2_float,\n"
        + "t2_double, t2_decimal, t2_int, t2_date, t2_timestamp, t2_varchar20, t2_int from t2";
    sql(sql).type(expectRowType);

    final String sql1 = "insert into ^t1^(t1_varchar20, t1_date, t1_int)\n"
        + "select t2_smallint, t2_timestamp, t2_float from t2";
    sql(sql1).fails("(?s).*Column 't1_smallint' has no default value and does not allow NULLs.*");

    final String sql2 = "update t1 set t1_varchar20=123, "
        + "t1_date=TIMESTAMP '2020-01-03 10:14:34', t1_int=12.3";
    sql(sql2).type(expectRowType);
  }

  //~ Inner Class ------------------------------------------------------------

  /** Everything you need to run a test. */
  static class Fixture {
    final TypeCoercion typeCoercion;
    final RelDataTypeFactory typeFactory;

    // type category.
    final ImmutableList<RelDataType> numericTypes;
    final ImmutableList<RelDataType> atomicTypes;
    final ImmutableList<RelDataType> allTypes;
    final ImmutableList<RelDataType> charTypes;
    final ImmutableList<RelDataType> binaryTypes;
    final ImmutableList<RelDataType> booleanTypes;
    final ImmutableList<RelDataType> geometryTypes;

    // single types
    final RelDataType nullType;
    final RelDataType booleanType;
    final RelDataType tinyintType;
    final RelDataType smallintType;
    final RelDataType intType;
    final RelDataType bigintType;
    final RelDataType floatType;
    final RelDataType doubleType;
    final RelDataType decimalType;
    final RelDataType dateType;
    final RelDataType timeType;
    final RelDataType timestampType;
    final RelDataType binaryType;
    final RelDataType varbinaryType;
    final RelDataType charType;
    final RelDataType varcharType;
    final RelDataType varchar20Type;
    final RelDataType geometryType;

    /** Creates a Fixture. */
    public static Fixture create(SqlTestFactory testFactory) {
      final SqlValidator validator = testFactory.createValidator();
      return new Fixture(validator.getTypeFactory(), validator.getTypeCoercion());
    }

    protected Fixture(RelDataTypeFactory typeFactory,
        TypeCoercion typeCoercion) {
      this.typeFactory = typeFactory;
      this.typeCoercion = typeCoercion;

      // Initialize single types
      nullType = this.typeFactory.createSqlType(SqlTypeName.NULL);
      booleanType = this.typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      tinyintType = this.typeFactory.createSqlType(SqlTypeName.TINYINT);
      smallintType = this.typeFactory.createSqlType(SqlTypeName.SMALLINT);
      intType = this.typeFactory.createSqlType(SqlTypeName.INTEGER);
      bigintType = this.typeFactory.createSqlType(SqlTypeName.BIGINT);
      floatType = this.typeFactory.createSqlType(SqlTypeName.FLOAT);
      doubleType = this.typeFactory.createSqlType(SqlTypeName.DOUBLE);
      decimalType = this.typeFactory.createSqlType(SqlTypeName.DECIMAL);
      dateType = this.typeFactory.createSqlType(SqlTypeName.DATE);
      timeType = this.typeFactory.createSqlType(SqlTypeName.TIME);
      timestampType = this.typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      binaryType = this.typeFactory.createSqlType(SqlTypeName.BINARY);
      varbinaryType = this.typeFactory.createSqlType(SqlTypeName.VARBINARY);
      charType = this.typeFactory.createSqlType(SqlTypeName.CHAR);
      varcharType = this.typeFactory.createSqlType(SqlTypeName.VARCHAR);
      varchar20Type = this.typeFactory.createSqlType(SqlTypeName.VARCHAR, 20);
      geometryType = this.typeFactory.createSqlType(SqlTypeName.GEOMETRY);

      // Initialize category types

      // INT
      ImmutableList.Builder<RelDataType> builder = ImmutableList.builder();
      for (SqlTypeName typeName : SqlTypeName.INT_TYPES) {
        builder.add(this.typeFactory.createSqlType(typeName));
      }
      numericTypes = builder.build();
      // ATOMIC
      ImmutableList.Builder<RelDataType> builder3 = ImmutableList.builder();
      for (SqlTypeName typeName : SqlTypeName.DATETIME_TYPES) {
        builder3.add(this.typeFactory.createSqlType(typeName));
      }
      builder3.addAll(numericTypes);
      for (SqlTypeName typeName : SqlTypeName.STRING_TYPES) {
        builder3.add(this.typeFactory.createSqlType(typeName));
      }
      for (SqlTypeName typeName : SqlTypeName.BOOLEAN_TYPES) {
        builder3.add(this.typeFactory.createSqlType(typeName));
      }
      for (SqlTypeName typeName : SqlTypeName.GEOMETRY_TYPES) {
        builder3.add(this.typeFactory.createSqlType(typeName));
      }
      atomicTypes = builder3.build();
      // COMPLEX
      ImmutableList.Builder<RelDataType> builder4 = ImmutableList.builder();
      builder4.add(this.typeFactory.createArrayType(intType, -1));
      builder4.add(this.typeFactory.createArrayType(varcharType, -1));
      builder4.add(this.typeFactory.createMapType(varcharType, varcharType));
      builder4.add(this.typeFactory.createStructType(ImmutableList.of(Pair.of("a1", varcharType))));
      List<? extends Map.Entry<String, RelDataType>> ll =
          ImmutableList.of(Pair.of("a1", varbinaryType), Pair.of("a2", intType));
      builder4.add(this.typeFactory.createStructType(ll));
      ImmutableList<RelDataType> complexTypes = builder4.build();
      // ALL
      SqlIntervalQualifier intervalQualifier =
          new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.MINUTE, SqlParserPos.ZERO);
      allTypes = combine(atomicTypes, complexTypes,
          ImmutableList.of(nullType, this.typeFactory.createSqlIntervalType(intervalQualifier)));

      // CHARACTERS
      ImmutableList.Builder<RelDataType> builder6 = ImmutableList.builder();
      for (SqlTypeName typeName : SqlTypeName.CHAR_TYPES) {
        builder6.add(this.typeFactory.createSqlType(typeName));
      }
      charTypes = builder6.build();
      // BINARY
      ImmutableList.Builder<RelDataType> builder7 = ImmutableList.builder();
      for (SqlTypeName typeName : SqlTypeName.BINARY_TYPES) {
        builder7.add(this.typeFactory.createSqlType(typeName));
      }
      binaryTypes = builder7.build();
      // BOOLEAN
      ImmutableList.Builder<RelDataType> builder8 = ImmutableList.builder();
      for (SqlTypeName typeName : SqlTypeName.BOOLEAN_TYPES) {
        builder8.add(this.typeFactory.createSqlType(typeName));
      }
      booleanTypes = builder8.build();
      // GEOMETRY
      ImmutableList.Builder<RelDataType> builder9 = ImmutableList.builder();
      for (SqlTypeName typeName : SqlTypeName.GEOMETRY_TYPES) {
        builder9.add(this.typeFactory.createSqlType(typeName));
      }
      geometryTypes = builder9.build();
    }

    public Fixture withTypeFactory(RelDataTypeFactory typeFactory) {
      return new Fixture(typeFactory, typeCoercion);
    }

    //~ Tool methods -----------------------------------------------------------

    RelDataType arrayType(RelDataType type) {
      return typeFactory.createArrayType(type, -1);
    }

    RelDataType mapType(RelDataType keyType, RelDataType valType) {
      return typeFactory.createMapType(keyType, valType);
    }

    RelDataType recordType(String name, RelDataType type) {
      return typeFactory.createStructType(ImmutableList.of(Pair.of(name, type)));
    }

    RelDataType recordType(List<? extends Map.Entry<String, RelDataType>> pairs) {
      return typeFactory.createStructType(pairs);
    }

    RelDataType decimalType(int precision, int scale) {
      return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
    }

    /** Decision method for {@link AbstractTypeCoercion#implicitCast}. */
    private void shouldCast(
        RelDataType from,
        SqlTypeFamily family,
        RelDataType expected) {
      if (family == null) {
        // ROW type do not have a family.
        return;
      }
      RelDataType castedType =
          ((AbstractTypeCoercion) typeCoercion).implicitCast(from, family);
      String reason = "Failed to cast from " + from.getSqlTypeName()
          + " to " + family;
      assertThat(reason, castedType, notNullValue());
      assertThat(reason,
          from.equals(castedType)
              || SqlTypeUtil.equalSansNullability(typeFactory, castedType, expected)
              || expected.getSqlTypeName().getFamily().contains(castedType),
          is(true));
    }

    private void shouldNotCast(
        RelDataType from,
        SqlTypeFamily family) {
      if (family == null) {
        // ROW type do not have a family.
        return;
      }
      RelDataType castedType =
          ((AbstractTypeCoercion) typeCoercion).implicitCast(from, family);
      assertThat("Should not be able to cast from " + from.getSqlTypeName()
          + " to " + family,
          castedType, nullValue());
    }

    private void checkShouldCast(RelDataType checked, List<RelDataType> types) {
      for (RelDataType type : allTypes) {
        if (contains(types, type)) {
          shouldCast(checked, type.getSqlTypeName().getFamily(), type);
        } else {
          shouldNotCast(checked, type.getSqlTypeName().getFamily());
        }
      }
    }

    // some data types has the same type family, i.e. TIMESTAMP and
    // TIMESTAMP_WITH_LOCAL_TIME_ZONE all have TIMESTAMP family.
    private static boolean contains(List<RelDataType> types, RelDataType type) {
      for (RelDataType type1 : types) {
        if (type1.equals(type)
            || type1.getSqlTypeName().getFamily() == type.getSqlTypeName().getFamily()) {
          return true;
        }
      }
      return false;
    }

    private String toStringNullable(Object o1) {
      if (o1 == null) {
        return "NULL";
      }
      return o1.toString();
    }

    /** Decision method for finding a common type. */
    private void checkCommonType(
        RelDataType type1,
        RelDataType type2,
        RelDataType expected,
        boolean isSymmetric) {
      RelDataType result = typeCoercion.getTightestCommonType(type1, type2);
      assertThat("Expected " + toStringNullable(expected)
          + " as common type for " + type1.toString()
          + " and " + type2.toString()
          + ", but found " + toStringNullable(result),
          result,
          sameInstance(expected));
      if (isSymmetric) {
        RelDataType result1 = typeCoercion.getTightestCommonType(type2, type1);
        assertThat("Expected " + toStringNullable(expected)
            + " as common type for " + type2
            + " and " + type1
            + ", but found " + toStringNullable(result1),
            result1, sameInstance(expected));
      }
    }

    /** Decision method for finding a wider type. */
    private void checkWiderType(
        RelDataType type1,
        RelDataType type2,
        RelDataType expected,
        boolean stringPromotion,
        boolean symmetric) {
      RelDataType result =
          typeCoercion.getWiderTypeForTwo(type1, type2, stringPromotion);
      assertThat("Expected "
          + toStringNullable(expected)
          + " as common type for " + type1.toString()
          + " and " + type2.toString()
          + ", but found " + toStringNullable(result),
          result, sameInstance(expected));
      if (symmetric) {
        RelDataType result1 =
            typeCoercion.getWiderTypeForTwo(type2, type1, stringPromotion);
        assertThat("Expected " + toStringNullable(expected)
            + " as common type for " + type2
            + " and " + type1
            + ", but found " + toStringNullable(result1),
            result1, sameInstance(expected));
      }
    }
  }
}
