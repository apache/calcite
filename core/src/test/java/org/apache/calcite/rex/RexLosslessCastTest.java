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
package org.apache.calcite.rex;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link org.apache.calcite.rex.RexUtil#isLosslessCast(RexNode)} and related cases.
 */
class RexLosslessCastTest extends RexProgramTestBase {
  /** Unit test for {@link org.apache.calcite.rex.RexUtil#isLosslessCast(RexNode)}. */
  @Test void testLosslessCast() {
    final RelDataType tinyIntType = typeFactory.createSqlType(SqlTypeName.TINYINT);
    final RelDataType smallIntType = typeFactory.createSqlType(SqlTypeName.SMALLINT);
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType bigIntType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    final RelDataType floatType = typeFactory.createSqlType(SqlTypeName.FLOAT);
    final RelDataType booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

    final RelDataType charType5 = typeFactory.createSqlType(SqlTypeName.CHAR, 5);
    final RelDataType charType6 = typeFactory.createSqlType(SqlTypeName.CHAR, 6);

    final RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    final RelDataType varCharType10 = typeFactory.createSqlType(SqlTypeName.VARCHAR, 10);
    final RelDataType varCharType11 = typeFactory.createSqlType(SqlTypeName.VARCHAR, 11);

    // Negative
    assertThat(RexUtil.isLosslessCast(rexBuilder.makeInputRef(intType, 0)), is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                tinyIntType, rexBuilder.makeInputRef(smallIntType, 0))), is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                smallIntType, rexBuilder.makeInputRef(intType, 0))), is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                intType, rexBuilder.makeInputRef(bigIntType, 0))), is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                bigIntType, rexBuilder.makeInputRef(floatType, 0))), is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                booleanType, rexBuilder.makeInputRef(bigIntType, 0))), is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                intType, rexBuilder.makeInputRef(charType5, 0))), is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                intType, rexBuilder.makeInputRef(varCharType10, 0))), is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                varCharType10, rexBuilder.makeInputRef(varCharType11, 0))), is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                charType5, rexBuilder.makeInputRef(bigIntType, 0))), is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                charType5, rexBuilder.makeInputRef(smallIntType, 0))), is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                varCharType10, rexBuilder.makeInputRef(intType, 0))), is(false));

    // Positive
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                smallIntType, rexBuilder.makeInputRef(tinyIntType, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                intType, rexBuilder.makeInputRef(smallIntType, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                bigIntType, rexBuilder.makeInputRef(intType, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                intType, rexBuilder.makeInputRef(intType, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                charType6, rexBuilder.makeInputRef(smallIntType, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                varCharType10, rexBuilder.makeInputRef(smallIntType, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                varCharType11, rexBuilder.makeInputRef(intType, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                varCharType11, rexBuilder.makeInputRef(charType6, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                varCharType11, rexBuilder.makeInputRef(varCharType10, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
                varcharType, rexBuilder.makeInputRef(intType, 0))), is(true));
  }

  @Test void testLosslessCastIntegerToApproximate() {
    final RelDataType tinyIntType = typeFactory.createSqlType(SqlTypeName.TINYINT);
    final RelDataType smallIntType = typeFactory.createSqlType(SqlTypeName.SMALLINT);
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType bigIntType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    final RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
    final RelDataType realType = typeFactory.createSqlType(SqlTypeName.REAL);

    // Positive: tiny/small/int -> double
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(doubleType,
        rexBuilder.makeInputRef(tinyIntType, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(doubleType,
        rexBuilder.makeInputRef(smallIntType, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(doubleType,
        rexBuilder.makeInputRef(intType, 0))), is(true));

    // Negative: bigint -> double is not guaranteed to be exact
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(doubleType,
        rexBuilder.makeInputRef(bigIntType, 0))), is(false));

    // Positive: tiny/small -> real
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(realType,
        rexBuilder.makeInputRef(tinyIntType, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(realType,
        rexBuilder.makeInputRef(smallIntType, 0))), is(true));

    // Negative: int/bigint -> real not guaranteed exact
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(realType,
        rexBuilder.makeInputRef(intType, 0))), is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(realType,
        rexBuilder.makeInputRef(bigIntType, 0))), is(false));
  }

  @Test void testLosslessCastIntegerToDecimal() {
    final RelDataType tinyIntType = typeFactory.createSqlType(SqlTypeName.TINYINT);
    final RelDataType smallIntType = typeFactory.createSqlType(SqlTypeName.SMALLINT);
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType bigIntType = typeFactory.createSqlType(SqlTypeName.BIGINT);

    final RelDataType decPrec3Scale0  = typeFactory.createSqlType(SqlTypeName.DECIMAL, 3, 0);
    final RelDataType decPrec5Scale0  = typeFactory.createSqlType(SqlTypeName.DECIMAL, 5, 0);
    final RelDataType decPrec9Scale0  = typeFactory.createSqlType(SqlTypeName.DECIMAL, 9, 0);
    final RelDataType decPrec10Scale0 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0);
    final RelDataType decPrec18Scale0 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 18, 0);
    final RelDataType decPrec19Scale0 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 19, 0);
    final RelDataType decPrec4Scale1  = typeFactory.createSqlType(SqlTypeName.DECIMAL, 4, 1);
    final RelDataType decPrec5Scale1  = typeFactory.createSqlType(SqlTypeName.DECIMAL, 5, 1);
    final RelDataType decPrec12Scale2 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 12, 2);

    // Positive: integer digits "precision - scale" is large enough
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(decPrec3Scale0,
        rexBuilder.makeInputRef(tinyIntType, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(decPrec5Scale0,
        rexBuilder.makeInputRef(smallIntType, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(decPrec10Scale0,
        rexBuilder.makeInputRef(intType, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(decPrec19Scale0,
        rexBuilder.makeInputRef(bigIntType, 0))), is(true));

    // Negative: not enough integer digits
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(decPrec9Scale0,
        rexBuilder.makeInputRef(intType, 0))), is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(decPrec18Scale0,
        rexBuilder.makeInputRef(bigIntType, 0))), is(false));

    // Non-zero scale is still lossless if "precision - scale" covers integer digits
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(decPrec12Scale2,
        rexBuilder.makeInputRef(intType, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(decPrec4Scale1,
        rexBuilder.makeInputRef(smallIntType, 0))), is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(decPrec5Scale1,
        rexBuilder.makeInputRef(tinyIntType, 0))), is(true));
  }

  @Test void testLosslessCastUnsignedAndSignedIntegers() {
    final RelDataType uTinyIntType = typeFactory.createSqlType(SqlTypeName.UTINYINT);
    final RelDataType uIntType = typeFactory.createSqlType(SqlTypeName.UINTEGER);
    final RelDataType uBigIntType = typeFactory.createSqlType(SqlTypeName.UBIGINT);

    final RelDataType tinyIntType = typeFactory.createSqlType(SqlTypeName.TINYINT);
    final RelDataType smallIntType = typeFactory.createSqlType(SqlTypeName.SMALLINT);
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType bigIntType = typeFactory.createSqlType(SqlTypeName.BIGINT);

    // unsigned -> signed of same width should be considered lossy
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(tinyIntType, rexBuilder.makeInputRef(uTinyIntType, 0))),
        is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(intType, rexBuilder.makeInputRef(uIntType, 0))),
        is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(bigIntType, rexBuilder.makeInputRef(uBigIntType, 0))),
        is(false));

    // unsigned -> wider signed is safe
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(smallIntType, rexBuilder.makeInputRef(uTinyIntType, 0))),
        is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(bigIntType, rexBuilder.makeInputRef(uIntType, 0))),
        is(true));

    // signed -> unsigned stays conservative for now
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(uIntType, rexBuilder.makeInputRef(intType, 0))),
        is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(uBigIntType, rexBuilder.makeInputRef(bigIntType, 0))),
        is(false));

    final RelDataType dec9 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 9, 0);
    final RelDataType dec10 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0);
    final RelDataType dec10Scale1 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 1);
    final RelDataType dec19 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 19, 0);

    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(dec10, rexBuilder.makeInputRef(uIntType, 0))),
        is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(dec9, rexBuilder.makeInputRef(uIntType, 0))),
        is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(uBigIntType, rexBuilder.makeInputRef(dec19, 0))),
        is(false));

    // DECIMAL -> integer: requires scale 0 and fitting range
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(intType, rexBuilder.makeInputRef(dec9, 0))),
        is(true));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(intType, rexBuilder.makeInputRef(dec10, 0))),
        is(false));
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(intType, rexBuilder.makeInputRef(dec10Scale1, 0))),
        is(false));
  }

  @Test void testLosslessCastDecimalToApproximate() {
    final RelDataType realType   = typeFactory.createSqlType(SqlTypeName.REAL);
    final RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

    // DECIMAL(s = 0) -> APPROX: lossless iff precision <= target digits
    final RelDataType dec7_0 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 7, 0);
    final RelDataType dec8_0 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 8, 0);
    final RelDataType dec15_0 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 15, 0);
    final RelDataType dec16_0 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 16, 0);

    // real: 7 digits in default type system
    assertThat(
        RexUtil.isLosslessCast(
        rexBuilder.makeCast(realType, rexBuilder.makeInputRef(dec7_0, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
        rexBuilder.makeCast(realType, rexBuilder.makeInputRef(dec8_0, 0))), is(false));

    // double: 15 digits in default type system
    assertThat(
        RexUtil.isLosslessCast(
        rexBuilder.makeCast(doubleType, rexBuilder.makeInputRef(dec15_0, 0))), is(true));
    assertThat(
        RexUtil.isLosslessCast(
        rexBuilder.makeCast(doubleType, rexBuilder.makeInputRef(dec16_0, 0))), is(false));

    // DECIMAL with scale > 0 -> APPROX: conservative false
    final RelDataType dec10_2 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2);
    assertThat(
        RexUtil.isLosslessCast(
        rexBuilder.makeCast(doubleType, rexBuilder.makeInputRef(dec10_2, 0))), is(false));
  }

  @Test void testLosslessCastApproximateToApproximate() {
    final RelDataType realType = typeFactory.createSqlType(SqlTypeName.REAL);
    final RelDataType floatType = typeFactory.createSqlType(SqlTypeName.FLOAT);
    final RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

    // real -> double: target has >= digits
    assertThat(
        RexUtil.isLosslessCast(
        rexBuilder.makeCast(doubleType, rexBuilder.makeInputRef(realType, 0))), is(true));
    // double -> real: target has fewer digits
    assertThat(
        RexUtil.isLosslessCast(
        rexBuilder.makeCast(realType, rexBuilder.makeInputRef(doubleType, 0))), is(false));
    // real -> float: under default type system, float has >= digits than real
    assertThat(
        RexUtil.isLosslessCast(
        rexBuilder.makeCast(floatType, rexBuilder.makeInputRef(realType, 0))), is(true));
  }

  @Test void testLosslessCastApproximateToExact() {
    final RelDataType realType = typeFactory.createSqlType(SqlTypeName.REAL);
    final RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType dec19_0 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 19, 0);

    // approx -> exact is conservative false
    assertThat(
        RexUtil.isLosslessCast(
        rexBuilder.makeCast(intType, rexBuilder.makeInputRef(realType, 0))), is(false));
    assertThat(
        RexUtil.isLosslessCast(
        rexBuilder.makeCast(dec19_0, rexBuilder.makeInputRef(doubleType, 0))), is(false));
  }

  @Test void testLosslessCastWithCustomTypeSystem() {
    final RelDataType dec10_0 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0);
    final RelDataType decDefault = typeFactory.createSqlType(SqlTypeName.DECIMAL);
    assertThat(
        RexUtil.isLosslessCast(
            rexBuilder.makeCast(
        dec10_0, rexBuilder.makeInputRef(decDefault, 0))), is(false));

    RelDataTypeSystem customTypeSystem = new RelDataTypeSystemImpl() {
      @Override public int getDefaultPrecision(SqlTypeName typeName) {
        if (typeName == SqlTypeName.DECIMAL) {
          return 9;
        }
        return super.getDefaultPrecision(typeName);
      }
    };
    final JavaTypeFactoryImpl customFactory = new JavaTypeFactoryImpl(customTypeSystem);
    final RexBuilder customBuilder = new RexBuilder(customFactory);

    final RelDataType customDec10_0 = customFactory.createSqlType(SqlTypeName.DECIMAL, 10, 0);
    final RelDataType customDecDefault = customFactory.createSqlType(SqlTypeName.DECIMAL);

    // custom target has 9 integer digits, so decimal with 10 won't fit
    assertThat(
        RexUtil.isLosslessCast(
            customBuilder.makeCast(
        customDecDefault, customBuilder.makeInputRef(customDec10_0, 0))), is(false));
  }

  @Test void removeRedundantCast() {
    checkSimplify(cast(vInt(), nullable(tInt())), "?0.int0");
    checkSimplifyUnchanged(cast(vInt(), tInt()));
    checkSimplify(cast(vIntNotNull(), nullable(tInt())), "?0.notNullInt0");
    checkSimplify(cast(vIntNotNull(), tInt()), "?0.notNullInt0");

    // Nested int cast is removed
    checkSimplify(cast(cast(vVarchar(), tInt()), tInt()),
        "CAST(?0.varchar0):INTEGER NOT NULL");
    checkSimplifyUnchanged(cast(cast(vVarchar(), tInt()), tVarchar()));
  }

  @Test void removeLosslessCastInt() {
    checkSimplifyUnchanged(cast(vInt(), tBigInt()));
    checkSimplifyUnchanged(cast(vInt(), tFloat()));
    // A.1 INT -> BIGINT
    checkSimplify(cast(cast(vInt(), tBigInt()), tInt()), "CAST(?0.int0):INTEGER NOT NULL");
    checkSimplify(cast(cast(vInt(), tFloat()), tInt()), "CAST(?0.int0):INTEGER NOT NULL");
    RexNode core = cast(vIntNotNull(), tBigInt());
    checkSimplify(cast(core, tInt()), "?0.notNullInt0");
    checkSimplify(
        cast(cast(core, tInt()), tBigInt()),
        "CAST(?0.notNullInt0):BIGINT NOT NULL");
    checkSimplify(
        cast(cast(cast(core, tInt()), tBigInt()), tInt()),
        "?0.notNullInt0");
    // A.1 INT -> FLOAT
    core = cast(vIntNotNull(), tFloat());
    checkSimplify(cast(core, tInt()), "?0.notNullInt0");
    checkSimplify(
        cast(cast(core, tInt()), tFloat()),
        "CAST(?0.notNullInt0):FLOAT NOT NULL");
    checkSimplify(
        cast(cast(cast(core, tInt()), tFloat()), tInt()),
        "?0.notNullInt0");
    // A.1 INT -> DECIMAL
    core = cast(vIntNotNull(), tDecimal());
    checkSimplify(cast(core, tInt()), "?0.notNullInt0");
    checkSimplify(
        cast(cast(core, tInt()), tDecimal()),
        "CAST(?0.notNullInt0):DECIMAL(19, 0) NOT NULL");
    checkSimplify(
        cast(cast(cast(core, tInt()), tDecimal()), tInt()),
        "?0.notNullInt0");
    // A.1 SMALLINT -> REAL
    core = cast(vSmallIntNotNull(), tReal());
    checkSimplify(cast(core, tSmallInt()), "?0.notNullSmallint0");
    checkSimplify(
        cast(cast(core, tSmallInt()), tReal()),
        "CAST(?0.notNullSmallint0):REAL NOT NULL");
    checkSimplify(
        cast(cast(cast(core, tSmallInt()), tReal()), tSmallInt()),
        "?0.notNullSmallint0");
    // A.1 INT -> VARCHAR
    checkSimplify(cast(cast(vInt(), tVarchar()), tInt()), "CAST(?0.int0):INTEGER NOT NULL");
  }

  @Test void removeLosslessCastChar() {
    checkSimplifyUnchanged(cast(vVarchar(), tChar(3)));
    checkSimplifyUnchanged(cast(cast(vVarchar(), tChar(3)), tVarchar(5)));

    RexNode char2 = vParam("char(2)_", tChar(2));
    RexNode char6 = vParam("char(6)_", tChar(6));
    RexNode varchar2 = vParam("varchar(2)_", tChar(2));
    // A.2 in RexSimplify
    checkSimplify(
        cast(cast(char2, tChar(5)), tChar(2)),
        "CAST(?0.char(2)_0):CHAR(2) NOT NULL");
    // B.1
    checkSimplify(
        cast(cast(char2, tChar(4)), tChar(5)),
        "CAST(?0.char(2)_0):CHAR(5) NOT NULL");
    // B.2
    checkSimplify(
        cast(cast(char2, tChar(10)), tChar(5)),
        "CAST(?0.char(2)_0):CHAR(5) NOT NULL");
    // B.3
    checkSimplify(
        cast(cast(char2, tVarchar(10)), tChar(5)),
        "CAST(?0.char(2)_0):CHAR(5) NOT NULL");
    // B.4
    checkSimplify(
        cast(cast(char6, tVarchar(10)), tChar(5)),
        "CAST(?0.char(6)_0):CHAR(5) NOT NULL");
    // C.1
    checkSimplifyUnchanged(
        cast(cast(char6, tChar(3)), tChar(5)));
    // C.2
    checkSimplifyUnchanged(
        cast(cast(varchar2, tChar(5)), tVarchar(2)));
    // C.3
    checkSimplifyUnchanged(
        cast(cast(char2, tChar(4)), tVarchar(5)));
  }
}
