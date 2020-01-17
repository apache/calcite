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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link org.apache.calcite.rex.RexUtil#isLosslessCast(RexNode)} and related cases.
 */
public class RexLosslessCastTest extends RexProgramTestBase {
  /** Unit test for {@link org.apache.calcite.rex.RexUtil#isLosslessCast(RexNode)}. */
  @Test public void testLosslessCast() {
    final RelDataType tinyIntType = typeFactory.createSqlType(SqlTypeName.TINYINT);
    final RelDataType smallIntType = typeFactory.createSqlType(SqlTypeName.SMALLINT);
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType bigIntType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    final RelDataType floatType = typeFactory.createSqlType(SqlTypeName.FLOAT);
    final RelDataType booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    final RelDataType charType5 = typeFactory.createSqlType(SqlTypeName.CHAR, 5);
    final RelDataType charType6 = typeFactory.createSqlType(SqlTypeName.CHAR, 6);
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
  }

  @Test public void removeRedundantCast() {
    checkSimplify(cast(vInt(), nullable(tInt())), "?0.int0");
    checkSimplifyUnchanged(cast(vInt(), tInt()));
    checkSimplify(cast(vIntNotNull(), nullable(tInt())), "?0.notNullInt0");
    checkSimplify(cast(vIntNotNull(), tInt()), "?0.notNullInt0");

    // Nested int int cast is removed
    checkSimplify(cast(cast(vVarchar(), tInt()), tInt()),
        "CAST(?0.varchar0):INTEGER NOT NULL");
    checkSimplifyUnchanged(cast(cast(vVarchar(), tInt()), tVarchar()));
  }

  @Test public void removeLosslesssCastInt() {
    checkSimplifyUnchanged(cast(vInt(), tBigInt()));
    // A.1
    checkSimplify(cast(cast(vInt(), tBigInt()), tInt()), "CAST(?0.int0):INTEGER NOT NULL");
    RexNode core = cast(vIntNotNull(), tBigInt());
    checkSimplify(cast(core, tInt()), "?0.notNullInt0");
    checkSimplify(
        cast(cast(core, tInt()), tBigInt()),
        "CAST(?0.notNullInt0):BIGINT NOT NULL");
    checkSimplify(
        cast(cast(cast(core, tInt()), tBigInt()), tInt()),
        "?0.notNullInt0");
  }

  @Test public void removeLosslesssCastChar() {
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
