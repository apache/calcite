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

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeAssignmentRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.RangeSets;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.TimestampWithTimeZoneString;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

import static org.apache.calcite.test.Matchers.isRangeSet;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit tests for {@link RexProgram} and
 * {@link org.apache.calcite.rex.RexProgramBuilder}.
 */
class RexProgramTest extends RexProgramTestBase {
  /**
   * Tests construction of a RexProgram.
   */
  @Test void testBuildProgram() {
    final RexProgramBuilder builder = createProg(0);
    final RexProgram program = builder.getProgram(false);
    final String programString = program.toString();
    TestUtil.assertEqualsVerbose(
        "(expr#0..1=[{inputs}], expr#2=[+($0, 1)], expr#3=[77], "
            + "expr#4=[+($0, $1)], expr#5=[+($0, $0)], expr#6=[+($t4, $t2)], "
            + "a=[$t6], b=[$t5])",
        programString);

    // Normalize the program using the RexProgramBuilder.normalize API.
    // Note that unused expression '77' is eliminated, input refs (e.g. $0)
    // become local refs (e.g. $t0), and constants are assigned to locals.
    final RexProgram normalizedProgram = program.normalize(rexBuilder, null);
    final String normalizedProgramString = normalizedProgram.toString();
    TestUtil.assertEqualsVerbose(
        "(expr#0..1=[{inputs}], expr#2=[+($t0, $t1)], expr#3=[1], "
            + "expr#4=[+($t0, $t3)], expr#5=[+($t2, $t4)], "
            + "expr#6=[+($t0, $t0)], a=[$t5], b=[$t6])",
        normalizedProgramString);
  }

  /**
   * Tests construction and normalization of a RexProgram.
   */
  @Test void testNormalize() {
    final RexProgramBuilder builder = createProg(0);
    final String program = builder.getProgram(true).toString();
    TestUtil.assertEqualsVerbose(
        "(expr#0..1=[{inputs}], expr#2=[+($t0, $t1)], expr#3=[1], "
            + "expr#4=[+($t0, $t3)], expr#5=[+($t2, $t4)], "
            + "expr#6=[+($t0, $t0)], a=[$t5], b=[$t6])",
        program);
  }

  /**
   * Tests construction and normalization of a RexProgram.
   */
  @Test void testElimDups() {
    final RexProgramBuilder builder = createProg(1);
    final String unnormalizedProgram = builder.getProgram(false).toString();
    TestUtil.assertEqualsVerbose(
        "(expr#0..1=[{inputs}], expr#2=[+($0, 1)], expr#3=[77], "
            + "expr#4=[+($0, $1)], expr#5=[+($0, 1)], expr#6=[+($0, $t5)], "
            + "expr#7=[+($t4, $t2)], a=[$t7], b=[$t6])",
        unnormalizedProgram);

    // normalize eliminates duplicates (specifically "+($0, $1)")
    final RexProgramBuilder builder2 = createProg(1);
    final String program2 = builder2.getProgram(true).toString();
    TestUtil.assertEqualsVerbose(
        "(expr#0..1=[{inputs}], expr#2=[+($t0, $t1)], expr#3=[1], "
            + "expr#4=[+($t0, $t3)], expr#5=[+($t2, $t4)], "
            + "expr#6=[+($t0, $t4)], a=[$t5], b=[$t6])",
        program2);
  }

  /**
   * Tests how the condition is simplified.
   */
  @Test void testSimplifyCondition() {
    final RexProgram program = createProg(3).getProgram(false);
    assertThat(program.toString(),
        is("(expr#0..1=[{inputs}], expr#2=[+($0, 1)], expr#3=[77], "
            + "expr#4=[+($0, $1)], expr#5=[+($0, 1)], expr#6=[+($0, $t5)], "
            + "expr#7=[+($t4, $t2)], expr#8=[5], expr#9=[>($t2, $t8)], "
            + "expr#10=[true], expr#11=[IS NOT NULL($t5)], expr#12=[false], "
            + "expr#13=[null:BOOLEAN], expr#14=[CASE($t9, $t10, $t11, $t12, $t13)], "
            + "expr#15=[NOT($t14)], a=[$t7], b=[$t6], $condition=[$t15])"));

    assertThat(program.normalize(rexBuilder, simplify).toString(),
        is("(expr#0..1=[{inputs}], expr#2=[+($t0, $t1)], expr#3=[1], "
            + "expr#4=[+($t0, $t3)], expr#5=[+($t2, $t4)], "
            + "expr#6=[+($t0, $t4)], expr#7=[5], expr#8=[<=($t4, $t7)], "
            + "a=[$t5], b=[$t6], $condition=[$t8])"));
  }

  /**
   * Tests how the condition is simplified.
   */
  @Test void testSimplifyCondition2() {
    final RexProgram program = createProg(4).getProgram(false);
    assertThat(program.toString(),
        is("(expr#0..1=[{inputs}], expr#2=[+($0, 1)], expr#3=[77], "
            + "expr#4=[+($0, $1)], expr#5=[+($0, 1)], expr#6=[+($0, $t5)], "
            + "expr#7=[+($t4, $t2)], expr#8=[5], expr#9=[>($t2, $t8)], "
            + "expr#10=[true], expr#11=[IS NOT NULL($t5)], expr#12=[false], "
            + "expr#13=[null:BOOLEAN], expr#14=[CASE($t9, $t10, $t11, $t12, $t13)], "
            + "expr#15=[NOT($t14)], expr#16=[IS TRUE($t15)], a=[$t7], b=[$t6], "
            + "$condition=[$t16])"));

    assertThat(program.normalize(rexBuilder, simplify).toString(),
        is("(expr#0..1=[{inputs}], expr#2=[+($t0, $t1)], expr#3=[1], "
            + "expr#4=[+($t0, $t3)], expr#5=[+($t2, $t4)], "
            + "expr#6=[+($t0, $t4)], expr#7=[5], expr#8=[<=($t4, $t7)], "
            + "a=[$t5], b=[$t6], $condition=[$t8])"));
  }

  /**
   * Checks translation of AND(x, x).
   */
  @Test void testDuplicateAnd() {
    // RexProgramBuilder used to translate AND(x, x) to x.
    // Now it translates it to AND(x, x).
    // The optimization of AND(x, x) => x occurs at a higher level.
    final RexProgramBuilder builder = createProg(2);
    final String program = builder.getProgram(true).toString();
    TestUtil.assertEqualsVerbose(
        "(expr#0..1=[{inputs}], expr#2=[+($t0, $t1)], expr#3=[1], "
            + "expr#4=[+($t0, $t3)], expr#5=[+($t2, $t4)], "
            + "expr#6=[+($t0, $t0)], expr#7=[>($t2, $t0)], "
            + "a=[$t5], b=[$t6], $condition=[$t7])",
        program);
  }

  /**
   * Creates one of several programs. The program generated depends on the
   * {@code variant} parameter, as follows:
   *
   * <ol>
   * <li><code>select (x + y) + (x + 1) as a, (x + x) as b from t(x, y)</code>
   * <li><code>select (x + y) + (x + 1) as a, (x + (x + 1)) as b
   * from t(x, y)</code>
   * <li><code>select (x + y) + (x + 1) as a, (x + x) as b from t(x, y)
   * where ((x + y) &gt; 1) and ((x + y) &gt; 1)</code>
   * <li><code>select (x + y) + (x + 1) as a, (x + x) as b from t(x, y)
   * where not case
   *           when x + 1 &gt; 5 then true
   *           when y is null then null
   *           else false
   *           end</code>
   * </ol>
   */
  private RexProgramBuilder createProg(int variant) {
    assert variant >= 0 && variant <= 4;
    List<RelDataType> types =
        Arrays.asList(
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            typeFactory.createSqlType(SqlTypeName.INTEGER));
    List<String> names = Arrays.asList("x", "y");
    RelDataType inputRowType = typeFactory.createStructType(types, names);
    final RexProgramBuilder builder =
        new RexProgramBuilder(inputRowType, rexBuilder);
    // $t0 = x
    // $t1 = y
    // $t2 = $t0 + 1 (i.e. x + 1)
    final RexNode i0 = rexBuilder.makeInputRef(
        types.get(0), 0);
    RexLocalRef t2 =
        builder.addExpr(
            rexBuilder.makeCall(
                SqlStdOperatorTable.PLUS,
                i0, literal(1)));
    // $t3 = 77 (not used)
    RexLocalRef t3 =
        builder.addExpr(literal(77));
    Util.discard(t3);
    // $t4 = $t0 + $t1 (i.e. x + y)
    final RexNode i1 = rexBuilder.makeInputRef(
        types.get(1), 1);
    RexLocalRef t4 =
        builder.addExpr(
            rexBuilder.makeCall(
                SqlStdOperatorTable.PLUS,
                i0,
                i1));
    RexLocalRef t5;
    final RexLocalRef t1;
    switch (variant) {
    case 0:
    case 2:
      // $t5 = $t0 + $t0 (i.e. x + x)
      t5 = builder.addExpr(
          rexBuilder.makeCall(
              SqlStdOperatorTable.PLUS,
              i0,
              i0));
      t1 = null;
      break;
    case 1:
    case 3:
    case 4:
      // $tx = $t0 + 1
      t1 =
          builder.addExpr(
              rexBuilder.makeCall(
                  SqlStdOperatorTable.PLUS,
                  i0, literal(1)));
      // $t5 = $t0 + $tx (i.e. x + (x + 1))
      t5 =
          builder.addExpr(
              rexBuilder.makeCall(
                  SqlStdOperatorTable.PLUS,
                  i0,
                  t1));
      break;
    default:
      throw new AssertionError("unexpected variant " + variant);
    }
    // $t6 = $t4 + $t2 (i.e. (x + y) + (x + 1))
    RexLocalRef t6 =
        builder.addExpr(
            rexBuilder.makeCall(
                SqlStdOperatorTable.PLUS,
                t4,
                t2));
    builder.addProject(t6.getIndex(), "a");
    builder.addProject(t5.getIndex(), "b");

    final RexLocalRef t7;
    final RexLocalRef t8;
    switch (variant) {
    case 2:
      // $t7 = $t4 > $i0 (i.e. (x + y) > 0)
      t7 =
          builder.addExpr(
              rexBuilder.makeCall(
                  SqlStdOperatorTable.GREATER_THAN,
                  t4,
                  i0));
      // $t8 = $t7 AND $t7
      t8 =
          builder.addExpr(
              and(t7, t7));
      builder.addCondition(t8);
      builder.addCondition(t7);
      break;
    case 3:
    case 4:
      // $t7 = 5
      t7 = builder.addExpr(literal(5));
      // $t8 = $t2 > $t7 (i.e. (x + 1) > 5)
      t8 = builder.addExpr(gt(t2, t7));
      // $t9 = true
      final RexLocalRef t9 =
          builder.addExpr(trueLiteral);
      // $t10 = $t1 is not null (i.e. y is not null)
      assert t1 != null;
      final RexLocalRef t10 =
          builder.addExpr(
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, t1));
      // $t11 = false
      final RexLocalRef t11 =
          builder.addExpr(falseLiteral);
      // $t12 = unknown
      final RexLocalRef t12 =
          builder.addExpr(nullBool);
      // $t13 = case when $t8 then $t9 when $t10 then $t11 else $t12 end
      final RexLocalRef t13 =
          builder.addExpr(case_(t8, t9, t10, t11, t12));
      // $t14 = not $t13 (i.e. not case ... end)
      final RexLocalRef t14 =
          builder.addExpr(not(t13));
      // don't add 't14 is true' - that is implicit
      if (variant == 3) {
        builder.addCondition(t14);
      } else {
        // $t15 = $14 is true
        final RexLocalRef t15 =
            builder.addExpr(
                isTrue(t14));
        builder.addCondition(t15);
      }
    }
    return builder;
  }

  /** Unit test for {@link org.apache.calcite.plan.Strong}. */
  @Test void testStrong() {
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

    final ImmutableBitSet c = ImmutableBitSet.of();
    final ImmutableBitSet c0 = ImmutableBitSet.of(0);
    final ImmutableBitSet c1 = ImmutableBitSet.of(1);
    final ImmutableBitSet c01 = ImmutableBitSet.of(0, 1);
    final ImmutableBitSet c13 = ImmutableBitSet.of(1, 3);

    // input ref
    final RexInputRef i0 = rexBuilder.makeInputRef(intType, 0);
    final RexInputRef i1 = rexBuilder.makeInputRef(intType, 1);

    assertThat(Strong.isNull(i0, c0), is(true));
    assertThat(Strong.isNull(i0, c1), is(false));
    assertThat(Strong.isNull(i0, c01), is(true));
    assertThat(Strong.isNull(i0, c13), is(false));

    // literals are strong iff they are always null
    assertThat(Strong.isNull(trueLiteral, c), is(false));
    assertThat(Strong.isNull(trueLiteral, c13), is(false));
    assertThat(Strong.isNull(falseLiteral, c13), is(false));
    assertThat(Strong.isNull(nullInt, c), is(true));
    assertThat(Strong.isNull(nullInt, c13), is(true));
    assertThat(Strong.isNull(nullBool, c13), is(true));

    // AND is strong if one of its arguments is strong
    final RexNode andUnknownTrue = and(nullBool, trueLiteral);
    final RexNode andTrueUnknown = and(trueLiteral, nullBool);
    final RexNode andFalseTrue = and(falseLiteral, trueLiteral);

    assertThat(Strong.isNull(andUnknownTrue, c), is(false));
    assertThat(Strong.isNull(andTrueUnknown, c), is(false));
    assertThat(Strong.isNull(andFalseTrue, c), is(false));

    // If i0 is null, "i0 and i1 is null" is null
    assertThat(Strong.isNull(and(i0, isNull(i1)), c0), is(false));
    // If i1 is null, "i0 and i1" is false
    assertThat(Strong.isNull(and(i0, isNull(i1)), c1), is(false));
    // If i0 and i1 are both null, "i0 and i1" is null
    assertThat(Strong.isNull(and(i0, i1), c01), is(true));
    assertThat(Strong.isNull(and(i0, i1), c1), is(false));
    // If i0 and i1 are both null, "i0 and isNull(i1) is false"
    assertThat(Strong.isNull(and(i0, isNull(i1)), c01), is(false));
    // If i0 and i1 are both null, "i0 or i1" is null
    assertThat(Strong.isNull(or(i0, i1), c01), is(true));
    // If i0 is null, "i0 or i1" is not necessarily null
    assertThat(Strong.isNull(or(i0, i1), c0), is(false));
    assertThat(Strong.isNull(or(i0, i1), c1), is(false));

    // If i0 is null, then "i0 is not null" is false
    RexNode i0NotNull = isNotNull(i0);
    RexNode i1NotNull = isNotNull(i1);
    assertThat(Strong.isNull(i0NotNull, c0), is(false));
    assertThat(Strong.isNotTrue(i0NotNull, c0), is(true));
    assertThat(Strong.isNotTrue(or(i0NotNull, i1NotNull), c01), is(true));
    assertThat(Strong.isNotTrue(and(i0NotNull, i1NotNull), c1), is(true));

    // If i0 is null, then "not(i0 is not null)" is true.
    // Join-strengthening relies on this.
    RexNode notI0NotNull = not(isNotNull(i0));
    assertThat(Strong.isNull(notI0NotNull, c0), is(false));
    assertThat(Strong.isNotTrue(notI0NotNull, c0), is(false));

    // NULLIF(null, null): null
    // NULLIF(null, X): null
    // NULLIF(X, X/Y): null or X
    // NULLIF(X, null): X
    assertThat(Strong.isNull(nullIf(nullInt, nullInt), c), is(true));
    assertThat(Strong.isNull(nullIf(nullInt, trueLiteral), c), is(true));
    assertThat(Strong.isNull(nullIf(trueLiteral, trueLiteral), c), is(false));
    assertThat(Strong.isNull(nullIf(trueLiteral, falseLiteral), c), is(false));
    assertThat(Strong.isNull(nullIf(trueLiteral, nullInt), c), is(false));

    // ISNULL(null) is true, ISNULL(not null value) is false
    assertThat(Strong.isNull(isNull(nullInt), c01), is(false));
    assertThat(Strong.isNull(isNull(trueLiteral), c01), is(false));

    // CASE ( <predicate1> <value1> <predicate2> <value2> <predicate3> <value3> ...)
    // only definitely null if all values are null.
    assertThat(
        Strong.isNull(
            case_(eq(i0, i1), nullInt, ge(i0, i1), nullInt, nullInt), c01),
        is(true));
    assertThat(
        Strong.isNull(
            case_(eq(i0, i1), i0, ge(i0, i1), nullInt, nullInt), c01),
        is(true));
    assertThat(
        Strong.isNull(
            case_(eq(i0, i1), i0, ge(i0, i1), nullInt, nullInt), c1),
        is(false));
    assertThat(
        Strong.isNull(
            case_(eq(i0, i1), nullInt, ge(i0, i1), i0, nullInt), c01),
        is(true));
    assertThat(
        Strong.isNull(
            case_(eq(i0, i1), nullInt, ge(i0, i1), i0, nullInt), c1),
        is(false));
    assertThat(
        Strong.isNull(
            case_(eq(i0, i1), nullInt, ge(i0, i1), nullInt, i0), c01),
        is(true));
    assertThat(
        Strong.isNull(
            case_(eq(i0, i1), nullInt, ge(i0, i1), nullInt, i0), c1),
        is(false));
    assertThat(
        Strong.isNull(
            case_(isNotNull(i0), i0, i1), c),
        is(false));
    assertThat(
        Strong.isNull(
            case_(isNotNull(i0), i0, i1), c0),
        is(false));
    assertThat(
        Strong.isNull(
            case_(isNotNull(i0), i0, i1), c1),
        is(false));
    assertThat(
        Strong.isNull(
            case_(isNotNull(i0), i0, i1), c01),
        is(true));

  }

  @Test void testItemStrong() {
    final ImmutableBitSet c0 = ImmutableBitSet.of(0);
    RexNode item = item(input(tArray(tInt()), 0), literal(0));

    assertThat(Strong.isStrong(item), is(true));
    assertThat(Strong.isNull(item, c0), is(true));

    RelDataType mapType = typeFactory.createMapType(tVarchar(), tVarchar());
    item = item(input(mapType, 0), literal("abc"));

    assertThat(Strong.isStrong(item), is(true));
    assertThat(Strong.isNull(item, c0), is(true));
  }

  @Test void xAndNotX() {
    checkSimplify2(
        and(vBool(), not(vBool()),
            vBool(1), not(vBool(1))),
        "AND(null, IS NULL(?0.bool0), IS NULL(?0.bool1))",
        "false");

    checkSimplify2(
        and(vBool(),
            vBool(1), not(vBool(1))),
        "AND(?0.bool0, null, IS NULL(?0.bool1))",
        "false");

    checkSimplify(
        and(vBool(), not(vBool()),
            vBoolNotNull(1), not(vBoolNotNull(1))),
        "false");
  }

  @Disabled("CALCITE-3457: AssertionError in RexSimplify.validateStrongPolicy")
  @Test void reproducerFor3457() {
    // Identified with RexProgramFuzzyTest#testFuzzy, seed=4887662474363391810L
    checkSimplify(
        eq(unaryMinus(abstractCast(literal(1), tInt(true))),
          unaryMinus(abstractCast(literal(1), tInt(true)))),
        "true");
  }

  @Test void testNoCommonReturnTypeFails() {
    try {
      final RexNode node = coalesce(vVarchar(1), vInt(2));
      fail("expected exception, got " + node);
    } catch (IllegalArgumentException e) {
      final String expected = "Cannot infer return type for COALESCE;"
          + " operand types: [VARCHAR, INTEGER]";
      assertThat(e.getMessage(), is(expected));
    }
  }

  /** Unit test for {@link org.apache.calcite.rex.RexUtil#toCnf}. */
  @Test void testCnf() {
    final RelDataType booleanType =
        typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType rowType = typeFactory.builder()
        .add("a", booleanType)
        .add("b", booleanType)
        .add("c", booleanType)
        .add("d", booleanType)
        .add("e", booleanType)
        .add("f", booleanType)
        .add("g", booleanType)
        .add("h", intType)
        .build();

    final RexDynamicParam range = rexBuilder.makeDynamicParam(rowType, 0);
    final RexNode aRef = rexBuilder.makeFieldAccess(range, 0);
    final RexNode bRef = rexBuilder.makeFieldAccess(range, 1);
    final RexNode cRef = rexBuilder.makeFieldAccess(range, 2);
    final RexNode dRef = rexBuilder.makeFieldAccess(range, 3);
    final RexNode eRef = rexBuilder.makeFieldAccess(range, 4);
    final RexNode fRef = rexBuilder.makeFieldAccess(range, 5);
    final RexNode gRef = rexBuilder.makeFieldAccess(range, 6);
    final RexNode hRef = rexBuilder.makeFieldAccess(range, 7);

    final RexNode hEqSeven = eq(hRef, literal(7));

    checkCnf(aRef, "?0.a");
    checkCnf(trueLiteral, "true");
    checkCnf(falseLiteral, "false");
    checkCnf(nullBool, "null:BOOLEAN");
    checkCnf(and(aRef, bRef), "AND(?0.a, ?0.b)");
    checkCnf(and(aRef, bRef, cRef), "AND(?0.a, ?0.b, ?0.c)");

    checkCnf(and(or(aRef, bRef), or(cRef, dRef)),
        "AND(OR(?0.a, ?0.b), OR(?0.c, ?0.d))");
    checkCnf(or(and(aRef, bRef), and(cRef, dRef)),
        "AND(OR(?0.a, ?0.c), OR(?0.a, ?0.d), OR(?0.b, ?0.c), OR(?0.b, ?0.d))");
    // Input has nested ORs, output ORs are flat
    checkCnf(or(and(aRef, bRef), or(cRef, dRef)),
        "AND(OR(?0.a, ?0.c, ?0.d), OR(?0.b, ?0.c, ?0.d))");

    checkCnf(or(aRef, not(and(bRef, not(hEqSeven)))),
        "OR(?0.a, NOT(?0.b), =(?0.h, 7))");

    // apply de Morgan's theorem
    checkCnf(not(or(aRef, not(bRef))), "AND(NOT(?0.a), ?0.b)");

    // apply de Morgan's theorem,
    // filter out 'OR ... FALSE' and 'AND ... TRUE'
    checkCnf(not(or(and(aRef, trueLiteral), not(bRef), falseLiteral)),
        "AND(NOT(?0.a), ?0.b)");

    checkCnf(and(aRef, or(bRef, and(cRef, dRef))),
        "AND(?0.a, OR(?0.b, ?0.c), OR(?0.b, ?0.d))");

    checkCnf(
        and(aRef, or(bRef, and(cRef, or(dRef, and(eRef, or(fRef, gRef)))))),
        "AND(?0.a, OR(?0.b, ?0.c), OR(?0.b, ?0.d, ?0.e), OR(?0.b, ?0.d, ?0.f, ?0.g))");

    checkCnf(
        and(aRef,
            or(bRef,
                and(cRef,
                    or(dRef,
                        and(eRef,
                            or(fRef,
                                and(gRef, or(trueLiteral, falseLiteral)))))))),
        "AND(?0.a, OR(?0.b, ?0.c), OR(?0.b, ?0.d, ?0.e), OR(?0.b, ?0.d, ?0.f, ?0.g))");
  }

  /** Unit test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-394">[CALCITE-394]
   * Add RexUtil.toCnf, to convert expressions to conjunctive normal form
   * (CNF)</a>. */
  @Test void testCnf2() {
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType rowType = typeFactory.builder()
        .add("x", intType)
        .add("y", intType)
        .add("z", intType)
        .add("a", intType)
        .add("b", intType)
        .build();

    final RexDynamicParam range = rexBuilder.makeDynamicParam(rowType, 0);
    final RexNode xRef = rexBuilder.makeFieldAccess(range, 0);
    final RexNode yRef = rexBuilder.makeFieldAccess(range, 1);
    final RexNode zRef = rexBuilder.makeFieldAccess(range, 2);
    final RexNode aRef = rexBuilder.makeFieldAccess(range, 3);
    final RexNode bRef = rexBuilder.makeFieldAccess(range, 4);

    checkCnf(
        or(
            and(eq(xRef, literal(1)),
                eq(yRef, literal(1)),
                eq(zRef, literal(1))),
            and(eq(xRef, literal(2)),
                eq(yRef, literal(2)),
                eq(aRef, literal(2))),
            and(eq(xRef, literal(3)),
                eq(aRef, literal(3)),
                eq(bRef, literal(3)))),
        "AND("
            + "OR(=(?0.x, 1), =(?0.x, 2), =(?0.x, 3)), "
            + "OR(=(?0.x, 1), =(?0.x, 2), =(?0.a, 3)), "
            + "OR(=(?0.x, 1), =(?0.x, 2), =(?0.b, 3)), "
            + "OR(=(?0.x, 1), =(?0.y, 2), =(?0.x, 3)), "
            + "OR(=(?0.x, 1), =(?0.y, 2), =(?0.a, 3)), "
            + "OR(=(?0.x, 1), =(?0.y, 2), =(?0.b, 3)), "
            + "OR(=(?0.x, 1), =(?0.a, 2), =(?0.x, 3)), "
            + "OR(=(?0.x, 1), =(?0.a, 2), =(?0.a, 3)), "
            + "OR(=(?0.x, 1), =(?0.a, 2), =(?0.b, 3)), "
            + "OR(=(?0.y, 1), =(?0.x, 2), =(?0.x, 3)), "
            + "OR(=(?0.y, 1), =(?0.x, 2), =(?0.a, 3)), "
            + "OR(=(?0.y, 1), =(?0.x, 2), =(?0.b, 3)), "
            + "OR(=(?0.y, 1), =(?0.y, 2), =(?0.x, 3)), "
            + "OR(=(?0.y, 1), =(?0.y, 2), =(?0.a, 3)), "
            + "OR(=(?0.y, 1), =(?0.y, 2), =(?0.b, 3)), "
            + "OR(=(?0.y, 1), =(?0.a, 2), =(?0.x, 3)), "
            + "OR(=(?0.y, 1), =(?0.a, 2), =(?0.a, 3)), "
            + "OR(=(?0.y, 1), =(?0.a, 2), =(?0.b, 3)), "
            + "OR(=(?0.z, 1), =(?0.x, 2), =(?0.x, 3)), "
            + "OR(=(?0.z, 1), =(?0.x, 2), =(?0.a, 3)), "
            + "OR(=(?0.z, 1), =(?0.x, 2), =(?0.b, 3)), "
            + "OR(=(?0.z, 1), =(?0.y, 2), =(?0.x, 3)), "
            + "OR(=(?0.z, 1), =(?0.y, 2), =(?0.a, 3)), "
            + "OR(=(?0.z, 1), =(?0.y, 2), =(?0.b, 3)), "
            + "OR(=(?0.z, 1), =(?0.a, 2), =(?0.x, 3)), "
            + "OR(=(?0.z, 1), =(?0.a, 2), =(?0.a, 3)), "
            + "OR(=(?0.z, 1), =(?0.a, 2), =(?0.b, 3)))");
  }

  /** Unit test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1290">[CALCITE-1290]
   * When converting to CNF, fail if the expression exceeds a threshold</a>. */
  @Test void testThresholdCnf() {
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType rowType = typeFactory.builder()
        .add("x", intType)
        .add("y", intType)
        .build();

    final RexDynamicParam range = rexBuilder.makeDynamicParam(rowType, 0);
    final RexNode xRef = rexBuilder.makeFieldAccess(range, 0);
    final RexNode yRef = rexBuilder.makeFieldAccess(range, 1);

    // Expression
    //   OR(=(?0.x, 1), AND(=(?0.x, 2), =(?0.y, 3)))
    // transformation creates 7 nodes
    //   AND(OR(=(?0.x, 1), =(?0.x, 2)), OR(=(?0.x, 1), =(?0.y, 3)))
    // Thus, it is triggered.
    checkThresholdCnf(
        or(eq(xRef, literal(1)),
            and(eq(xRef, literal(2)), eq(yRef, literal(3)))),
        8, "AND(OR(=(?0.x, 1), =(?0.x, 2)), OR(=(?0.x, 1), =(?0.y, 3)))");

    // Expression
    //   OR(=(?0.x, 1), =(?0.x, 2), AND(=(?0.x, 3), =(?0.y, 4)))
    // transformation creates 9 nodes
    //   AND(OR(=(?0.x, 1), =(?0.x, 2), =(?0.x, 3)),
    //       OR(=(?0.x, 1), =(?0.x, 2), =(?0.y, 8)))
    // Thus, it is NOT triggered.
    checkThresholdCnf(
        or(eq(xRef, literal(1)), eq(xRef, literal(2)),
            and(eq(xRef, literal(3)), eq(yRef, literal(4)))),
                8, "OR(=(?0.x, 1), =(?0.x, 2), AND(=(?0.x, 3), =(?0.y, 4)))");
  }

  /** Tests formulas of various sizes whose size is exponential when converted
   * to CNF. */
  @Test void testCnfExponential() {
    // run out of memory if limit is higher than about 20
    int limit = 16;
    for (int i = 2; i < limit; i++) {
      checkExponentialCnf(i);
    }
  }

  private void checkExponentialCnf(int n) {
    final RelDataType booleanType =
        typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (int i = 0; i < n; i++) {
      builder.add("x" + i, booleanType)
          .add("y" + i, booleanType);
    }
    final RelDataType rowType3 = builder.build();
    final RexDynamicParam range3 = rexBuilder.makeDynamicParam(rowType3, 0);
    final List<RexNode> list = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      list.add(
          and(rexBuilder.makeFieldAccess(range3, i * 2),
              rexBuilder.makeFieldAccess(range3, i * 2 + 1)));
    }
    final RexNode cnf = RexUtil.toCnf(rexBuilder, or(list));
    final int nodeCount = cnf.nodeCount();
    assertThat((n + 1) * (int) Math.pow(2, n) + 1, equalTo(nodeCount));
    if (n == 3) {
      assertThat(cnf.toString(),
          equalTo("AND(OR(?0.x0, ?0.x1, ?0.x2), OR(?0.x0, ?0.x1, ?0.y2),"
              + " OR(?0.x0, ?0.y1, ?0.x2), OR(?0.x0, ?0.y1, ?0.y2),"
              + " OR(?0.y0, ?0.x1, ?0.x2), OR(?0.y0, ?0.x1, ?0.y2),"
              + " OR(?0.y0, ?0.y1, ?0.x2), OR(?0.y0, ?0.y1, ?0.y2))"));
    }
  }

  /** Unit test for {@link org.apache.calcite.rex.RexUtil#pullFactors}. */
  @Test void testPullFactors() {
    final RelDataType booleanType =
        typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType rowType = typeFactory.builder()
        .add("a", booleanType)
        .add("b", booleanType)
        .add("c", booleanType)
        .add("d", booleanType)
        .add("e", booleanType)
        .add("f", booleanType)
        .add("g", booleanType)
        .add("h", intType)
        .build();

    final RexDynamicParam range = rexBuilder.makeDynamicParam(rowType, 0);
    final RexNode aRef = rexBuilder.makeFieldAccess(range, 0);
    final RexNode bRef = rexBuilder.makeFieldAccess(range, 1);
    final RexNode cRef = rexBuilder.makeFieldAccess(range, 2);
    final RexNode dRef = rexBuilder.makeFieldAccess(range, 3);
    final RexNode eRef = rexBuilder.makeFieldAccess(range, 4);
    final RexNode fRef = rexBuilder.makeFieldAccess(range, 5);
    final RexNode gRef = rexBuilder.makeFieldAccess(range, 6);
    final RexNode hRef = rexBuilder.makeFieldAccess(range, 7);

    final RexNode hEqSeven = eq(hRef, literal(7));

    // Most of the expressions in testCnf are unaffected by pullFactors.
    checkPullFactors(
        or(and(aRef, bRef),
            and(cRef, aRef, dRef, aRef)),
        "AND(?0.a, OR(?0.b, AND(?0.c, ?0.d)))");

    checkPullFactors(aRef, "?0.a");
    checkPullFactors(trueLiteral, "true");
    checkPullFactors(falseLiteral, "false");
    checkPullFactors(nullBool, "null:BOOLEAN");
    checkPullFactors(and(aRef, bRef), "AND(?0.a, ?0.b)");
    checkPullFactors(and(aRef, bRef, cRef), "AND(?0.a, ?0.b, ?0.c)");

    checkPullFactorsUnchanged(and(or(aRef, bRef), or(cRef, dRef)));
    checkPullFactorsUnchanged(or(and(aRef, bRef), and(cRef, dRef)));
    // Input has nested ORs, output ORs are flat; different from CNF
    checkPullFactors(or(and(aRef, bRef), or(cRef, dRef)),
        "OR(AND(?0.a, ?0.b), ?0.c, ?0.d)");

    checkPullFactorsUnchanged(or(aRef, not(and(bRef, not(hEqSeven)))));
    checkPullFactorsUnchanged(not(or(aRef, not(bRef))));
    checkPullFactorsUnchanged(
        not(or(and(aRef, trueLiteral), not(bRef), falseLiteral)));
    checkPullFactorsUnchanged(and(aRef, or(bRef, and(cRef, dRef))));

    checkPullFactorsUnchanged(
        and(aRef,
            or(bRef,
                and(cRef,
                    or(dRef, and(eRef, or(fRef, gRef)))))));

    checkPullFactorsUnchanged(
        and(aRef,
            or(bRef,
                and(cRef,
                    or(dRef,
                        and(eRef,
                            or(fRef,
                               and(gRef, or(trueLiteral, falseLiteral)))))))));
  }

  @Test void testSimplify() {
    final RelDataType booleanType =
        typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType intNullableType =
        typeFactory.createTypeWithNullability(intType, true);
    final RelDataType rowType = typeFactory.builder()
        .add("a", booleanType)
        .add("b", booleanType)
        .add("c", booleanType)
        .add("d", booleanType)
        .add("e", booleanType)
        .add("f", booleanType)
        .add("g", booleanType)
        .add("h", intType)
        .add("i", intNullableType)
        .add("j", intType)
        .add("k", intType)
        .build();

    final RexDynamicParam range = rexBuilder.makeDynamicParam(rowType, 0);
    final RexNode aRef = rexBuilder.makeFieldAccess(range, 0);
    final RexNode bRef = rexBuilder.makeFieldAccess(range, 1);
    final RexNode cRef = rexBuilder.makeFieldAccess(range, 2);
    final RexNode dRef = rexBuilder.makeFieldAccess(range, 3);
    final RexNode eRef = rexBuilder.makeFieldAccess(range, 4);
    final RexNode hRef = rexBuilder.makeFieldAccess(range, 7);
    final RexNode iRef = rexBuilder.makeFieldAccess(range, 8);
    final RexNode jRef = rexBuilder.makeFieldAccess(range, 9);
    final RexNode kRef = rexBuilder.makeFieldAccess(range, 10);

    // and: remove duplicates
    checkSimplify(and(aRef, bRef, aRef), "AND(?0.a, ?0.b)");

    // and: remove true
    checkSimplify(and(aRef, bRef, trueLiteral),
        "AND(?0.a, ?0.b)");

    // and: false falsifies
    checkSimplify(and(aRef, bRef, falseLiteral),
        "false");

    // and: remove duplicate "not"s
    checkSimplify(and(not(aRef), bRef, not(cRef), not(aRef)),
        "AND(?0.b, NOT(?0.a), NOT(?0.c))");

    // and: "not true" falsifies
    checkSimplify(and(not(aRef), bRef, not(trueLiteral)),
        "false");

    // and: flatten and remove duplicates
    checkSimplify(
        and(aRef, and(and(bRef, not(cRef), dRef, not(eRef)), not(eRef))),
        "AND(?0.a, ?0.b, ?0.d, NOT(?0.c), NOT(?0.e))");

    // and: expand "... and not(or(x, y))" to "... and not(x) and not(y)"
    checkSimplify(and(aRef, bRef, not(or(cRef, or(dRef, eRef)))),
        "AND(?0.a, ?0.b, NOT(?0.c), NOT(?0.d), NOT(?0.e))");

    checkSimplify(and(aRef, bRef, not(or(not(cRef), dRef, not(eRef)))),
        "AND(?0.a, ?0.b, ?0.c, ?0.e, NOT(?0.d))");

    // or: remove duplicates
    checkSimplify(or(aRef, bRef, aRef), "OR(?0.a, ?0.b)");

    // or: remove false
    checkSimplify(or(aRef, bRef, falseLiteral),
        "OR(?0.a, ?0.b)");

    // or: true makes everything true
    checkSimplify(or(aRef, bRef, trueLiteral), "true");

    // case: remove false branches
    checkSimplify(case_(eq(bRef, cRef), dRef, falseLiteral, aRef, eRef),
        "OR(AND(=(?0.b, ?0.c), ?0.d), AND(?0.e, <>(?0.b, ?0.c)))");

    // case: true branches become the last branch
    checkSimplify(
        case_(eq(bRef, cRef), dRef, trueLiteral, aRef, eq(cRef, dRef), eRef, cRef),
        "OR(AND(=(?0.b, ?0.c), ?0.d), AND(?0.a, <>(?0.b, ?0.c)))");

    // case: singleton
    checkSimplify(case_(trueLiteral, aRef, eq(cRef, dRef), eRef, cRef), "?0.a");

    // case: always same value
    checkSimplify(
        case_(aRef, literal(1), bRef, literal(1), cRef, literal(1), dRef,
            literal(1), literal(1)), "1");

    // case: trailing false and null, no simplification
    checkSimplify3(
        case_(aRef, trueLiteral, bRef, trueLiteral, cRef, falseLiteral, nullBool),
        "OR(?0.a, ?0.b, AND(null, NOT(?0.a), NOT(?0.b), NOT(?0.c)))",
        "OR(?0.a, ?0.b)",
        "OR(?0.a, ?0.b, NOT(?0.c))");

    // case: form an AND of branches that return true
    checkSimplify(
        case_(aRef, trueLiteral, bRef,
            falseLiteral, cRef,
            falseLiteral, dRef, trueLiteral,
            falseLiteral),
        "OR(?0.a, AND(?0.d, NOT(?0.b), NOT(?0.c)))");

    checkSimplify(
        case_(aRef, trueLiteral, bRef,
            falseLiteral, cRef,
            falseLiteral, dRef, trueLiteral, eRef,
            falseLiteral, trueLiteral),
        "OR(?0.a, AND(?0.d, NOT(?0.b), NOT(?0.c)), AND(NOT(?0.b), NOT(?0.c), NOT(?0.e)))");

    checkSimplify(
        case_(eq(falseLiteral, falseLiteral), falseLiteral,
              eq(falseLiteral, falseLiteral), trueLiteral,
              trueLiteral),
        "false");

    // is null, applied to not-null value
    checkSimplify(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, aRef),
        "false");

    // is not null, applied to not-null value
    checkSimplify(rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, aRef),
        "true");

    // condition, and the inverse
    checkSimplify3(and(le(hRef, literal(1)), gt(hRef, literal(1))),
        "<>(?0.h, ?0.h)",
        "false",
        "false");

    checkSimplify(and(le(hRef, literal(1)), ge(hRef, literal(1))), "=(?0.h, 1)");

    checkSimplify3(and(lt(hRef, literal(1)), eq(hRef, literal(1)), ge(hRef, literal(1))),
        "<>(?0.h, ?0.h)",
        "false",
        "false");

    checkSimplify(and(lt(hRef, literal(1)), or(falseLiteral, falseLiteral)),
        "false");
    checkSimplify(and(lt(hRef, literal(1)), or(falseLiteral, gt(jRef, kRef))),
        "AND(<(?0.h, 1), >(?0.j, ?0.k))");
    checkSimplify(or(lt(hRef, literal(1)), and(trueLiteral, trueLiteral)),
        "true");
    checkSimplify(
        or(lt(hRef, literal(1)),
            and(trueLiteral, or(trueLiteral, falseLiteral))),
        "true");
    checkSimplify(
        or(lt(hRef, literal(1)),
            and(trueLiteral, and(trueLiteral, falseLiteral))),
        "<(?0.h, 1)");
    checkSimplify(
        or(lt(hRef, literal(1)),
            and(trueLiteral, or(falseLiteral, falseLiteral))),
        "<(?0.h, 1)");

    // "x = x" simplifies to "x is not null"
    checkSimplify(eq(literal(1), literal(1)), "true");
    checkSimplify(eq(hRef, hRef), "true");
    checkSimplify3(eq(iRef, iRef), "OR(null, IS NOT NULL(?0.i))", "IS NOT NULL(?0.i)", "true");
    checkSimplifyUnchanged(eq(iRef, hRef));

    // "x <= x" simplifies to "x is not null"
    checkSimplify(le(literal(1), literal(1)), "true");
    checkSimplify(le(hRef, hRef), "true");
    checkSimplify3(le(iRef, iRef), "OR(null, IS NOT NULL(?0.i))", "IS NOT NULL(?0.i)", "true");
    checkSimplifyUnchanged(le(iRef, hRef));

    // "x >= x" simplifies to "x is not null"
    checkSimplify(ge(literal(1), literal(1)), "true");
    checkSimplify(ge(hRef, hRef), "true");
    checkSimplify3(ge(iRef, iRef), "OR(null, IS NOT NULL(?0.i))", "IS NOT NULL(?0.i)", "true");
    checkSimplifyUnchanged(ge(iRef, hRef));

    // "x <> x" simplifies to "false"
    checkSimplify(ne(literal(1), literal(1)), "false");
    checkSimplify(ne(hRef, hRef), "false");
    checkSimplify3(ne(iRef, iRef), "AND(null, IS NULL(?0.i))",
        "false", "IS NULL(?0.i)");
    checkSimplifyUnchanged(ne(iRef, hRef));

    // "x < x" simplifies to "false"
    checkSimplify(lt(literal(1), literal(1)), "false");
    checkSimplify(lt(hRef, hRef), "false");
    checkSimplify3(lt(iRef, iRef), "AND(null, IS NULL(?0.i))",
        "false", "IS NULL(?0.i)");
    checkSimplifyUnchanged(lt(iRef, hRef));

    // "x > x" simplifies to "false"
    checkSimplify(gt(literal(1), literal(1)), "false");
    checkSimplify(gt(hRef, hRef), "false");
    checkSimplify3(gt(iRef, iRef), "AND(null, IS NULL(?0.i))",
        "false", "IS NULL(?0.i)");
    checkSimplifyUnchanged(gt(iRef, hRef));

    // "x = 1 or not x = 1 or x is null" simplifies to "true"
    checkSimplify(or(eq(hRef, literal(1)), not(eq(hRef, literal(1))), isNull(hRef)), "true");
    checkSimplify(or(eq(iRef, literal(1)), not(eq(iRef, literal(1))), isNull(iRef)), "true");

    // "(not x) is null" to "x is null"
    checkSimplify(isNull(not(vBool())), "IS NULL(?0.bool0)");
    checkSimplify(isNull(not(vBoolNotNull())), "false");

    // "(not x) is not null" to "x is not null"
    checkSimplify(isNotNull(not(vBool())), "IS NOT NULL(?0.bool0)");
    checkSimplify(isNotNull(not(vBoolNotNull())), "true");

    // "null is null" to "true"
    checkSimplify(isNull(nullBool), "true");
    // "(x + y) is null" simplifies to "x is null or y is null"
    checkSimplify(isNull(plus(vInt(0), vInt(1))),
        "OR(IS NULL(?0.int0), IS NULL(?0.int1))");
    checkSimplify(isNull(plus(vInt(0), vIntNotNull(1))), "IS NULL(?0.int0)");
    checkSimplify(isNull(plus(vIntNotNull(0), vIntNotNull(1))), "false");
    checkSimplify(isNull(plus(vIntNotNull(0), vInt(1))), "IS NULL(?0.int1)");

    // "(x + y) is not null" simplifies to "x is not null and y is not null"
    checkSimplify(isNotNull(plus(vInt(0), vInt(1))),
        "AND(IS NOT NULL(?0.int0), IS NOT NULL(?0.int1))");
    checkSimplify(isNotNull(plus(vInt(0), vIntNotNull(1))),
        "IS NOT NULL(?0.int0)");
    checkSimplify(isNotNull(plus(vIntNotNull(0), vIntNotNull(1))), "true");
    checkSimplify(isNotNull(plus(vIntNotNull(0), vInt(1))),
        "IS NOT NULL(?0.int1)");
  }

  @Test void simplifyStrong() {
    checkSimplify(ge(trueLiteral, falseLiteral), "true");
    checkSimplify3(ge(trueLiteral, nullBool), "null:BOOLEAN", "false", "true");
    checkSimplify3(ge(nullBool, nullBool), "null:BOOLEAN", "false", "true");
    checkSimplify3(gt(trueLiteral, nullBool), "null:BOOLEAN", "false", "true");
    checkSimplify3(le(trueLiteral, nullBool), "null:BOOLEAN", "false", "true");
    checkSimplify3(lt(trueLiteral, nullBool), "null:BOOLEAN", "false", "true");

    checkSimplify3(not(nullBool), "null:BOOLEAN", "false", "true");
    checkSimplify3(ne(vInt(), nullBool), "null:BOOLEAN", "false", "true");
    checkSimplify3(eq(vInt(), nullBool), "null:BOOLEAN", "false", "true");

    checkSimplify(plus(vInt(), nullInt), "null:INTEGER");
    checkSimplify(sub(vInt(), nullInt), "null:INTEGER");
    checkSimplify(mul(vInt(), nullInt), "null:INTEGER");
    checkSimplify(div(vInt(), nullInt), "null:INTEGER");
  }

  @Test void testSimplifyFilter() {
    final RelDataType booleanType =
        typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType rowType = typeFactory.builder()
        .add("a", intType)
        .add("b", intType)
        .add("c", booleanType)
        .add("d", booleanType)
        .add("e", booleanType)
        .add("f", booleanType)
        .add("g", booleanType)
        .add("h", intType)
        .build();

    final RexDynamicParam range = rexBuilder.makeDynamicParam(rowType, 0);
    final RexNode aRef = rexBuilder.makeFieldAccess(range, 0);
    final RexNode bRef = rexBuilder.makeFieldAccess(range, 1);
    final RexNode cRef = rexBuilder.makeFieldAccess(range, 2);
    final RexNode dRef = rexBuilder.makeFieldAccess(range, 3);
    final RexNode eRef = rexBuilder.makeFieldAccess(range, 4);
    final RexNode fRef = rexBuilder.makeFieldAccess(range, 5);

    // condition, and the inverse
    checkSimplifyFilter(and(le(aRef, literal(1)), gt(aRef, literal(1))),
        "false");

    checkSimplifyFilter(and(le(aRef, literal(1)), ge(aRef, literal(1))),
        "=(?0.a, 1)");

    checkSimplifyFilter(
        and(lt(aRef, literal(1)), eq(aRef, literal(1)), ge(aRef, literal(1))),
        "false");

    // simplify equals boolean
    final ImmutableList<RexNode> args =
        ImmutableList.of(eq(eq(aRef, literal(1)), trueLiteral),
            eq(bRef, literal(1)));
    checkSimplifyFilter(and(args),
        "AND(=(?0.a, 1), =(?0.b, 1))");

    // as previous, using simplifyFilterPredicates
    assertThat(simplify
            .simplifyFilterPredicates(args)
            .toString(),
        equalTo("AND(=(?0.a, 1), =(?0.b, 1))"));

    // "a = 1 and a = 10" is always false
    final ImmutableList<RexNode> args2 =
        ImmutableList.of(eq(aRef, literal(1)), eq(aRef, literal(10)));
    checkSimplifyFilter(and(args2), "false");

    assertThat(simplify
            .simplifyFilterPredicates(args2),
        nullValue());

    // equality on constants, can remove the equality on the variables
    checkSimplifyFilter(and(eq(aRef, literal(1)), eq(bRef, literal(1)), eq(aRef, bRef)),
        "AND(=(?0.a, 1), =(?0.b, 1))");

    // condition not satisfiable
    checkSimplifyFilter(and(eq(aRef, literal(1)), eq(bRef, literal(10)), eq(aRef, bRef)),
        "false");

    // condition not satisfiable
    checkSimplifyFilter(and(gt(aRef, literal(10)), ge(bRef, literal(1)), lt(aRef, literal(10))),
        "false");

    // one "and" containing three "or"s
    checkSimplifyFilter(
        or(gt(aRef, literal(10)), gt(bRef, literal(1)), gt(aRef, literal(10))),
        "OR(>(?0.a, 10), >(?0.b, 1))");

    // case: trailing false and null, remove
    checkSimplifyFilter(
        case_(cRef, trueLiteral, dRef, trueLiteral, eRef, falseLiteral, fRef,
            falseLiteral, nullBool),
        "OR(?0.c, ?0.d)");

    // condition with null value for range
    checkSimplifyFilter(and(gt(aRef, nullBool), ge(bRef, literal(1))), "false");

    // condition "1 < a && 5 < a" yields "5 < a"
    checkSimplifyFilter(
        and(lt(literal(1), aRef), lt(literal(5), aRef)),
        RelOptPredicateList.EMPTY,
        ">(?0.a, 5)");

    // condition "1 < a && a < 5" is converted to a Sarg
    checkSimplifyFilter(
        and(lt(literal(1), aRef), lt(aRef, literal(5))),
        RelOptPredicateList.EMPTY,
        "SEARCH(?0.a, Sarg[(1..5)])");

    // condition "1 > a && 5 > a" yields "1 > a"
    checkSimplifyFilter(
        and(gt(literal(1), aRef), gt(literal(5), aRef)),
        RelOptPredicateList.EMPTY,
        "<(?0.a, 1)");

    // condition "1 > a && a > 5" yields false
    checkSimplifyFilter(
        and(gt(literal(1), aRef), gt(aRef, literal(5))),
        RelOptPredicateList.EMPTY,
        "false");

    // range with no predicates;
    // condition "a > 1 && a < 10 && a < 5" yields "a < 1 && a < 5"
    checkSimplifyFilter(
        and(gt(aRef, literal(1)), lt(aRef, literal(10)), lt(aRef, literal(5))),
        RelOptPredicateList.EMPTY,
        "SEARCH(?0.a, Sarg[(1..5)])");

    // condition "a > 1 && a < 10 && a < 5"
    // with pre-condition "a > 5"
    // yields "false"
    checkSimplifyFilter(
        and(gt(aRef, literal(1)), lt(aRef, literal(10)), lt(aRef, literal(5))),
        RelOptPredicateList.of(rexBuilder,
            ImmutableList.of(gt(aRef, literal(5)))),
        "false");

    // condition "a > 1 && a < 10 && a <= 5"
    // with pre-condition "a >= 5"
    // yields "a = 5"
    // "a <= 5" would also be correct, just a little less concise.
    checkSimplifyFilter(
        and(gt(aRef, literal(1)), lt(aRef, literal(10)), le(aRef, literal(5))),
        RelOptPredicateList.of(rexBuilder,
            ImmutableList.of(ge(aRef, literal(5)))),
        "=(?0.a, 5)");

    // condition "a > 1 && a < 10 && a < 5"
    // with pre-condition "b < 10 && a > 5"
    // yields "a > 1 and a < 5"
    checkSimplifyFilter(
        and(gt(aRef, literal(1)), lt(aRef, literal(10)), lt(aRef, literal(5))),
        RelOptPredicateList.of(rexBuilder,
            ImmutableList.of(lt(bRef, literal(10)), ge(aRef, literal(1)))),
        "SEARCH(?0.a, Sarg[(1..5)])");

    // condition "a > 1"
    // with pre-condition "b < 10 && a > 5"
    // yields "true"
    checkSimplifyFilter(gt(aRef, literal(1)),
        RelOptPredicateList.of(rexBuilder,
            ImmutableList.of(lt(bRef, literal(10)), gt(aRef, literal(5)))),
        "true");

    // condition "a < 1"
    // with pre-condition "b < 10 && a > 5"
    // yields "false"
    checkSimplifyFilter(lt(aRef, literal(1)),
        RelOptPredicateList.of(rexBuilder,
            ImmutableList.of(lt(bRef, literal(10)), gt(aRef, literal(5)))),
        "false");

    // condition "a > 5"
    // with pre-condition "b < 10 && a >= 5"
    // yields "a > 5"
    checkSimplifyFilter(gt(aRef, literal(5)),
        RelOptPredicateList.of(rexBuilder,
            ImmutableList.of(lt(bRef, literal(10)), ge(aRef, literal(5)))),
        ">(?0.a, 5)");

    // condition "a > 5"
    // with pre-condition "a <= 5"
    // yields "false"
    checkSimplifyFilter(gt(aRef, literal(5)),
        RelOptPredicateList.of(rexBuilder,
            ImmutableList.of(le(aRef, literal(5)))),
        "false");

    // condition "a > 5"
    // with pre-condition "a <= 5 and b <= 5"
    // yields "false"
    checkSimplifyFilter(gt(aRef, literal(5)),
        RelOptPredicateList.of(rexBuilder,
            ImmutableList.of(le(aRef, literal(5)), le(bRef, literal(5)))),
        "false");

    // condition "a > 5 or b > 5"
    // with pre-condition "a <= 5 and b <= 5"
    // should yield "false" but yields "a = 5 or b = 5"
    checkSimplifyFilter(or(gt(aRef, literal(5)), gt(bRef, literal(5))),
        RelOptPredicateList.of(rexBuilder,
            ImmutableList.of(le(aRef, literal(5)), le(bRef, literal(5)))),
        "false");

    // condition "(a >= 1 and a <= 3) or (a >= 2 and a <= 4)"
    // yelds "a >= 1 and a <= 4"
    checkSimplifyFilter(
        or(and(ge(aRef, literal(1)), le(aRef, literal(3))),
            and(ge(aRef, literal(2)), le(aRef, literal(4)))),
        "SEARCH(?0.a, Sarg[[1..4]])");

    // condition "(a >= 1 and a <= 3) or (a >= 0 and a <= 2)"
    // yelds "a >= 0 and a <= 3"
    checkSimplifyFilter(
        or(and(ge(aRef, literal(1)), le(aRef, literal(3))),
            and(ge(aRef, literal(0)), le(aRef, literal(2)))),
        "SEARCH(?0.a, Sarg[[0..3]])");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testSimplifyOrNotEqualsNotNullable() {
    checkSimplify(
        or(
            ne(vIntNotNull(), literal(1)),
            ne(vIntNotNull(), literal(2))),
        "true");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testSimplifyOrNotEqualsNotNullable2() {
    checkSimplify(
        or(
            ne(vIntNotNull(0), literal(1)),
            eq(vIntNotNull(1), literal(10)),
            ne(vIntNotNull(0), literal(2))),
        "true");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testSimplifyOrNotEqualsNullable() {
    checkSimplify3(
        or(
            ne(vInt(), literal(1)),
            ne(vInt(), literal(2))),
        "OR(IS NOT NULL(?0.int0), null)",
        "IS NOT NULL(?0.int0)",
        "true");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testSimplifyOrNotEqualsNullable2() {
    checkSimplify3(
        or(
            ne(vInt(0), literal(1)),
            eq(vInt(1), literal(10)),
            ne(vInt(0), literal(2))),
        "OR(IS NOT NULL(?0.int0), null, =(?0.int1, 10))",
        "OR(IS NOT NULL(?0.int0), =(?0.int1, 10))",
        "true");
  }

  @Test void testSimplifyOrNotEqualsNull() {
    checkSimplify3(
        or(
            ne(vInt(0), literal(1)),
            eq(vInt(1), nullInt),
            ne(vInt(0), literal(2))),
        "OR(IS NOT NULL(?0.int0), null)",
        "IS NOT NULL(?0.int0)",
        "true");
  }

  @Test void testSimplifyAndPush() {
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType rowType = typeFactory.builder()
        .add("a", intType)
        .add("b", intType)
        .build();

    final RexDynamicParam range = rexBuilder.makeDynamicParam(rowType, 0);
    final RexNode aRef = rexBuilder.makeFieldAccess(range, 0);
    final RexNode bRef = rexBuilder.makeFieldAccess(range, 1);

    checkSimplifyFilter(
        or(
            or(eq(aRef, literal(1)),
                eq(aRef, literal(1))),
            eq(aRef, literal(1))),
        "=(?0.a, 1)");

    checkSimplifyFilter(
        or(
            and(eq(aRef, literal(1)),
                eq(aRef, literal(1))),
            and(eq(aRef, literal(10)),
                eq(aRef, literal(1)))),
        "=(?0.a, 1)");

    checkSimplifyFilter(
        and(
            eq(aRef, literal(1)),
            or(eq(aRef, literal(1)),
                eq(aRef, literal(10)))),
        "=(?0.a, 1)");
    checkSimplifyFilter(
        and(
            or(eq(aRef, literal(1)),
                eq(aRef, literal(10))),
            eq(aRef, literal(1))),
        "=(?0.a, 1)");

    checkSimplifyFilter(
        and(gt(aRef, literal(10)),
            gt(aRef, literal(1))),
        ">(?0.a, 10)");

    checkSimplifyFilter(
        and(gt(aRef, literal(1)),
            gt(aRef, literal(10))),
        ">(?0.a, 10)");

    // "null AND NOT(null OR x)" => "null AND NOT(x)"
    checkSimplify3(
        and(nullBool,
            not(or(nullBool, vBool()))),
        "AND(null, NOT(?0.bool0))",
        "false",
        "NOT(?0.bool0)");

    // "x1 AND x2 AND x3 AND NOT(x1) AND NOT(x2) AND NOT(x0)" =>
    // "x3 AND null AND x1 IS NULL AND x2 IS NULL AND NOT(x0)"
    checkSimplify2(
        and(vBool(1), vBool(2),
            vBool(3), not(vBool(1)),
            not(vBool(2)), not(vBool())),
        "AND(?0.bool3, null, IS NULL(?0.bool1),"
            + " IS NULL(?0.bool2), NOT(?0.bool0))",
        "false");
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test void testRangeSetMinus() {
    final RangeSet<Integer> setNone = ImmutableRangeSet.of();
    final RangeSet<Integer> setAll = setNone.complement();
    final RangeSet<Integer> setGt2 = ImmutableRangeSet.of(Range.greaterThan(2));
    final RangeSet<Integer> setGt1 = ImmutableRangeSet.of(Range.greaterThan(1));
    final RangeSet<Integer> setGe1 = ImmutableRangeSet.of(Range.atLeast(1));
    final RangeSet<Integer> setGt0 = ImmutableRangeSet.of(Range.greaterThan(0));
    final RangeSet<Integer> setComplex =
        ImmutableRangeSet.<Integer>builder()
            .add(Range.closed(0, 2))
            .add(Range.singleton(3))
            .add(Range.greaterThan(5))
            .build();
    assertThat(setComplex, isRangeSet("[[0..2], [3..3], (5..+\u221e)]"));

    assertThat(RangeSets.minus(setAll, Range.singleton(1)),
        isRangeSet("[(-\u221e..1), (1..+\u221e)]"));
    assertThat(RangeSets.minus(setNone, Range.singleton(1)), is(setNone));
    assertThat(RangeSets.minus(setGt2, Range.singleton(1)), is(setGt2));
    assertThat(RangeSets.minus(setGt1, Range.singleton(1)), is(setGt1));
    assertThat(RangeSets.minus(setGe1, Range.singleton(1)), is(setGt1));
    assertThat(RangeSets.minus(setGt0, Range.singleton(1)),
        isRangeSet("[(0..1), (1..+\u221e)]"));
    assertThat(RangeSets.minus(setComplex, Range.singleton(1)),
        isRangeSet("[[0..1), (1..2], [3..3], (5..+\u221e)]"));
    assertThat(RangeSets.minus(setComplex, Range.singleton(2)),
        isRangeSet("[[0..2), [3..3], (5..+\u221e)]"));
    assertThat(RangeSets.minus(setComplex, Range.singleton(3)),
        isRangeSet("[[0..2], (5..+\u221e)]"));
    assertThat(RangeSets.minus(setComplex, Range.open(2, 3)),
        isRangeSet("[[0..2], [3..3], (5..+\u221e)]"));
    assertThat(RangeSets.minus(setComplex, Range.closed(2, 3)),
        isRangeSet("[[0..2), (5..+\u221e)]"));
    assertThat(RangeSets.minus(setComplex, Range.closed(2, 7)),
        isRangeSet("[[0..2), (7..+\u221e)]"));
  }

  @Test void testSimplifyOrTerms() {
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType boolType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    final RelDataType rowType = typeFactory.builder()
        .add("a", intType).nullable(false)
        .add("b", intType).nullable(true)
        .add("c", intType).nullable(true)
        .add("d", boolType).nullable(true)
        .build();

    final RexDynamicParam range = rexBuilder.makeDynamicParam(rowType, 0);
    final RexNode aRef = rexBuilder.makeFieldAccess(range, 0);
    final RexNode bRef = rexBuilder.makeFieldAccess(range, 1);
    final RexNode cRef = rexBuilder.makeFieldAccess(range, 2);
    final RexNode dRef = rexBuilder.makeFieldAccess(range, 3);
    final RexLiteral literal1 = rexBuilder.makeExactLiteral(BigDecimal.ONE);
    final RexLiteral literal2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(2));
    final RexLiteral literal3 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(3));
    final RexLiteral literal4 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(4));
    final RexLiteral literal5 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(5));

    // "a <> 1 or a = 1" ==> "true"
    checkSimplifyFilter(
        or(ne(aRef, literal1),
            eq(aRef, literal1)),
        "true");

    // "a = 1 or a <> 1" ==> "true"
    checkSimplifyFilter(
        or(eq(aRef, literal1),
            ne(aRef, literal1)),
        "true");

    // "a = 1 or a <> 2" could (and should) be simplified to "a <> 2"
    checkSimplifyFilter(
        or(eq(aRef, literal1),
            ne(aRef, literal2)),
        "<>(?0.a, 2)");

    // "a < 1 or a > 1" ==> "a <> 1"
    checkSimplifyFilter(
        or(lt(aRef, literal1),
            gt(aRef, literal1)),
        "<>(?0.a, 1)");

    // "(a >= 1 and a <= 3) or a <> 2", or equivalently
    // "a between 1 and 3 or a <> 2" ==> "true"
    checkSimplifyFilter(
        or(
            and(ge(aRef, literal1),
                le(aRef, literal3)),
            ne(aRef, literal2)),
        "true");

    // "(a >= 1 and a <= 3) or a < 4" ==> "a < 4"
    checkSimplifyFilter(
        or(
            and(ge(aRef, literal1),
                le(aRef, literal3)),
            lt(aRef, literal4)),
        "<(?0.a, 4)");

    // "(a >= 1 and a <= 2) or (a >= 4 and a <= 5) or a <> 3" ==> "a <> 3"
    checkSimplifyFilter(
        or(
            and(ge(aRef, literal1),
                le(aRef, literal2)),
            and(ge(aRef, literal4),
                le(aRef, literal5)),
            ne(aRef, literal3)),
        "<>(?0.a, 3)");

    // "(a >= 1 and a <= 2) or (a >= 4 and a <= 5) or a <> 4" ==> "true"
    checkSimplifyFilter(
        or(
            and(ge(aRef, literal1),
                le(aRef, literal2)),
            and(ge(aRef, literal4),
                le(aRef, literal5)),
            ne(aRef, literal4)),
        "true");

    // "(a >= 1 and a <= 2) or (a > 4 and a <= 5) or a <> 4" ==> "a <> 4"
    checkSimplifyFilter(
        or(
            and(ge(aRef, literal1),
                le(aRef, literal2)),
            and(gt(aRef, literal4),
                le(aRef, literal5)),
            ne(aRef, literal4)),
        "<>(?0.a, 4)");

    // "b <> 1 or b = 1" ==> "b is not null" with unknown as false
    final RexNode neOrEq =
        or(ne(bRef, literal(1)),
            eq(bRef, literal(1)));
    checkSimplifyFilter(neOrEq, "IS NOT NULL(?0.b)");

    // Careful of the excluded middle!
    // We cannot simplify "b <> 1 or b = 1" to "true" because if b is null, the
    // result is unknown.
    // TODO: "b = b" would be the best simplification.
    final RexNode simplified =
        this.simplify.simplifyUnknownAs(neOrEq, RexUnknownAs.UNKNOWN);
    assertThat(simplified.toString(),
        equalTo("OR(IS NOT NULL(?0.b), null)"));

    // "a is null or a is not null" ==> "true"
    checkSimplifyFilter(
        or(isNull(aRef),
            isNotNull(aRef)),
        "true");

    // "a is not null or a is null" ==> "true"
    checkSimplifyFilter(
        or(isNotNull(aRef),
            isNull(aRef)),
        "true");

    // "b is not null or b is null" ==> "true" (valid even though b nullable)
    checkSimplifyFilter(
        or(isNotNull(bRef),
            isNull(bRef)),
        "true");

    // "b is null b > 1 or b <= 1" ==> "true"
    checkSimplifyFilter(
        or(isNull(bRef),
            gt(bRef, literal(1)),
            le(bRef, literal(1))),
        "true");

    // "b > 1 or b <= 1 or b is null" ==> "true"
    checkSimplifyFilter(
        or(gt(bRef, literal(1)),
            le(bRef, literal(1)),
            isNull(bRef)),
        "true");

    // "b <= 1 or b > 1 or b is null" ==> "true"
    checkSimplifyFilter(
        or(le(bRef, literal(1)),
            gt(bRef, literal(1)),
            isNull(bRef)),
        "true");

    // "b < 2 or b > 0 or b is null" ==> "true"
    checkSimplifyFilter(
        or(lt(bRef, literal(2)),
            gt(bRef, literal(0)),
            isNull(bRef)),
        "true");

    // "b is not null or c is null" unchanged,
    // but "c is null" is moved to front
    checkSimplifyFilter(
        or(isNotNull(bRef),
            isNull(cRef)),
        "OR(IS NULL(?0.c), IS NOT NULL(?0.b))");

    // "d is null or d is not false" => "d is null or d"
    // (because after the first term we know that d cannot be null)
    checkSimplifyFilter(
        or(isNull(dRef),
            isNotFalse(dRef)),
        "OR(IS NULL(?0.d), ?0.d)");

    // multiple predicates are handled correctly
    checkSimplifyFilter(
        and(
            or(eq(bRef, literal(1)),
                eq(bRef, literal(2))),
            eq(bRef, literal(2)),
            eq(aRef, literal(3)),
            or(eq(aRef, literal(3)),
                eq(aRef, literal(4)))),
        "AND(=(?0.b, 2), =(?0.a, 3))");

    checkSimplify3(
        or(lt(vInt(), nullInt),
            ne(literal(0), vInt())),
        "OR(null, <>(0, ?0.int0))",
        "<>(0, ?0.int0)",
        "true");
  }

  @Test void testSimplifyRange() {
    final RexNode aRef = input(tInt(), 0);
    // ((0 < a and a <= 10) or a >= 15) and a <> 6 and a <> 12
    RexNode expr = and(
        or(
            and(lt(literal(0), aRef),
                le(aRef, literal(10))),
            ge(aRef, literal(15))),
        ne(aRef, literal(6)),
        ne(aRef, literal(12)));
    final String simplified =
        "SEARCH($0, Sarg[(0..6), (6..10], [15..+\u221e)])";
    final String expanded = "OR(AND(>($0, 0), <($0, 6)), AND(>($0, 6),"
        + " <=($0, 10)), >=($0, 15))";
    checkSimplify(expr, simplified)
        .expandedSearch(expanded);
  }

  @Test void testSimplifyRange2() {
    final RexNode aRef = input(tInt(true), 0);
    // a is null or a >= 15
    RexNode expr = or(isNull(aRef),
        ge(aRef, literal(15)));
    checkSimplify(expr, "SEARCH($0, Sarg[[15..+\u221e); NULL AS TRUE])")
        .expandedSearch("OR(IS NULL($0), >=($0, 15))");
  }

  /** Unit test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4190">[CALCITE-4190]
   * OR simplification incorrectly loses term</a>. */
  @Test void testSimplifyRange3() {
    final RexNode aRef = input(tInt(true), 0);
    // (0 < a and a <= 10) or a is null or (8 < a and a < 12) or a >= 15
    RexNode expr = or(
        and(lt(literal(0), aRef),
            le(aRef, literal(10))),
        isNull(aRef),
        and(lt(literal(8), aRef),
            lt(aRef, literal(12))),
        ge(aRef, literal(15)));
    // [CALCITE-4190] causes "or a >= 15" to disappear from the simplified form.
    final String simplified =
        "SEARCH($0, Sarg[(0..12), [15..+\u221e); NULL AS TRUE])";
    final String expanded =
        "OR(IS NULL($0), AND(>($0, 0), <($0, 12)), >=($0, 15))";
    checkSimplify(expr, simplified)
        .expandedSearch(expanded);
  }

  @Test void testSimplifyRange4() {
    final RexNode aRef = input(tInt(true), 0);
    // not (a = 3 or a = 5)
    RexNode expr = not(
        or(eq(aRef, literal(3)),
            eq(aRef, literal(5))));
    final String expected =
        "SEARCH($0, Sarg[(-\u221e..3), (3..5), (5..+\u221e)])";
    final String expanded = "AND(<>($0, 3), <>($0, 5))";
    checkSimplify(expr, expected)
        .expandedSearch(expanded);
  }

  @Test void testSimplifyRange5() {
    final RexNode aRef = input(tInt(true), 0);
    // not (a = 3 or a = 5) or a is null
    RexNode expr = or(
        not(
            or(eq(aRef, literal(3)),
                eq(aRef, literal(5)))),
        isNull(aRef));
    final String simplified =
        "SEARCH($0, Sarg[(-\u221e..3), (3..5), (5..+\u221e); NULL AS TRUE])";
    final String expanded = "OR(IS NULL($0), AND(<>($0, 3), <>($0, 5)))";
    checkSimplify(expr, simplified)
        .expandedSearch(expanded);
  }

  @Test void testSimplifyRange6() {
    // An IS NULL condition would not usually become a Sarg,
    // but here it is combined with another condition, and together they cross
    // the complexity threshold.
    final RexNode aRef = input(tInt(true), 0);
    final RexNode bRef = input(tInt(true), 1);
    // a in (1, 2) or b is null
    RexNode expr = or(eq(aRef, literal(1)), eq(aRef, literal(2)), isNull(bRef));
    final String simplified =
        "OR(IS NULL($1), SEARCH($0, Sarg[1, 2]))";
    final String expanded = "OR(IS NULL($1), =($0, 1), =($0, 2))";
    checkSimplify(expr, simplified)
        .expandedSearch(expanded);
  }

  @Test void testSimplifyRange7() {
    final RexNode aRef = input(tInt(true), 0);
    // a is not null and a > 3 and a < 10
    RexNode expr = and(
        isNotNull(aRef),
        gt(aRef, literal(3)),
        lt(aRef, literal(10)));
    final String simplified = "SEARCH($0, Sarg[(3..10); NULL AS FALSE])";
    final String expanded = "AND(IS NOT NULL($0), AND(>($0, 3), <($0, 10)))";
    checkSimplify(expr, simplified)
        .expandedSearch(expanded);
  }

  /** Unit test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4352">[CALCITE-4352]
   * OR simplification incorrectly loses term</a>. */
  @Test void testSimplifyAndIsNotNull() {
    final RexNode aRef = input(tInt(true), 0);
    final RexNode bRef = input(tInt(true), 1);
    // (0 < a and a < 10) and b is not null
    RexNode expr = and(
        and(lt(literal(0), aRef),
            lt(aRef, literal(10))),
        isNotNull(bRef));
    // [CALCITE-4352] causes "and b is not null" to disappear from the expanded
    // form.
    final String simplified = "AND(SEARCH($0, Sarg[(0..10)]), IS NOT NULL($1))";
    final String expanded = "AND(>($0, 0), <($0, 10), IS NOT NULL($1))";
    checkSimplify(expr, simplified)
        .expandedSearch(expanded);
  }

  @Test void testSimplifyAndIsNotNullWithEquality() {
    // "AND(IS NOT NULL(x), =(x, y)) => AND(IS NOT NULL(x), =(x, y)) (unknownAsFalse=false),
    // "=(x, y)" (unknownAsFalse=true)
    checkSimplify2(and(isNotNull(vInt(0)), eq(vInt(0), vInt(1))),
        "AND(IS NOT NULL(?0.int0), =(?0.int0, ?0.int1))",
        "=(?0.int0, ?0.int1)");

    // "AND(IS NOT NULL(x), =(x, y)) => "=(x, y)"
    checkSimplify(and(isNotNull(vIntNotNull(0)), eq(vIntNotNull(0), vInt(1))),
        "=(?0.notNullInt0, ?0.int1)");
  }

  @Test void testSimplifyEqualityAndNotEqualityWithOverlapping() {
    final RexLiteral literal3 = literal(3);
    final RexLiteral literal5 = literal(5);
    final RexNode intExpr = vInt(0);
    final RelDataType intType = literal3.getType();

    // "AND(<>(?0.int0, 3), =(?0.int0, 5))" => "=(?0.int0, 5)"
    checkSimplify(and(ne(intExpr, literal3), eq(intExpr, literal5)), "=(?0.int0, 5)");
    // "AND(=(?0.int0, 5), <>(?0.int0, 3))" => "=(?0.int0, 5)"
    checkSimplify(and(eq(intExpr, literal5), ne(intExpr, literal3)), "=(?0.int0, 5)");
    // "AND(=(CAST(?0.int0):INTEGER NOT NULL, 5), <>(CAST(?0.int0):INTEGER NOT NULL, 3))"
    // =>
    // "=(CAST(?0.int0):INTEGER NOT NULL, 5)"
    checkSimplify(
        and(ne(rexBuilder.makeCast(intType, intExpr, true), literal3),
                    eq(rexBuilder.makeCast(intType, intExpr, true), literal5)),
            "=(CAST(?0.int0):INTEGER NOT NULL, 5)");
    // "AND(<>(CAST(?0.int0):INTEGER NOT NULL, 3), =(CAST(?0.int0):INTEGER NOT NULL, 5))"
    // =>
    // "=(CAST(?0.int0):INTEGER NOT NULL, 5)"
    checkSimplify(
        and(ne(rexBuilder.makeCast(intType, intExpr, true), literal3),
                    eq(rexBuilder.makeCast(intType, intExpr, true), literal5)),
            "=(CAST(?0.int0):INTEGER NOT NULL, 5)");
    // "AND(<>(CAST(?0.int0):INTEGER NOT NULL, 3), =(?0.int0, 5))"
    // =>
    // "AND(<>(CAST(?0.int0):INTEGER NOT NULL, 3), =(?0.int0, 5))"
    checkSimplifyUnchanged(
        and(ne(rexBuilder.makeCast(intType, intExpr, true), literal3), eq(intExpr, literal5)));
  }

  @Test void testSimplifyAndIsNull() {
    final RexNode aRef = input(tInt(true), 0);
    final RexNode bRef = input(tInt(true), 1);
    // (0 < a and a < 10) and b is null
    RexNode expr = and(
        and(lt(literal(0), aRef),
            lt(aRef, literal(10))),
        isNull(bRef));
    // [CALCITE-4352] causes "and b is null" to disappear from the expanded
    // form.
    final String simplified = "AND(SEARCH($0, Sarg[(0..10)]), IS NULL($1))";
    final String expanded = "AND(>($0, 0), <($0, 10), IS NULL($1))";
    checkSimplify(expr, simplified)
        .expandedSearch(expanded);
  }

  @Test void testSimplifyItemRangeTerms() {
    RexNode item = item(input(tArray(tInt()), 3), literal(1));
    // paranoid validation doesn't support array types, disable it for a moment
    simplify = this.simplify.withParanoid(false);
    // (a=1 or a=2 or (arr[1]>4 and arr[1]<3 and a=3)) => a=1 or a=2
    checkSimplifyFilter(
        or(
            eq(vInt(), literal(1)),
            eq(vInt(), literal(2)),
            and(gt(item, literal(4)), lt(item, literal(3)),
                eq(vInt(), literal(3)))),
        "SEARCH(?0.int0, Sarg[1, 2])");
    simplify = simplify.withParanoid(true);
  }

  @Test void testSimplifyNotAnd() {
    final RexNode e = or(
        le(
            vBool(1),
            literal(true)),
        eq(
            literal(false),
            eq(literal(false), vBool(1))));
    checkSimplify(e, "OR(<=(?0.bool1, true), ?0.bool1)");
  }

  @Test void testSimplifyNeOrIsNullAndEq() {
    // (deptno <> 20 OR deptno IS NULL) AND deptno = 10
    //   ==>
    // deptno = 10
    final RexNode e =
        and(
            or(ne(vInt(), literal(20)),
                isNull(vInt())),
        eq(vInt(), literal(10)));
    checkSimplify(e, "=(?0.int0, 10)");
  }

  @Test void testSimplifyEqOrIsNullAndEq() {
    // (deptno = 20 OR deptno IS NULL) AND deptno = 10
    //   ==>
    // deptno <> deptno
    final RexNode e =
        and(
            or(eq(vInt(), literal(20)),
                isNull(vInt())),
        eq(vInt(), literal(10)));
    checkSimplify3(e, "<>(?0.int0, ?0.int0)", "false", "IS NULL(?0.int0)");
  }

  @Test void testSimplifyEqOrIsNullAndEqSame() {
    // (deptno = 10 OR deptno IS NULL) AND deptno = 10
    //   ==>
    // false
    final RexNode e =
        and(
            or(eq(vInt(), literal(10)),
                isNull(vInt())),
        eq(vInt(), literal(10)));
    checkSimplify(e, "=(?0.int0, 10)");
  }

  @Test void testSimplifyInAnd() {
    // deptno in (20, 10) and deptno = 10
    //   ==>
    // deptno = 10
    checkSimplify(
        and(
            in(vInt(), literal(20), literal(10)),
            eq(vInt(), literal(10))),
        "=(?0.int0, 10)");

    // deptno in (20, 10) and deptno = 30
    //   ==>
    // false
    checkSimplify3(
        and(
        in(vInt(), literal(20), literal(10)),
        eq(vInt(), literal(30))),
        "<>(?0.int0, ?0.int0)",
        "false",
        "IS NULL(?0.int0)");
  }

  @Test void testSimplifyInOr() {
    // deptno > 0 or deptno in (20, 10)
    //   ==>
    // deptno > 0
    checkSimplify(
        or(
            gt(vInt(), literal(0)),
            in(vInt(), literal(20), literal(10))),
        ">(?0.int0, 0)");
  }

  /** Test strategies for {@code SargCollector.canMerge(Sarg, RexUnknownAs)}. */
  @Test void testSargMerge() {
    checkSimplify3(
        or(ne(vInt(), literal(1)),
            eq(vInt(), literal(1))),
        "OR(IS NOT NULL(?0.int0), null)",
        "IS NOT NULL(?0.int0)",
        "true");
    checkSimplify3(
        and(gt(vInt(), literal(5)),
            lt(vInt(), literal(3))),
        "<>(?0.int0, ?0.int0)",
        "false",
        "IS NULL(?0.int0)");
    checkSimplify(
        or(falseLiteral,
            isNull(vInt())),
        "IS NULL(?0.int0)");
    checkSimplify(
        and(trueLiteral,
            isNotNull(vInt())),
        "IS NOT NULL(?0.int0)");
  }

  @Test void testSimplifyUnknown() {
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType rowType = typeFactory.builder()
        .add("a", intType).nullable(true)
        .build();

    final RexDynamicParam range = rexBuilder.makeDynamicParam(rowType, 0);
    final RexNode aRef = rexBuilder.makeFieldAccess(range, 0);

    checkSimplify2(
        and(eq(aRef, literal(1)),
            nullInt),
        "AND(=(?0.a, 1), null:INTEGER)",
        "false");
    checkSimplify3(
        and(trueLiteral,
            nullBool),
        "null:BOOLEAN",
        "false",
        "true");
    checkSimplify(
        and(falseLiteral,
            nullBool),
        "false");

    checkSimplify3(
        and(nullBool,
            eq(aRef, literal(1))),
        "AND(null, =(?0.a, 1))",
        "false",
        "=(?0.a, 1)");

    checkSimplify3(
        or(eq(aRef, literal(1)),
            nullBool),
        "OR(=(?0.a, 1), null)",
        "=(?0.a, 1)",
        "true");
    checkSimplify(
        or(trueLiteral,
            nullBool),
        "true");
    checkSimplify3(
        or(falseLiteral,
            nullBool),
        "null:BOOLEAN",
        "false",
        "true");
  }

  @Test void testSimplifyAnd3() {
    // in the case of 3-valued logic, the result must be unknown if a is unknown
    checkSimplify2(
        and(vBool(), not(vBool())),
        "AND(null, IS NULL(?0.bool0))",
        "false");
  }

  /** Unit test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2840">[CALCITE-2840]
   * Simplification should use more specific UnknownAs modes during simplification</a>. */
  @Test void testNestedAndSimplification() {
    // to have the correct mode for the AND at the bottom,
    // both the OR and AND parent should retain the UnknownAs mode
    checkSimplify(
        and(
            eq(vInt(2), literal(2)),
            or(
                eq(vInt(3), literal(3)),
                and(
                    ge(vInt(), literal(1)),
                    le(vInt(), literal(1))))),
        "AND(=(?0.int2, 2), OR(=(?0.int3, 3), =(?0.int0, 1)))");
  }

  @Test void fieldAccessEqualsHashCode() {
    assertEquals(vBool(), vBool(), "vBool() instances should be equal");
    assertEquals(vBool().hashCode(), vBool().hashCode(), "vBool().hashCode()");
    assertNotSame(vBool(), vBool(), "vBool() is expected to produce new RexFieldAccess");
    assertNotEquals(vBool(0), vBool(1), "vBool(0) != vBool(1)");
  }

  @Test void testSimplifyDynamicParam() {
    checkSimplify(or(vBool(), vBool()), "?0.bool0");
  }

  /** Unit test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1289">[CALCITE-1289]
   * RexUtil.simplifyCase() should account for nullability</a>. */
  @Test void testSimplifyCaseNotNullableBoolean() {
    RexNode condition = eq(vVarchar(), literal("S"));
    RexCall caseNode = (RexCall) case_(condition, trueLiteral, falseLiteral);

    final RexCall result = (RexCall) simplify.simplifyUnknownAs(caseNode, RexUnknownAs.UNKNOWN);
    assertThat("The case should be nonNullable", caseNode.getType().isNullable(), is(false));
    assertThat("Expected a nonNullable type", result.getType().isNullable(), is(false));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.BOOLEAN));
    assertThat(result.getOperator(), is(SqlStdOperatorTable.IS_TRUE));
    assertThat(result.getOperands().get(0), is(condition));
  }

  @Test void testSimplifyCaseNullableBoolean() {
    RexNode condition = eq(input(tVarchar(), 0), literal("S"));
    RexNode caseNode = case_(condition, trueLiteral, falseLiteral);

    RexCall result =
        (RexCall) simplify.simplifyUnknownAs(caseNode, RexUnknownAs.UNKNOWN);
    assertThat(result.getType().isNullable(), is(false));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.BOOLEAN));
    assertThat(result, is(condition));
  }

  @Test void testSimplifyRecurseIntoArithmetics() {
    checkSimplify(
        plus(literal(1),
            case_(
                falseLiteral, literal(1),
                trueLiteral, literal(2),
                literal(3))),
        "+(1, 2)");
  }

  @Test void testSimplifyCaseBranchesCollapse() {
    // case when x is true then 1 when x is not true then 1 else 2 end
    // => case when x is true or x is not true then 1 else 2 end
    checkSimplify(
        case_(
            isTrue(vBool()), literal(1),
            isNotTrue(vBool()), literal(1),
            literal(2)),
        "CASE(OR(?0.bool0, IS NOT TRUE(?0.bool0)), 1, 2)");
  }

  @Test void testSimplifyCaseBranchesCollapse2() {
    // case when x is true then 1 when true then 1 else 2 end
    // => 1
    checkSimplify(
        case_(
            isTrue(vBool()), literal(1),
            trueLiteral, literal(1),
            literal(2)),
        "1");
  }

  @Test void testSimplifyCaseNullableVarChar() {
    RexNode condition = eq(input(tVarchar(), 0), literal("S"));
    RexNode caseNode = case_(condition, literal("A"), literal("B"));

    RexCall result =
        (RexCall) simplify.simplifyUnknownAs(caseNode, RexUnknownAs.UNKNOWN);
    assertThat(result.getType().isNullable(), is(false));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.CHAR));
    assertThat(result, is(caseNode));
  }

  @Test void testSimplifyCaseCasting() {
    RexNode caseNode = case_(eq(vIntNotNull(), literal(3)), nullBool, falseLiteral);

    checkSimplify3(caseNode, "AND(=(?0.notNullInt0, 3), null)",
        "false",
        "=(?0.notNullInt0, 3)");
  }

  @Test void testSimplifyCaseAndNotSimplificationIsInAction() {
    RexNode caseNode = case_(
        eq(vIntNotNull(), literal(0)), falseLiteral,
        eq(vIntNotNull(), literal(1)), trueLiteral,
        falseLiteral);
    checkSimplify(caseNode, "=(?0.notNullInt0, 1)");
  }

  @Test void testSimplifyCaseBranchRemovalStrengthensType() {
    RexNode caseNode =
        case_(falseLiteral, nullBool, eq(div(vInt(), literal(2)), literal(3)), trueLiteral,
            falseLiteral);
    assertThat("Expected to have a nullable type for " + caseNode + ".",
        caseNode.getType().isNullable(), is(true));
    RexNode res = simplify.simplify(caseNode);
    assertThat("Expected to have a nonNullable type for " + res + ".",
        res.getType().isNullable(), is(false));
  }

  @Test void testSimplifyCaseCompaction() {
    RexNode caseNode = case_(vBool(0), vInt(0), vBool(1), vInt(0), vInt(1));
    checkSimplify(caseNode, "CASE(OR(?0.bool0, ?0.bool1), ?0.int0, ?0.int1)");
  }

  @Test void testSimplifyCaseCompaction2() {
    RexNode caseNode = case_(vBool(0), vInt(0), vBool(1), vInt(1), vInt(1));
    checkSimplify(caseNode, "CASE(?0.bool0, ?0.int0, ?0.int1)");
  }

  @Test void testSimplifyCaseCompactionDiv() {
    // FIXME: RexInterpreter currently evaluates children beforehand.
    simplify = simplify.withParanoid(false);
    RexNode caseNode = case_(vBool(0), vInt(0),
        eq(div(literal(3), vIntNotNull()), literal(11)), vInt(0),
        vInt(1));
    // expectation here is that the 2 branches are not merged.
    checkSimplifyUnchanged(caseNode);
  }

  /** Tests a CASE value branch that contains division. */
  @Test void testSimplifyCaseDiv1() {
    // FIXME: RexInterpreter currently evaluates children beforehand.
    simplify = simplify.withParanoid(false);
    RexNode caseNode = case_(
        ne(vIntNotNull(), literal(0)),
        eq(div(literal(3), vIntNotNull()), literal(11)),
        falseLiteral);
    checkSimplifyUnchanged(caseNode);
  }

  /** Tests a CASE condition that contains division. */
  @Test void testSimplifyCaseDiv2() {
    // FIXME: RexInterpreter currently evaluates children beforehand.
    simplify = simplify.withParanoid(false);
    RexNode caseNode = case_(
        eq(vIntNotNull(), literal(0)), trueLiteral,
        gt(div(literal(3), vIntNotNull()), literal(1)), trueLiteral,
        falseLiteral);
    checkSimplifyUnchanged(caseNode);
  }

  @Test void testSimplifyCaseFirstBranchIsSafe() {
    RexNode caseNode = case_(
        gt(div(vIntNotNull(), literal(1)), literal(1)), falseLiteral,
        trueLiteral);
    checkSimplify(caseNode, "<=(?0.notNullInt0, 1)");
  }

  @Test void testPushNotIntoCase() {
    checkSimplify(
        not(
            case_(
                isTrue(vBool()), vBool(1),
                gt(div(vIntNotNull(), literal(2)), literal(1)), vBool(2),
                vBool(3))),
        "CASE(?0.bool0, NOT(?0.bool1), >(/(?0.notNullInt0, 2), 1), NOT(?0.bool2), NOT(?0.bool3))");
  }

  @Test void testNotRecursion() {
    checkSimplify(
        not(coalesce(nullBool, trueLiteral)),
        "false");
  }

  @Test void testSimplifyAnd() {
    RelDataType booleanNotNullableType =
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.BOOLEAN), false);
    RelDataType booleanNullableType =
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.BOOLEAN), true);
    RexNode andCondition =
        and(rexBuilder.makeInputRef(booleanNotNullableType, 0),
            rexBuilder.makeInputRef(booleanNullableType, 1),
            rexBuilder.makeInputRef(booleanNotNullableType, 2));
    RexNode result =
        simplify.simplifyUnknownAs(andCondition, RexUnknownAs.UNKNOWN);
    assertThat(result.getType().isNullable(), is(true));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.BOOLEAN));
  }

  @Test void testSimplifyIsNotNull() {
    RelDataType intType =
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.INTEGER), false);
    RelDataType intNullableType =
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    final RexInputRef i0 = rexBuilder.makeInputRef(intNullableType, 0);
    final RexInputRef i1 = rexBuilder.makeInputRef(intNullableType, 1);
    final RexInputRef i2 = rexBuilder.makeInputRef(intType, 2);
    final RexInputRef i3 = rexBuilder.makeInputRef(intType, 3);
    final RexLiteral null_ = rexBuilder.makeNullLiteral(intType);
    checkSimplify(isNotNull(lt(i0, i1)),
        "AND(IS NOT NULL($0), IS NOT NULL($1))");
    checkSimplify(isNotNull(lt(i0, i2)), "IS NOT NULL($0)");
    checkSimplify(isNotNull(lt(i2, i3)), "true");
    checkSimplify(isNotNull(lt(i0, literal(1))), "IS NOT NULL($0)");
    checkSimplify(isNotNull(lt(i0, null_)), "false");
    // test simplify operand of case when expression
    checkSimplify(
        isNull(case_(falseLiteral, unaryPlus(i0), literal(-1))),
        "false");
    checkSimplify(
        isNull(case_(trueLiteral, unaryPlus(i0), literal(-1))),
        "IS NULL($0)");
    checkSimplify(
        isNotNull(case_(falseLiteral, unaryPlus(i0), literal(-1))),
        "true");
    checkSimplify(
        isNotNull(case_(trueLiteral, unaryPlus(i0), literal(-1))),
        "IS NOT NULL($0)");
    // test simplify operand of redundant cast
    checkSimplify(isNull(cast(i2, intType)), "false");
    checkSimplify(isNotNull(cast(i2, intType)), "true");
  }

  /**
   * Unit test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4988">[CALCITE-4988]
   * ((A IS NOT NULL OR B) AND A IS NOT NULL) can't be simplify to (A IS NOT NULL)
   * When A is deterministic</a>. */
  @Test void testSimplifyIsNotNullWithDeterministic() {
    // "(A IS NOT NULL OR B) AND A IS NOT NULL" when A is deterministic
    // ==>
    // "A IS NOT NULL"
    SqlOperator dc = getDeterministicOperator();
    checkSimplify2(
        and(or(isNotNull(rexBuilder.makeCall(dc)), gt(vInt(2), literal(2))),
            isNotNull(rexBuilder.makeCall(dc))),
        "AND(OR(IS NOT NULL(DC()), >(?0.int2, 2)), IS NOT NULL(DC()))",
        "IS NOT NULL(DC())");
  }

  @Test void testSimplifyIsNotNullWithDeterministic2() {
    // "(A IS NOT NULL AND B) OR A IS NULL" when A is deterministic
    // ==>
    // "A IS NULL OR B"
    SqlOperator dc = getDeterministicOperator();
    checkSimplify(
            or(and(isNotNull(rexBuilder.makeCall(dc)), gt(vInt(2), literal(2))),
                    isNull(rexBuilder.makeCall(dc))),
            "OR(IS NULL(DC()), >(?0.int2, 2))");
  }

  @Test void testSimplifyIsNotNullWithNoDeterministic() {
    // "(A IS NOT NULL OR B) AND A IS NOT NULL" when A is not deterministic
    // ==>
    // "(A IS NOT NULL OR B) AND A IS NOT NULL"
    SqlOperator ndc = getNoDeterministicOperator();
    checkSimplifyUnchanged(
        and(or(isNotNull(rexBuilder.makeCall(ndc)), gt(vInt(2), literal(2))),
            isNotNull(rexBuilder.makeCall(ndc))));
  }

  @Test void testSimplifyIsNotNullWithNoDeterministic2() {
    // "(A IS NOT NULL AND B) OR A IS NOT NULL" when A is not deterministic
    // ==>
    // "(A IS NOT NULL AND B) OR A IS NOT NULL"
    SqlOperator ndc = getNoDeterministicOperator();
    checkSimplifyUnchanged(
            and(or(isNotNull(rexBuilder.makeCall(ndc)), gt(vInt(2), literal(2))),
                    isNotNull(rexBuilder.makeCall(ndc))));
  }

  private SqlOperator getDeterministicOperator() {
    return new SqlSpecialOperator(
            "DC",
            SqlKind.OTHER_FUNCTION,
            0,
            false,
            ReturnTypes.BOOLEAN_FORCE_NULLABLE,
            null, null) {
      @Override public boolean isDeterministic() {
        return true;
      }
    };
  }

  private SqlOperator getNoDeterministicOperator() {
    return new SqlSpecialOperator(
            "NDC",
            SqlKind.OTHER_FUNCTION,
            0,
            false,
            ReturnTypes.BOOLEAN_FORCE_NULLABLE,
            null, null) {
      @Override public boolean isDeterministic() {
        return false;
      }
    };
  }

  /** Unit test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2929">[CALCITE-2929]
   * Simplification of IS NULL checks are incorrectly assuming that CAST-s are possible</a>. */
  @Test void testSimplifyCastIsNull() {
    checkSimplifyUnchanged(isNull(cast(vVarchar(), tInt(true))));
  }

  /** Unit test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2929">[CALCITE-2929]
   * Simplification of IS NULL checks are incorrectly assuming that CAST-s are possible</a>. */
  @Test void testSimplifyCastIsNull2() {
    checkSimplifyUnchanged(isNull(cast(vVarcharNotNull(), tInt(false))));
  }

  @Test void checkSimplifyDynamicParam() {
    checkSimplify(isNotNull(lt(vInt(0), vInt(1))),
        "AND(IS NOT NULL(?0.int0), IS NOT NULL(?0.int1))");
    checkSimplify(isNotNull(lt(vInt(0), vIntNotNull(2))),
        "IS NOT NULL(?0.int0)");
    checkSimplify(isNotNull(lt(vIntNotNull(2), vIntNotNull(3))), "true");
    checkSimplify(isNotNull(lt(vInt(0), literal(BigDecimal.ONE))),
        "IS NOT NULL(?0.int0)");
    checkSimplify(isNotNull(lt(vInt(0), null_(tInt()))), "false");
  }

  @Test void testSimplifyCastLiteral() {
    final List<RexLiteral> literals = new ArrayList<>();
    literals.add(
        rexBuilder.makeExactLiteral(BigDecimal.ONE,
            typeFactory.createSqlType(SqlTypeName.INTEGER)));
    literals.add(
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(2),
            typeFactory.createSqlType(SqlTypeName.BIGINT)));
    literals.add(
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(3),
            typeFactory.createSqlType(SqlTypeName.SMALLINT)));
    literals.add(
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(4),
            typeFactory.createSqlType(SqlTypeName.TINYINT)));
    literals.add(
        rexBuilder.makeExactLiteral(new BigDecimal("1234"),
            typeFactory.createSqlType(SqlTypeName.DECIMAL, 4, 0)));
    literals.add(
        rexBuilder.makeExactLiteral(new BigDecimal("123.45"),
            typeFactory.createSqlType(SqlTypeName.DECIMAL, 5, 2)));
    literals.add(
        rexBuilder.makeApproxLiteral(new BigDecimal("3.1415"),
            typeFactory.createSqlType(SqlTypeName.REAL)));
    literals.add(
        rexBuilder.makeApproxLiteral(BigDecimal.valueOf(Math.E),
            typeFactory.createSqlType(SqlTypeName.FLOAT)));
    literals.add(
        rexBuilder.makeApproxLiteral(BigDecimal.valueOf(Math.PI),
            typeFactory.createSqlType(SqlTypeName.DOUBLE)));
    literals.add(rexBuilder.makeLiteral(true));
    literals.add(rexBuilder.makeLiteral(false));
    literals.add(rexBuilder.makeLiteral("hello world"));
    literals.add(rexBuilder.makeLiteral("1969-07-20 12:34:56"));
    literals.add(rexBuilder.makeLiteral("1969-07-20"));
    literals.add(rexBuilder.makeLiteral("12:34:45"));
    literals.add(
        rexBuilder.makeLiteral(new ByteString(new byte[] {1, 2, -34, 0, -128}),
            typeFactory.createSqlType(SqlTypeName.BINARY, 5)));
    literals.add(rexBuilder.makeDateLiteral(new DateString(1974, 8, 9)));
    literals.add(rexBuilder.makeTimeLiteral(new TimeString(1, 23, 45), 0));
    literals.add(
        rexBuilder.makeTimestampLiteral(
            new TimestampString(1974, 8, 9, 1, 23, 45), 0));

    final Multimap<SqlTypeName, RexLiteral> map = LinkedHashMultimap.create();
    for (RexLiteral literal : literals) {
      map.put(literal.getTypeName(), literal);
    }

    final List<RelDataType> types = new ArrayList<>();
    types.add(typeFactory.createSqlType(SqlTypeName.INTEGER));
    types.add(typeFactory.createSqlType(SqlTypeName.BIGINT));
    types.add(typeFactory.createSqlType(SqlTypeName.SMALLINT));
    types.add(typeFactory.createSqlType(SqlTypeName.TINYINT));
    types.add(typeFactory.createSqlType(SqlTypeName.REAL));
    types.add(typeFactory.createSqlType(SqlTypeName.FLOAT));
    types.add(typeFactory.createSqlType(SqlTypeName.DOUBLE));
    types.add(typeFactory.createSqlType(SqlTypeName.BOOLEAN));
    types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR, 10));
    types.add(typeFactory.createSqlType(SqlTypeName.CHAR, 5));
    types.add(typeFactory.createSqlType(SqlTypeName.VARBINARY, 60));
    types.add(typeFactory.createSqlType(SqlTypeName.BINARY, 3));
    types.add(typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
    types.add(typeFactory.createSqlType(SqlTypeName.TIME));
    types.add(typeFactory.createSqlType(SqlTypeName.DATE));

    for (RelDataType fromType : types) {
      for (RelDataType toType : types) {
        if (SqlTypeAssignmentRule.instance()
            .canApplyFrom(toType.getSqlTypeName(), fromType.getSqlTypeName())) {
          for (RexLiteral literal : map.get(fromType.getSqlTypeName())) {
            final RexNode cast = rexBuilder.makeCast(toType, literal);
            if (cast instanceof RexLiteral) {
              assertThat(cast.getType(), is(toType));
              continue; // makeCast already simplified
            }
            final RexNode simplified =
                simplify.simplifyUnknownAs(cast, RexUnknownAs.UNKNOWN);
            boolean expectedSimplify =
                literal.getTypeName() != toType.getSqlTypeName()
                || (literal.getTypeName() == SqlTypeName.CHAR
                    && ((NlsString) literal.getValue()).getValue().length()
                        > toType.getPrecision())
                || (literal.getTypeName() == SqlTypeName.BINARY
                    && ((ByteString) literal.getValue()).length()
                        > toType.getPrecision());
            boolean couldSimplify = !cast.equals(simplified);
            final String reason = (expectedSimplify
                ? "expected to simplify, but could not: "
                : "simplified, but did not expect to: ")
                + cast + " --> " + simplified;
            assertThat(reason, couldSimplify, is(expectedSimplify));
          }
        }
      }
    }
  }

  @Test void testCastLiteral() {
    assertNode("cast(literal int not null)",
        "42:INTEGER NOT NULL", cast(literal(42), tInt()));
    assertNode("cast(literal int)",
        "42:INTEGER NOT NULL", cast(literal(42), nullable(tInt())));

    assertNode("abstractCast(literal int not null)",
        "CAST(42):INTEGER NOT NULL", abstractCast(literal(42), tInt()));
    assertNode("abstractCast(literal int)",
        "CAST(42):INTEGER", abstractCast(literal(42), nullable(tInt())));
  }

  @Test void testSimplifyCastLiteral2() {
    final RexLiteral literalAbc = rexBuilder.makeLiteral("abc");
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType varcharType =
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 10);
    final RelDataType booleanType =
        typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    final RelDataType dateType = typeFactory.createSqlType(SqlTypeName.DATE);
    final RelDataType timestampType =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    checkSimplifyUnchanged(cast(literalAbc, intType));
    checkSimplifyUnchanged(cast(literal(1), intType));
    checkSimplifyUnchanged(cast(literalAbc, varcharType));
    checkSimplify(cast(literal(1), varcharType), "'1':VARCHAR(10)");
    checkSimplifyUnchanged(cast(literalAbc, booleanType));
    checkSimplify(cast(literal(1), booleanType),
        "false"); // different from Hive
    checkSimplifyUnchanged(cast(literalAbc, dateType));
    checkSimplify(cast(literal(1), dateType),
        "1970-01-02"); // different from Hive
    checkSimplifyUnchanged(cast(literalAbc, timestampType));
    checkSimplify(cast(literal(1), timestampType),
        "1970-01-01 00:00:00"); // different from Hive
  }

  @Test void testSimplifyCastLiteral3() {
    // Default TimeZone is "America/Los_Angeles" (DummyDataContext)
    final RexLiteral literalDate = rexBuilder.makeDateLiteral(new DateString("2011-07-20"));
    final RexLiteral literalTime = rexBuilder.makeTimeLiteral(new TimeString("12:34:56"), 0);
    final RexLiteral literalTimestamp = rexBuilder.makeTimestampLiteral(
        new TimestampString("2011-07-20 12:34:56"), 0);
    final RexLiteral literalTimeLTZ =
        rexBuilder.makeTimeWithLocalTimeZoneLiteral(
            new TimeString(1, 23, 45), 0);
    final RexLiteral timeLTZChar1 = rexBuilder.makeLiteral("12:34:45 America/Los_Angeles");
    final RexLiteral timeLTZChar2 = rexBuilder.makeLiteral("12:34:45 UTC");
    final RexLiteral timeLTZChar3 = rexBuilder.makeLiteral("12:34:45 GMT+01");
    final RexLiteral timestampLTZChar1 = rexBuilder.makeLiteral("2011-07-20 12:34:56 Asia/Tokyo");
    final RexLiteral timestampLTZChar2 = rexBuilder.makeLiteral("2011-07-20 12:34:56 GMT+01");
    final RexLiteral timestampLTZChar3 = rexBuilder.makeLiteral("2011-07-20 12:34:56 UTC");
    final RexLiteral literalTimestampLTZ =
        rexBuilder.makeTimestampWithLocalTimeZoneLiteral(
            new TimestampString(2011, 7, 20, 8, 23, 45), 0);

    final RelDataType dateType =
        typeFactory.createSqlType(SqlTypeName.DATE);
    final RelDataType timeType =
        typeFactory.createSqlType(SqlTypeName.TIME);
    final RelDataType timestampType =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    final RelDataType timeLTZType =
        typeFactory.createSqlType(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE);
    final RelDataType timestampLTZType =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    final RelDataType varCharType =
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 40);

    checkSimplify(cast(timeLTZChar1, timeLTZType),
        "20:34:45:TIME_WITH_LOCAL_TIME_ZONE(0)");
    checkSimplify(cast(timeLTZChar2, timeLTZType),
        "12:34:45:TIME_WITH_LOCAL_TIME_ZONE(0)");
    checkSimplify(cast(timeLTZChar3, timeLTZType),
        "11:34:45:TIME_WITH_LOCAL_TIME_ZONE(0)");
    checkSimplifyUnchanged(cast(literalTimeLTZ, timeLTZType));
    checkSimplify(cast(timestampLTZChar1, timestampLTZType),
        "2011-07-20 03:34:56:TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)");
    checkSimplify(cast(timestampLTZChar2, timestampLTZType),
        "2011-07-20 11:34:56:TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)");
    checkSimplify(cast(timestampLTZChar3, timestampLTZType),
        "2011-07-20 12:34:56:TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)");
    checkSimplifyUnchanged(cast(literalTimestampLTZ, timestampLTZType));
    checkSimplify(cast(literalDate, timestampLTZType),
        "2011-07-20 07:00:00:TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)");
    checkSimplify(cast(literalTime, timestampLTZType),
        "2011-07-20 19:34:56:TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)");
    checkSimplify(cast(literalTimestamp, timestampLTZType),
        "2011-07-20 19:34:56:TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)");
    checkSimplify(cast(literalTimestamp, dateType),
        "2011-07-20");
    checkSimplify(cast(literalTimestampLTZ, dateType),
        "2011-07-20");
    checkSimplify(cast(literalTimestampLTZ, timeType),
        "01:23:45");
    checkSimplify(cast(literalTimestampLTZ, timestampType),
        "2011-07-20 01:23:45");
    checkSimplify(cast(literalTimeLTZ, timeType),
        "17:23:45");
    checkSimplify(cast(literalTime, timeLTZType),
        "20:34:56:TIME_WITH_LOCAL_TIME_ZONE(0)");
    checkSimplify(cast(literalTimestampLTZ, timeLTZType),
        "08:23:45:TIME_WITH_LOCAL_TIME_ZONE(0)");
    checkSimplify(cast(literalTimeLTZ, varCharType),
        "'17:23:45 America/Los_Angeles':VARCHAR(40)");
    checkSimplify(cast(literalTimestampLTZ, varCharType),
        "'2011-07-20 01:23:45 America/Los_Angeles':VARCHAR(40)");
    checkSimplify(cast(literalTimeLTZ, timestampType),
        "2011-07-19 18:23:45");
    checkSimplify(cast(literalTimeLTZ, timestampLTZType),
        "2011-07-20 01:23:45:TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)");
  }

  @Test void testRemovalOfNullabilityWideningCast() {
    RexNode expr = cast(isTrue(vBoolNotNull()), tBool(true));
    assertThat(expr.getType().isNullable(), is(true));
    RexNode result = simplify.simplifyUnknownAs(expr, RexUnknownAs.UNKNOWN);
    assertThat(result.getType().isNullable(), is(false));
  }

  @Test void testCompareTimestampWithTimeZone() {
    final TimestampWithTimeZoneString timestampLTZChar1 =
        new TimestampWithTimeZoneString("2011-07-20 10:34:56 America/Los_Angeles");
    final TimestampWithTimeZoneString timestampLTZChar2 =
        new TimestampWithTimeZoneString("2011-07-20 19:34:56 Europe/Rome");
    final TimestampWithTimeZoneString timestampLTZChar3 =
        new TimestampWithTimeZoneString("2011-07-20 01:34:56 Asia/Tokyo");
    final TimestampWithTimeZoneString timestampLTZChar4 =
        new TimestampWithTimeZoneString("2011-07-20 10:34:56 America/Los_Angeles");

    assertThat(timestampLTZChar1.equals(timestampLTZChar2), is(false));
    assertThat(timestampLTZChar1.equals(timestampLTZChar3), is(false));
    assertThat(timestampLTZChar1.equals(timestampLTZChar4), is(true));
  }

  @Test void testSimplifyLiterals() {
    final RexLiteral literalAbc = rexBuilder.makeLiteral("abc");
    final RexLiteral literalDef = rexBuilder.makeLiteral("def");
    final RexLiteral literalOneDotZero =
        rexBuilder.makeExactLiteral(new BigDecimal(1D));

    // Check string comparison
    checkSimplify(eq(literalAbc, literalAbc), "true");
    checkSimplify(eq(literalAbc, literalDef), "false");
    checkSimplify(ne(literalAbc, literalAbc), "false");
    checkSimplify(ne(literalAbc, literalDef), "true");
    checkSimplify(gt(literalAbc, literalDef), "false");
    checkSimplify(gt(literalDef, literalAbc), "true");
    checkSimplify(gt(literalDef, literalDef), "false");
    checkSimplify(ge(literalAbc, literalDef), "false");
    checkSimplify(ge(literalDef, literalAbc), "true");
    checkSimplify(ge(literalDef, literalDef), "true");
    checkSimplify(lt(literalAbc, literalDef), "true");
    checkSimplify(lt(literalAbc, literalDef), "true");
    checkSimplify(lt(literalDef, literalDef), "false");
    checkSimplify(le(literalAbc, literalDef), "true");
    checkSimplify(le(literalDef, literalAbc), "false");
    checkSimplify(le(literalDef, literalDef), "true");

    // Check whole number comparison
    checkSimplify(eq(literal(0), literal(1)), "false");
    checkSimplify(eq(literal(1), literal(0)), "false");
    checkSimplify(ne(literal(0), literal(1)), "true");
    checkSimplify(ne(literal(1), literal(0)), "true");
    checkSimplify(gt(literal(0), literal(1)), "false");
    checkSimplify(gt(literal(1), literal(0)), "true");
    checkSimplify(gt(literal(1), literal(1)), "false");
    checkSimplify(ge(literal(0), literal(1)), "false");
    checkSimplify(ge(literal(1), literal(0)), "true");
    checkSimplify(ge(literal(1), literal(1)), "true");
    checkSimplify(lt(literal(0), literal(1)), "true");
    checkSimplify(lt(literal(1), literal(0)), "false");
    checkSimplify(lt(literal(1), literal(1)), "false");
    checkSimplify(le(literal(0), literal(1)), "true");
    checkSimplify(le(literal(1), literal(0)), "false");
    checkSimplify(le(literal(1), literal(1)), "true");

    // Check decimal equality comparison
    checkSimplify(eq(literal(1), literalOneDotZero), "true");
    checkSimplify(eq(literalOneDotZero, literal(1)), "true");
    checkSimplify(ne(literal(1), literalOneDotZero), "false");
    checkSimplify(ne(literalOneDotZero, literal(1)), "false");

    // Check different types shouldn't change simplification
    checkSimplifyUnchanged(eq(literal(0), literalAbc));
    checkSimplifyUnchanged(eq(literalAbc, literal(0)));
    checkSimplifyUnchanged(ne(literal(0), literalAbc));
    checkSimplifyUnchanged(ne(literalAbc, literal(0)));
    checkSimplifyUnchanged(gt(literal(0), literalAbc));
    checkSimplifyUnchanged(gt(literalAbc, literal(0)));
    checkSimplifyUnchanged(ge(literal(0), literalAbc));
    checkSimplifyUnchanged(ge(literalAbc, literal(0)));
    checkSimplifyUnchanged(lt(literal(0), literalAbc));
    checkSimplifyUnchanged(lt(literalAbc, literal(0)));
    checkSimplifyUnchanged(le(literal(0), literalAbc));
    checkSimplifyUnchanged(le(literalAbc, literal(0)));
  }

  /** Unit test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2421">[CALCITE-2421]
   * RexSimplify#simplifyAnds foregoes some simplifications if unknownAsFalse
   * set to true</a>. */
  @Test void testSelfComparisons() {
    checkSimplify3(and(eq(vInt(), vInt()), eq(vInt(1), vInt(1))),
        "AND(OR(null, IS NOT NULL(?0.int0)), OR(null, IS NOT NULL(?0.int1)))",
        "AND(IS NOT NULL(?0.int0), IS NOT NULL(?0.int1))",
        "true");
    checkSimplify3(and(ne(vInt(), vInt()), ne(vInt(1), vInt(1))),
        "AND(null, IS NULL(?0.int0), IS NULL(?0.int1))",
        "false",
        "AND(IS NULL(?0.int0), IS NULL(?0.int1))");
  }

  @Test void testBooleanComparisons() {
    checkSimplify(eq(vBool(), trueLiteral), "?0.bool0");
    checkSimplify(ge(vBool(), trueLiteral), "?0.bool0");
    checkSimplify(ne(vBool(), trueLiteral), "NOT(?0.bool0)");
    checkSimplify(lt(vBool(), trueLiteral), "NOT(?0.bool0)");

    checkSimplifyUnchanged(gt(vBool(), trueLiteral));
    checkSimplifyUnchanged(le(vBool(), trueLiteral));
    checkSimplify(gt(vBoolNotNull(), trueLiteral), "false");
    checkSimplify(le(vBoolNotNull(), trueLiteral), "true");

    checkSimplify(eq(vBool(), falseLiteral), "NOT(?0.bool0)");
    checkSimplify(ne(vBool(), falseLiteral), "?0.bool0");
    checkSimplify(gt(vBool(), falseLiteral), "?0.bool0");
    checkSimplify(le(vBool(), falseLiteral), "NOT(?0.bool0)");

    checkSimplifyUnchanged(ge(vBool(), falseLiteral));
    checkSimplifyUnchanged(lt(vBool(), falseLiteral));

    checkSimplify(ge(vBoolNotNull(), falseLiteral), "true");
    checkSimplify(lt(vBoolNotNull(), falseLiteral), "false");
  }

  @Test void testSimpleDynamicVars() {
    assertTypeAndToString(
        vBool(2), "?0.bool2", "BOOLEAN");
    assertTypeAndToString(
        vBoolNotNull(0), "?0.notNullBool0", "BOOLEAN NOT NULL");

    assertTypeAndToString(
        vInt(2), "?0.int2", "INTEGER");
    assertTypeAndToString(
        vIntNotNull(0), "?0.notNullInt0", "INTEGER NOT NULL");

    assertTypeAndToString(
        vVarchar(), "?0.varchar0", "VARCHAR");
    assertTypeAndToString(
        vVarcharNotNull(9), "?0.notNullVarchar9", "VARCHAR NOT NULL");
  }

  private void assertTypeAndToString(
      RexNode rexNode, String representation, String type) {
    assertEquals(representation, rexNode.toString());
    assertEquals(type, rexNode.getType().toString()
        + (rexNode.getType().isNullable() ? "" : RelDataTypeImpl.NON_NULLABLE_SUFFIX),
        "type of " + rexNode);
  }

  @Test void testIsDeterministic() {
    SqlOperator ndc = new SqlSpecialOperator(
            "NDC",
            SqlKind.OTHER_FUNCTION,
            0,
            false,
            ReturnTypes.BOOLEAN,
            null, null) {
      @Override public boolean isDeterministic() {
        return false;
      }
    };
    RexNode n = rexBuilder.makeCall(ndc);
    assertFalse(RexUtil.isDeterministic(n));
    assertEquals(0,
            RexUtil.retainDeterministic(RelOptUtil.conjunctions(n)).size());
  }

  @Test void testConstantMap() {
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    final RelDataType decimalType = typeFactory.createSqlType(SqlTypeName.DECIMAL, 4, 2);
    final RelDataType charType = typeFactory.createSqlType(SqlTypeName.CHAR, 5);
    final RelDataType rowType = typeFactory.builder()
        .add("a", intType)
        .add("b", intType)
        .add("c", intType)
        .add("d", intType)
        .add("e", bigintType)
        .add("f", decimalType)
        .add("g", charType)
        .build();

    final RexDynamicParam range = rexBuilder.makeDynamicParam(rowType, 0);
    final RexNode aRef = rexBuilder.makeFieldAccess(range, 0);
    final RexNode bRef = rexBuilder.makeFieldAccess(range, 1);
    final RexNode cRef = rexBuilder.makeFieldAccess(range, 2);
    final RexNode dRef = rexBuilder.makeFieldAccess(range, 3);
    final RexNode eRef = rexBuilder.makeFieldAccess(range, 4);
    final RexNode fRef = rexBuilder.makeFieldAccess(range, 5);
    final RexNode gRef = rexBuilder.makeFieldAccess(range, 6);

    final ImmutableMap<RexNode, RexNode> map =
        RexUtil.predicateConstants(RexNode.class, rexBuilder,
            ImmutableList.of(eq(aRef, bRef),
                eq(cRef, literal(1)),
                eq(cRef, aRef),
                eq(dRef, eRef)));
    assertThat(getString(map),
        is("{1=?0.c, ?0.a=?0.b, ?0.b=?0.a, ?0.c=1, ?0.d=?0.e, ?0.e=?0.d}"));

    // Contradictory constraints yield no constants
    final RexNode ref0 = rexBuilder.makeInputRef(rowType, 0);
    final ImmutableMap<RexNode, RexNode> map2 =
        RexUtil.predicateConstants(RexNode.class, rexBuilder,
            ImmutableList.of(eq(ref0, literal(1)),
                eq(ref0, literal(2))));
    assertThat(getString(map2), is("{}"));

    // Contradictory constraints on field accesses SHOULD yield no constants
    // but currently there's a bug
    final ImmutableMap<RexNode, RexNode> map3 =
        RexUtil.predicateConstants(RexNode.class, rexBuilder,
            ImmutableList.of(eq(aRef, literal(1)),
                eq(aRef, literal(2))));
    assertThat(getString(map3), is("{1=?0.a, 2=?0.a}"));

    // Different precision and scale in decimal
    final ImmutableMap<RexNode, RexNode> map4 =
        RexUtil.predicateConstants(RexNode.class, rexBuilder,
            ImmutableList.of(
                eq(cast(fRef, typeFactory.createSqlType(SqlTypeName.DECIMAL, 3, 1)),
                    rexBuilder.makeExactLiteral(BigDecimal.valueOf(21.2)))));
    assertThat(
        getString(map4), is("{21.2:DECIMAL(3, 1)=CAST(?0.f):DECIMAL(3, 1) NOT NULL,"
        + " CAST(?0.f):DECIMAL(3, 1) NOT NULL=21.2:DECIMAL(3, 1)}"));

    // Different precision in char
    final ImmutableMap<RexNode, RexNode> map5 =
        RexUtil.predicateConstants(RexNode.class, rexBuilder,
            ImmutableList.of(
                eq(cast(gRef, typeFactory.createSqlType(SqlTypeName.CHAR, 3)),
                    rexBuilder.makeLiteral("abc"))));
    assertThat(
        getString(map5), is("{'abc'=CAST(?0.g):CHAR(3) NOT NULL,"
        + " CAST(?0.g):CHAR(3) NOT NULL='abc'}"));

    // Cast bigint to int
    final ImmutableMap<RexNode, RexNode> map6 =
        RexUtil.predicateConstants(RexNode.class, rexBuilder,
            ImmutableList.of(
                eq(cast(eRef, typeFactory.createSqlType(SqlTypeName.INTEGER)),
                    literal(1))));
    assertThat(
        getString(map6), is("{1=CAST(?0.e):INTEGER NOT NULL, CAST(?0.e):INTEGER NOT NULL=1}"));

    // Cast int to bigint
    final ImmutableMap<RexNode, RexNode> map7 =
        RexUtil.predicateConstants(RexNode.class, rexBuilder,
            ImmutableList.of(
                eq(cast(aRef, typeFactory.createSqlType(SqlTypeName.BIGINT)),
                    literal(1))));
    assertThat(getString(map7), is("{1=CAST(?0.a):BIGINT NOT NULL, ?0.a=1}"));
  }

  @Test void notDistinct() {
    checkSimplify(
        isFalse(isNotDistinctFrom(vBool(0), vBool(1))),
        "IS DISTINCT FROM(?0.bool0, ?0.bool1)");
  }

  /** Unit test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2505">[CALCITE-2505]
   * RexSimplify wrongly simplifies "COALESCE(+(NULL), x)" to "NULL"</a>. */
  @Test void testSimplifyCoalesce() {
    // first arg not null
    checkSimplify(coalesce(vIntNotNull(), vInt()),
        "?0.notNullInt0");
    checkSimplifyUnchanged(coalesce(vInt(), vIntNotNull()));
    // repeated arg
    checkSimplify(coalesce(vInt(), vInt()),
        "?0.int0");
    // repeated arg
    checkSimplify(coalesce(vIntNotNull(), vIntNotNull()),
        "?0.notNullInt0");
    checkSimplify(coalesce(vIntNotNull(), literal(1)), "?0.notNullInt0");
    checkSimplifyUnchanged(coalesce(vInt(), literal(1)));
    checkSimplify(
        coalesce(vInt(), plus(vInt(), vIntNotNull()), literal(1),
            vIntNotNull()),
        "COALESCE(?0.int0, +(?0.int0, ?0.notNullInt0), 1)");
    checkSimplify(coalesce(gt(nullInt, nullInt), trueLiteral),
        "true");
    checkSimplify(coalesce(unaryPlus(nullInt), unaryPlus(vInt())),
        "?0.int0");
    checkSimplifyUnchanged(coalesce(vInt(1), vInt()));

    checkSimplify(coalesce(nullInt, vInt()), "?0.int0");
    checkSimplify(coalesce(vInt(), nullInt, vInt(1)),
        "COALESCE(?0.int0, ?0.int1)");

    // first arg not null
    checkSimplify(coalesce(vDecimalNotNull(), vDecimal()),
        "?0.notNullDecimal0");
    checkSimplifyUnchanged(coalesce(vDecimal(), vDecimalNotNull()));
    // repeated arg
    checkSimplify(coalesce(vDecimal(), vDecimal()),
        "?0.decimal0");
    // repeated arg
    checkSimplify(coalesce(vDecimalNotNull(), vDecimalNotNull()),
        "?0.notNullDecimal0");
    checkSimplify(coalesce(vDecimalNotNull(), literal(1)), "?0.notNullDecimal0");
    checkSimplifyUnchanged(coalesce(vDecimal(), literal(1)));
    checkSimplify(
        coalesce(vDecimal(), plus(vDecimal(), vDecimalNotNull()), literal(1),
            vDecimalNotNull()),
        "COALESCE(?0.decimal0, +(?0.decimal0, ?0.notNullDecimal0), 1)");
    checkSimplify(coalesce(gt(nullDecimal, nullDecimal), trueLiteral),
        "true");
    checkSimplify(coalesce(unaryPlus(nullDecimal), unaryPlus(vDecimal())),
        "?0.decimal0");
    checkSimplifyUnchanged(coalesce(vDecimal(1), vDecimal()));

    checkSimplify(coalesce(nullDecimal, vDecimal()), "?0.decimal0");
    checkSimplify(coalesce(vDecimal(), nullInt, vDecimal(1)),
        "COALESCE(?0.decimal0, ?0.decimal1)");
  }

  @Test void simplifyNull() {
    checkSimplify3(nullBool, "null:BOOLEAN", "false", "true");
    // null int must not be simplified to false
    checkSimplifyUnchanged(nullInt);
  }

  /** Converts a map to a string, sorting on the string representation of its
   * keys. */
  private static String getString(ImmutableMap<RexNode, RexNode> map) {
    final TreeMap<String, RexNode> map2 = new TreeMap<>();
    for (Map.Entry<RexNode, RexNode> entry : map.entrySet()) {
      map2.put(entry.getKey().toString(), entry.getValue());
    }
    return map2.toString();
  }

  @Test void testSimplifyFalse() {
    final RelDataType booleanNullableType =
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.BOOLEAN), true);
    final RexNode booleanInput = input(booleanNullableType, 0);
    final RexNode isFalse = isFalse(booleanInput);
    final RexCall result = (RexCall) simplify(isFalse);
    assertThat(result.getType().isNullable(), is(false));
    assertThat(result.getOperator(), is(SqlStdOperatorTable.IS_FALSE));
    assertThat(result.getOperands().size(), is(1));
    assertThat(result.getOperands().get(0), is(booleanInput));

    // Make sure that IS_FALSE(IS_FALSE(nullable boolean)) != IS_TRUE(nullable boolean)
    // IS_FALSE(IS_FALSE(null)) = IS_FALSE(false) = true
    // IS_TRUE(null) = false
    final RexNode isFalseIsFalse = isFalse(isFalse);
    final RexCall result2 = (RexCall) simplify(isFalseIsFalse);
    assertThat(result2.getType().isNullable(), is(false));
    assertThat(result2.getOperator(), is(SqlStdOperatorTable.IS_NOT_FALSE));
    assertThat(result2.getOperands().size(), is(1));
    assertThat(result2.getOperands().get(0), is(booleanInput));
  }

  @Test void testSimplifyNot() {
    // "NOT(NOT(x))" => "x"
    checkSimplify(not(not(vBool())), "?0.bool0");
    // "NOT(true)"  => "false"
    checkSimplify(not(trueLiteral), "false");
    // "NOT(false)" => "true"
    checkSimplify(not(falseLiteral), "true");
    // "NOT(IS FALSE(x))" => "IS NOT FALSE(x)"
    checkSimplify3(not(isFalse(vBool())),
        "IS NOT FALSE(?0.bool0)", "IS NOT FALSE(?0.bool0)", "?0.bool0");
    // "NOT(IS TRUE(x))" => "IS NOT TRUE(x)"
    checkSimplify3(not(isTrue(vBool())),
        "IS NOT TRUE(?0.bool0)",
        "IS NOT TRUE(?0.bool0)",
        "NOT(?0.bool0)");
    // "NOT(IS NULL(x))" => "IS NOT NULL(x)"
    checkSimplify(not(isNull(vBool())), "IS NOT NULL(?0.bool0)");
    // "NOT(IS NOT NULL(x)) => "IS NULL(x)"
    checkSimplify(not(isNotNull(vBool())), "IS NULL(?0.bool0)");
    // "NOT(AND(x0,x1))" => "OR(NOT(x0),NOT(x1))"
    checkSimplify(not(and(vBool(0), vBool(1))),
        "OR(NOT(?0.bool0), NOT(?0.bool1))");
    // "NOT(OR(x0,x1))" => "AND(NOT(x0),NOT(x1))"
    checkSimplify(not(or(vBool(0), vBool(1))),
        "AND(NOT(?0.bool0), NOT(?0.bool1))");
  }

  @Test void testSimplifyAndNot() {
    // "x > 1 AND NOT (y > 2)" -> "x > 1 AND y <= 2"
    checkSimplify(and(gt(vInt(1), literal(1)), not(gt(vInt(2), literal(2)))),
        "AND(>(?0.int1, 1), <=(?0.int2, 2))");
    // "x = x AND NOT (y >= y)"
    //    -> "x = x AND y < y" (treating unknown as unknown)
    //    -> false (treating unknown as false)
    checkSimplify3(and(eq(vInt(1), vInt(1)), not(ge(vInt(2), vInt(2)))),
        "AND(OR(null, IS NOT NULL(?0.int1)), null, IS NULL(?0.int2))",
        "false",
        "IS NULL(?0.int2)");

    // "NOT(x = x AND NOT (y = y))"
    //   -> "OR(x <> x, y >= y)" (treating unknown as unknown)
    //   -> "y IS NOT NULL" (treating unknown as false)
    checkSimplify3(not(and(eq(vInt(1), vInt(1)), not(ge(vInt(2), vInt(2))))),
        "OR(AND(null, IS NULL(?0.int1)), null, IS NOT NULL(?0.int2))",
        "IS NOT NULL(?0.int2)",
        "true");
  }

  @Test void testSimplifyOrIsNull() {
    String expected = "SEARCH(?0.int0, Sarg[10; NULL AS TRUE])";
    // x = 10 OR x IS NULL
    checkSimplify(or(eq(vInt(0), literal(10)), isNull(vInt(0))), expected);
    // 10 = x OR x IS NULL
    checkSimplify(or(eq(literal(10), vInt(0)), isNull(vInt(0))), expected);
  }

  @Test void testSimplifyOrNot() {
    // "x > 1 OR NOT (y > 2)" -> "x > 1 OR y <= 2"
    checkSimplify(or(gt(vInt(1), literal(1)), not(gt(vInt(2), literal(2)))),
        "OR(>(?0.int1, 1), <=(?0.int2, 2))");

    // "x = x OR NOT (y >= y)"
    //    -> "x = x OR y < y" (treating unknown as unknown)
    //    -> "x IS NOT NULL" (treating unknown as false)
    checkSimplify3(or(eq(vInt(1), vInt(1)), not(ge(vInt(2), vInt(2)))),
        "OR(null, IS NOT NULL(?0.int1), AND(null, IS NULL(?0.int2)))",
        "IS NOT NULL(?0.int1)",
        "true");

    // "NOT(x = x OR NOT (y = y))"
    //   -> "AND(x <> x, y >= y)" (treating unknown as unknown)
    //   -> "FALSE" (treating unknown as false)
    checkSimplify3(not(or(eq(vInt(1), vInt(1)), not(ge(vInt(2), vInt(2))))),
        "AND(null, IS NULL(?0.int1), OR(null, IS NOT NULL(?0.int2)))",
        "false",
        "IS NULL(?0.int1)");
  }

  private void checkSarg(String message, Sarg sarg,
      Matcher<Integer> complexityMatcher, Matcher<String> stringMatcher) {
    assertThat(message, sarg.complexity(), complexityMatcher);
    assertThat(message, sarg.toString(), stringMatcher);
  }

  /** Tests {@link Sarg#complexity()}. */
  @SuppressWarnings("UnstableApiUsage")
  @Test void testSargComplexity() {
    checkSarg("complexity of 'x is not null'",
        Sarg.of(RexUnknownAs.FALSE, RangeSets.<Integer>rangeSetAll()),
        is(1), is("Sarg[IS NOT NULL]"));
    checkSarg("complexity of 'x is null'",
        Sarg.of(RexUnknownAs.TRUE, ImmutableRangeSet.<Integer>of()),
        is(1), is("Sarg[IS NULL]"));
    checkSarg("complexity of 'false'",
        Sarg.of(RexUnknownAs.FALSE, ImmutableRangeSet.<Integer>of()),
        is(0), is("Sarg[FALSE]"));
    checkSarg("complexity of 'true'",
        Sarg.of(RexUnknownAs.TRUE, RangeSets.<Integer>rangeSetAll()),
        is(2), is("Sarg[TRUE]"));

    checkSarg("complexity of 'x = 1'",
        Sarg.of(RexUnknownAs.UNKNOWN, ImmutableRangeSet.of(Range.singleton(1))),
        is(1), is("Sarg[1]"));
    checkSarg("complexity of 'x > 1'",
        Sarg.of(RexUnknownAs.UNKNOWN,
            ImmutableRangeSet.of(Range.greaterThan(1))),
        is(1), is("Sarg[(1..+\u221E)]"));
    checkSarg("complexity of 'x >= 1'",
        Sarg.of(RexUnknownAs.UNKNOWN, ImmutableRangeSet.of(Range.atLeast(1))),
        is(1), is("Sarg[[1..+\u221E)]"));
    checkSarg("complexity of 'x > 1 or x is null'",
        Sarg.of(RexUnknownAs.TRUE, ImmutableRangeSet.of(Range.greaterThan(1))),
        is(2), is("Sarg[(1..+\u221E); NULL AS TRUE]"));
    checkSarg("complexity of 'x <> 1'",
        Sarg.of(RexUnknownAs.UNKNOWN,
            ImmutableRangeSet.of(Range.singleton(1)).complement()),
        is(1), is("Sarg[(-\u221E..1), (1..+\u221E)]"));
    checkSarg("complexity of 'x <> 1 or x is null'",
        Sarg.of(RexUnknownAs.TRUE,
            ImmutableRangeSet.of(Range.singleton(1)).complement()),
        is(2), is("Sarg[(-\u221E..1), (1..+\u221E); NULL AS TRUE]"));
    checkSarg("complexity of 'x < 10 or x >= 20'",
        Sarg.of(RexUnknownAs.UNKNOWN,
            ImmutableRangeSet.<Integer>builder()
                .add(Range.lessThan(10))
                .add(Range.atLeast(20))
                .build()),
        is(2), is("Sarg[(-\u221E..10), [20..+\u221E)]"));
    checkSarg("complexity of 'x in (2, 4, 6) or x > 20'",
        Sarg.of(RexUnknownAs.UNKNOWN,
            ImmutableRangeSet.<Integer>builder()
                .add(Range.singleton(2))
                .add(Range.singleton(4))
                .add(Range.singleton(6))
                .add(Range.greaterThan(20))
                .build()),
        is(4), is("Sarg[2, 4, 6, (20..+\u221E)]"));
    checkSarg("complexity of 'x between 3 and 8 or x between 10 and 20'",
        Sarg.of(RexUnknownAs.UNKNOWN,
            ImmutableRangeSet.<Integer>builder()
                .add(Range.closed(3, 8))
                .add(Range.closed(10, 20))
                .build()),
        is(2), is("Sarg[[3..8], [10..20]]"));
  }

  @Test void testInterpreter() {
    assertThat(eval(trueLiteral), is(true));
    assertThat(eval(nullInt), is(NullSentinel.INSTANCE));
    assertThat(eval(eq(nullInt, nullInt)),
        is(NullSentinel.INSTANCE));
    assertThat(eval(eq(this.trueLiteral, nullInt)),
        is(NullSentinel.INSTANCE));
    assertThat(eval(eq(falseLiteral, trueLiteral)),
        is(false));
    assertThat(eval(ne(falseLiteral, trueLiteral)),
        is(true));
    assertThat(eval(ne(falseLiteral, nullInt)),
        is(NullSentinel.INSTANCE));
    assertThat(eval(and(this.trueLiteral, falseLiteral)),
        is(false));
  }

  @Test void testIsNullRecursion() {
    // make sure that simplification is visiting below isX expressions
    checkSimplify(
        isNull(or(coalesce(nullBool, trueLiteral), falseLiteral)),
        "false");
  }

  @Test void testRedundantIsTrue() {
    checkSimplify2(
        isTrue(isTrue(vBool())),
        "IS TRUE(?0.bool0)",
        "?0.bool0");
  }

  @Test void testRedundantIsFalse() {
    checkSimplify2(
        isTrue(isFalse(vBool())),
        "IS FALSE(?0.bool0)",
        "NOT(?0.bool0)");
  }

  @Test void testRedundantIsNotTrue() {
    checkSimplify3(
        isNotFalse(isNotTrue(vBool())),
        "IS NOT TRUE(?0.bool0)",
        "IS NOT TRUE(?0.bool0)",
        "NOT(?0.bool0)");
  }

  @Test void testRedundantIsNotFalse() {
    checkSimplify3(
        isNotFalse(isNotFalse(vBool())),
        "IS NOT FALSE(?0.bool0)",
        "IS NOT FALSE(?0.bool0)",
        "?0.bool0");
  }

  @Test void testSimplifyIsTrue() {
    final RexNode ref = input(tVarchar(true, 10), 0);
    checkSimplify(isTrue(like(ref, literal("%"))), "IS NOT NULL($0)");
  }

  /** Unit tests for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2438">[CALCITE-2438]
   * RexCall#isAlwaysTrue returns incorrect result</a>. */
  @Test void testIsAlwaysTrueAndFalseXisNullisNotNullisFalse() {
    // "((x IS NULL) IS NOT NULL) IS FALSE" -> false
    checkIs(isFalse(isNotNull(isNull(vBool()))), false);
  }

  @Test void testIsAlwaysTrueAndFalseNotXisNullisNotNullisFalse() {
    // "(NOT ((x IS NULL) IS NOT NULL)) IS FALSE" -> true
    checkIs(isFalse(not(isNotNull(isNull(vBool())))), true);
  }

  @Test void testIsAlwaysTrueAndFalseXisNullisNotNullisTrue() {
    // "((x IS NULL) IS NOT NULL) IS TRUE" -> true
    checkIs(isTrue(isNotNull(isNull(vBool()))), true);
  }

  @Test void testIsAlwaysTrueAndFalseNotXisNullisNotNullisTrue() {
    // "(NOT ((x IS NULL) IS NOT NULL)) IS TRUE" -> false
    checkIs(isTrue(not(isNotNull(isNull(vBool())))), false);
  }

  @Test void testIsAlwaysTrueAndFalseNotXisNullisNotNullisNotTrue() {
    // "(NOT ((x IS NULL) IS NOT NULL)) IS NOT TRUE" -> true
    checkIs(isNotTrue(not(isNotNull(isNull(vBool())))), true);
  }

  @Test void testIsAlwaysTrueAndFalseXisNullisNotNull() {
    // "(x IS NULL) IS NOT NULL" -> true
    checkIs(isNotNull(isNull(vBool())), true);
  }

  @Test void testIsAlwaysTrueAndFalseXisNotNullisNotNull() {
    // "(x IS NOT NULL) IS NOT NULL" -> true
    checkIs(isNotNull(isNotNull(vBool())), true);
  }

  @Test void testIsAlwaysTrueAndFalseXisNullisNull() {
    // "(x IS NULL) IS NULL" -> false
    checkIs(isNull(isNull(vBool())), false);
  }

  @Test void testIsAlwaysTrueAndFalseXisNotNullisNull() {
    // "(x IS NOT NULL) IS NULL" -> false
    checkIs(isNull(isNotNull(vBool())), false);
  }

  @Test void testIsAlwaysTrueAndFalseXisNullisNotNullisNotFalse() {
    // "((x IS NULL) IS NOT NULL) IS NOT FALSE" -> true
    checkIs(isNotFalse(isNotNull(isNull(vBool()))), true);
  }

  @Test void testIsAlwaysTrueAndFalseXisNullisNotNullisNotTrue() {
    // "((x IS NULL) IS NOT NULL) IS NOT TRUE" -> false
    checkIs(isNotTrue(isNotNull(isNull(vBool()))), false);
  }

  /** Unit test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2842">[CALCITE-2842]
   * Computing digest of IN expressions leads to Exceptions</a>. */
  @Test void testInDigest() {
    RexNode e = in(vInt(), literal(1), literal(2));
    assertThat(e.toString(), is("SEARCH(?0.int0, Sarg[1, 2])"));
  }

  /** Tests that {@link #in} does not generate SEARCH if any of the arguments
   * are not literals. */
  @Test void testInDigest2() {
    RexNode e = in(vInt(0), literal(1), plus(literal(2), vInt(1)));
    assertThat(e.toString(),
        is("OR(=(?0.int0, 1), =(?0.int0, +(2, ?0.int1)))"));
  }

  /** Unit test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3192">[CALCITE-3192]
   * Simplify OR incorrectly weaks condition</a>. */
  @Test void testOrSimplificationNotWeakensCondition() {
    // "1 < a or (a < 3 and b = 2)" can't be simplified if a is nullable.
    checkSimplifyUnchanged(
        or(
            lt(literal(1), vInt()),
            and(
                lt(vInt(), literal(3)),
                vBoolNotNull(2))));
  }

  @Test void testIsNullSimplificationWithUnaryPlus() {
    RexNode expr =
        isNotNull(coalesce(unaryPlus(vInt(1)), vIntNotNull(0)));
    RexNode s = simplify.simplifyUnknownAs(expr, RexUnknownAs.UNKNOWN);

    assertThat(expr.isAlwaysTrue(), is(true));
    assertThat(s, is(trueLiteral));
  }

  @Test void testIsNullSimplificationWithIsDistinctFrom() {
    RexNode expr =
        isNotNull(
            case_(vBool(),
                isDistinctFrom(falseLiteral, vBoolNotNull(0)),
                vBoolNotNull(2)));
    RexNode s = simplify.simplifyUnknownAs(expr, RexUnknownAs.UNKNOWN);

    assertThat(expr.isAlwaysTrue(), is(true));
    assertThat(s, is(trueLiteral));
  }

  @Test void testSimplifyCastUnaryMinus() {
    RexNode expr =
        isNull(ne(unaryMinus(cast(unaryMinus(vIntNotNull(1)), nullable(tInt()))), vIntNotNull(1)));
    RexNode s = simplify.simplifyUnknownAs(expr, RexUnknownAs.UNKNOWN);

    assertThat(s, is(falseLiteral));
  }

  @Test void testSimplifyUnaryMinus() {
    RexNode origExpr = vIntNotNull(1);
    RexNode expr = unaryMinus(unaryMinus(origExpr));
    RexNode simplifiedExpr = simplify.simplifyUnknownAs(expr, RexUnknownAs.UNKNOWN);
    assertThat(simplifiedExpr, is(origExpr));
  }

  @Test void testSimplifyUnaryPlus() {
    RexNode origExpr = vIntNotNull(1);
    RexNode expr = unaryPlus(origExpr);
    RexNode simplifiedExpr = simplify.simplifyUnknownAs(expr, RexUnknownAs.UNKNOWN);
    assertThat(simplifiedExpr, is(origExpr));
  }

  @Test void testSimplifyRangeWithMultiPredicates() {
    final RexNode ref = input(tInt(), 0);
    RelOptPredicateList relOptPredicateList = RelOptPredicateList.of(rexBuilder,
        ImmutableList.of(gt(ref, literal(1)), le(ref, literal(5))));
    checkSimplifyFilter(gt(ref, literal(9)), relOptPredicateList, "false");
  }

  @Test void testSimplifyNotEqual() {
    final RexNode ref = input(tInt(), 0);
    RelOptPredicateList relOptPredicateList = RelOptPredicateList.of(rexBuilder,
        ImmutableList.of(eq(ref, literal(9))));
    checkSimplifyFilter(ne(ref, literal(9)), relOptPredicateList, "false");
    checkSimplifyFilter(ne(ref, literal(5)), relOptPredicateList, "true");

    final RexNode refNullable = input(tInt(true), 0);
    checkSimplifyFilter(ne(refNullable, literal(9)), relOptPredicateList,
        "false");
    checkSimplifyFilter(ne(refNullable, literal(5)), relOptPredicateList,
        "IS NOT NULL($0)");
  }

  /** Tests
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4094">[CALCITE-4094]
   * RexSimplify should simplify more always true OR expressions</a>. */
  @Test void testSimplifyLike() {
    final RexNode ref = input(tVarchar(true, 10), 0);
    checkSimplify3(like(ref, literal("%")),
        "OR(null, IS NOT NULL($0))", "IS NOT NULL($0)", "true");
    checkSimplify3(like(ref, literal("%"), literal("#")),
        "OR(null, IS NOT NULL($0))", "IS NOT NULL($0)", "true");
    checkSimplify3(
        or(like(ref, literal("%")),
            like(ref, literal("% %"))),
        "OR(null, IS NOT NULL($0), LIKE($0, '% %'))",
        "OR(IS NOT NULL($0), LIKE($0, '% %'))", "true");
    checkSimplify(or(isNull(ref), like(ref, literal("%"))),
        "true");
    checkSimplify(or(isNull(ref), like(ref, literal("%"), literal("#"))),
        "true");
    checkSimplifyUnchanged(like(ref, literal("%A")));
    checkSimplifyUnchanged(like(ref, literal("%A"), literal("#")));

    // As above, but ref is NOT NULL
    final RexNode refMandatory = vVarcharNotNull(0);
    checkSimplify(like(refMandatory, literal("%")), "true");
    checkSimplify(
        or(like(refMandatory, literal("%")),
            like(refMandatory, literal("% %"))), "true");

    // NOT LIKE and NOT SIMILAR TO are not allowed in Rex land
    try {
      rexBuilder.makeCall(SqlStdOperatorTable.NOT_LIKE, ref, literal("%"));
    } catch (AssertionError e) {
      assertThat(e.getMessage(), is("unsupported negated operator NOT LIKE"));
    }
    try {
      rexBuilder.makeCall(SqlStdOperatorTable.NOT_SIMILAR_TO, ref, literal("%"));
    } catch (AssertionError e) {
      assertThat(e.getMessage(),
          is("unsupported negated operator NOT SIMILAR TO"));
    }

    // NOT(LIKE)
    checkSimplify3(not(like(ref, literal("%"))),
        "NOT(OR(null, IS NOT NULL($0)))", "false", "NOT(IS NOT NULL($0))");
    // SIMILAR TO is not optimized
    checkSimplifyUnchanged(
        rexBuilder.makeCall(SqlStdOperatorTable.SIMILAR_TO, ref, literal("%")));
    // NOT(SIMILAR TO) is not optimized
    checkSimplifyUnchanged(
        not(rexBuilder.makeCall(SqlStdOperatorTable.SIMILAR_TO, ref, literal("%"))));
  }

  @Test void testSimplifyNonDeterministicFunction() {
    final SqlOperator ndc = new SqlSpecialOperator(
        "NDC",
        SqlKind.OTHER_FUNCTION,
        0,
        false,
        ReturnTypes.BOOLEAN,
        null, null) {
      @Override public boolean isDeterministic() {
        return false;
      }
    };
    final RexNode call1 = rexBuilder.makeCall(ndc);
    final RexNode call2 = rexBuilder.makeCall(ndc);
    final RexNode expr = eq(call1, call2);
    checkSimplifyUnchanged(expr);
  }

  /** An operator that overrides the {@link #getStrongPolicyInference}
   * method. */
  private static class SqlSpecialOperatorWithPolicy extends SqlSpecialOperator {
    private final Strong.Policy policy;
    private SqlSpecialOperatorWithPolicy(String name, SqlKind kind, int prec, boolean leftAssoc,
        SqlReturnTypeInference returnTypeInference, SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker, Strong.Policy policy) {
      super(name, kind, prec, leftAssoc, returnTypeInference, operandTypeInference,
          operandTypeChecker);
      this.policy = policy;
    }
    @Override public Supplier<Strong.Policy> getStrongPolicyInference() {
      return () -> policy;
    }
  }

  /** Unit test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4094">[CALCITE-4094]
   * Allow SqlUserDefinedFunction to define an optional Strong.Policy</a>. */
  @Test void testSimplifyFunctionWithStrongPolicy() {
    final SqlOperator op = new SqlSpecialOperator(
        "OP1",
        SqlKind.OTHER_FUNCTION,
        0,
        false,
        ReturnTypes.BOOLEAN,
        null,
        null) {
    };
    // Operator with no Strong.Policy defined: no simplification can be made
    checkSimplifyUnchanged(rexBuilder.makeCall(op, vInt()));
    checkSimplifyUnchanged(rexBuilder.makeCall(op, vIntNotNull()));
    checkSimplifyUnchanged(rexBuilder.makeCall(op, nullInt));

    final SqlOperator opPolicyAsIs = new SqlSpecialOperatorWithPolicy(
        "OP2",
        SqlKind.OTHER_FUNCTION,
        0,
        false,
        ReturnTypes.BOOLEAN,
        null,
        null,
        Strong.Policy.AS_IS) {
    };
    // Operator with Strong.Policy.AS_IS: no simplification can be made
    checkSimplifyUnchanged(rexBuilder.makeCall(opPolicyAsIs, vInt()));
    checkSimplifyUnchanged(rexBuilder.makeCall(opPolicyAsIs, vIntNotNull()));
    checkSimplifyUnchanged(rexBuilder.makeCall(opPolicyAsIs, nullInt));

    final SqlOperator opPolicyAny = new SqlSpecialOperatorWithPolicy(
        "OP3",
        SqlKind.OTHER_FUNCTION,
        0,
        false,
        ReturnTypes.BOOLEAN,
        null,
        null,
        Strong.Policy.ANY) {
    };
    // Operator with Strong.Policy.ANY: simplification possible with null parameter
    checkSimplifyUnchanged(rexBuilder.makeCall(opPolicyAny, vInt()));
    checkSimplifyUnchanged(rexBuilder.makeCall(opPolicyAny, vIntNotNull()));
    checkSimplify3(rexBuilder.makeCall(opPolicyAny, nullInt), "null:BOOLEAN", "false", "true");
  }

  @Test void testSimplifyVarbinary() {
    checkSimplifyUnchanged(cast(cast(vInt(), tVarchar(true, 100)), tVarbinary(true)));
  }

  @Test void testSimplifySimpleArithmetic() {
    RexNode a = vIntNotNull(1);
    RexNode zero = literal(0);
    RexNode one = literal(1);

    RexNode b = vDecimalNotNull(2);
    RexNode half = literal(new BigDecimal(0.5), b.getType());

    checkSimplify(add(a, zero), "?0.notNullInt1");
    checkSimplify(add(zero, a), "?0.notNullInt1");
    checkSimplify(add(a, nullInt), "null:INTEGER");
    checkSimplify(add(nullInt, a), "null:INTEGER");

    checkSimplify(sub(a, zero), "?0.notNullInt1");
    checkSimplify(sub(a, nullInt), "null:INTEGER");

    checkSimplify(mul(a, one), "?0.notNullInt1");
    checkSimplify(mul(one, a), "?0.notNullInt1");
    checkSimplify(mul(a, nullInt), "null:INTEGER");
    checkSimplify(mul(nullInt, a), "null:INTEGER");

    checkSimplify(div(a, one), "?0.notNullInt1");
    checkSimplify(div(a, nullInt), "null:INTEGER");

    checkSimplifyUnchanged(add(b, half));

    checkSimplify(add(zero, sub(nullInt, nullInt)), "null:INTEGER");
  }
}
