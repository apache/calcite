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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeAssignmentRules;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link RexProgram} and
 * {@link org.apache.calcite.rex.RexProgramBuilder}.
 */
public class RexProgramTest {
  //~ Instance fields --------------------------------------------------------
  private JavaTypeFactory typeFactory;
  private RexBuilder rexBuilder;
  private RexLiteral trueLiteral;
  private RexLiteral falseLiteral;
  private RexNode nullLiteral;
  private RexNode unknownLiteral;

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a RexProgramTest.
   */
  public RexProgramTest() {
    super();
  }

  @Before
  public void setUp() {
    typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
    trueLiteral = rexBuilder.makeLiteral(true);
    falseLiteral = rexBuilder.makeLiteral(false);
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    nullLiteral = rexBuilder.makeNullLiteral(intType);
    unknownLiteral = rexBuilder.makeNullLiteral(trueLiteral.getType());
  }

  private void checkCnf(RexNode node, String expected) {
    assertThat(RexUtil.toCnf(rexBuilder, node).toString(), equalTo(expected));
  }

  private void checkThresholdCnf(RexNode node, int threshold, String expected) {
    assertThat(RexUtil.toCnf(rexBuilder, threshold, node).toString(),
        equalTo(expected));
  }

  private void checkPullFactorsUnchanged(RexNode node) {
    checkPullFactors(node, node.toString());
  }

  private void checkPullFactors(RexNode node, String expected) {
    assertThat(RexUtil.pullFactors(rexBuilder, node).toString(),
        equalTo(expected));
  }

  /** Simplifies an expression and checks that the result is as expected. */
  private void checkSimplify(RexNode node, String expected) {
    checkSimplify2(node, expected, expected);
  }

  /** Simplifies an expression and checks the result if unknowns remain
   * unknown, or if unknown becomes false. If the result is the same, use
   * {@link #checkSimplify(RexNode, String)}.
   *
   * @param node Expression to simplify
   * @param expected Expected simplification
   * @param expectedFalse Expected simplification, if unknown is to be treated
   *     as false
   */
  private void checkSimplify2(RexNode node, String expected,
      String expectedFalse) {
    assertThat(RexUtil.simplify(rexBuilder, node).toString(),
        equalTo(expected));
    if (node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
      assertThat(RexUtil.simplify(rexBuilder, node, true).toString(),
          equalTo(expectedFalse));
    }
  }

  private void checkSimplifyFilter(RexNode node, String expected) {
    assertThat(RexUtil.simplify(rexBuilder, node, true).toString(),
        equalTo(expected));
  }

  /** Returns the number of nodes (including leaves) in a Rex tree. */
  private static int nodeCount(RexNode node) {
    int n = 1;
    if (node instanceof RexCall) {
      for (RexNode operand : ((RexCall) node).getOperands()) {
        n += nodeCount(operand);
      }
    }
    return n;
  }

  private RexNode isNull(RexNode node) {
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, node);
  }

  private RexNode isNotNull(RexNode node) {
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, node);
  }

  private RexNode not(RexNode node) {
    return rexBuilder.makeCall(SqlStdOperatorTable.NOT, node);
  }

  private RexNode and(RexNode... nodes) {
    return and(ImmutableList.copyOf(nodes));
  }

  private RexNode and(Iterable<? extends RexNode> nodes) {
    // Does not flatten nested ANDs. We want test input to contain nested ANDs.
    return rexBuilder.makeCall(SqlStdOperatorTable.AND,
        ImmutableList.copyOf(nodes));
  }

  private RexNode or(RexNode... nodes) {
    return or(ImmutableList.copyOf(nodes));
  }

  private RexNode or(Iterable<? extends RexNode> nodes) {
    // Does not flatten nested ORs. We want test input to contain nested ORs.
    return rexBuilder.makeCall(SqlStdOperatorTable.OR,
        ImmutableList.copyOf(nodes));
  }

  private RexNode case_(RexNode... nodes) {
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE, nodes);
  }

  private RexNode eq(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, n1, n2);
  }

  private RexNode le(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, n1, n2);
  }

  private RexNode lt(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, n1, n2);
  }

  private RexNode ge(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, n1, n2);
  }

  private RexNode gt(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, n1, n2);
  }

  /**
   * Tests construction of a RexProgram.
   */
  @Test public void testBuildProgram() {
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
    final RexProgram normalizedProgram = program.normalize(rexBuilder, false);
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
  @Test public void testNormalize() {
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
  @Test public void testElimDups() {
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
  @Test public void testSimplifyCondition() {
    final RexProgram program = createProg(3).getProgram(false);
    assertThat(program.toString(),
        is("(expr#0..1=[{inputs}], expr#2=[+($0, 1)], expr#3=[77], "
            + "expr#4=[+($0, $1)], expr#5=[+($0, 1)], expr#6=[+($0, $t5)], "
            + "expr#7=[+($t4, $t2)], expr#8=[5], expr#9=[>($t2, $t8)], "
            + "expr#10=[true], expr#11=[IS NOT NULL($t5)], expr#12=[false], "
            + "expr#13=[null], expr#14=[CASE($t9, $t10, $t11, $t12, $t13)], "
            + "expr#15=[NOT($t14)], a=[$t7], b=[$t6], $condition=[$t15])"));

    assertThat(program.normalize(rexBuilder, true).toString(),
        is("(expr#0..1=[{inputs}], expr#2=[+($t0, $t1)], expr#3=[1], "
            + "expr#4=[+($t0, $t3)], expr#5=[+($t2, $t4)], "
            + "expr#6=[+($t0, $t4)], expr#7=[5], expr#8=[>($t4, $t7)], "
            + "expr#9=[CAST($t8):BOOLEAN], expr#10=[IS FALSE($t9)], "
            + "a=[$t5], b=[$t6], $condition=[$t10])"));
  }

  /**
   * Tests how the condition is simplified.
   */
  @Test public void testSimplifyCondition2() {
    final RexProgram program = createProg(4).getProgram(false);
    assertThat(program.toString(),
        is("(expr#0..1=[{inputs}], expr#2=[+($0, 1)], expr#3=[77], "
            + "expr#4=[+($0, $1)], expr#5=[+($0, 1)], expr#6=[+($0, $t5)], "
            + "expr#7=[+($t4, $t2)], expr#8=[5], expr#9=[>($t2, $t8)], "
            + "expr#10=[true], expr#11=[IS NOT NULL($t5)], expr#12=[false], "
            + "expr#13=[null], expr#14=[CASE($t9, $t10, $t11, $t12, $t13)], "
            + "expr#15=[NOT($t14)], expr#16=[IS TRUE($t15)], a=[$t7], b=[$t6], "
            + "$condition=[$t16])"));

    assertThat(program.normalize(rexBuilder, true).toString(),
        is("(expr#0..1=[{inputs}], expr#2=[+($t0, $t1)], expr#3=[1], "
            + "expr#4=[+($t0, $t3)], expr#5=[+($t2, $t4)], "
            + "expr#6=[+($t0, $t4)], expr#7=[5], expr#8=[>($t4, $t7)], "
            + "expr#9=[CAST($t8):BOOLEAN], expr#10=[IS FALSE($t9)], "
            + "a=[$t5], b=[$t6], $condition=[$t10])"));
  }

  /**
   * Checks translation of AND(x, x).
   */
  @Test public void testDuplicateAnd() {
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
   * Creates a program, depending on variant:
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
    final RexLiteral c1 = rexBuilder.makeExactLiteral(BigDecimal.ONE);
    final RexLiteral c5 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(5L));
    RexLocalRef t2 =
        builder.addExpr(
            rexBuilder.makeCall(
                SqlStdOperatorTable.PLUS,
                i0,
                c1));
    // $t3 = 77 (not used)
    final RexLiteral c77 =
        rexBuilder.makeExactLiteral(
            BigDecimal.valueOf(77));
    RexLocalRef t3 =
        builder.addExpr(
            c77);
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
                  i0,
                  c1));
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
      t7 = builder.addExpr(c5);
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
          builder.addExpr(unknownLiteral);
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
                rexBuilder.makeCall(SqlStdOperatorTable.IS_TRUE, t14));
        builder.addCondition(t15);
      }
    }
    return builder;
  }

  /** Unit test for {@link org.apache.calcite.plan.Strong}. */
  @Test public void testStrong() {
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
    assertThat(Strong.isNull(nullLiteral, c), is(true));
    assertThat(Strong.isNull(nullLiteral, c13), is(true));
    assertThat(Strong.isNull(unknownLiteral, c13), is(true));

    // AND is strong if one of its arguments is strong
    final RexNode andUnknownTrue = and(unknownLiteral, trueLiteral);
    final RexNode andTrueUnknown = and(trueLiteral, unknownLiteral);
    final RexNode andFalseTrue = and(falseLiteral, trueLiteral);

    assertThat(Strong.isNull(andUnknownTrue, c), is(true));
    assertThat(Strong.isNull(andTrueUnknown, c), is(true));
    assertThat(Strong.isNull(andFalseTrue, c), is(false));

    // If i0 is null, "i0 and i1 is null" is null
    assertThat(Strong.isNull(and(i0, isNull(i1)), c0), is(true));
    // If i1 is null, "i0 and i1 is null" is not necessarily null
    assertThat(Strong.isNull(and(i0, isNull(i1)), c1), is(false));
    // If i0 and i1 are both null, "i0 and i1 is null" is null
    assertThat(Strong.isNull(and(i0, isNull(i1)), c01), is(true));
    // If i0 and i1 are both null, "i0 or i1" is null
    assertThat(Strong.isNull(or(i0, i1), c01), is(true));
    // If i0 is null, "i0 or i1" is not necessarily null
    assertThat(Strong.isNull(or(i0, i1), c0), is(false));
    assertThat(Strong.isNull(or(i0, i1), c1), is(false));

    // If i0 is null, then "i0 is not null" is false
    RexNode i0NotNull = isNotNull(i0);
    assertThat(Strong.isNull(i0NotNull, c0), is(false));
    assertThat(Strong.isNotTrue(i0NotNull, c0), is(true));

    // If i0 is null, then "not(i0 is not null)" is true.
    // Join-strengthening relies on this.
    RexNode notI0NotNull = not(isNotNull(i0));
    assertThat(Strong.isNull(notI0NotNull, c0), is(false));
    assertThat(Strong.isNotTrue(notI0NotNull, c0), is(false));
  }

  /** Unit test for {@link org.apache.calcite.rex.RexUtil#toCnf}. */
  @Test public void testCnf() {
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

    final RexLiteral sevenLiteral =
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(7));
    final RexNode hEqSeven = eq(hRef, sevenLiteral);

    checkCnf(aRef, "?0.a");
    checkCnf(trueLiteral, "true");
    checkCnf(falseLiteral, "false");
    checkCnf(unknownLiteral, "null");
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
  @Test public void testCnf2() {
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

    final RexLiteral literal1 =
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(1));
    final RexLiteral literal2 =
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(2));
    final RexLiteral literal3 =
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(3));

    checkCnf(
        or(
            and(eq(xRef, literal1),
                eq(yRef, literal1),
                eq(zRef, literal1)),
            and(eq(xRef, literal2),
                eq(yRef, literal2),
                eq(aRef, literal2)),
            and(eq(xRef, literal3),
                eq(aRef, literal3),
                eq(bRef, literal3))),
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
  @Test public void testThresholdCnf() {
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType rowType = typeFactory.builder()
        .add("x", intType)
        .add("y", intType)
        .build();

    final RexDynamicParam range = rexBuilder.makeDynamicParam(rowType, 0);
    final RexNode xRef = rexBuilder.makeFieldAccess(range, 0);
    final RexNode yRef = rexBuilder.makeFieldAccess(range, 1);

    final RexLiteral literal1 =
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(1));
    final RexLiteral literal2 =
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(2));
    final RexLiteral literal3 =
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(3));
    final RexLiteral literal4 =
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(4));

    // Expression
    //   OR(=(?0.x, 1), AND(=(?0.x, 2), =(?0.y, 3)))
    // transformation creates 7 nodes
    //   AND(OR(=(?0.x, 1), =(?0.x, 2)), OR(=(?0.x, 1), =(?0.y, 3)))
    // Thus, it is triggered.
    checkThresholdCnf(
        or(eq(xRef, literal1), and(eq(xRef, literal2), eq(yRef, literal3))),
        8, "AND(OR(=(?0.x, 1), =(?0.x, 2)), OR(=(?0.x, 1), =(?0.y, 3)))");

    // Expression
    //   OR(=(?0.x, 1), =(?0.x, 2), AND(=(?0.x, 3), =(?0.y, 4)))
    // transformation creates 9 nodes
    //   AND(OR(=(?0.x, 1), =(?0.x, 2), =(?0.x, 3)),
    //       OR(=(?0.x, 1), =(?0.x, 2), =(?0.y, 8)))
    // Thus, it is NOT triggered.
    checkThresholdCnf(
        or(eq(xRef, literal1), eq(xRef, literal2),
            and(eq(xRef, literal3), eq(yRef, literal4))),
                8, "OR(=(?0.x, 1), =(?0.x, 2), AND(=(?0.x, 3), =(?0.y, 4)))");
  }

  /** Tests formulas of various sizes whose size is exponential when converted
   * to CNF. */
  @Test public void testCnfExponential() {
    // run out of memory if limit is higher than about 20
    final int limit = CalciteAssert.ENABLE_SLOW ? 16 : 6;
    for (int i = 2; i < limit; i++) {
      checkExponentialCnf(i);
    }
  }

  private void checkExponentialCnf(int n) {
    final RelDataType booleanType =
        typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    final RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
    for (int i = 0; i < n; i++) {
      builder.add("x" + i, booleanType)
          .add("y" + i, booleanType);
    }
    final RelDataType rowType3 = builder.build();
    final RexDynamicParam range3 = rexBuilder.makeDynamicParam(rowType3, 0);
    final List<RexNode> list = Lists.newArrayList();
    for (int i = 0; i < n; i++) {
      list.add(
          and(rexBuilder.makeFieldAccess(range3, i * 2),
              rexBuilder.makeFieldAccess(range3, i * 2 + 1)));
    }
    final RexNode cnf = RexUtil.toCnf(rexBuilder, or(list));
    final int nodeCount = nodeCount(cnf);
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
  @Test public void testPullFactors() {
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

    final RexLiteral sevenLiteral =
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(7));
    final RexNode hEqSeven = eq(hRef, sevenLiteral);

    // Most of the expressions in testCnf are unaffected by pullFactors.
    checkPullFactors(
        or(and(aRef, bRef),
            and(cRef, aRef, dRef, aRef)),
        "AND(?0.a, OR(?0.b, AND(?0.c, ?0.d)))");

    checkPullFactors(aRef, "?0.a");
    checkPullFactors(trueLiteral, "true");
    checkPullFactors(falseLiteral, "false");
    checkPullFactors(unknownLiteral, "null");
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

  @Test public void testSimplify() {
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
    final RexLiteral literal1 = rexBuilder.makeExactLiteral(BigDecimal.ONE);

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
        "CASE(=(?0.b, ?0.c), ?0.d, ?0.e)");

    // case: true branches become the last branch
    checkSimplify(
        case_(eq(bRef, cRef), dRef, trueLiteral, aRef, eq(cRef, dRef), eRef, cRef),
        "CASE(=(?0.b, ?0.c), ?0.d, ?0.a)");

    // case: singleton
    checkSimplify(case_(trueLiteral, aRef, eq(cRef, dRef), eRef, cRef), "?0.a");

    // case: always same value
    checkSimplify(
        case_(aRef, literal1, bRef, literal1, cRef, literal1, dRef, literal1, literal1), "1");

    // case: trailing false and null, no simplification
    checkSimplify2(
        case_(aRef, trueLiteral, bRef, trueLiteral, cRef, falseLiteral, unknownLiteral),
        "CASE(?0.a, true, ?0.b, true, ?0.c, false, null)",
        "CAST(OR(?0.a, ?0.b)):BOOLEAN");

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

    // is null, applied to not-null value
    checkSimplify(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, aRef),
        "false");

    // is not null, applied to not-null value
    checkSimplify(rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, aRef),
        "true");

    // condition, and the inverse - nothing to do due to null values
    checkSimplify2(and(le(aRef, literal1), gt(aRef, literal1)),
        "AND(<=(?0.a, 1), >(?0.a, 1))",
        "false");

    checkSimplify(and(le(aRef, literal1), ge(aRef, literal1)),
        "AND(<=(?0.a, 1), >=(?0.a, 1))");

    checkSimplify2(and(lt(aRef, literal1), eq(aRef, literal1), ge(aRef, literal1)),
        "AND(<(?0.a, 1), =(?0.a, 1), >=(?0.a, 1))",
        "false");

    checkSimplify(and(lt(aRef, literal1), or(falseLiteral, falseLiteral)),
        "false");
    checkSimplify(and(lt(aRef, literal1), or(falseLiteral, gt(bRef, cRef))),
        "AND(<(?0.a, 1), >(?0.b, ?0.c))");
    checkSimplify(or(lt(aRef, literal1), and(trueLiteral, trueLiteral)),
        "true");
    checkSimplify(
        or(lt(aRef, literal1),
            and(trueLiteral, or(trueLiteral, falseLiteral))),
        "true");
    checkSimplify(
        or(lt(aRef, literal1),
            and(trueLiteral, and(trueLiteral, falseLiteral))),
        "<(?0.a, 1)");
    checkSimplify(
        or(lt(aRef, literal1),
            and(trueLiteral, or(falseLiteral, falseLiteral))),
        "<(?0.a, 1)");
  }

  @Test public void testSimplifyFilter() {
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
    final RexLiteral literal1 = rexBuilder.makeExactLiteral(BigDecimal.ONE);
    final RexLiteral literal10 = rexBuilder.makeExactLiteral(BigDecimal.TEN);


    // condition, and the inverse
    checkSimplifyFilter(and(le(aRef, literal1), gt(aRef, literal1)),
        "false");

    checkSimplifyFilter(and(le(aRef, literal1), ge(aRef, literal1)),
        "AND(<=(?0.a, 1), >=(?0.a, 1))");

    checkSimplifyFilter(and(lt(aRef, literal1), eq(aRef, literal1), ge(aRef, literal1)),
        "false");

    // simplify equals boolean
    checkSimplifyFilter(and(eq(eq(aRef, literal1), trueLiteral), eq(bRef, literal1)),
        "AND(=(?0.a, 1), =(?0.b, 1))");

    // equality on constants, can remove the equality on the variables
    checkSimplifyFilter(and(eq(aRef, literal1), eq(bRef, literal1), eq(aRef, bRef)),
        "AND(=(?0.a, 1), =(?0.b, 1))");

    // condition not satisfiable
    checkSimplifyFilter(and(eq(aRef, literal1), eq(bRef, literal10), eq(aRef, bRef)),
        "false");

    // case: trailing false and null, remove
    checkSimplifyFilter(
        case_(aRef, trueLiteral, bRef, trueLiteral, cRef, falseLiteral, dRef, falseLiteral,
            unknownLiteral), "CAST(OR(?0.a, ?0.b)):BOOLEAN");
  }

  /** Unit test for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1289">[CALCITE-1289]
   * RexUtil.simplifyCase() should account for nullability</a>. */
  @Test public void testSimplifyCaseNotNullableBoolean() {
    RexNode condition = eq(
        rexBuilder.makeInputRef(
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.VARCHAR), true),
            0),
        rexBuilder.makeLiteral("S"));
    RexCall caseNode = (RexCall) case_(condition, trueLiteral, falseLiteral);

    RexCall result = (RexCall) RexUtil.simplify(rexBuilder, caseNode, false);
    assertThat(result.getType().isNullable(), is(false));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.BOOLEAN));
    assertThat(result.getOperator(), is((SqlOperator) SqlStdOperatorTable.CASE));
    assertThat(result.getOperands().size(), is((Object) 3));
    assertThat(result.getOperands().get(0), is(condition));
    assertThat(result.getOperands().get(1), is((RexNode) trueLiteral));
    assertThat(result.getOperands().get(2), is((RexNode) falseLiteral));
  }

  @Test public void testSimplifyCaseNullableBoolean() {
    RexNode condition = eq(
        rexBuilder.makeInputRef(
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.VARCHAR), false),
            0),
        rexBuilder.makeLiteral("S"));
    RexCall caseNode = (RexCall) case_(condition, trueLiteral, falseLiteral);

    RexCall result = (RexCall) RexUtil.simplify(rexBuilder, caseNode, false);
    assertThat(result.getType().isNullable(), is(false));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.BOOLEAN));
    assertThat(result, is(condition));
  }

  @Test public void testSimplifyCaseNullableVarChar() {
    RexNode condition = eq(
        rexBuilder.makeInputRef(
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.VARCHAR), false),
            0),
        rexBuilder.makeLiteral("S"));
    RexLiteral aLiteral = rexBuilder.makeLiteral("A");
    RexLiteral bLiteral = rexBuilder.makeLiteral("B");
    RexCall caseNode = (RexCall) case_(condition, aLiteral, bLiteral);


    RexCall result = (RexCall) RexUtil.simplify(rexBuilder, caseNode, false);
    assertThat(result.getType().isNullable(), is(false));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.CHAR));
    assertThat(result, is(caseNode));
  }

  @Test public void testSimplifyAnd() {
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
    RexNode result = RexUtil.simplify(rexBuilder, andCondition, false);
    assertThat(result.getType().isNullable(), is(true));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.BOOLEAN));
  }

  @Test public void testSimplifyIsNotNull() {
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
    final RexLiteral one = rexBuilder.makeExactLiteral(BigDecimal.ONE);
    final RexLiteral null_ = rexBuilder.makeNullLiteral(intType);
    checkSimplify(isNotNull(lt(i0, i1)),
        "AND(IS NOT NULL($0), IS NOT NULL($1))");
    checkSimplify(isNotNull(lt(i0, i2)), "IS NOT NULL($0)");
    checkSimplify(isNotNull(lt(i2, i3)), "true");
    checkSimplify(isNotNull(lt(i0, one)), "IS NOT NULL($0)");
    checkSimplify(isNotNull(lt(i0, null_)), "false");
  }

  @Test public void testSimplifyCastLiteral() {
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
    literals.add((RexLiteral)
        rexBuilder.makeLiteral(new ByteString(new byte[] {1, 2, -34, 0, -128}),
            typeFactory.createSqlType(SqlTypeName.BINARY, 5), false));
    literals.add(
        rexBuilder.makeDateLiteral(cal(1974, Calendar.AUGUST, 9, 0, 0, 0)));
    literals.add(rexBuilder.makeTimeLiteral(cal(0, 0, 0, 1, 23, 45), 0));
    literals.add(
        rexBuilder.makeTimestampLiteral(
            cal(1974, Calendar.AUGUST, 9, 1, 23, 45), 0));

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
        if (SqlTypeAssignmentRules.instance().canCastFrom(
            toType.getSqlTypeName(), fromType.getSqlTypeName(), false)) {
          for (RexLiteral literal : map.get(fromType.getSqlTypeName())) {
            final RexNode cast = rexBuilder.makeCast(toType, literal);
            if (cast instanceof RexLiteral) {
              assertThat(cast.getType(), is(toType));
              continue; // makeCast already simplified
            }
            final RexNode simplified = RexUtil.simplify(rexBuilder, cast);
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

  private Calendar cal(int y, int m, int d, int h, int mm, int s) {
    final Calendar c = Calendar.getInstance(DateTimeUtils.GMT_ZONE);
    c.set(Calendar.YEAR, y);
    c.set(Calendar.MONTH, m);
    c.set(Calendar.DAY_OF_MONTH, d);
    c.set(Calendar.HOUR_OF_DAY, h);
    c.set(Calendar.MINUTE, mm);
    c.set(Calendar.SECOND, s);
    return c;
  }

  @Test public void testConstantMap() {
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType rowType = typeFactory.builder()
        .add("a", intType)
        .add("b", intType)
        .add("c", intType)
        .add("d", intType)
        .add("e", intType)
        .build();

    final RexDynamicParam range = rexBuilder.makeDynamicParam(rowType, 0);
    final RexNode aRef = rexBuilder.makeFieldAccess(range, 0);
    final RexNode bRef = rexBuilder.makeFieldAccess(range, 1);
    final RexNode cRef = rexBuilder.makeFieldAccess(range, 2);
    final RexNode dRef = rexBuilder.makeFieldAccess(range, 3);
    final RexNode eRef = rexBuilder.makeFieldAccess(range, 4);
    final RexLiteral literal1 = rexBuilder.makeExactLiteral(BigDecimal.ONE);
    final RexLiteral literal2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(2));

    final ImmutableMap<RexNode, RexNode> map =
        RexUtil.predicateConstants(RexNode.class, rexBuilder,
            ImmutableList.of(eq(aRef, bRef),
                eq(cRef, literal1),
                eq(cRef, aRef),
                eq(dRef, eRef)));
    assertThat(getString(map),
        is("{1=?0.c, ?0.a=?0.b, ?0.b=?0.a, ?0.c=1, ?0.d=?0.e, ?0.e=?0.d}"));

    // Contradictory constraints yield no constants
    final RexNode ref0 = rexBuilder.makeInputRef(rowType, 0);
    final RexNode ref1 = rexBuilder.makeInputRef(rowType, 1);
    final ImmutableMap<RexNode, RexNode> map2 =
        RexUtil.predicateConstants(RexNode.class, rexBuilder,
            ImmutableList.of(eq(ref0, literal1),
                eq(ref0, literal2)));
    assertThat(getString(map2), is("{}"));

    // Contradictory constraints on field accesses SHOULD yield no constants
    // but currently there's a bug
    final ImmutableMap<RexNode, RexNode> map3 =
        RexUtil.predicateConstants(RexNode.class, rexBuilder,
            ImmutableList.of(eq(aRef, literal1),
                eq(aRef, literal2)));
    assertThat(getString(map3), is("{1=?0.a, 2=?0.a}"));
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
}

// End RexProgramTest.java
