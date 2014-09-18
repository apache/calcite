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
package org.eigenbase.test;

import java.math.BigDecimal;
import java.util.*;

import org.eigenbase.relopt.Strong;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.*;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;
import net.hydromatic.optiq.test.OptiqAssert;
import net.hydromatic.optiq.util.BitSets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link RexProgram} and
 * {@link org.eigenbase.rex.RexProgramBuilder}.
 */
public class RexProgramTest {
  //~ Instance fields --------------------------------------------------------
  private JavaTypeFactory typeFactory;
  private RexBuilder rexBuilder;

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
  }

  private void checkCnf(RexNode node, String expected) {
    assertThat(RexUtil.toCnf(rexBuilder, node).toString(), equalTo(expected));
  }

  private void checkPullFactorsUnchanged(RexNode node) {
    checkPullFactors(node, node.toString());
  }

  private void checkPullFactors(RexNode node, String expected) {
    assertThat(RexUtil.pullFactors(rexBuilder, node).toString(),
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

  private RexNode eq(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, n1, n2);
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
    final RexProgram normalizedProgram =
        RexProgramBuilder.normalize(
            rexBuilder,
            program);
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
        + "expr#8=[AND($t7, $t7)], expr#9=[AND($t8, $t7)], "
        + "a=[$t5], b=[$t6], $condition=[$t9])",
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
   * where ((x + y) > 1) and ((x + y) > 1)</code>
   * </ol>
   */
  private RexProgramBuilder createProg(int variant) {
    assert variant == 0 || variant == 1 || variant == 2;
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
    final RexLiteral c1 = rexBuilder.makeExactLiteral(
        BigDecimal.ONE);
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
    switch (variant) {
    case 0:
    case 2:
      // $t5 = $t0 + $t0 (i.e. x + x)
      t5 = builder.addExpr(
          rexBuilder.makeCall(
              SqlStdOperatorTable.PLUS,
              i0,
              i0));
      break;
    case 1:
      // $tx = $t0 + 1
      RexLocalRef tx =
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
                  tx));
      break;
    default:
      throw Util.newInternal("unexpected variant " + variant);
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

    if (variant == 2) {
      // $t7 = $t4 > $i0 (i.e. (x + y) > 0)
      RexLocalRef t7 =
          builder.addExpr(
              rexBuilder.makeCall(
                  SqlStdOperatorTable.GREATER_THAN,
                  t4,
                  i0));
      // $t8 = $t7 AND $t7
      RexLocalRef t8 =
          builder.addExpr(
              rexBuilder.makeCall(
                  SqlStdOperatorTable.AND,
                  t7,
                  t7));
      builder.addCondition(t8);
      builder.addCondition(t7);
    }
    return builder;
  }

  static boolean strongIf(RexNode e, BitSet b) {
    return Strong.is(e, b);
  }

  /** Unit test for {@link org.eigenbase.relopt.Strong}. */
  @Test public void testStrong() {
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

    final BitSet c = BitSets.of();
    final BitSet c0 = BitSets.of(0);
    final BitSet c1 = BitSets.of(1);
    final BitSet c01 = BitSets.of(0, 1);
    final BitSet c13 = BitSets.of(1, 3);

    // input ref
    final RexInputRef aRef = rexBuilder.makeInputRef(intType, 0);

    assertThat(strongIf(aRef, c0), is(true));
    assertThat(strongIf(aRef, c1), is(false));
    assertThat(strongIf(aRef, c01), is(true));
    assertThat(strongIf(aRef, c13), is(false));

    // literals are strong iff they are always null
    final RexLiteral trueLiteral = rexBuilder.makeLiteral(true);
    final RexLiteral falseLiteral = rexBuilder.makeLiteral(false);
    final RexNode nullLiteral = rexBuilder.makeNullLiteral(SqlTypeName.INTEGER);
    final RexNode unknownLiteral =
        rexBuilder.makeNullLiteral(SqlTypeName.BOOLEAN);

    assertThat(strongIf(trueLiteral, c), is(false));
    assertThat(strongIf(trueLiteral, c13), is(false));
    assertThat(strongIf(falseLiteral, c13), is(false));
    assertThat(strongIf(nullLiteral, c), is(true));
    assertThat(strongIf(nullLiteral, c13), is(true));
    assertThat(strongIf(unknownLiteral, c13), is(true));

    // AND is strong if one of its arguments is strong
    final RexNode andUnknownTrue =
        rexBuilder.makeCall(SqlStdOperatorTable.AND,
            unknownLiteral, trueLiteral);
    final RexNode andTrueUnknown =
        rexBuilder.makeCall(SqlStdOperatorTable.AND,
            trueLiteral, unknownLiteral);
    final RexNode andFalseTrue =
        rexBuilder.makeCall(SqlStdOperatorTable.AND,
            falseLiteral, trueLiteral);

    assertThat(strongIf(andUnknownTrue, c), is(true));
    assertThat(strongIf(andTrueUnknown, c), is(true));
    assertThat(strongIf(andFalseTrue, c), is(false));
  }

  /** Unit test for {@link org.eigenbase.rex.RexUtil#toCnf}. */
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

    final RexLiteral trueLiteral = rexBuilder.makeLiteral(true);
    final RexLiteral falseLiteral = rexBuilder.makeLiteral(false);
    final RexNode unknownLiteral =
        rexBuilder.makeNullLiteral(SqlTypeName.BOOLEAN);

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

    checkCnf(and(aRef, or(bRef,
            and(cRef, or(dRef,
                and(eRef, or(fRef, gRef)))))),
        "AND(?0.a, OR(?0.b, ?0.c), OR(?0.b, ?0.d, ?0.e), OR(?0.b, ?0.d, ?0.f, ?0.g))");

    checkCnf(and(aRef, or(bRef,
            and(cRef, or(dRef,
                and(eRef, or(fRef,
                    and(gRef, or(trueLiteral, falseLiteral)))))))),
        "AND(?0.a, OR(?0.b, ?0.c), OR(?0.b, ?0.d, ?0.e), OR(?0.b, ?0.d, ?0.f, ?0.g))");
  }

  /** Unit test for
   * <a href="https://issues.apache.org/jira/browse/OPTIQ-394">OPTIQ-394,
   * "Add RexUtil.toCnf, to convert expressions to conjunctive normal form
   * (CNF)"</a>. */
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

  /** Tests formulas of various sizes whose size is exponential when converted
   * to CNF. */
  @Test public void testCnfExponential() {
    // run out of memory if limit is higher than about 20
    final int limit = OptiqAssert.ENABLE_SLOW ? 16 : 6;
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
          equalTo(
              "AND(OR(?0.x0, ?0.x1, ?0.x2), OR(?0.x0, ?0.x1, ?0.y2),"
              + " OR(?0.x0, ?0.y1, ?0.x2), OR(?0.x0, ?0.y1, ?0.y2),"
              + " OR(?0.y0, ?0.x1, ?0.x2), OR(?0.y0, ?0.x1, ?0.y2),"
              + " OR(?0.y0, ?0.y1, ?0.x2), OR(?0.y0, ?0.y1, ?0.y2))"));
    }
  }

  /** Unit test for {@link org.eigenbase.rex.RexUtil#pullFactors}. */
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

    final RexLiteral trueLiteral = rexBuilder.makeLiteral(true);
    final RexLiteral falseLiteral = rexBuilder.makeLiteral(false);
    final RexNode unknownLiteral =
        rexBuilder.makeNullLiteral(SqlTypeName.BOOLEAN);

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
                and(cRef, or(dRef,
                    and(eRef, or(fRef, gRef)))))));

    checkPullFactorsUnchanged(
        and(aRef,
            or(bRef,
                and(cRef, or(dRef,
                    and(eRef, or(fRef,
                        and(gRef, or(trueLiteral, falseLiteral)))))))));
  }
}

// End RexProgramTest.java
