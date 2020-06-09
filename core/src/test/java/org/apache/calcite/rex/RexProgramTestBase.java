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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableMap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RexProgramTestBase extends RexProgramBuilderBase {

  protected void checkDigest(RexNode node, String expected) {
    assertEquals(expected, node.toString(), () -> "Digest of " + node.toString());
  }

  protected void checkCnf(RexNode node, String expected) {
    assertThat("RexUtil.toCnf(rexBuilder, " + node + ")",
        RexUtil.toCnf(rexBuilder, node).toString(), equalTo(expected));
  }

  protected void checkThresholdCnf(RexNode node, int threshold, String expected) {
    assertThat("RexUtil.toCnf(rexBuilder, threshold=" + threshold + " , " + node + ")",
        RexUtil.toCnf(rexBuilder, threshold, node).toString(),
        equalTo(expected));
  }

  protected void checkPullFactorsUnchanged(RexNode node) {
    checkPullFactors(node, node.toString());
  }

  protected void checkPullFactors(RexNode node, String expected) {
    assertThat("RexUtil.pullFactors(rexBuilder, " + node + ")",
        RexUtil.pullFactors(rexBuilder, node).toString(),
        equalTo(expected));
  }

  /**
   * Asserts that given node has expected string representation with account of node type
   * @param message extra message that clarifies where the node came from
   * @param expected expected string representation of the node
   * @param node node to check
   */
  protected void assertNode(String message, String expected, RexNode node) {
    String actual;
    if (node.isA(SqlKind.CAST) || node.isA(SqlKind.NEW_SPECIFICATION)) {
      // toString contains type (see RexCall.toString)
      actual = node.toString();
    } else {
      actual = node + ":" + node.getType() + (node.getType().isNullable() ? "" : " NOT NULL");
    }
    assertEquals(expected, actual, message);
  }

  /** Simplifies an expression and checks that the result is as expected. */
  protected void checkSimplify(RexNode node, String expected) {
    final String nodeString = node.toString();
    checkSimplify3_(node, expected, expected, expected);
    if (expected.equals(nodeString)) {
      throw new AssertionError("expected == node.toString(); "
          + "use checkSimplifyUnchanged");
    }
  }

  /** Simplifies an expression and checks that the result is unchanged. */
  protected void checkSimplifyUnchanged(RexNode node) {
    final String expected = node.toString();
    checkSimplify3_(node, expected, expected, expected);
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
  protected void checkSimplify2(RexNode node, String expected,
                              String expectedFalse) {
    checkSimplify3_(node, expected, expectedFalse, expected);
    if (expected.equals(expectedFalse)) {
      throw new AssertionError("expected == expectedFalse; use checkSimplify");
    }
  }

  protected void checkSimplify3(RexNode node, String expected,
                              String expectedFalse, String expectedTrue) {
    checkSimplify3_(node, expected, expectedFalse, expectedTrue);
    if (expected.equals(expectedFalse) && expected.equals(expectedTrue)) {
      throw new AssertionError("expected == expectedFalse == expectedTrue; "
          + "use checkSimplify");
    }
    if (expected.equals(expectedTrue)) {
      throw new AssertionError("expected == expectedTrue; use checkSimplify2");
    }
  }

  protected void checkSimplify3_(RexNode node, String expected,
                               String expectedFalse, String expectedTrue) {
    final RexNode simplified =
        simplify.simplifyUnknownAs(node, RexUnknownAs.UNKNOWN);
    assertThat("simplify(unknown as unknown): " + node,
        simplified.toString(), equalTo(expected));
    if (node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
      final RexNode simplified2 =
          simplify.simplifyUnknownAs(node, RexUnknownAs.FALSE);
      assertThat("simplify(unknown as false): " + node,
          simplified2.toString(), equalTo(expectedFalse));
      final RexNode simplified3 =
          simplify.simplifyUnknownAs(node, RexUnknownAs.TRUE);
      assertThat("simplify(unknown as true): " + node,
          simplified3.toString(), equalTo(expectedTrue));
    } else {
      assertThat("node type is not BOOLEAN, so <<expectedFalse>> should match <<expected>>",
          expectedFalse, is(expected));
      assertThat("node type is not BOOLEAN, so <<expectedTrue>> should match <<expected>>",
          expectedTrue, is(expected));
    }
  }

  protected void checkSimplifyFilter(RexNode node, String expected) {
    final RexNode simplified =
        this.simplify.simplifyUnknownAs(node, RexUnknownAs.FALSE);
    assertThat(simplified.toString(), equalTo(expected));
  }

  protected void checkSimplifyFilter(RexNode node, RelOptPredicateList predicates,
                                   String expected) {
    final RexNode simplified =
        simplify.withPredicates(predicates)
            .simplifyUnknownAs(node, RexUnknownAs.FALSE);
    assertThat(simplified.toString(), equalTo(expected));
  }

  /** Checks that {@link RexNode#isAlwaysTrue()},
   * {@link RexNode#isAlwaysTrue()} and {@link RexSimplify} agree that
   * an expression reduces to true or false. */
  protected void checkIs(RexNode e, boolean expected) {
    assertThat(
        "isAlwaysTrue() of expression: " + e.toString(), e.isAlwaysTrue(), is(expected));
    assertThat(
        "isAlwaysFalse() of expression: " + e.toString(), e.isAlwaysFalse(), is(!expected));
    assertThat(
        "Simplification is not using isAlwaysX informations", simplify(e).toString(),
        is(expected ? "true" : "false"));
  }

  protected Comparable eval(RexNode e) {
    return RexInterpreter.evaluate(e, ImmutableMap.of());
  }

  protected RexNode simplify(RexNode e) {
    final RexSimplify simplify =
        new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, RexUtil.EXECUTOR)
            .withParanoid(true);
    return simplify.simplifyUnknownAs(e, RexUnknownAs.UNKNOWN);
  }
}
