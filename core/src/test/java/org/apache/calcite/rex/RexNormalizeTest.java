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

import org.hamcrest.core.IsNot;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test cases for {@link RexNormalize}. */
class RexNormalizeTest extends RexProgramTestBase {

  @Test void digestIsNormalized() {
    assertNodeEquals(
        and(or(vBool(1), vBool(0)), vBool(0)),
        and(vBool(0), or(vBool(0), vBool(1))));

    assertNodeEquals(
        and(or(vBool(1), vBool(0)), vBool(0)),
        and(or(vBool(0), vBool(1)), vBool(0)));

    assertNodeEquals(
        eq(vVarchar(0), literal("0123456789012345")),
        eq(literal("0123456789012345"), vVarchar(0)));

    assertNodeEquals(
        eq(vVarchar(0), literal("01")),
        eq(literal("01"), vVarchar(0)));
  }

  @Test void reversibleNormalizedToLess() {
    // Same type operands.
    assertNodeEquals(
        lt(vBool(0), vBool(0)),
        gt(vBool(0), vBool(0)));

    assertNodeEquals(
        le(vBool(0), vBool(0)),
        ge(vBool(0), vBool(0)));

    // Different type operands.
    assertNodeEquals(
        lt(vSmallInt(0), vInt(1)),
        gt(vInt(1), vSmallInt(0)));

    assertNodeEquals(
        le(vSmallInt(0), vInt(1)),
        ge(vInt(1), vSmallInt(0)));
  }

  @Test void reversibleDifferentArgTypesShouldNotBeShuffled() {
    assertNodeNotEqual(
        plus(vSmallInt(1), vInt(0)),
        plus(vInt(0), vSmallInt(1)));

    assertNodeNotEqual(
        mul(vSmallInt(0), vInt(1)),
        mul(vInt(1), vSmallInt(0)));
  }

  @Test void reversibleDifferentNullabilityArgsAreNormalized() {
    assertNodeEquals(
        plus(vIntNotNull(0), vInt(1)),
        plus(vInt(1), vIntNotNull(0)));

    assertNodeEquals(
        mul(vIntNotNull(1), vInt(0)),
        mul(vInt(0), vIntNotNull(1)));
  }

  @Test void symmetricalDifferentArgOps() {
    assertNodeEquals(
        eq(vBool(0), vBool(1)),
        eq(vBool(1), vBool(0)));

    assertNodeEquals(
        ne(vBool(0), vBool(1)),
        ne(vBool(1), vBool(0)));

    assertNodeEquals(
        greatest(vInt(0), vInt(1)),
        greatest(vInt(1), vInt(0)));

    assertNodeEquals(
        least(vInt(0), vInt(1)),
        least(vInt(1), vInt(0)));
  }

  @Test void reversibleDifferentArgOps() {
    assertNodeNotEqual(
        lt(vBool(0), vBool(1)),
        lt(vBool(1), vBool(0)));

    assertNodeNotEqual(
        le(vBool(0), vBool(1)),
        le(vBool(1), vBool(0)));

    assertNodeNotEqual(
        gt(vBool(0), vBool(1)),
        gt(vBool(1), vBool(0)));

    assertNodeNotEqual(
        ge(vBool(0), vBool(1)),
        ge(vBool(1), vBool(0)));
  }

  /** Asserts two rex nodes are equal. */
  private static void assertNodeEquals(RexNode node1, RexNode node2) {
    final String reason = getReason(node1, node2, true);
    assertThat(reason, node1, equalTo(node2));
    assertThat(reason, node1.hashCode(), equalTo(node2.hashCode()));
  }

  /** Asserts two rex nodes are not equal. */
  private static void assertNodeNotEqual(RexNode node1, RexNode node2) {
    final String reason = getReason(node1, node2, false);
    assertThat(reason, node1, IsNot.not(equalTo(node2)));
    assertThat(reason, node1.hashCode(), IsNot.not(equalTo(node2.hashCode())));
  }

  /** Returns the assertion reason. */
  private static String getReason(RexNode node1, RexNode node2, boolean equal) {
    StringBuilder reason = new StringBuilder("Rex nodes [");
    reason.append(node1);
    reason.append("] and [");
    reason.append(node2);
    reason.append("] expect to be ");
    if (!equal) {
      reason.append("not ");
    }
    reason.append("equal");
    return reason.toString();
  }
}
