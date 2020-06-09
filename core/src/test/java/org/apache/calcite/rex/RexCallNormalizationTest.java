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

import org.junit.jupiter.api.Test;

class RexCallNormalizationTest extends RexProgramTestBase {
  @Test void digestIsNormalized() {
    final RexNode node = and(or(input(tBool(), 1), input(tBool(), 0)), input(tBool(), 0));
    checkDigest(node, "AND(OR($0, $1), $0)");

    checkDigest(eq(input(tVarchar(), 0), literal("0123456789012345")),
        "=($0, '0123456789012345')");
    checkDigest(eq(input(tVarchar(), 0), literal("01")), "=($0, '01')");
  }

  @Test void reversibleSameArgOpsNormalizedToLess() {
    checkDigest(lt(input(tBool(), 0), input(tBool(), 0)), "<($0, $0)");
    checkDigest(gt(input(tBool(), 0), input(tBool(), 0)), "<($0, $0)");
    checkDigest(le(input(tBool(), 0), input(tBool(), 0)), "<=($0, $0)");
    checkDigest(ge(input(tBool(), 0), input(tBool(), 0)), "<=($0, $0)");
  }

  @Test void reversibleDifferentArgTypesShouldNotBeShuffled() {
    checkDigest(plus(input(tSmallInt(), 0), input(tInt(), 1)), "+($0, $1)");
    checkDigest(plus(input(tInt(), 0), input(tSmallInt(), 1)), "+($0, $1)");
    checkDigest(mul(input(tSmallInt(), 0), input(tInt(), 1)), "*($0, $1)");
    checkDigest(mul(input(tInt(), 0), input(tSmallInt(), 1)), "*($0, $1)");
  }

  @Test void reversibleDifferentNullabilityArgsAreNormalized() {
    checkDigest(plus(input(tInt(false), 1), input(tInt(), 0)), "+($0, $1)");
    checkDigest(plus(input(tInt(), 1), input(tInt(false), 0)), "+($0, $1)");
    checkDigest(mul(input(tInt(false), 1), input(tInt(), 0)), "*($0, $1)");
    checkDigest(mul(input(tInt(), 1), input(tInt(false), 0)), "*($0, $1)");
  }

  @Test void symmetricalDifferentArgOps() {
    for (int i = 0; i < 2; i++) {
      int j = 1 - i;
      checkDigest(eq(input(tBool(), i), input(tBool(), j)), "=($0, $1)");
      checkDigest(ne(input(tBool(), i), input(tBool(), j)), "<>($0, $1)");
    }
  }

  @Test void reversibleDifferentArgOps() {
    for (int i = 0; i < 2; i++) {
      int j = 1 - i;
      checkDigest(
          lt(input(tBool(), i), input(tBool(), j)),
          i < j
              ? "<($0, $1)"
              : ">($0, $1)");
      checkDigest(
          le(input(tBool(), i), input(tBool(), j)),
          i < j
              ? "<=($0, $1)"
              : ">=($0, $1)");
      checkDigest(
          gt(input(tBool(), i), input(tBool(), j)),
          i < j
              ? ">($0, $1)"
              : "<($0, $1)");
      checkDigest(
          ge(input(tBool(), i), input(tBool(), j)),
          i < j
              ? ">=($0, $1)"
              : "<=($0, $1)");
    }
  }
}
