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
    final RexNode node = and(or(vBool(1), vBool()), vBool());
    checkDigest(node, "AND(?0.bool0, OR(?0.bool0, ?0.bool1))");
    checkRaw(node, "AND(OR(?0.bool1, ?0.bool0), ?0.bool0)");

    checkDigest(eq(vVarchar(), literal("0123456789012345")),
        "=(?0.varchar0, '0123456789012345')");
    checkDigest(eq(vVarchar(), literal("01")), "=('01', ?0.varchar0)");
  }

  @Test void skipNormalizationWorks() {
    final RexNode node = and(or(vBool(1), vBool()), vBool());
    try (RexNode.Closeable ignored = RexNode.skipNormalize()) {
      checkDigest(node, "AND(OR(?0.bool1, ?0.bool0), ?0.bool0)");
      checkRaw(node, "AND(OR(?0.bool1, ?0.bool0), ?0.bool0)");
    }
  }

  @Test void skipNormalizeWorks() {
    checkDigest(and(or(vBool(1), vBool()), vBool()),
        "AND(?0.bool0, OR(?0.bool0, ?0.bool1))");
  }

  @Test void reversibleSameArgOpsNormalizedToLess() {
    checkDigest(lt(vBool(), vBool()), "<(?0.bool0, ?0.bool0)");
    checkDigest(gt(vBool(), vBool()), "<(?0.bool0, ?0.bool0)");
    checkDigest(le(vBool(), vBool()), "<=(?0.bool0, ?0.bool0)");
    checkDigest(ge(vBool(), vBool()), "<=(?0.bool0, ?0.bool0)");
  }

  @Test void reversibleDifferentArgTypesShouldNotBeShuffled() {
    checkDigest(plus(vSmallInt(), vInt()), "+(?0.smallint0, ?0.int0)");
    checkDigest(plus(vInt(), vSmallInt()), "+(?0.int0, ?0.smallint0)");
    checkDigest(mul(vSmallInt(), vInt()), "*(?0.smallint0, ?0.int0)");
    checkDigest(mul(vInt(), vSmallInt()), "*(?0.int0, ?0.smallint0)");
  }

  @Test void reversibleDifferentNullabilityArgsAreNormalized() {
    checkDigest(plus(vIntNotNull(), vInt()), "+(?0.int0, ?0.notNullInt0)");
    checkDigest(plus(vInt(), vIntNotNull()), "+(?0.int0, ?0.notNullInt0)");
    checkDigest(mul(vIntNotNull(), vInt()), "*(?0.int0, ?0.notNullInt0)");
    checkDigest(mul(vInt(), vIntNotNull()), "*(?0.int0, ?0.notNullInt0)");
  }

  @Test void symmetricalDifferentArgOps() {
    for (int i = 0; i < 2; i++) {
      int j = 1 - i;
      checkDigest(eq(vBool(i), vBool(j)), "=(?0.bool0, ?0.bool1)");
      checkDigest(ne(vBool(i), vBool(j)), "<>(?0.bool0, ?0.bool1)");
    }
  }

  @Test void reversibleDifferentArgOps() {
    for (int i = 0; i < 2; i++) {
      int j = 1 - i;
      checkDigest(
          lt(vBool(i), vBool(j)),
          i < j
              ? "<(?0.bool0, ?0.bool1)"
              : ">(?0.bool0, ?0.bool1)");
      checkDigest(
          le(vBool(i), vBool(j)),
          i < j
              ? "<=(?0.bool0, ?0.bool1)"
              : ">=(?0.bool0, ?0.bool1)");
      checkDigest(
          gt(vBool(i), vBool(j)),
          i < j
              ? ">(?0.bool0, ?0.bool1)"
              : "<(?0.bool0, ?0.bool1)");
      checkDigest(
          ge(vBool(i), vBool(j)),
          i < j
              ? ">=(?0.bool0, ?0.bool1)"
              : "<=(?0.bool0, ?0.bool1)");
    }
  }
}
