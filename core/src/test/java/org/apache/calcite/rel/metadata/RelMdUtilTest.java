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
package org.apache.calcite.rel.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link RelMdUtil}.
 */
public class RelMdUtilTest {

  @Test void testNumDistinctVals() {
    // the first element must be distinct, the second one has half chance of being distinct
    assertEquals(1.5, RelMdUtil.numDistinctVals(2.0, 2.0), 1e-5);

    // when no selection is made, we get no distinct value
    double domainSize = 100;
    assertEquals(0, RelMdUtil.numDistinctVals(domainSize, 0.0), 1e-5);

    // when we perform one selection, we always have 1 distinct value,
    // regardless of the domain size
    for (double dSize = 1; dSize < 100; dSize += 1) {
      assertEquals(1.0, RelMdUtil.numDistinctVals(dSize, 1.0), 1e-5);
    }

    // when we select n objects from a set with n values
    // we get no more than n distinct values
    for (double dSize = 1; dSize < 100; dSize += 1) {
      assertTrue(RelMdUtil.numDistinctVals(dSize, dSize) <= dSize);
    }

    // when the number of selections is large enough
    // we get all distinct values, w.h.p.
    assertEquals(domainSize, RelMdUtil.numDistinctVals(domainSize, domainSize * 100), 1e-5);
  }

}
