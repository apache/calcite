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
package org.apache.calcite.rel.mutable;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.RelBuilder;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for sub-classes of {@link MutableRel} to verify their equivalence.
 */
public class MutableRelNodesEquivalenceTest {

  @Test
  public void testMutableScan() {
    final RelBuilder builder = RelBuilder.create(RelBuilderTest.config().build());
    final RelNode scan1 = builder.scan("EMP").build();
    final RelNode scan2 = builder.scan("EMP").build();
    final RelNode scan3 = builder.scan("DEPT").build();

    final MutableScan mutableScan1 = (MutableScan) MutableRels.toMutable(scan1);
    final MutableScan mutableScan2 = (MutableScan) MutableRels.toMutable(scan2);
    final MutableScan mutableScan3 = (MutableScan) MutableRels.toMutable(scan3);

    Assert.assertEquals(mutableScan1, mutableScan2);
    Assert.assertNotEquals(mutableScan1, mutableScan3);
    Assert.assertNotEquals(mutableScan2, mutableScan3);
  }

}

// End MutableRelNodesEquivalenceTest.java
