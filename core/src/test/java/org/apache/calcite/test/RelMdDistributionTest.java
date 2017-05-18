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

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.metadata.RelMdDistribution;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by ted on 3/2/2016.
 */
public class RelMdDistributionTest {
  @Test public void testDistribution() {
    RelDistribution dist = RelDistributions.hash(ImmutableList.of(1, 2));
    RelDistribution newDist = RelMdDistribution.exchange(dist);
    Assert.assertEquals(dist, newDist);
  }
}
// End RelMdDistributionTest.java
