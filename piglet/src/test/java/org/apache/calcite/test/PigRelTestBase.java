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

import org.apache.calcite.piglet.PigConverter;
import org.apache.calcite.piglet.PigRelBuilder;
import org.apache.calcite.piglet.PigRelPlanner;
import org.apache.calcite.piglet.PigUDFWrapper;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;

import org.junit.Before;

/**
 * Abstract class for Pig to {@link RelNode} tests.
 */
public abstract class PigRelTestBase {
  PigConverter converter;
  PigRelPlanner pigRelPlanner;
  RelOptPlanner calcitePlanner;

  @Before
  public void testSetup() throws Exception {
    final PigRelBuilder builder = PigRelBuilder.create(
        PigRelBuilderTest.config().build());
    converter = new PigConverter(builder);
    calcitePlanner = builder.getCluster().getPlanner();
    pigRelPlanner = PigRelPlanner.createPlanner(calcitePlanner);
    PigUDFWrapper.useUDFWrapper = true;
  }
}

// End PigRelTestBase.java
