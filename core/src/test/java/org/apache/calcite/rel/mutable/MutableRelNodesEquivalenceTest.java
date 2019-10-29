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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.test.ScannableTableTest;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

/**
 * Unit test for sub-classes of {@link MutableRel} to verify their equivalence.
 */
public class MutableRelNodesEquivalenceTest {

  @Test
  public void testMutableScan() {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final SchemaPlus schema = config.getDefaultSchema();
    final RelBuilder builder = RelBuilder.create(config);
    schema.add("scott.EMP", new ScannableTableTest.SimpleTable());

    assertThat(mutableScanOf(builder, "EMP"),
        equalTo(mutableScanOf(builder, "EMP")));
    assertThat(mutableScanOf(builder, "scott.EMP"),
        equalTo(mutableScanOf(builder, "scott.EMP")));
    assertThat(mutableScanOf(builder, "scott", "scott.EMP"),
        equalTo(mutableScanOf(builder, "scott.EMP")));
    assertThat(mutableScanOf(builder, "scott", "scott.EMP"),
        equalTo(mutableScanOf(builder, "scott", "scott.EMP")));

    assertThat(mutableScanOf(builder, "EMP"),
        not(equalTo(mutableScanOf(builder, "DEPT"))));
    assertThat(mutableScanOf(builder, "EMP"),
        not(equalTo(mutableScanOf(builder, "scott.EMP"))));
    assertThat(mutableScanOf(builder, "scott", "EMP"),
        not(equalTo(mutableScanOf(builder, "scott.EMP"))));
    assertThat(mutableScanOf(builder, "scott", "EMP"),
        not(equalTo(mutableScanOf(builder, "scott", "scott.EMP"))));
  }

  private MutableScan mutableScanOf(RelBuilder builder, String... tableNames) {
    final RelNode scan = builder.scan(tableNames).build();
    return (MutableScan) MutableRels.toMutable(scan);
  }

}

// End MutableRelNodesEquivalenceTest.java
