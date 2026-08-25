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
package org.apache.calcite.piglet;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import org.junit.jupiter.api.Test;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit test for {@link PigRelBuilder}.
 */
class PigRelBuilderTest {
  private static FrameworkConfig config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT))
        .build();
  }

  /** A plain table scan has no Pig operator and no alias for
   * {@link PigRelBuilder#collect()} to carry over to the projection it adds. */
  @Test void collectAcceptsAnInputWithoutPigOperator() {
    final PigRelBuilder builder = PigRelBuilder.create(config());
    builder.scan("DEPT");
    final RelNode root = builder.collect().build();
    final String expected = ""
        + "LogicalProject($f1=[$1])\n"
        + "  LogicalAggregate(group=[{0}], agg#0=[COLLECT($1)])\n"
        + "    LogicalProject($f0=['all'], $f1=[ROW($0, $1, $2)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }
}
