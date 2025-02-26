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
package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit test for {@link RelSet}.
 */
public class RelSetTest {

  /**
   * Tests for adding RelNode with same RelDataType.
   */
  @Test void testAddRelNodeWithSameRowType() {
    RelBuilder builder = createRelBuilder();
    RelNode relNodeA =
        builder.scan("myTable").project(builder.field("a")).build();
    RelNode relNodeE =
        builder.scan("myTable").project(builder.field("e")).build();
    RelSet relSet =
        new RelSet(1,
            Util.minus(RelOptUtil.getVariablesSet(relNodeA),
                relNodeA.getVariablesSet()),
            RelOptUtil.getVariablesUsed(relNodeA));
    relSet.add(relNodeA);
    relSet.add(relNodeE);
  }

  /**
   * Tests for adding RelNode with different RelDataType.
   */
  @Test void testAddRelNodeWithDifferentRowType() {
    RelBuilder builder = createRelBuilder();
    RelNode relNodeA =
        builder.scan("myTable").project(builder.field("a")).build();
    RelNode relNodeN =
        builder.scan("myTable").project(builder.field("n1")).build();
    RelSet relSet =
        new RelSet(1,
            Util.minus(RelOptUtil.getVariablesSet(relNodeA),
                relNodeA.getVariablesSet()),
            RelOptUtil.getVariablesUsed(relNodeA));
    relSet.add(relNodeA);
    assertThrows(AssertionError.class, () -> relSet.add(relNodeN));
  }

  private RelBuilder createRelBuilder() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus defaultSchema = CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.MY_DB);
    FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(defaultSchema)
        .build();
    return RelBuilder.create(config);
  }
}
