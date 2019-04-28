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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for {@link RexUtil}. **/
public class RexUtilTest {
  /** Creates a config based on the "scott" schema. */
  private static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT));
  }

  /** Test {@link RexUtil#simplifyCondition(RexBuilder, RexNode, RelNode)}. */
  @Test public void testSimplifyCondition() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RexBuilder rexBuilder = builder.getRexBuilder();
    final RelNode scan = builder.scan("EMP").build();
    final RexNode inputRef0 = RexInputRef.of(0, scan.getRowType());
    final RexNode inputRef1 = RexInputRef.of(1, scan.getRowType());
    final RelNode project = builder.push(scan).project(inputRef0, inputRef1).build();
    // ref1 > ref0
    final RexNode expr1 = builder.call(SqlStdOperatorTable.GREATER_THAN, inputRef1, inputRef0);

    // ref0 = ref0 => true
    final RexNode condition1 = builder.call(SqlStdOperatorTable.EQUALS, inputRef0, inputRef0);
    final RexNode simplified1 = RexUtil.simplifyCondition(rexBuilder,
        condition1, project);
    assertThat(simplified1, is(builder.literal(true)));
    // ref0 = ref0 AND ref1 > ref0 => ref1 > ref0
    final RexNode condition2 = builder.call(SqlStdOperatorTable.AND, condition1, expr1);
    final RexNode simplified2 = RexUtil.simplifyCondition(rexBuilder,
        condition2, project);
    assertThat(simplified2, is(expr1));
    // multiple ANDs
    final RexNode condition3 = builder.call(SqlStdOperatorTable.AND,
        condition1,
        builder.call(SqlStdOperatorTable.AND, condition1, expr1));
    final RexNode simplified3 = RexUtil.simplifyCondition(rexBuilder, condition3, project);
    assertThat(simplified3, is(expr1));
    // ref0 = ref0 OR ref1 > ref0 => true
    final RexNode condition4 = builder.call(SqlStdOperatorTable.OR, condition1, expr1);
    final RexNode simplified4 = RexUtil.simplifyCondition(rexBuilder,
        condition4, project);
    assertThat(simplified4, is(builder.literal(true)));
    // multiple ORs
    final RexNode condition5 = builder.call(SqlStdOperatorTable.OR,
        expr1,
        builder.call(SqlStdOperatorTable.OR, condition1, expr1));
    final RexNode simplified5 = RexUtil.simplifyCondition(rexBuilder, condition5, project);
    assertThat(simplified5, is(builder.literal(true)));
    // f0 = f0 + 1 - 1
    final RexNode condition6 = builder.call(SqlStdOperatorTable.EQUALS,
        inputRef0,
        builder.call(SqlStdOperatorTable.MINUS,
            builder.call(SqlStdOperatorTable.PLUS, inputRef0, builder.literal(1)),
            builder.literal(1)));
    final RexNode simplified6 = RexUtil.simplifyCondition(rexBuilder, condition6, project);
    assertThat(simplified6, is(builder.literal(true)));
    // f0 = f0 - 1 + 1
    final RexNode condition7 = builder.call(SqlStdOperatorTable.EQUALS,
        inputRef0,
        builder.call(SqlStdOperatorTable.PLUS,
            builder.call(SqlStdOperatorTable.MINUS, inputRef0, builder.literal(1)),
            builder.literal(1)));
    final RexNode simplified7 = RexUtil.simplifyCondition(rexBuilder, condition7, project);
    assertThat(simplified7, is(builder.literal(true)));
  }
}

// End RexUtilTest.java
