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
package org.apache.calcite.rel.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schemas.HrClusteredSchema;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests the application of the {@code EnumerableLimitRule}.
 */
public class EnumerableLimitRuleTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2941">[CALCITE-2941]
   * EnumerableLimitRule on Sort with no collation creates EnumerableLimit with
   * wrong traitSet and cluster</a>.
   */
  @Test public void enumerableLimitOnEmptySort() throws Exception {
    RuleSet prepareRules =
        RuleSets.ofList(
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_LIMIT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus defSchema = rootSchema.add("hr", new HrClusteredSchema());
    FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(defSchema)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .programs(Programs.of(prepareRules))
        .build();

    RelBuilder builder = RelBuilder.create(config);
    RelNode planBefore = builder
        .scan("hr", "emps")
        .sort(builder.field(0)) // will produce collation [0] in the plan
        .filter(
            builder.notEquals(
                builder.field(0),
                builder.literal(100)))
        .limit(1, 5) // force a limit inside an "empty" Sort (with no collation)
        .build();

    RelTraitSet desiredTraits = planBefore.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    Program program = Programs.of(prepareRules);
    RelNode planAfter = program.run(planBefore.getCluster().getPlanner(), planBefore,
        desiredTraits, ImmutableList.of(), ImmutableList.of());

    // verify that the collation [0] is not lost in the final plan
    final RelCollation collation =
        planAfter.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
    assertThat(collation, notNullValue());
    final List<RelFieldCollation> fieldCollationList =
        collation.getFieldCollations();
    assertThat(fieldCollationList, notNullValue());
    assertThat(fieldCollationList.size(), is(1));
    assertThat(fieldCollationList.get(0).getFieldIndex(), is(0));
  }
}

// End EnumerableLimitRuleTest.java
