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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Unit Tests for code generator.
 */
public class CodeGeneratorTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6740">[CALCITE-6740]
   * Case statements may generate too many same code</a>. */
  @Test public void testCaseWhen() throws SqlParseException {
    String sql = "with \"t\" as (select case "
        + "when \"store_id\" = 1 then '1' "
        + "when \"store_id\" = 2 then '2' "
        + "when \"store_id\" = 3 then '3' "
        + "when \"store_id\" = 4 then '4' "
        + "else '0' end as \"myid\" from (values (1),(2),(3),(6)) as \"tt\"(\"store_id\"))\n"
        + "select case "
        + "when \"myid\" = '1' then '11' "
        + "when \"myid\" = '2' then '22' "
        + "when \"myid\" = '3' then '33' "
        + "when \"myid\" = '4' then '44' "
        + "else '0' end as \"res\" from \"t\"";

    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRule(CoreRules.PROJECT_TO_CALC);
    planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);

    SqlTestFactory factory = SqlTestFactory.INSTANCE.withPlannerFactory(context -> planner);
    SqlParser parser = factory.createParser(sql);
    SqlNode sqlNode = parser.parseQuery();
    SqlToRelConverter converter = factory.createSqlToRelConverter();
    SqlValidator validator = converter.validator;
    SqlNode validated = validator.validate(sqlNode);
    RelRoot relRoot = converter.convertQuery(validated, false, true);

    RelDataTypeFactory typeFactory =
        new JavaTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    RelTraitSet desiredTraits =
        cluster.traitSet().replace(EnumerableConvention.INSTANCE);

    RelNode rel = planner.changeTraits(relRoot.rel, desiredTraits);
    planner.setRoot(rel);

    EnumerableRel plan = (EnumerableRel) planner.findBestExp();
    EnumerableRelImplementor relImplementor =
        new EnumerableRelImplementor(plan.getCluster().getRexBuilder(), new HashMap<>());
    ClassDeclaration classExpr = relImplementor.implementRoot(plan, EnumerableRel.Prefer.ARRAY);
    String javaCode =
        Expressions.toString(classExpr.memberDeclarations, "\n", false);
    assertFalse(javaCode.contains("case_when_value1"));
    assertFalse(javaCode.contains("case_when_value2"));
    assertFalse(javaCode.contains("case_when_value3"));
    assertFalse(javaCode.contains("case_when_value4"));
  }
}
