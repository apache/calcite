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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Planner rule that projects from a {@link ArrowTableScan} scan just the columns
 * needed to satisfy a projection. If the projection's expressions are trivial,
 * the projection is removed.
 *
 * @see ArrowRules#PROJECT_SCAN
 */
public class ArrowProjectRule extends ArrowConverterRule {

  /** Default configuration. */
  protected static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(LogicalProject.class, Convention.NONE,
          ArrowRel.CONVENTION, "ArrowProjectRule")
      .withRuleFactory(ArrowProjectRule::new);

  /** Creates a ArrowProjectRule. */
  protected ArrowProjectRule(Config config) {
    super(config);
  }

  @Override public RelNode convert(RelNode rel) {
    return null;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = call.rel(0);
    int[] fields = getProjectFields(project.getProjects());
    if (fields == null) {
      // Project contains expressions more complex than just field references.
      return;
    }
    final RelTraitSet traitSet = project.getTraitSet().replace(ArrowRel.CONVENTION);

    call.transformTo(
        new ArrowProject(
          project.getCluster(),
          traitSet,
          convert(project.getInput(), ArrowRel.CONVENTION),
          project.getProjects(), project.getRowType()));
  }

  private int[] getProjectFields(List<RexNode> exps) {
    final int[] fields = new int[exps.size()];
    for (int i = 0; i < exps.size(); i++) {
      final RexNode exp = exps.get(i);
      if (exp instanceof RexInputRef) {
        fields[i] = ((RexInputRef) exp).getIndex();
      } else {
        return null; // not a simple projection
      }
    }
    return fields;
  }
}

/** Base class for planner rules that convert a relational expression to
 * Arrow calling convention. */
abstract class ArrowConverterRule extends ConverterRule {
  ArrowConverterRule(Config config) {
    super(config);
  }
}
