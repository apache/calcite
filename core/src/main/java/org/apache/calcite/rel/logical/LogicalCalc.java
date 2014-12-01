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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.List;
import java.util.Set;

/**
 * A relational expression which computes project expressions and also filters.
 *
 * <p>This relational expression combines the functionality of
 * {@link LogicalProject} and {@link LogicalFilter}.
 * It should be created in the later
 * stages of optimization, by merging consecutive {@link LogicalProject} and
 * {@link LogicalFilter} nodes together.
 *
 * <p>The following rules relate to <code>LogicalCalc</code>:</p>
 *
 * <ul>
 * <li>{@link FilterToCalcRule} creates this from a {@link LogicalFilter}
 * <li>{@link ProjectToCalcRule} creates this from a {@link LogicalFilter}
 * <li>{@link org.apache.calcite.rel.rules.FilterCalcMergeRule}
 *     merges this with a {@link LogicalFilter}
 * <li>{@link org.apache.calcite.rel.rules.ProjectCalcMergeRule}
 *     merges this with a {@link LogicalProject}
 * <li>{@link org.apache.calcite.rel.rules.CalcMergeRule}
 *     merges two {@code LogicalCalc}s
 * </ul>
 */
public final class LogicalCalc extends Calc {
  //~ Static fields/initializers ---------------------------------------------

  //~ Constructors -----------------------------------------------------------

  /** Creates a LogicalCalc. */
  public LogicalCalc(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RelDataType rowType,
      RexProgram program,
      List<RelCollation> collationList) {
    super(cluster, traits, child, rowType, program, collationList);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalCalc copy(RelTraitSet traitSet, RelNode child,
      RexProgram program, List<RelCollation> collationList) {
    return new LogicalCalc(getCluster(), traitSet, child,
        program.getOutputRowType(), program, collationList);
  }

  @Override public void collectVariablesUsed(Set<String> variableSet) {
    final RelOptUtil.VariableUsedVisitor vuv =
        new RelOptUtil.VariableUsedVisitor();
    for (RexNode expr : program.getExprList()) {
      expr.accept(vuv);
    }
    variableSet.addAll(vuv.variables);
  }
}

// End LogicalCalc.java
