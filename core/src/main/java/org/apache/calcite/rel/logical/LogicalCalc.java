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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

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
 * <li>{@link ProjectToCalcRule} creates this from a {@link LogicalProject}
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
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode child,
      RexProgram program) {
    super(cluster, traitSet, hints, child, program);
  }

  @Deprecated // to be removed before 2.0
  public LogicalCalc(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexProgram program) {
    this(cluster, traitSet, ImmutableList.of(), child, program);
  }

  /**
   * Creates a LogicalCalc by parsing serialized output.
   */
  public LogicalCalc(RelInput input) {
    this(input.getCluster(),
        input.getTraitSet(),
        ImmutableList.of(),
        input.getInput(),
        RexProgram.create(input));
  }

  @Deprecated // to be removed before 2.0
  public LogicalCalc(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexProgram program,
      List<RelCollation> collationList) {
    this(cluster, traitSet, ImmutableList.of(), child, program);
    Util.discard(collationList);
  }

  public static LogicalCalc create(final RelNode input,
      final RexProgram program) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet = cluster.traitSet()
        .replace(Convention.NONE)
        .replaceIfs(RelCollationTraitDef.INSTANCE,
            () -> RelMdCollation.calc(mq, input, program))
        .replaceIf(RelDistributionTraitDef.INSTANCE,
            () -> RelMdDistribution.calc(mq, input, program));
    return new LogicalCalc(cluster, traitSet, ImmutableList.of(), input, program);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalCalc copy(RelTraitSet traitSet, RelNode child,
      RexProgram program) {
    return new LogicalCalc(getCluster(), traitSet, hints, child, program);
  }

  @Override public void collectVariablesUsed(Set<CorrelationId> variableSet) {
    final RelOptUtil.VariableUsedVisitor vuv =
        new RelOptUtil.VariableUsedVisitor(null);
    vuv.visitEach(program.getExprList());
    variableSet.addAll(vuv.variables);
  }

  @Override public RelNode withHints(List<RelHint> hintList) {
    return new LogicalCalc(getCluster(), traitSet,
        ImmutableList.copyOf(hintList), input, program);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}
