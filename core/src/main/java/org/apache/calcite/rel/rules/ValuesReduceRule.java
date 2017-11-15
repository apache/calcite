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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that folds projections and filters into an underlying
 * {@link org.apache.calcite.rel.logical.LogicalValues}.
 *
 * <p>Returns a simplified {@code Values}, perhaps containing zero tuples
 * if all rows are filtered away.
 *
 * <p>For example,</p>
 *
 * <blockquote><code>select a - b from (values (1, 2), (3, 5), (7, 11)) as t (a,
 * b) where a + b &gt; 4</code></blockquote>
 *
 * <p>becomes</p>
 *
 * <blockquote><code>select x from (values (-2), (-4))</code></blockquote>
 *
 * <p>Ignores an empty {@code Values}; this is better dealt with by
 * {@link PruneEmptyRules}.
 */
public abstract class ValuesReduceRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /**
   * Instance of this rule that applies to the pattern
   * Filter(Values).
   */
  public static final ValuesReduceRule FILTER_INSTANCE =
      new ValuesReduceRule(
          operand(LogicalFilter.class,
              operand(LogicalValues.class, null, Values.IS_NOT_EMPTY, none())),
          RelFactories.LOGICAL_BUILDER,
          "ValuesReduceRule(Filter)") {
        public void onMatch(RelOptRuleCall call) {
          LogicalFilter filter = call.rel(0);
          LogicalValues values = call.rel(1);
          apply(call, null, filter, values);
        }
      };

  /**
   * Instance of this rule that applies to the pattern
   * Project(Values).
   */
  public static final ValuesReduceRule PROJECT_INSTANCE =
      new ValuesReduceRule(
          operand(LogicalProject.class,
              operand(LogicalValues.class, null, Values.IS_NOT_EMPTY, none())),
          RelFactories.LOGICAL_BUILDER,
          "ValuesReduceRule(Project)") {
        public void onMatch(RelOptRuleCall call) {
          LogicalProject project = call.rel(0);
          LogicalValues values = call.rel(1);
          apply(call, project, null, values);
        }
      };

  /**
   * Singleton instance of this rule that applies to the pattern
   * Project(Filter(Values)).
   */
  public static final ValuesReduceRule PROJECT_FILTER_INSTANCE =
      new ValuesReduceRule(
          operand(LogicalProject.class,
              operand(LogicalFilter.class,
                  operand(LogicalValues.class, null, Values.IS_NOT_EMPTY,
                      none()))),
          RelFactories.LOGICAL_BUILDER,
          "ValuesReduceRule(Project-Filter)") {
        public void onMatch(RelOptRuleCall call) {
          LogicalProject project = call.rel(0);
          LogicalFilter filter = call.rel(1);
          LogicalValues values = call.rel(2);
          apply(call, project, filter, values);
        }
      };

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ValuesReduceRule.
   *
   * @param operand           Class of rels to which this rule should apply
   * @param relBuilderFactory Builder for relational expressions
   * @param desc              Description, or null to guess description
   */
  public ValuesReduceRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String desc) {
    super(operand, relBuilderFactory, desc);
    Util.discard(LOGGER);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Does the work.
   *
   * @param call    Rule call
   * @param project Project, may be null
   * @param filter  Filter, may be null
   * @param values  Values rel to be reduced
   */
  protected void apply(RelOptRuleCall call, LogicalProject project,
      LogicalFilter filter, LogicalValues values) {
    assert values != null;
    assert filter != null || project != null;
    final RexNode conditionExpr =
        (filter == null) ? null : filter.getCondition();
    final List<RexNode> projectExprs =
        (project == null) ? null : project.getProjects();
    RexBuilder rexBuilder = values.getCluster().getRexBuilder();

    // Find reducible expressions.
    final List<RexNode> reducibleExps = new ArrayList<>();
    final MyRexShuttle shuttle = new MyRexShuttle();
    for (final List<RexLiteral> literalList : values.getTuples()) {
      shuttle.literalList = literalList;
      if (conditionExpr != null) {
        RexNode c = conditionExpr.accept(shuttle);
        reducibleExps.add(c);
      }
      if (projectExprs != null) {
        int k = -1;
        for (RexNode projectExpr : projectExprs) {
          ++k;
          RexNode e = projectExpr.accept(shuttle);
          if (RexLiteral.isNullLiteral(e)) {
            e = rexBuilder.makeAbstractCast(
                project.getRowType().getFieldList().get(k).getType(),
                e);
          }
          reducibleExps.add(e);
        }
      }
    }
    int fieldsPerRow =
        ((conditionExpr == null) ? 0 : 1)
            + ((projectExprs == null) ? 0 : projectExprs.size());
    assert fieldsPerRow > 0;
    assert reducibleExps.size() == (values.getTuples().size() * fieldsPerRow);

    // Compute the values they reduce to.
    final RelOptPredicateList predicates = RelOptPredicateList.EMPTY;
    ReduceExpressionsRule.reduceExpressions(values, reducibleExps, predicates);

    int changeCount = 0;
    final ImmutableList.Builder<ImmutableList<RexLiteral>> tuplesBuilder =
        ImmutableList.builder();
    for (int row = 0; row < values.getTuples().size(); ++row) {
      int i = 0;
      RexNode reducedValue;
      if (conditionExpr != null) {
        reducedValue = reducibleExps.get((row * fieldsPerRow) + i);
        ++i;
        if (!reducedValue.isAlwaysTrue()) {
          ++changeCount;
          continue;
        }
      }

      ImmutableList<RexLiteral> valuesList;
      if (projectExprs != null) {
        ++changeCount;
        final ImmutableList.Builder<RexLiteral> tupleBuilder =
            ImmutableList.builder();
        for (; i < fieldsPerRow; ++i) {
          reducedValue = reducibleExps.get((row * fieldsPerRow) + i);
          if (reducedValue instanceof RexLiteral) {
            tupleBuilder.add((RexLiteral) reducedValue);
          } else if (RexUtil.isNullLiteral(reducedValue, true)) {
            tupleBuilder.add(rexBuilder.constantNull());
          } else {
            return;
          }
        }
        valuesList = tupleBuilder.build();
      } else {
        valuesList = values.getTuples().get(row);
      }
      tuplesBuilder.add(valuesList);
    }

    if (changeCount > 0) {
      final RelDataType rowType;
      if (projectExprs != null) {
        rowType = project.getRowType();
      } else {
        rowType = values.getRowType();
      }
      final RelNode newRel =
          LogicalValues.create(values.getCluster(), rowType,
              tuplesBuilder.build());
      call.transformTo(newRel);
    } else {
      // Filter had no effect, so we can say that Filter(Values) ==
      // Values.
      call.transformTo(values);
    }

    // New plan is absolutely better than old plan. (Moreover, if
    // changeCount == 0, we've proved that the filter was trivial, and that
    // can send the volcano planner into a loop; see dtbug 2070.)
    if (filter != null) {
      call.getPlanner().setImportance(filter, 0.0);
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Shuttle that converts inputs to literals. */
  private static class MyRexShuttle extends RexShuttle {
    private List<RexLiteral> literalList;

    public RexNode visitInputRef(RexInputRef inputRef) {
      return literalList.get(inputRef.getIndex());
    }
  }
}

// End ValuesReduceRule.java
