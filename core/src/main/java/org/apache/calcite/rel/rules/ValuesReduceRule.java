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
package org.eigenbase.rel.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.Util;

/**
 * Planner rule that folds projections and filters into an underlying
 * {@link ValuesRel}. Returns an {@link EmptyRel} if all rows are filtered away.
 *
 * <p>For example,</p>
 *
 * <blockquote><code>select a - b from (values (1, 2), (3, 5), (7, 11)) as t (a,
 * b) where a + b &gt; 4</code></blockquote>
 *
 * <p>becomes</p>
 *
 * <blockquote><code>select x from (values (-2), (-4))</code></blockquote>
 */
public abstract class ReduceValuesRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  private static final Logger LOGGER = EigenbaseTrace.getPlannerTracer();

  /**
   * Instance of this rule that applies to the pattern
   * Filter(Values).
   */
  public static final ReduceValuesRule FILTER_INSTANCE =
      new ReduceValuesRule(
          operand(FilterRel.class,
              operand(ValuesRel.class, none())),
          "ReduceValuesRule[Filter") {
        public void onMatch(RelOptRuleCall call) {
          FilterRel filter = call.rel(0);
          ValuesRel values = call.rel(1);
          apply(call, null, filter, values);
        }
      };

  /**
   * Instance of this rule that applies to the pattern
   * Project(Values).
   */
  public static final ReduceValuesRule PROJECT_INSTANCE =
      new ReduceValuesRule(
          operand(ProjectRel.class,
              operand(ValuesRel.class, none())),
          "ReduceValuesRule[Project]") {
        public void onMatch(RelOptRuleCall call) {
          ProjectRel project = call.rel(0);
          ValuesRel values = call.rel(1);
          apply(call, project, null, values);
        }
      };

  /**
   * Singleton instance of this rule that applies to the pattern
   * Project(Filter(Values)).
   */
  public static final ReduceValuesRule PROJECT_FILTER_INSTANCE =
      new ReduceValuesRule(
          operand(ProjectRel.class,
              operand(FilterRel.class,
                  operand(ValuesRel.class, none()))),
          "ReduceValuesRule[Project+Filter]") {
        public void onMatch(RelOptRuleCall call) {
          ProjectRel project = call.rel(0);
          FilterRel filter = call.rel(1);
          ValuesRel values = call.rel(2);
          apply(call, project, filter, values);
        }
      };

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ReduceValuesRule.
   *
   * @param operand class of rels to which this rule should apply
   */
  private ReduceValuesRule(RelOptRuleOperand operand, String desc) {
    super(operand, desc);
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
  protected void apply(RelOptRuleCall call, ProjectRel project,
      FilterRel filter, ValuesRel values) {
    assert values != null;
    assert filter != null || project != null;
    final RexNode conditionExpr =
        (filter == null) ? null : filter.getCondition();
    final List<RexNode> projectExprs =
        (project == null) ? null : project.getProjects();
    RexBuilder rexBuilder = values.getCluster().getRexBuilder();

    // Find reducible expressions.
    List<RexNode> reducibleExps = new ArrayList<RexNode>();
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
    ReduceExpressionsRule.reduceExpressions(values, reducibleExps);

    int changeCount = 0;
    final List<List<RexLiteral>> tupleList =
        new ArrayList<List<RexLiteral>>();
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

      List<RexLiteral> valuesList = new ArrayList<RexLiteral>();
      if (projectExprs != null) {
        ++changeCount;
        for (; i < fieldsPerRow; ++i) {
          reducedValue = reducibleExps.get((row * fieldsPerRow) + i);
          if (reducedValue instanceof RexLiteral) {
            valuesList.add((RexLiteral) reducedValue);
          } else if (RexUtil.isNullLiteral(reducedValue, true)) {
            valuesList.add(rexBuilder.constantNull());
          } else {
            return;
          }
        }
      } else {
        valuesList = values.getTuples().get(row);
      }
      tupleList.add(valuesList);
    }

    if (changeCount > 0) {
      final RelDataType rowType;
      if (projectExprs != null) {
        rowType = project.getRowType();
      } else {
        rowType = values.getRowType();
      }
      final RelNode newRel;
      if (tupleList.isEmpty()) {
        newRel =
            new EmptyRel(
                values.getCluster(),
                rowType);
      } else {
        newRel =
            new ValuesRel(
                values.getCluster(),
                rowType,
                tupleList);
      }
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

// End ReduceValuesRule.java
