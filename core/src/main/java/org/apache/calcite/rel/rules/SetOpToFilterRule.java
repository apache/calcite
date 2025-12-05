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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Rule that replaces {@link SetOp} operator with {@link Filter}
 * when both inputs are from the same source with only filter conditions differing.
 * For nested filters, the rule {@link CoreRules#FILTER_MERGE}
 * should be used prior to invoking this one.
 *
 * <p>Example:
 *
 * <p>UNION
 * <blockquote><pre>
 * SELECT mgr, comm FROM emp WHERE mgr = 12
 * UNION
 * SELECT mgr, comm FROM emp WHERE comm = 5
 *
 * is rewritten to
 *
 * SELECT DISTINCT mgr, comm FROM emp
 * WHERE mgr = 12 OR comm = 5
 * </pre></blockquote>
 *
 * <p>UNION with multiple inputs
 * <blockquote><pre>
 * SELECT deptno FROM emp WHERE deptno = 12
 * UNION
 * SELECT deptno FROM dept WHERE deptno = 5
 * UNION
 * SELECT deptno FROM emp WHERE deptno = 6
 * UNION
 * SELECT deptno FROM dept WHERE deptno = 10
 *
 * is rewritten to
 *
 * SELECT deptno FROM emp WHERE deptno = 12 OR deptno = 6
 * UNION
 * SELECT deptno FROM dept WHERE deptno = 5 OR deptno = 10
 * </pre></blockquote>
 *
 * <p>INTERSECT
 * <blockquote><pre>
 * SELECT mgr, comm FROM emp WHERE mgr = 12
 * INTERSECT
 * SELECT mgr, comm FROM emp WHERE comm = 5
 *
 * is rewritten to
 *
 * SELECT DISTINCT mgr, comm FROM emp
 * WHERE mgr = 12 AND comm = 5
 * </pre></blockquote>
 *
 * <p>EXCEPT
 * <blockquote><pre>
 * SELECT mgr, comm FROM emp WHERE mgr = 12
 * EXCEPT
 * SELECT mgr, comm FROM emp WHERE comm = 5
 *
 * is rewritten to
 *
 * SELECT DISTINCT mgr, comm FROM emp
 * WHERE mgr = 12 AND NOT(comm = 5)
 * </pre></blockquote>
 */
@Value.Enclosing
public class SetOpToFilterRule
    extends RelRule<SetOpToFilterRule.Config>
    implements TransformationRule {

  /** Creates an SetOpToFilterRule. */
  protected SetOpToFilterRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    config.matchHandler().accept(this, call);
  }

  private static void match(RelOptRuleCall call) {
    final SetOp setOp = call.rel(0);
    final List<RelNode> inputs = setOp.getInputs();
    if (setOp.all || inputs.size() < 2) {
      return;
    }

    final RelBuilder builder = call.builder();
    Pair<RelNode, @Nullable RexNode> first = extractSourceAndCond(inputs.get(0).stripped());

    // Groups conditions by their source relational node and input position.
    // - Key: Pair of (sourceRelNode, inputPosition)
    //   - inputPosition is null for mergeable conditions
    //   - inputPosition contains original index for non-mergeable inputs
    // - Value: List of conditions
    //
    // For invalid conditions (non-deterministic expressions or containing subqueries),
    // positions are tagged with their input indices to skip unmergeable inputs
    // during map-based grouping. Other positions are set to null.
    Map<Pair<RelNode, @Nullable Integer>, List<@Nullable RexNode>> sourceToConds =
        new LinkedHashMap<>();

    RelNode firstSource = first.left;
    sourceToConds.computeIfAbsent(Pair.of(firstSource, null),
        k -> new ArrayList<>()).add(first.right);

    for (int i = 1; i < inputs.size(); i++) {
      final RelNode input = inputs.get(i).stripped();
      final Pair<RelNode, @Nullable RexNode> pair = extractSourceAndCond(input);
      sourceToConds.computeIfAbsent(Pair.of(pair.left, pair.right != null ? null : i),
          k -> new ArrayList<>()).add(pair.right);
    }

    if (sourceToConds.size() == inputs.size()) {
      return;
    }

    int branchCount = 0;
    for (Map.Entry<Pair<RelNode, @Nullable Integer>, List<@Nullable RexNode>> entry
        : sourceToConds.entrySet()) {
      Pair<RelNode, @Nullable Integer> left = entry.getKey();
      List<@Nullable RexNode> conds = entry.getValue();
      // Single null condition indicates pass-through branch,
      // directly add its corresponding input to the new inputs list.
      if (conds.size() == 1 && conds.get(0) == null) {
        builder.push(left.left);
        branchCount++;
        continue;
      }

      List<RexNode> condsNonNull = conds.stream().map(e -> {
        assert e != null;
        return e;
      }).collect(Collectors.toList());

      RexNode combinedCond =
          combineConditions(builder, condsNonNull, setOp, left.left == firstSource);

      builder.push(left.left)
          .filter(combinedCond);
      branchCount++;
    }

    // RelBuilder will not create 1-input SetOp
    // and remove the distinct after a SetOp
    buildSetOp(builder, branchCount, setOp)
        .distinct();
    call.transformTo(builder.build());
  }

  private static RelBuilder buildSetOp(RelBuilder builder, int count, RelNode setOp) {
    if (setOp instanceof Union) {
      return builder.union(false, count);
    } else if (setOp instanceof Intersect) {
      return builder.intersect(false, count);
    } else if (setOp instanceof Minus) {
      return builder.minus(false, count);
    }
    // unreachable
    throw new IllegalStateException("unreachable code");
  }

  private static Pair<RelNode, @Nullable RexNode> extractSourceAndCond(RelNode input) {
    if (input instanceof Filter) {
      Filter filter = (Filter) input;
      if (!RexUtil.isDeterministic(filter.getCondition())
          || RexUtil.SubQueryFinder.containsSubQuery(filter)) {
        // Skip non-deterministic conditions or those containing subqueries
        return Pair.of(input, null);
      }
      return Pair.of(filter.getInput().stripped(), filter.getCondition());
    }
    // For non-filter inputs, use TRUE literal as default condition.
    return Pair.of(input.stripped(),
        input.getCluster().getRexBuilder().makeLiteral(true));
  }

  /**
   * Creates a combined condition where the first condition
   * is kept as-is and all subsequent conditions are negated,
   * then joined with AND operators.
   *
   * <p>For example, given conditions [cond1, cond2, cond3],
   * this constructs (cond1 AND NOT(cond2) AND NOT(cond3)).
   */
  private static RexNode andFirstNotRest(RelBuilder builder, List<RexNode> conds) {
    List<RexNode> allConds = new ArrayList<>();
    allConds.add(conds.get(0));
    for (int i = 1; i < conds.size(); i++) {
      allConds.add(builder.not(conds.get(i)));
    }
    return builder.and(allConds);
  }

  /**
   * Combines conditions according to set operation:
   * UNION: OR combination
   * INTERSECT: AND combination
   * MINUS: Special handling where first source uses AND-NOT combination.
   */
  private static RexNode combineConditions(RelBuilder builder, List<RexNode> conds,
      SetOp setOp, boolean isFirstSource) {
    if (setOp instanceof Union) {
      return builder.or(conds);
    } else if (setOp instanceof Intersect) {
      return builder.and(conds);
    } else if (setOp instanceof Minus) {
      return isFirstSource
          ? andFirstNotRest(builder, conds)
          : builder.or(conds);
    }
    // unreachable
    throw new IllegalStateException("unreachable code");
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config UNION = ImmutableSetOpToFilterRule.Config.builder()
        .withMatchHandler((rule, call) -> SetOpToFilterRule.match(call))
        .build()
        .withOperandSupplier(
            b0 -> b0.operand(Union.class).anyInputs())
        .as(Config.class);

    Config INTERSECT = ImmutableSetOpToFilterRule.Config.builder()
        .withMatchHandler((rule, call) -> SetOpToFilterRule.match(call))
        .build()
        .withOperandSupplier(
            b0 -> b0.operand(Intersect.class).anyInputs())
        .as(Config.class);

    Config MINUS = ImmutableSetOpToFilterRule.Config.builder()
        .withMatchHandler((rule, call) -> SetOpToFilterRule.match(call))
        .build()
        .withOperandSupplier(
            b0 -> b0.operand(Minus.class).anyInputs())
        .as(Config.class);

    @Override default SetOpToFilterRule toRule() {
      return new SetOpToFilterRule(this);
    }

    /** Forwards a call to {@link #onMatch(RelOptRuleCall)}. */
    MatchHandler<SetOpToFilterRule> matchHandler();

    /** Sets {@link #matchHandler()}. */
    Config withMatchHandler(MatchHandler<SetOpToFilterRule> matchHandler);
  }
}
