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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.immutables.value.Value;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *  Planner rule that derives more equivalent predicates from inner
 *  {@link Join} and creates {@link Filter} with those predicates.
 *  Then {@link FilterJoinRule} will try to push these new predicates down.
 *  (So if you enable this rule, please make sure to enable {@link FilterJoinRule} also).
 *  <p>The basic idea is that, for example, in the query
 *  <blockquote>SELECT * FROM ta INNER JOIN tb ON ta.x = tb.y WHERE ta.x &gt; 10</blockquote>
 *  we can infer condition tb.y &gt; 10 and push it down to the table tb.
 *  In this way, maybe we can reduce the amount of data involved in the {@link Join}.
 *  <p>For example, the query plan
 *  <blockquote><pre>{@code
 *  LogicalJoin(condition=[=($1, $5)], joinType=[inner])
 *   LogicalTableScan(table=[[hr, emps]])
 *   LogicalFilter(condition=[>($0, 10)])
 *     LogicalTableScan(table=[[hr, depts]])
 *  }</pre></blockquote>
 *  <p> will convert to
 *  <blockquote><pre>{@code
 *  LogicalJoin(condition=[=($1, $5)], joinType=[inner])
 *   LogicalFilter(condition=[>($1, 20)])
 *     LogicalTableScan(table=[[hr, emps]])
 *   LogicalFilter(condition=[>($0, 20)])
 *     LogicalTableScan(table=[[hr, depts]])
 *  }</pre></blockquote>
 *  <p>the query plan
 *  <blockquote><pre>{@code
 *  LogicalJoin(condition=[=($1, $5)], joinType=[inner])
 *   LogicalFilter(condition=[SEARCH($1, Sarg[(10..30)])])
 *     LogicalTableScan(table=[[hr, emps]])
 *   LogicalFilter(condition=[SEARCH($0, Sarg[(20..40)])])
 *     LogicalTableScan(table=[[hr, depts]])
 *  }</pre></blockquote>
 *  <p> will convert to
 *  <blockquote><pre>{@code
 *  LogicalJoin(condition=[=($1, $5)], joinType=[inner])
 *   LogicalFilter(condition=[SEARCH($1, Sarg[(20..30)])])
 *     LogicalTableScan(table=[[hr, emps]])
 *   LogicalFilter(condition=[SEARCH($0, Sarg[(20..30)])])
 *     LogicalTableScan(table=[[hr, depts]])
 *  }</pre></blockquote>
 *  <p>Currently, the rule has some limitations:
 *  <p>1. only handle partial predicates (comparison), but this can be extended to
 *      support more predicates such as 'LIKE', 'RLIKE' and 'SIMILAR' in the future.
 *  <p>2. only support simple condition inference, such as: {$1 = $2, $2 = 10} =&gt; {$1 = 10},
 *     can not handle complex condition inference, such as conditions with functions, like
 *     {a = b, b = abs(c), c = 1} =&gt; {a = abs(1)}
 *  <p>3. only support discomposed literal, for example
 *     it can infer {$1 = $2, $1 = 10} =&gt; {$2 = 10}
 *     it can not infer {$1 = $2, $1 = 10 + 10} =&gt; {$2 = 10 + 10}
 */

@Value.Enclosing
public class JoinDeriveEquivalenceFilterRule
    extends RelRule<JoinDeriveEquivalenceFilterRule.Config> implements TransformationRule {

  public JoinDeriveEquivalenceFilterRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Join join = call.rel(1);

    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RexSimplify simplify =
        new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, RexUtil.EXECUTOR);

    final RexNode originalCondition =
        prepare(rexBuilder, filter.getCondition(), join.getCondition());

    final RexNode newCondition =
        deriveEquivalenceCondition(simplify, rexBuilder, originalCondition);

    if (arePredicatesEquivalent(rexBuilder, simplify, originalCondition, newCondition)) {
      // if originalCondition and newCondition are equivalent, it means that the current
      // Filter has either been derived or there is no room for derivation. if so,
      // then we can stop.
      return;
    }

    final Filter newFilter = filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);

    call.transformTo(newFilter);

    // after derivation, the original filter can be pruned
    call.getPlanner().prune(filter);
  }

  /**
   * normalized expressions are easier to compare. so here try to normalize conditions.
   */
  private RexNode prepare(final RexBuilder rexBuilder,
      final RexNode filterCondition, final RexNode joinCondition) {
    final RexNode condition =
        RexUtil.composeConjunction(rexBuilder,
            ImmutableList.of(filterCondition, joinCondition));

    // 1. reorder operands to make sure that
    //    a. literal/constant is always in right, such as: 10 > $1 -> $1 < 10
    //    b. input ref with smaller index is in left, such as: $1 = $0 -> $0 = $1
    // 2. expand search to comparison predicates, such as:
    //    SEARCH($1, Sarg[(10..30)]) -> $1 > 10 AND $1 < 30
    // DO NOT simply expression now
    return RexUtil.canonizeNode(rexBuilder, condition);
  }

  /**
   *  determine whether two predicate expressions are equivalent.
   */
  private boolean arePredicatesEquivalent(final RexBuilder rexBuilder,
      final RexSimplify simplify, final RexNode left, final RexNode right) {
    // simplify expression first to avoid redundancy
    final RexNode simplifiedLeftPredicate = simplify.simplify(left);
    final RexNode simplifiedRightPredicate = simplify.simplify(right);

    // reorder operands and expand Search
    final RexNode canonizedLeftPredicate =
        RexUtil.canonizeNode(rexBuilder, simplifiedLeftPredicate);
    final RexNode canonizedRightPredicate =
        RexUtil.canonizeNode(rexBuilder, simplifiedRightPredicate);

    // split into conjunctions to avoid (A AND B) not equals (B AND A)
    final List<RexNode> leftPredicates = RelOptUtil.conjunctions(canonizedLeftPredicate);
    final List<RexNode> rightPredicates = RelOptUtil.conjunctions(canonizedRightPredicate);

    if (leftPredicates.size() != rightPredicates.size()) {
      return false;
    }
    return Sets.newHashSet(leftPredicates).containsAll(rightPredicates);
  }

  /**
   * derive more conditions based on inputRef-inputRef equality and inputRef-value equality.
   */
  private RexNode deriveEquivalenceCondition(final RexSimplify simplify,
      final RexBuilder rexBuilder, final RexNode originalCondition) {
    // map for inputRef to corresponding predicate such as: $1 -> [$1 > 10, $1 < 20, $1 = $2]
    final Multimap<RexInputRef, RexNode> predicateMultimap
        = LinkedHashMultimap.create();
    // map for inputRef to corresponding equivalent values or inputRefs such as: $1 -> [$2, 1]
    final Multimap<RexInputRef, RexNode> equivalenceMultimap
        = LinkedHashMultimap.create();

    // 1. construct predicate map and equivalence map
    final List<RexNode> originalConjunctions = RelOptUtil.conjunctions(originalCondition);
    for (RexNode rexNode : originalConjunctions) {
      if (rexNode instanceof RexCall) {
        // only handle partial predicates, will try to handle more predicates such as
        // 'LIKE', 'RLIKE' or 'SIMILAR' later
        if (!rexNode.isA(SqlKind.COMPARISON) && !rexNode.isA(SqlKind.OR)) {
          continue;
        }

        final RexNode operand0 = ((RexCall) rexNode).getOperands().get(0);
        final RexNode operand1 = ((RexCall) rexNode).getOperands().get(1);
        final List<RexInputRef> leftInputRefs = RexUtil.gatherRexInputReferences(operand0);
        final List<RexInputRef> rightInputRefs = RexUtil.gatherRexInputReferences(operand1);

        // only handle inputRef-inputRef predicate like $1 = $2
        // or inputRef-literal predicate like $1 > 10
        if (rexNode.isA(SqlKind.COMPARISON)
            && (leftInputRefs.size() != 1 || rightInputRefs.size() > 1)) {
          continue;
        }
        // only handle single-inputRef disjunctions like {$0 = 10 or $0 = 20}
        // can't handle multi-inputRef disjunctions like {$0 = 10 or $1 = 20} now
        if (rexNode.isA(SqlKind.OR)
            && RexUtil.gatherRexInputReferences(rexNode).size() > 1) {
          continue;
        }

        // record equivalence relation
        if (rexNode.isA(SqlKind.EQUALS)
            && RexUtil.isInputReference(operand0, /* allowCast= */true)
            && operand1.isA(ImmutableList.of(SqlKind.INPUT_REF, SqlKind.LITERAL))) {
          equivalenceMultimap.put(leftInputRefs.get(0), operand1);
          if (operand1.isA(SqlKind.INPUT_REF)) {
            equivalenceMultimap.put(rightInputRefs.get(0), leftInputRefs.get(0));
          }
        }

        // record predicate
        predicateMultimap.put(leftInputRefs.get(0), rexNode);
      }
    }

    // 2. search map and rewrite predicates with equivalent inputRefs or literals
    //
    // first, find all inputRefs that are equivalent to the current inputRef, and then
    // rewrite all predicates involving equivalent inputRefs using inputRef, such as:
    // if we have inputRef $1 = equivInputRef $2, then we can rewrite {$2 = 10} to {$1 = 10}
    //
    // then, find all predicates involving current inputRef. If any predicate refers
    // to another inputRef, rewrite the predicate with the literal/constant equivalent
    // to that inputRef, such as: if we have inputRef {$1 > $2} and {$2 = 10} then we
    // can infer new condition {$1 > 10}
    //
    // finally, derive new predicates based on equivalence relation in equivalenceMultimap
    //
    // all derived predicates need to be canonized before recorded in predicateMultimap

    final Set<RexInputRef> allInputRefs =
        Sets.union(equivalenceMultimap.keySet(), predicateMultimap.keySet());

    // derive new equivalence condition
    for (RexInputRef inputRef : allInputRefs) {
      for (RexInputRef equiv : getEquivalentInputRefs(inputRef, equivalenceMultimap)) {
        equivalenceMultimap.putAll(inputRef, equivalenceMultimap.get(equiv));
      }
    }

    // rewrite predicate with new inputRef
    for (RexInputRef inputRef : allInputRefs) {
      for (RexInputRef equiv : getEquivalentInputRefs(inputRef, equivalenceMultimap)) {
        for (RexNode predicate : predicateMultimap.get(equiv)) {
          RexNode newPredicate =
              rewriteWithNewInputRef(rexBuilder, predicate, equiv, inputRef);
          newPredicate = RexUtil.canonizeNode(rexBuilder, newPredicate);
          predicateMultimap.put(inputRef, newPredicate);
        }
      }
    }

    // rewrite predicate with new value
    for (RexInputRef inputRef : allInputRefs) {
      for (RexNode predicate : ImmutableList.copyOf(predicateMultimap.get(inputRef))) {
        final List<RexInputRef> inputRefs = RexUtil.gatherRexInputReferences(predicate);
        inputRefs.remove(inputRef);
        if (inputRefs.isEmpty()) {
          continue;
        }
        final RexInputRef relatedInputRef = inputRefs.get(0);
        for (RexLiteral literal : getEquivalentLiterals(relatedInputRef,
            equivalenceMultimap)) {
          RexNode newPredicate =
              rewriteWithNewValue(rexBuilder, predicate, relatedInputRef, literal);
          newPredicate = RexUtil.canonizeNode(rexBuilder, newPredicate);
          predicateMultimap.put(inputRef, newPredicate);
        }
      }
    }

    // derive new equivalence predicates
    for (RexInputRef inputRef : allInputRefs) {
      for (RexNode rexNode : equivalenceMultimap.get(inputRef)) {
        RexNode newPredicate =
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, inputRef, rexNode);
        newPredicate = RexUtil.canonizeNode(rexBuilder, newPredicate);
        predicateMultimap.put(inputRef, newPredicate);
      }
    }

    // 3. compose all original predicates and derived predicates with AND.
    //
    // currently some predicates can not be handled, so we need to compose with
    // original conditions with AND to avoid missing any conditions
    final Set<RexNode> predicates = Sets.newHashSet(originalConjunctions);
    predicates.addAll(predicateMultimap.values());
    final RexNode composeConjunction = RexUtil.composeConjunction(rexBuilder, predicates);

    // 4. simplify expression such as range merging, like {$1 > 10, $1 > 20} => {$1 > 20}
    return simplify.simplify(composeConjunction);
  }

  private Set<RexInputRef> getEquivalentInputRefs(final RexInputRef inputRef,
      final Multimap<RexInputRef, RexNode> equivalenceMultimap) {
    return equivalenceMultimap.get(inputRef).stream()
        .filter(rexNode -> rexNode.isA(SqlKind.INPUT_REF))
        .map(rexNode -> (RexInputRef) rexNode)
        .collect(Collectors.toSet());
  }

  private Set<RexLiteral> getEquivalentLiterals(final RexInputRef inputRef,
      final Multimap<RexInputRef, RexNode> equivalenceMultimap) {
    return equivalenceMultimap.get(inputRef).stream()
        .filter(rexNode -> rexNode.isA(SqlKind.LITERAL))
        .map(rexNode -> (RexLiteral) rexNode)
        .collect(Collectors.toSet());
  }


  /**
   *  rewrite expression with the equivalent inputRef such as:
   *  based on {$1 = $2}, rewrite {$1 = 10} to {$2 = 10}.
   *  This operation will modify the original expression, so always use a copy.
   */
  private RexNode rewriteWithNewInputRef(final RexBuilder rexBuilder, final RexNode rexNode,
      final RexInputRef originalInputRef, final RexInputRef newInputRef) {
    return rexBuilder.copy(rexNode).accept(new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef inputRef) {
        if (originalInputRef.equals(inputRef)) {
          return newInputRef;
        }
        return super.visitInputRef(inputRef);
      }
    });
  }

  /**
   *  rewrite expression with the equivalent value such as:
   *  based on {$1 = 10}, rewrite {$1 > $2} to> {$2 < 10}.
   *  This operation will modify the original expression, so always use a copy.
   */
  private RexNode rewriteWithNewValue(final RexBuilder rexBuilder, final RexNode rexNode,
      final RexInputRef originalInputRef, final RexLiteral newValue) {
    return rexBuilder.copy(rexNode).accept(new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef inputRef) {
        if (originalInputRef.equals(inputRef)) {
          return newValue;
        }
        return super.visitInputRef(inputRef);
      }
    });
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable public interface Config extends RelRule.Config {
    ImmutableJoinDeriveEquivalenceFilterRule.Config DEFAULT =
        ImmutableJoinDeriveEquivalenceFilterRule.Config
            .of().withOperandSupplier(
                b0 -> b0.operand(LogicalFilter.class)
                    .oneInput(b1 -> b1.operand(LogicalJoin.class)
                        .predicate(join -> join.getJoinType() == JoinRelType.INNER).anyInputs()));

    @Override default JoinDeriveEquivalenceFilterRule toRule() {
      return new JoinDeriveEquivalenceFilterRule(this);
    }
  }
}
