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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Planner rule that matches a
 * {@link org.apache.calcite.rel.core.Join}
 * and expands OR clauses in join conditions.
 *
 * <p>This rule expands OR conditions in join clauses into
 * multiple separate join conditions, allowing the optimizer
 * to handle these conditions more efficiently.
 *
 * <p>The following is an example of inner join,
 * and other examples for other kinds of joins
 * can be found in the code below.
 *
 * <pre>{@code
 * Project[*]
 *    └── Join[OR(t1.id=t2.id, t1.age=t2.age), inner]
 *        ├── TableScan[t1]
 *        └── TableScan[t2]
 * }</pre>
 *
 * <p>into
 *
 * <pre>{@code
 * Project[*]
 *    └── UnionAll
 *        ├── Join[t1.id=t2.id, inner]
 *        │   ├── TableScan[t1]
 *        │   └── TableScan[t2]
 *        └── Join[t1.age=t2.age AND t1.id≠t2.id, inner]
 *            ├─── TableScan[t1]
 *            └─── TableScan[t2]
 * }</pre>
 */
@Value.Enclosing
public class JoinExpandOrToUnionRule
    extends RelRule<JoinExpandOrToUnionRule.Config>
    implements TransformationRule {

  /** Creates an JoinExpandOrToUnionRule. */
  protected JoinExpandOrToUnionRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    Join join = call.rel(0);
    List<RexNode> orConds = RelOptUtil.disjunctions(join.getCondition());
    return orConds.size() > 1;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    RelBuilder relBuilder = call.builder();

    RelNode expanded;
    switch (join.getJoinType()) {
    case INNER:
      expanded = expandInnerJoin(join, relBuilder);
      break;
    case ANTI:
      expanded = expandAntiJoin(join, relBuilder);
      break;
    case LEFT:
      expanded = expandLeftOrRightJoin(join, true, relBuilder);
      break;
    case RIGHT:
      expanded = expandLeftOrRightJoin(join, false, relBuilder);
      break;
    case FULL:
      expanded = expandFullJoin(join, relBuilder);
      break;
    default:
      return;
    }
    if (expanded instanceof Join
        && ((Join) expanded).getCondition().equals(join.getCondition())) {
      return;
    }
    call.transformTo(expanded);
  }

  private List<RexNode> splitCond(Join join) {
    final RexBuilder builder = join.getCluster().getRexBuilder();
    final List<RexNode> orConds = RelOptUtil.disjunctions(join.getCondition());
    final int leftFieldCount = join.getLeft().getRowType().getFieldCount();

    final List<RexNode> result = new ArrayList<>();
    final List<RexNode> otherBuffer = new ArrayList<>();

    for (RexNode cond : orConds) {
      if (isValidCond(cond, leftFieldCount)) {
        if (!otherBuffer.isEmpty()) {
          result.add(RexUtil.composeDisjunction(builder, otherBuffer));
          otherBuffer.clear();
        }
        result.add(cond);
      } else {
        otherBuffer.add(cond);
      }
    }

    if (!otherBuffer.isEmpty()) {
      result.add(RexUtil.composeDisjunction(builder, otherBuffer));
    }

    return result;
  }

  private boolean isValidCond(RexNode node, int leftFieldCount) {
    boolean hasJoinKeyCond = false;
    List<RexNode> conds = RelOptUtil.conjunctions(node);
    for (RexNode cond : conds) {
      // When components of the "call" are predicates that contain
      // equality (when the above conditions are met), that are
      // single-side (refer to only on of the collections joined),
      // or which are constant, they will all trigger the expansion.
      if (!doesNotReferToBothInputs(cond, leftFieldCount)) {
        return false;
      }

      if (RexUtil.SubQueryFinder.find(cond) != null
              || RexUtil.containsCorrelation(cond)) {
        // The "call" does not support sub-queries or correlation yet
        return false;
      }

      if (cond instanceof RexCall) {
        RexCall call = (RexCall) cond;
        // Checks if the "call" is valid for use as a join key.
        if (isEquiJoinCond(call, leftFieldCount)) {
          hasJoinKeyCond = true;
        }
      }
    }
    return hasJoinKeyCond;
  }

  private boolean isEquiJoinCond(RexCall call, int leftFieldCount) {
    if (call.getKind() != SqlKind.EQUALS
        && call.getKind() != SqlKind.IS_NOT_DISTINCT_FROM) {
      return false;
    }

    RexNode left = call.getOperands().get(0);
    RexNode right = call.getOperands().get(1);

    if (left instanceof RexInputRef && right instanceof RexInputRef) {
      int leftIndex = ((RexInputRef) left).getIndex();
      int rightIndex = ((RexInputRef) right).getIndex();

      return (leftIndex < leftFieldCount && rightIndex >= leftFieldCount)
          || (rightIndex < leftFieldCount && leftIndex >= leftFieldCount);
    }
    return false;
  }

  private boolean doesNotReferToBothInputs(RexNode rex, int leftFieldCount) {
    RexInputRefCounter counter = new RexInputRefCounter(leftFieldCount);
    rex.accept(counter);
    return counter.doesNotReferToBothInputs();
  }

  /**
   * Counts the number of InputRefs in a RexNode expression. */
  private static class RexInputRefCounter extends RexVisitorImpl<Void> {
    private int leftFieldCount;
    public int leftInputRefCount = 0;
    public int rightInputRefCount = 0;

    RexInputRefCounter(int leftFieldCount) {
      super(true);
      this.leftFieldCount = leftFieldCount;
    }

    @Override public Void visitInputRef(RexInputRef inputRef) {
      if (inputRef.getIndex() < leftFieldCount) {
        leftFieldCount++;
      } else {
        rightInputRefCount++;
      }
      return null;
    }

    public boolean doesNotReferToBothInputs() {
      return leftInputRefCount == 0 || rightInputRefCount == 0;
    }
  }

  /**
   * This method will make the following conversions.
   *
   * <pre>{@code
   * Project[*]
   *    └── Join[OR(t1.id=t2.id, t1.age=t2.age), left]
   *        ├── TableScan[t1]
   *        └── TableScan[t2]
   *}</pre>
   *
   * <p>into
   *
   * <pre>{@code
   * Project[*]
   *    └── UnionAll
   *        ├── Join[t1.id=t2.id, inner]
   *        │   ├── TableScan[t1]
   *        │   └── TableScan[t2]
   *        ├── Join[t1.age=t2.age AND t1.id≠t2.id, inner]
   *        │   ├── TableScan[t1]
   *        │   └── TableScan[t2]
   *        └── Project[t1-side cols + NULLs]
   *            └── Join[t1.id=t2.id, anti]
   *                ├── Join[t1.age=t2.age, anti]
   *                │   ├── TableScan[t1]
   *                │   └── TableScan[t2]
   *                └── TableScan[t2]
   * }</pre>
   */
  private RelNode expandLeftOrRightJoin(Join join, boolean isLeftJoin,
      RelBuilder relBuilder) {
    List<RexNode> orConds = splitCond(join);
    List<RelNode> joins = expandLeftOrRightJoinToRelNodes(join, orConds, isLeftJoin, relBuilder);
    return relBuilder.pushAll(joins)
        .union(true, joins.size())
        .build();
  }

  private List<RelNode> expandLeftOrRightJoinToRelNodes(Join join, List<RexNode> orConds,
      boolean isLeftJoin, RelBuilder relBuilder) {
    List<RelNode> joins = new ArrayList<>();
    joins.addAll(expandInnerJoinToRelNodes(join, orConds, relBuilder));
    joins.add(expandAntiJoinToRelNode(join, orConds, isLeftJoin, true, relBuilder));
    return joins;
  }

  /**
   * This method will make the following conversions.
   *
   * <pre>{@code
   * Project[*]
   *    └── Join[OR(t1.id=t2.id, t1.age=t2.age), full]
   *        ├── TableScan[t1]
   *        └── TableScan[t2]
   * }</pre>
   *
   * <p>into
   *
   * <pre>{@code
   * Project[*]
   *    └── UnionAll
   *        ├── Join[t1.id=t2.id, inner]
   *        │   ├── TableScan[t1]
   *        │   └── TableScan[t2]
   *        ├── Join[t1.age=t2.age AND t1.id≠t2.id, inner]
   *        │   ├── TableScan[t1]
   *        │   └── TableScan[t2]
   *        ├── Project[t1-side cols + NULLs]
   *        │   └── Join[t1.id=t2.id, anti]
   *        │       ├── Join[t1.age=t2.age, anti]
   *        │       │   ├── TableScan[t1]
   *        │       │   └── TableScan[t2]
   *        │       └── TableScan[t2]
   *        └── Project[NULLs + t2-side cols]
   *            └── Join[t2.id=t1.id, anti]
   *                ├── Join[t2.age=t1.age, anti]
   *                │   ├── TableScan[t2]
   *                │   └── TableScan[t1]
   *                └── TableScan[t1]
   * }</pre>
   */
  private RelNode expandFullJoin(Join join, RelBuilder relBuilder) {
    List<RexNode> orConds = splitCond(join);
    List<RelNode> joins = new ArrayList<>();
    joins.addAll(expandInnerJoinToRelNodes(join, orConds, relBuilder));
    joins.add(expandAntiJoinToRelNode(join, orConds, false, true, relBuilder));
    joins.add(expandAntiJoinToRelNode(join, orConds, true, true, relBuilder));

    relBuilder.pushAll(joins)
        .union(true, joins.size());

    final List<RexNode> projects = join.getRowType().getFieldList().stream()
        .map(field -> {
          RexNode rexNode = relBuilder.field(field.getIndex());
          return field.getType().equals(rexNode.getType())
              ? rexNode
              : relBuilder.getRexBuilder().makeCast(field.getType(), rexNode, true, false);
        }).collect(Collectors.toList());

    return relBuilder.project(projects)
        .build();
  }

  /**
   * This method will make the following conversions.
   *
   * <pre>{@code
   * Project[*]
   *    └── Join[OR(t1.id=t2.id, t1.age=t2.age), inner]
   *        ├── TableScan[t1]
   *        └── TableScan[t2]
   * }</pre>
   *
   * <p>into
   *
   * <pre>{@code
   * Project[*]
   *    └── UnionAll
   *        ├── Join[t1.id=t2.id, inner]
   *        │   ├── TableScan[t1]
   *        │   └── TableScan[t2]
   *        └── Join[t1.age=t2.age AND t1.id≠t2.id, inner]
   *            ├─── TableScan[t1]
   *            └─── TableScan[t2]
   * }</pre>
   */
  private RelNode expandInnerJoin(Join join, RelBuilder relBuilder) {
    List<RexNode> orConds = splitCond(join);
    List<RelNode> joins = expandInnerJoinToRelNodes(join, orConds, relBuilder);
    return relBuilder.pushAll(joins)
        .union(true, joins.size())
        .build();
  }

  private List<RelNode> expandInnerJoinToRelNodes(Join join, List<RexNode> orConds,
      RelBuilder relBuilder) {
    List<RelNode> joins = new ArrayList<>();
    for (int i = 0; i < orConds.size(); i++) {
      RexNode orCond = orConds.get(i);
      for (int j = 0; j < i; j++) {
        orCond = relBuilder.and(orCond, relBuilder.not(orConds.get(j)));
      }

      relBuilder.push(join.getLeft())
          .push(join.getRight())
          .join(JoinRelType.INNER, orCond);

      joins.add(relBuilder.build());
    }
    return joins;
  }

  /**
   * This method will make the following conversions.
   *
   * <pre>{@code
   * Project[*]
   *    └── Join[OR(id=id0, age=age0), anti]
   *        ├── TableScan[tbl]
   *        └── TableScan[tbl]
   * }</pre>
   *
   * <p>into
   *
   * <pre>{@code
   * HashJoin[id=id0, anti]
   *    ├── HashJoin[age=age0, anti]
   *    │   ├── TableScan[tbl]
   *    │   └── TableScan[tbl]
   *    └── TableScan[tbl]
   * }</pre>
   */
  private RelNode expandAntiJoin(Join join, RelBuilder relBuilder) {
    List<RexNode> orConds = splitCond(join);
    return expandAntiJoinToRelNode(join, orConds, true, false, relBuilder);
  }

  private RelNode expandAntiJoinToRelNode(Join join, List<RexNode> orConds,
      boolean isLeftAnti, boolean isAppendNulls, RelBuilder relBuilder) {
    RelNode left = isLeftAnti ? join.getLeft() : join.getRight();
    RelNode right = isLeftAnti ? join.getRight() : join.getLeft();

    RelNode top = left;
    for (int i = 0; i < orConds.size(); i++) {
      RexNode orCond = orConds.get(i);
      relBuilder.push(top)
          .push(right)
          .join(JoinRelType.ANTI,
              isLeftAnti
                  ? orCond
                  : JoinCommuteRule.swapJoinCond(orCond, join, relBuilder.getRexBuilder()));
      top = relBuilder.build();
    }

    if (!isAppendNulls) {
      return top;
    }

    relBuilder.push(top);
    List<RexNode> fields = new ArrayList<>(relBuilder.fields());
    List<RexNode> nulls = new ArrayList<>();
    for (int i = 0; i < right.getRowType().getFieldCount(); i++) {
      nulls.add(
          relBuilder.getRexBuilder().makeNullLiteral(
              right.getRowType().getFieldList().get(i).getType()));
    }

    List<RexNode> projects = isLeftAnti
        ? Stream.concat(fields.stream(), nulls.stream()).collect(Collectors.toList())
        : Stream.concat(nulls.stream(), fields.stream()).collect(Collectors.toList());

    return relBuilder.project(projects)
        .build();
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableJoinExpandOrToUnionRule.Config.of()
        .withOperandFor(Join.class);

    @Override default JoinExpandOrToUnionRule toRule() {
      return new JoinExpandOrToUnionRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Join> joinClass) {
      return withOperandSupplier(b -> b.operand(joinClass).anyInputs())
          .as(Config.class);
    }
  }
}
