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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.plan.RelOptRule.any;
import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.some;
import static org.apache.calcite.plan.RelOptRule.unordered;

/**
 * Collection of rules which remove sections of a query plan known never to
 * produce any rows.
 *
 * <p>Conventionally, the way to represent an empty relational expression is
 * with a {@link Values} that has no tuples.
 *
 * @see LogicalValues#createEmpty
 */
public abstract class PruneEmptyRules {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * Rule that removes empty children of a
   * {@link org.apache.calcite.rel.logical.LogicalUnion}.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Union(Rel, Empty, Rel2) becomes Union(Rel, Rel2)
   * <li>Union(Rel, Empty, Empty) becomes Rel
   * <li>Union(Empty, Empty) becomes Empty
   * </ul>
   */
  public static final RelOptRule UNION_INSTANCE =
      new RelOptRule(
          operand(LogicalUnion.class,
              unordered(operand(Values.class, null, Values.IS_EMPTY, none()))),
          "Union") {
        public void onMatch(RelOptRuleCall call) {
          final LogicalUnion union = call.rel(0);
          final List<RelNode> inputs = call.getChildRels(union);
          assert inputs != null;
          final List<RelNode> newInputs = new ArrayList<>();
          for (RelNode input : inputs) {
            if (!isEmpty(input)) {
              newInputs.add(input);
            }
          }
          assert newInputs.size() < inputs.size()
              : "planner promised us at least one Empty child";
          final RelBuilder builder = call.builder();
          switch (newInputs.size()) {
          case 0:
            builder.push(union).empty();
            break;
          case 1:
            builder.push(
                RelOptUtil.createCastRel(
                    newInputs.get(0),
                    union.getRowType(),
                    true));
            break;
          default:
            builder.push(LogicalUnion.create(newInputs, union.all));
            break;
          }
          call.transformTo(builder.build());
        }
      };

  /**
   * Rule that removes empty children of a
   * {@link org.apache.calcite.rel.logical.LogicalMinus}.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Minus(Rel, Empty, Rel2) becomes Minus(Rel, Rel2)
   * <li>Minus(Empty, Rel) becomes Empty
   * </ul>
   */
  public static final RelOptRule MINUS_INSTANCE =
      new RelOptRule(
          operand(LogicalMinus.class,
              unordered(operand(Values.class, null, Values.IS_EMPTY, none()))),
          "Minus") {
        public void onMatch(RelOptRuleCall call) {
          final LogicalMinus minus = call.rel(0);
          final List<RelNode> inputs = call.getChildRels(minus);
          assert inputs != null;
          final List<RelNode> newInputs = new ArrayList<>();
          for (RelNode input : inputs) {
            if (!isEmpty(input)) {
              newInputs.add(input);
            } else if (newInputs.isEmpty()) {
              // If the first input of Minus is empty, the whole thing is
              // empty.
              break;
            }
          }
          assert newInputs.size() < inputs.size()
              : "planner promised us at least one Empty child";
          final RelBuilder builder = call.builder();
          switch (newInputs.size()) {
          case 0:
            builder.push(minus).empty();
            break;
          case 1:
            builder.push(
                RelOptUtil.createCastRel(
                    newInputs.get(0),
                    minus.getRowType(),
                    true));
            break;
          default:
            builder.push(LogicalMinus.create(newInputs, minus.all));
            break;
          }
          call.transformTo(builder.build());
        }
      };

  /**
   * Rule that converts a
   * {@link org.apache.calcite.rel.logical.LogicalIntersect} to
   * empty if any of its children are empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Intersect(Rel, Empty, Rel2) becomes Empty
   * <li>Intersect(Empty, Rel) becomes Empty
   * </ul>
   */
  public static final RelOptRule INTERSECT_INSTANCE =
      new RelOptRule(
          operand(LogicalIntersect.class,
              unordered(operand(Values.class, null, Values.IS_EMPTY, none()))),
          "Intersect") {
        public void onMatch(RelOptRuleCall call) {
          LogicalIntersect intersect = call.rel(0);
          final RelBuilder builder = call.builder();
          builder.push(intersect).empty();
          call.transformTo(builder.build());
        }
      };

  private static boolean isEmpty(RelNode node) {
    return node instanceof Values
        && ((Values) node).getTuples().isEmpty();
  }

  /**
   * Rule that converts a {@link org.apache.calcite.rel.logical.LogicalProject}
   * to empty if its child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Project(Empty) becomes Empty
   * </ul>
   */
  public static final RelOptRule PROJECT_INSTANCE =
      new RemoveEmptySingleRule(Project.class, Predicates.<Project>alwaysTrue(),
          RelFactories.LOGICAL_BUILDER, "PruneEmptyProject");

  /**
   * Rule that converts a {@link org.apache.calcite.rel.logical.LogicalFilter}
   * to empty if its child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Filter(Empty) becomes Empty
   * </ul>
   */
  public static final RelOptRule FILTER_INSTANCE =
      new RemoveEmptySingleRule(Filter.class, "PruneEmptyFilter");

  /**
   * Rule that converts a {@link org.apache.calcite.rel.core.Sort}
   * to empty if its child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Sort(Empty) becomes Empty
   * </ul>
   */
  public static final RelOptRule SORT_INSTANCE =
      new RemoveEmptySingleRule(Sort.class, "PruneEmptySort");

  /**
   * Rule that converts a {@link org.apache.calcite.rel.core.Sort}
   * to empty if it has {@code LIMIT 0}.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Sort(Empty) becomes Empty
   * </ul>
   */
  public static final RelOptRule SORT_FETCH_ZERO_INSTANCE =
      new RelOptRule(
          operand(Sort.class, any()), "PruneSortLimit0") {
        @Override public void onMatch(RelOptRuleCall call) {
          Sort sort = call.rel(0);
          if (sort.fetch != null
              && !(sort.fetch instanceof RexDynamicParam)
              && RexLiteral.intValue(sort.fetch) == 0) {
            call.transformTo(call.builder().push(sort).empty().build());
          }
        }
      };

  /**
   * Rule that converts an {@link org.apache.calcite.rel.core.Aggregate}
   * to empty if its child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>{@code Aggregate(key: [1, 3], Empty)} &rarr; {@code Empty}
   *
   * <li>{@code Aggregate(key: [], Empty)} is unchanged, because an aggregate
   * without a GROUP BY key always returns 1 row, even over empty input
   * </ul>
   *
   * @see AggregateValuesRule
   */
  public static final RelOptRule AGGREGATE_INSTANCE =
      new RemoveEmptySingleRule(Aggregate.class, Aggregate.IS_NOT_GRAND_TOTAL,
          RelFactories.LOGICAL_BUILDER, "PruneEmptyAggregate");

  /**
   * Rule that converts a {@link org.apache.calcite.rel.core.Join}
   * to empty if its left child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Join(Empty, Scan(Dept), INNER) becomes Empty
   * </ul>
   */
  public static final RelOptRule JOIN_LEFT_INSTANCE =
      new RelOptRule(
          operand(Join.class,
              some(
                  operand(Values.class, null, Values.IS_EMPTY, none()),
                  operand(RelNode.class, any()))),
              "PruneEmptyJoin(left)") {
        @Override public void onMatch(RelOptRuleCall call) {
          Join join = call.rel(0);
          if (join.getJoinType().generatesNullsOnLeft()) {
            // "select * from emp right join dept" is not necessarily empty if
            // emp is empty
            return;
          }
          call.transformTo(call.builder().push(join).empty().build());
        }
      };

  /**
   * Rule that converts a {@link org.apache.calcite.rel.core.Join}
   * to empty if its right child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Join(Scan(Emp), Empty, INNER) becomes Empty
   * </ul>
   */
  public static final RelOptRule JOIN_RIGHT_INSTANCE =
      new RelOptRule(
          operand(Join.class,
              some(
                  operand(RelNode.class, any()),
                  operand(Values.class, null, Values.IS_EMPTY, none()))),
              "PruneEmptyJoin(right)") {
        @Override public void onMatch(RelOptRuleCall call) {
          Join join = call.rel(0);
          if (join.getJoinType().generatesNullsOnRight()) {
            // "select * from emp left join dept" is not necessarily empty if
            // dept is empty
            return;
          }
          call.transformTo(call.builder().push(join).empty().build());
        }
      };

  /** Planner rule that converts a single-rel (e.g. project, sort, aggregate or
   * filter) on top of the empty relational expression into empty. */
  public static class RemoveEmptySingleRule extends RelOptRule {
    /** Creatse a simple RemoveEmptySingleRule. */
    public <R extends SingleRel> RemoveEmptySingleRule(Class<R> clazz,
        String description) {
      this(clazz, Predicates.<R>alwaysTrue(), RelFactories.LOGICAL_BUILDER,
          description);
    }

    /** Creates a RemoveEmptySingleRule. */
    public <R extends SingleRel> RemoveEmptySingleRule(Class<R> clazz,
        Predicate<R> predicate, RelBuilderFactory relBuilderFactory,
        String description) {
      super(
          operand(clazz, null, predicate,
              operand(Values.class, null, Values.IS_EMPTY, none())),
          relBuilderFactory, description);
    }

    public void onMatch(RelOptRuleCall call) {
      SingleRel single = call.rel(0);
      call.transformTo(call.builder().push(single).empty().build());
    }
  }
}

// End PruneEmptyRules.java
