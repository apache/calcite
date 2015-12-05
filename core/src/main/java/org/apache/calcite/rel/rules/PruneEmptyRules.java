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
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexLiteral;

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
          LogicalUnion union = call.rel(0);
          final List<RelNode> childRels = call.getChildRels(union);
          assert childRels != null;
          final List<RelNode> newChildRels = new ArrayList<>();
          for (RelNode childRel : childRels) {
            if (!isEmpty(childRel)) {
              newChildRels.add(childRel);
            }
          }
          assert newChildRels.size() < childRels.size()
              : "planner promised us at least one Empty child";
          RelNode newRel;
          switch (newChildRels.size()) {
          case 0:
            newRel = empty(union);
            break;
          case 1:
            newRel =
                RelOptUtil.createCastRel(
                    newChildRels.get(0),
                    union.getRowType(),
                    true);
            break;
          default:
            newRel = LogicalUnion.create(newChildRels, union.all);
            break;
          }
          call.transformTo(newRel);
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
      new RemoveEmptySingleRule(Project.class, "PruneEmptyProject");

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
              && RexLiteral.intValue(sort.fetch) == 0) {
            call.transformTo(empty(sort));
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
   * <li>Aggregate(Empty) becomes Empty
   * </ul>
   */
  public static final RelOptRule AGGREGATE_INSTANCE =
      new RemoveEmptySingleRule(Aggregate.class, "PruneEmptyAggregate");

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
          call.transformTo(empty(join));
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
          call.transformTo(empty(join));
        }
      };

  /** Creates a {@link org.apache.calcite.rel.core.Values} to replace
   * {@code node}. */
  private static Values empty(RelNode node) {
    return LogicalValues.createEmpty(node.getCluster(), node.getRowType());
  }

  /** Planner rule that converts a single-rel (e.g. project, sort, aggregate or
   * filter) on top of the empty relational expression into empty. */
  private static class RemoveEmptySingleRule extends RelOptRule {
    public RemoveEmptySingleRule(Class<? extends SingleRel> clazz,
        String description) {
      super(
          operand(clazz,
              operand(Values.class, null, Values.IS_EMPTY, none())),
          description);
    }

    public void onMatch(RelOptRuleCall call) {
      SingleRel single = call.rel(0);
      call.transformTo(empty(single));
    }
  }
}

// End PruneEmptyRules.java
