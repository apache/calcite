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

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.RexLiteral;

import static org.eigenbase.relopt.RelOptRule.*;

/**
 * Collection of rules which remove sections of a query plan known never to
 * produce any rows.
 *
 * @see EmptyRel
 */
public abstract class RemoveEmptyRules {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * Rule that removes empty children of a
   * {@link UnionRel}.
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
          operand(UnionRel.class,
              unordered(operand(EmptyRel.class, none()))),
          "Union") {
        public void onMatch(RelOptRuleCall call) {
          UnionRel union = call.rel(0);
          final List<RelNode> childRels = call.getChildRels(union);
          final List<RelNode> newChildRels = new ArrayList<RelNode>();
          for (RelNode childRel : childRels) {
            if (!(childRel instanceof EmptyRel)) {
              newChildRels.add(childRel);
            }
          }
          assert newChildRels.size() < childRels.size()
              : "planner promised us at least one EmptyRel child";
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
            newRel =
                new UnionRel(
                    union.getCluster(),
                    newChildRels,
                    union.all);
            break;
          }
          call.transformTo(newRel);
        }
      };

  /**
   * Rule that converts a {@link ProjectRel}
   * to empty if its child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Project(Empty) becomes Empty
   * </ul>
   */
  public static final RelOptRule PROJECT_INSTANCE =
      new RemoveEmptySingleRule(ProjectRelBase.class, "PruneEmptyProject");

  /**
   * Rule that converts a {@link FilterRel}
   * to empty if its child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Filter(Empty) becomes Empty
   * </ul>
   */
  public static final RelOptRule FILTER_INSTANCE =
      new RemoveEmptySingleRule(FilterRelBase.class, "PruneEmptyFilter");

  /**
   * Rule that converts a {@link SortRel}
   * to empty if its child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Sort(Empty) becomes Empty
   * </ul>
   */
  public static final RelOptRule SORT_INSTANCE =
      new RemoveEmptySingleRule(SortRel.class, "PruneEmptySort");

  /**
   * Rule that converts a {@link SortRel}
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
          operand(SortRel.class, any()), "PruneSortLimit0") {
        @Override
        public void onMatch(RelOptRuleCall call) {
          SortRel sort = call.rel(0);
          if (sort.fetch != null
              && RexLiteral.intValue(sort.fetch) == 0) {
            call.transformTo(empty(sort));
          }
        }
      };

  /**
   * Rule that converts an {@link AggregateRelBase}
   * to empty if its child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Aggregate(Empty) becomes Empty
   * </ul>
   */
  public static final RelOptRule AGGREGATE_INSTANCE =
      new RemoveEmptySingleRule(AggregateRelBase.class, "PruneEmptyAggregate");

  /**
   * Rule that converts a {@link JoinRelBase}
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
          operand(JoinRelBase.class,
              some(
                  operand(EmptyRel.class, none()),
                  operand(RelNode.class, any()))),
              "PruneEmptyJoin(left)") {
        @Override
        public void onMatch(RelOptRuleCall call) {
          JoinRelBase join = call.rel(0);
          if (join.getJoinType().generatesNullsOnLeft()) {
            // "select * from emp right join dept" is not necessarily empty if
            // emp is empty
            return;
          }
          call.transformTo(empty(join));
        }
      };

  /**
   * Rule that converts a {@link JoinRelBase}
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
          operand(JoinRelBase.class,
              some(
                  operand(RelNode.class, any()),
                  operand(EmptyRel.class, none()))),
              "PruneEmptyJoin(right)") {
        @Override
        public void onMatch(RelOptRuleCall call) {
          JoinRelBase join = call.rel(0);
          if (join.getJoinType().generatesNullsOnRight()) {
            // "select * from emp left join dept" is not necessarily empty if
            // dept is empty
            return;
          }
          call.transformTo(empty(join));
        }
      };

  /** Creates an {@link EmptyRel} to replace {@code node}. */
  private static EmptyRel empty(RelNode node) {
    return new EmptyRel(node.getCluster(), node.getRowType());
  }

  /** Planner rule that converts a single-rel (e.g. project, sort, aggregate or
   * filter) on top of the empty relational expression into empty. */
  private static class RemoveEmptySingleRule extends RelOptRule {
    public RemoveEmptySingleRule(Class<? extends SingleRel> clazz,
        String description) {
      super(
          operand(clazz,
              operand(EmptyRel.class, none())),
          description);
    }

    public void onMatch(RelOptRuleCall call) {
      SingleRel single = call.rel(0);
      call.transformTo(empty(single));
    }
  }
}

// End RemoveEmptyRules.java
