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
package org.apache.calcite.rel;

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.AsofJoin;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.ConditionalCorrelate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sample;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.Ordering;

import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Comparator;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * Utilities concerning relational expressions.
 */
public class RelNodes {
  /** Comparator that provides an arbitrary but stable ordering to
   * {@link RelNode}s. */
  public static final Comparator<RelNode> COMPARATOR =
      new RelNodeComparator();

  /** Ordering for {@link RelNode}s. */
  public static final Ordering<RelNode> ORDERING = Ordering.from(COMPARATOR);

  private RelNodes() {}

  /** Compares arrays of {@link RelNode}. */
  public static int compareRels(RelNode[] rels0, RelNode[] rels1) {
    int c = Integer.compare(rels0.length, rels1.length);
    if (c != 0) {
      return c;
    }
    for (int i = 0; i < rels0.length; i++) {
      c = COMPARATOR.compare(rels0[i], rels1[i]);
      if (c != 0) {
        return c;
      }
    }
    return 0;
  }

  /** Returns whether a tree of {@link RelNode}s contains a match for a
   * {@link RexNode} finder. */
  public static boolean contains(RelNode rel,
      Predicate<AggregateCall> aggPredicate, RexUtil.RexFinder finder) {
    try {
      findRex(rel, finder, aggPredicate, (relNode, rexNode) -> {
        throw Util.FoundOne.NULL;
      });
      return false;
    } catch (Util.FoundOne e) {
      return true;
    }
  }

  /** Searches for expressions in a tree of {@link RelNode}s. */
  // TODO: a new method RelNode.accept(RexVisitor, BiConsumer), with similar
  // overrides to RelNode.accept(RexShuttle), would be better.
  public static void findRex(RelNode rel, RexUtil.RexFinder finder,
      Predicate<AggregateCall> aggPredicate,
      BiConsumer<RelNode, @Nullable RexNode> consumer) {
    if (rel instanceof Filter) {
      Filter filter = (Filter) rel;
      try {
        filter.getCondition().accept(finder);
      } catch (Util.FoundOne e) {
        consumer.accept(filter, (RexNode) e.getNode());
      }
    }
    if (rel instanceof Project) {
      Project project = (Project) rel;
      for (RexNode node : project.getProjects()) {
        try {
          node.accept(finder);
        } catch (Util.FoundOne e) {
          consumer.accept(project, (RexNode) e.getNode());
        }
      }
    }
    if (rel instanceof Join) {
      Join join = (Join) rel;
      try {
        join.getCondition().accept(finder);
      } catch (Util.FoundOne e) {
        consumer.accept(join, (RexNode) e.getNode());
      }
    }
    if (rel instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) rel;
      for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
        if (aggPredicate.test(aggregateCall)) {
          consumer.accept(aggregate, null);
        }
      }
    }
    for (RelNode input : rel.getInputs()) {
      findRex(input, finder, aggPredicate, consumer);
    }
  }

  /** Visits a node's expression fields for read-only analysis.
   *
   * <p>Does not visit relational inputs, expand planner wrappers, visit
   * literal-only data, or apply rewritten expressions. The visitor controls
   * traversal inside expressions, including sub-query plans.
   *
   * @return Whether the operator family has known expression coverage;
   *     false means that unseen references or calls cannot be ruled out
   */
  @API(since = "1.43.0", status = API.Status.INTERNAL)
  public static boolean visitExpressions(RelNode rel, RexShuttle visitor) {
    rel.accept(visitor);
    if (rel instanceof Aggregate) {
      for (AggregateCall call : ((Aggregate) rel).getAggCallList()) {
        visitor.apply(call.rexList);
      }
    } else if (rel instanceof AsofJoin) {
      visitor.apply(((AsofJoin) rel).getMatchCondition());
    } else if (rel instanceof ConditionalCorrelate) {
      visitor.apply(((ConditionalCorrelate) rel).getCondition());
    } else if (rel instanceof Match) {
      final Match match = (Match) rel;
      match.getMeasures().values().forEach(e -> e.accept(visitor));
      match.getPatternDefinitions().values().forEach(e -> e.accept(visitor));
      visitor.apply(match.getPattern());
      visitor.apply(match.getAfter());
      visitor.apply(match.getInterval());
    } else if (rel instanceof Window) {
      for (Window.Group group : ((Window) rel).groups) {
        visitor.apply(group.aggCalls);
        visitor.apply(group.lowerBound.getOffset());
        visitor.apply(group.upperBound.getOffset());
      }
    } else if (rel instanceof TableModify) {
      visitor.apply(((TableModify) rel).getSourceExpressionList());
    } else {
      return rel instanceof Project
          || rel instanceof Filter
          || rel instanceof Calc
          || rel instanceof Join
          || rel instanceof Correlate
          || rel instanceof Sort
          || rel instanceof SetOp
          || rel instanceof Exchange
          || rel instanceof TableScan
          || rel instanceof Values
          || rel instanceof TableFunctionScan
          || rel instanceof Snapshot
          || rel instanceof Collect
          || rel instanceof Uncollect
          || rel instanceof Sample;
    }
    return true;
  }

  /** Arbitrary stable comparator for {@link RelNode}s. */
  private static class RelNodeComparator implements Comparator<RelNode> {
    @Override public int compare(RelNode o1, RelNode o2) {
      // Compare on field count first. It is more stable than id (when rules
      // are added to the set of active rules).
      final int c =
          Integer.compare(o1.getRowType().getFieldCount(),
              o2.getRowType().getFieldCount());
      if (c != 0) {
        return -c;
      }
      return Integer.compare(o1.getId(), o2.getId());
    }
  }
}
