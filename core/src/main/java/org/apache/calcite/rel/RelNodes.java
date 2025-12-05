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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.Ordering;

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
