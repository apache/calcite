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

package org.apache.calcite.rel.core;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;

/**
 * Contains factory interface and default implementation for creating various
 * rel nodes.
 */
public class RelFactories {
  public static final ProjectFactory DEFAULT_PROJECT_FACTORY =
      new ProjectFactoryImpl();

  public static final FilterFactory DEFAULT_FILTER_FACTORY =
      new FilterFactoryImpl();

  public static final JoinFactory DEFAULT_JOIN_FACTORY = new JoinFactoryImpl();

  public static final SemiJoinFactory DEFAULT_SEMI_JOIN_FACTORY =
      new SemiJoinFactoryImpl();

  public static final SortFactory DEFAULT_SORT_FACTORY =
    new SortFactoryImpl();

  public static final AggregateFactory DEFAULT_AGGREGATE_FACTORY =
    new AggregateFactoryImpl();

  public static final SetOpFactory DEFAULT_SET_OP_FACTORY =
      new SetOpFactoryImpl();

  private RelFactories() {
  }

  /**
   * Can create a
   * {@link org.apache.calcite.rel.logical.LogicalProject} of the
   * appropriate type for this rule's calling convention.
   */
  public interface ProjectFactory {
    /** Creates a project. */
    RelNode createProject(RelNode child, List<? extends RexNode> childExprs,
        List<String> fieldNames);
  }

  /**
   * Implementation of {@link ProjectFactory} that returns a vanilla
   * {@link org.apache.calcite.rel.logical.LogicalProject}.
   */
  private static class ProjectFactoryImpl implements ProjectFactory {
    public RelNode createProject(RelNode child,
        List<? extends RexNode> childExprs, List<String> fieldNames) {
      return RelOptUtil.createProject(child, childExprs, fieldNames);
    }
  }

  /**
   * Can create a {@link Sort} of the appropriate type
   * for this rule's calling convention.
   */
  public interface SortFactory {
    /** Creates a sort. */
    RelNode createSort(RelTraitSet traits, RelNode child,
        RelCollation collation, RexNode offset, RexNode fetch);
  }

  /**
   * Implementation of {@link RelFactories.SortFactory} that
   * returns a vanilla {@link Sort}.
   */
  private static class SortFactoryImpl implements SortFactory {
    public RelNode createSort(RelTraitSet traits, RelNode child,
        RelCollation collation, RexNode offset, RexNode fetch) {
      return new Sort(child.getCluster(), traits, child, collation,
          offset, fetch);
    }
  }

  /**
   * Can create a {@link SetOp} for a particular kind of
   * set operation (UNION, EXCEPT, INTERSECT) and of the appropriate type
   * for this rule's calling convention.
   */
  public interface SetOpFactory {
    /** Creates a set operation. */
    RelNode createSetOp(SqlKind kind, List<RelNode> inputs, boolean all);
  }

  /**
   * Implementation of {@link RelFactories.SetOpFactory} that
   * returns a vanilla {@link SetOp} for the particular kind of set
   * operation (UNION, EXCEPT, INTERSECT).
   */
  private static class SetOpFactoryImpl implements SetOpFactory {
    public RelNode createSetOp(SqlKind kind, List<RelNode> inputs,
        boolean all) {
      final RelOptCluster cluster = inputs.get(0).getCluster();
      switch (kind) {
      case UNION:
        return new LogicalUnion(cluster, inputs, all);
      case EXCEPT:
        return new LogicalMinus(cluster, inputs, all);
      case INTERSECT:
        return new LogicalIntersect(cluster, inputs, all);
      default:
        throw new AssertionError("not a set op: " + kind);
      }
    }
  }

  /**
   * Can create a {@link LogicalAggregate} of the appropriate type
   * for this rule's calling convention.
   */
  public interface AggregateFactory {
    /** Creates an aggregate. */
    RelNode createAggregate(RelNode child, boolean indicator,
        ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls);
  }

  /**
   * Implementation of {@link RelFactories.AggregateFactory}
   * that returns a vanilla {@link LogicalAggregate}.
   */
  private static class AggregateFactoryImpl implements AggregateFactory {
    public RelNode createAggregate(RelNode child, boolean indicator,
        ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls) {
      return new LogicalAggregate(child.getCluster(), child, indicator,
          groupSet, groupSets, aggCalls);
    }
  }

  /**
   * Can create a {@link LogicalFilter} of the appropriate type
   * for this rule's calling convention.
   */
  public interface FilterFactory {
    /** Creates a filter. */
    RelNode createFilter(RelNode child, RexNode condition);
  }

  /**
   * Implementation of {@link RelFactories.FilterFactory} that
   * returns a vanilla {@link LogicalFilter}.
   */
  private static class FilterFactoryImpl implements FilterFactory {
    public RelNode createFilter(RelNode child, RexNode condition) {
      return new LogicalFilter(child.getCluster(), child, condition);
    }
  }

  /**
   * Can create a join of the appropriate type for a rule's calling convention.
   *
   * <p>The result is typically a {@link Join}.
   */
  public interface JoinFactory {
    /**
     * Creates a join.
     *
     * @param left             Left input
     * @param right            Right input
     * @param condition        Join condition
     * @param joinType         Join type
     * @param variablesStopped Set of names of variables which are set by the
     *                         LHS and used by the RHS and are not available to
     *                         nodes above this LogicalJoin in the tree
     * @param semiJoinDone     Whether this join has been translated to a
     *                         semi-join
     */
    RelNode createJoin(RelNode left, RelNode right, RexNode condition,
        JoinRelType joinType, Set<String> variablesStopped,
        boolean semiJoinDone);
  }

  /**
   * Implementation of {@link JoinFactory} that returns a vanilla
   * {@link org.apache.calcite.rel.logical.LogicalJoin}.
   */
  private static class JoinFactoryImpl implements JoinFactory {
    public RelNode createJoin(RelNode left, RelNode right,
        RexNode condition, JoinRelType joinType,
        Set<String> variablesStopped, boolean semiJoinDone) {
      final RelOptCluster cluster = left.getCluster();
      return new LogicalJoin(cluster, left, right, condition, joinType,
          variablesStopped, semiJoinDone, ImmutableList.<RelDataTypeField>of());
    }
  }

  /**
   * Can create a semi-join of the appropriate type for a rule's calling
   * convention.
   */
  public interface SemiJoinFactory {
    /**
     * Creates a semi-join.
     *
     * @param left             Left input
     * @param right            Right input
     * @param condition        Join condition
     */
    RelNode createSemiJoin(RelNode left, RelNode right, RexNode condition);
  }

  /**
   * Implementation of {@link SemiJoinFactory} that returns a vanilla
   * {@link SemiJoin}.
   */
  private static class SemiJoinFactoryImpl implements SemiJoinFactory {
    public RelNode createSemiJoin(RelNode left, RelNode right,
        RexNode condition) {
      final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
      return new SemiJoin(left.getCluster(), left.getTraitSet(), left, right,
        condition, joinInfo.leftKeys, joinInfo.rightKeys);
    }
  }
}

// End RelFactories.java
