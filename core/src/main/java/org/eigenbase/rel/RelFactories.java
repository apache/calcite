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

package org.eigenbase.rel;

import java.util.AbstractList;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

import org.eigenbase.rel.rules.SemiJoinRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

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

  public static final SortRelFactory DEFAULT_SORTREL_FACTORY =
    new SortRelFactoryImpl();

  public static final AggrRelFactory DEFAULT_AGGRREL_FACTORY =
    new AggrRelFactoryImpl();

  public static final SetOpRelFactory DEFAULT_SETOPREL_FACTORY =
    new SetOptRelFactoryImpl();

  private RelFactories() {
  }

  /**
   * Can create a {@link org.eigenbase.rel.ProjectRel} of the appropriate type
   * for this rule's calling convention.
   */
  public interface ProjectFactory {
    /**
     * Can create a {@link org.eigenbase.rel.ProjectRel} of the appropriate type
     * for this rule's calling convention.
     */
    RelNode createProject(RelNode child, List<RexNode> childExprs,
        List<String> fieldNames);
  }

  /**
   * Implementation of {@link ProjectFactory} that returns vanilla
   * {@link ProjectRel}.
   */
  private static class ProjectFactoryImpl implements ProjectFactory {
    public RelNode createProject(RelNode child, List<RexNode> childExprs,
        List<String> fieldNames) {
      return CalcRel.createProject(child, childExprs, fieldNames);
    }
  }

  /**
   * Can create a {@link org.eigenbase.rel.SortRel} of the appropriate type
   * for this rule's calling convention.
   */
  public interface SortRelFactory {

    RelNode createSortRel(RelTraitSet traits, RelNode child,
      RelCollation collation, RexNode offset, RexNode fetch);
  }

  /**
   * Implementation of {@link org.eigenbase.rel.RelFactories.SortRelFactory} that
   * returns vanilla {@link SortRel}.
   */
  private static class SortRelFactoryImpl implements SortRelFactory {

    public RelNode createSortRel(RelTraitSet traits, RelNode child,
      RelCollation collation, RexNode offset, RexNode fetch) {
      return new SortRel(child.getCluster(), traits, child, collation,
        offset, fetch);
    }
  }

  /**
   * Can create a {@link org.eigenbase.rel.SetOpRel} of the appropriate type
   * for this rule's calling convention.
   */
  public interface SetOpRelFactory {
    RelNode createSetOpRelNode(SetOpRel setOpRel, List<RelNode> inputs);
  }

  /**
   * Implementation of {@link org.eigenbase.rel.RelFactories.SetOpRelFactory} that
   * returns vanilla {@link SetOpRel}.
   */
  private static class SetOptRelFactoryImpl implements SetOpRelFactory {

    public RelNode createSetOpRelNode(SetOpRel setOpRel, List<RelNode> inputs) {
      return setOpRel.copy(setOpRel.getTraitSet(), inputs);
    }
  }

  /**
   * Can create a {@link org.eigenbase.rel.AggregateRel} of the appropriate type
   * for this rule's calling convention.
   */
  public interface AggrRelFactory {
    RelNode createAggrRelNode(RelNode child, BitSet groupSet,
      List<AggregateCall> aggCalls);
  }

  /**
   * Implementation of {@link org.eigenbase.rel.RelFactories.AggrRelFactory} that
   * returns vanilla {@link AggregateRel}.
   */
  private static class AggrRelFactoryImpl implements AggrRelFactory {

    public RelNode createAggrRelNode(RelNode child, BitSet groupSet,
        List<AggregateCall> aggCalls) {
      return new AggregateRel(child.getCluster(), child, groupSet, aggCalls);
    }
  }

  /**
   * Can create a {@link org.eigenbase.rel.FilterRel} of the appropriate type
   * for this rule's calling convention.
   */
  public interface FilterFactory {
    /**
     * Can create a {@link org.eigenbase.rel.FilterRel} of the appropriate type
     * for this rule's calling convention.
     */
    RelNode createFilter(RelNode child, RexNode condition);
  }

  /**
   * Implementation of {@link org.eigenbase.rel.RelFactories.FilterFactory} that
   * returns vanilla {@link FilterRel}.
   */
  private static class FilterFactoryImpl implements FilterFactory {
    public RelNode createFilter(RelNode child, RexNode condition) {
      return new FilterRel(child.getCluster(), child, condition);
    }
  }

  /**
   * Can create a {@link org.eigenbase.rel.JoinRelBase} of the appropriate type
   * for this rule's calling convention.
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
     *                         nodes above this JoinRel in the tree
     * @param semiJoinDone     Whether this join has been translated to a
     *                         semi-join
     */
    RelNode createJoin(RelNode left, RelNode right, RexNode condition,
        JoinRelType joinType, Set<String> variablesStopped,
        boolean semiJoinDone);

    SemiJoinRel createSemiJoinRel(RelTraitSet traitSet, RelNode left,
      RelNode right, RexNode condition);
  }

  /**
   * Implementation of {@link JoinFactory} that returns vanilla
   * {@link JoinRel}.
   */
  private static class JoinFactoryImpl implements JoinFactory {
    public RelNode createJoin(RelNode left, RelNode right, RexNode condition,
        JoinRelType joinType, Set<String> variablesStopped,
        boolean semiJoinDone) {
      final RelOptCluster cluster = left.getCluster();
      return new JoinRel(cluster, left, right, condition, joinType,
          variablesStopped, semiJoinDone, ImmutableList.<RelDataTypeField>of());
    }

    public SemiJoinRel createSemiJoinRel(RelTraitSet traitSet, RelNode left,
        RelNode right, RexNode condition) {
      final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
      return new SemiJoinRel(left.getCluster(), traitSet, left, right,
        condition, joinInfo.leftKeys, joinInfo.rightKeys);
    }
  }

  /**
   * Creates a relational expression that projects the given fields of the
   * input.
   *
   * <p>Optimizes if the fields are the identity projection.
   *
   * @param factory
   *          ProjectFactory
   * @param child
   *          Input relational expression
   * @param posList
   *          Source of each projected field
   * @return Relational expression that projects given fields
   */
  public static RelNode createProject(final ProjectFactory factory,
      final RelNode child, final List<Integer> posList) {
    if (Mappings.isIdentity(posList, child.getRowType().getFieldCount())) {
      return child;
    }
    final RexBuilder rexBuilder = child.getCluster().getRexBuilder();
    return factory.createProject(child,
        new AbstractList<RexNode>() {
          @Override
          public int size() {
            return posList.size();
          }

          @Override
          public RexNode get(int index) {
            final int pos = posList.get(index);
            return rexBuilder.makeInputRef(child, pos);
          }
        },
        null);
  }
}

// End RelFactories.java
