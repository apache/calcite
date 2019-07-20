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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.mutable.MutableAggregate;
import org.apache.calcite.rel.mutable.MutableFilter;
import org.apache.calcite.rel.mutable.MutableProject;
import org.apache.calcite.rel.mutable.MutableRel;
import org.apache.calcite.rel.mutable.MutableRels;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Extension to {@link SubstitutionVisitor}.
 */
public class MaterializedViewSubstitutionVisitor extends SubstitutionVisitor {
  private static final ImmutableList<UnifyRule> EXTENDED_RULES =
      ImmutableList.<UnifyRule>builder()
          .addAll(DEFAULT_RULES)
          .add(ProjectToProjectUnifyRule1.INSTANCE)
          .add(FilterToFilterUnifyRule1.INSTANCE)
          .add(FilterToProjectUnifyRule1.INSTANCE)
          .add(ProjectAggregateToProjectAggregateUnifyRule.INSTANCE)
          .build();

  public MaterializedViewSubstitutionVisitor(RelNode target_, RelNode query_) {
    super(target_, query_, EXTENDED_RULES);
  }

  public MaterializedViewSubstitutionVisitor(RelNode target_, RelNode query_,
      RelBuilderFactory relBuilderFactory) {
    super(target_, query_, EXTENDED_RULES, relBuilderFactory);
  }

  public List<RelNode> go(RelNode replacement_) {
    return super.go(replacement_);
  }

  /**
   * Implementation of {@link SubstitutionVisitor.UnifyRule} that matches a
   * {@link MutableProject} to a
   * {@link MutableProject} where the condition of the target
   * relation is weaker.
   *
   * <p>Example: target has a weaker condition and contains all columns selected
   * by query</p>
   * <ul>
   * <li>query:   Project(projects: [$2, $0])
   *                Filter(condition: &gt;($1, 20))
   *                  Scan(table: [hr, emps])</li>
   * <li>target:  Project(projects: [$0, $1, $2])
   *                Filter(condition: &gt;($1, 10))
   *                  Scan(table: [hr, emps])</li>
   * </ul>
   */
  private static class ProjectToProjectUnifyRule1 extends AbstractUnifyRule {
    public static final ProjectToProjectUnifyRule1 INSTANCE =
        new ProjectToProjectUnifyRule1();

    private ProjectToProjectUnifyRule1() {
      super(operand(MutableProject.class, query(0)),
          operand(MutableProject.class, target(0)), 1);
    }

    @Override protected UnifyResult apply(UnifyRuleCall call) {
      final MutableProject query = (MutableProject) call.query;

      final List<RelDataTypeField> oldFieldList =
          query.getInput().rowType.getFieldList();
      final List<RelDataTypeField> newFieldList =
          call.target.rowType.getFieldList();
      List<RexNode> newProjects;
      try {
        newProjects = transformRex(query.projects, oldFieldList, newFieldList);
      } catch (MatchFailed e) {
        return null;
      }

      final MutableProject newProject =
          MutableProject.of(query.rowType, call.target, newProjects);

      final MutableRel newProject2 = MutableRels.strip(newProject);
      return call.result(newProject2);
    }

    @Override protected UnifyRuleCall match(SubstitutionVisitor visitor,
        MutableRel query, MutableRel target) {
      assert query instanceof MutableProject && target instanceof MutableProject;

      if (queryOperand.matches(visitor, query)) {
        if (targetOperand.matches(visitor, target)) {
          return null;
        } else if (targetOperand.isWeaker(visitor, target)) {

          final MutableProject queryProject = (MutableProject) query;
          if (queryProject.getInput() instanceof MutableFilter) {
            final MutableFilter innerFilter =
                (MutableFilter) queryProject.getInput();
            RexNode newCondition;
            try {
              newCondition = transformRex(innerFilter.condition,
                  innerFilter.getInput().rowType.getFieldList(),
                  target.rowType.getFieldList());
            } catch (MatchFailed e) {
              return null;
            }
            final MutableFilter newFilter = MutableFilter.of(target,
                newCondition);

            return visitor.new UnifyRuleCall(this, query, newFilter,
                copy(visitor.slots, slotCount));
          }
        }
      }
      return null;
    }
  }

  /**
   * Implementation of {@link SubstitutionVisitor.UnifyRule} that matches a
   * {@link MutableFilter} to a
   * {@link MutableFilter} where the condition of the target
   * relation is weaker.
   *
   * <p>Example: target has a weaker condition</p>
   * <ul>
   * <li>query:   Filter(condition: &gt;($1, 20))
   *                Scan(table: [hr, emps])</li>
   * <li>target:  Filter(condition: &gt;($1, 10))
   *                Scan(table: [hr, emps])</li>
   * </ul>
   */
  private static class FilterToFilterUnifyRule1 extends AbstractUnifyRule {
    public static final FilterToFilterUnifyRule1 INSTANCE =
        new FilterToFilterUnifyRule1();

    private FilterToFilterUnifyRule1() {
      super(operand(MutableFilter.class, query(0)),
          operand(MutableFilter.class, target(0)), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableFilter query = (MutableFilter) call.query;
      final MutableFilter target = (MutableFilter) call.target;
      final MutableFilter newFilter = MutableFilter.of(target, query.condition);
      return call.result(newFilter);
    }

    @Override protected UnifyRuleCall match(SubstitutionVisitor visitor,
        MutableRel query, MutableRel target) {
      if (queryOperand.matches(visitor, query)) {
        if (targetOperand.matches(visitor, target)) {
          if (visitor.isWeaker(query, target)) {
            return visitor.new UnifyRuleCall(this, query, target,
                copy(visitor.slots, slotCount));
          }
        }
      }
      return null;
    }
  }

  /**
   * Implementation of {@link SubstitutionVisitor.UnifyRule} that matches a
   * {@link MutableFilter} to a
   * {@link MutableProject} on top of a
   * {@link MutableFilter} where the condition of the target
   * relation is weaker.
   *
   * <p>Example: target has a weaker condition and is a permutation projection of
   * its child relation</p>
   * <ul>
   * <li>query:   Filter(condition: &gt;($1, 20))
   *                Scan(table: [hr, emps])</li>
   * <li>target:  Project(projects: [$1, $0, $2, $3, $4])
   *                Filter(condition: &gt;($1, 10))
   *                  Scan(table: [hr, emps])</li>
   * </ul>
   */
  private static class FilterToProjectUnifyRule1 extends AbstractUnifyRule {
    public static final FilterToProjectUnifyRule1 INSTANCE =
        new FilterToProjectUnifyRule1();

    private FilterToProjectUnifyRule1() {
      super(
          operand(MutableFilter.class, query(0)),
          operand(MutableProject.class,
              operand(MutableFilter.class, target(0))), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableRel query = call.query;

      final List<RelDataTypeField> oldFieldList =
          query.rowType.getFieldList();
      final List<RelDataTypeField> newFieldList =
          call.target.rowType.getFieldList();
      List<RexNode> newProjects;
      try {
        newProjects = transformRex(
            (List<RexNode>) call.getCluster().getRexBuilder().identityProjects(
                query.rowType),
            oldFieldList, newFieldList);
      } catch (MatchFailed e) {
        return null;
      }

      final MutableProject newProject =
          MutableProject.of(query.rowType, call.target, newProjects);

      final MutableRel newProject2 = MutableRels.strip(newProject);
      return call.result(newProject2);
    }

    @Override protected UnifyRuleCall match(SubstitutionVisitor visitor,
        MutableRel query, MutableRel target) {
      assert query instanceof MutableFilter && target instanceof MutableProject;

      if (queryOperand.matches(visitor, query)) {
        if (targetOperand.matches(visitor, target)) {
          if (visitor.isWeaker(query, ((MutableProject) target).getInput())) {
            final MutableFilter filter = (MutableFilter) query;
            RexNode newCondition;
            try {
              newCondition = transformRex(filter.condition,
                  filter.getInput().rowType.getFieldList(),
                  target.rowType.getFieldList());
            } catch (MatchFailed e) {
              return null;
            }
            final MutableFilter newFilter = MutableFilter.of(target,
                newCondition);
            return visitor.new UnifyRuleCall(this, query, newFilter,
                copy(visitor.slots, slotCount));
          }
        }
      }
      return null;
    }
  }

  /**
   * Implementation of {@link SubstitutionVisitor.UnifyRule} that matches a
   * {@link MutableProject} on top of a {@link MutableAggregate} to a
   * {@link MutableProject} on top of a {@link MutableAggregate}.
   *
   * <p>Example: aggregate calls of query is a subset of target.</p>
   * <ul>
   * <li>query:   Project(projects: [$0, *(2, $1)])
   *                Aggregate(groupSet: {0}, groupSets: [{0}], calls: [SUM($1)])
   *                  Scan(table: [hr, emps])</li>
   * <li>target:  Project(projects: [$0, *(2, $1), *(2, $2)])
   *                Aggregate(groupSet: {0}, groupSets: [{0}], calls: [SUM($1), COUNT()])
   *                  Scan(table: [hr, emps])</li>
   * </ul>
   */
  private static class ProjectAggregateToProjectAggregateUnifyRule
      extends AbstractUnifyRule {
    public static final ProjectAggregateToProjectAggregateUnifyRule INSTANCE =
        new ProjectAggregateToProjectAggregateUnifyRule();
    private ProjectAggregateToProjectAggregateUnifyRule() {
      super(
          operand(MutableProject.class,
              operand(MutableAggregate.class, query(0))),
          operand(MutableProject.class,
              operand(MutableAggregate.class, target(0))), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableProject query = (MutableProject) call.query;
      final MutableProject target = (MutableProject) call.target;
      final MutableAggregate agg0 = (MutableAggregate) query.getInput();
      final MutableAggregate agg1 = (MutableAggregate) target.getInput();

      if (agg0.getInput() != agg1.getInput()) {
        return null;
      }
      if (!agg0.groupSets.equals(agg1.groupSets)) {
        return null;
      }

      final List<Integer> mapping = new ArrayList<>();
      for (int i = 0; i < agg0.groupSet.cardinality(); i++) {
        mapping.add(i);
      }
      for (AggregateCall aggregateCall : agg0.aggCalls) {
        final int i = agg1.aggCalls.indexOf(aggregateCall);
        if (i < 0) {
          return null;
        } else {
          mapping.add(i + agg1.groupSet.cardinality());
        }
      }

      final RexShuttle shuttle0 = new RexShuttle() {
        @Override public RexNode visitInputRef(RexInputRef ref) {
          return new RexInputRef(mapping.get(ref.getIndex()), ref.getType());
        }
      };
      final List<RexNode> transformedProjects = shuttle0.apply(query.projects);
      final RexShuttle shuttle1 = getRexShuttle(target);
      try {
        final List<RexNode> newProjects = shuttle1.apply(transformedProjects);
        final MutableProject newProject = MutableProject.of(
            query.rowType, target, newProjects);
        return call.result(newProject);
      } catch (MatchFailed e) {
        return null;
      }
    }
  }

  private static RexNode transformRex(RexNode node,
      final List<RelDataTypeField> oldFields,
      final List<RelDataTypeField> newFields) {
    List<RexNode> nodes =
        transformRex(ImmutableList.of(node), oldFields, newFields);
    return nodes.get(0);
  }

  private static List<RexNode> transformRex(
      List<RexNode> nodes,
      final List<RelDataTypeField> oldFields,
      final List<RelDataTypeField> newFields) {
    RexShuttle shuttle = new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef ref) {
        RelDataTypeField f = oldFields.get(ref.getIndex());
        for (int index = 0; index < newFields.size(); index++) {
          RelDataTypeField newf = newFields.get(index);
          if (f.getKey().equals(newf.getKey())
              && f.getValue() == newf.getValue()) {
            return new RexInputRef(index, f.getValue());
          }
        }
        throw MatchFailed.INSTANCE;
      }
    };
    return shuttle.apply(nodes);
  }
}

// End MaterializedViewSubstitutionVisitor.java
