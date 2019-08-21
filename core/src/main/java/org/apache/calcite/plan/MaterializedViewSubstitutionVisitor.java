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
import org.apache.calcite.rel.mutable.MutableFilter;
import org.apache.calcite.rel.mutable.MutableJoin;
import org.apache.calcite.rel.mutable.MutableProject;
import org.apache.calcite.rel.mutable.MutableRel;
import org.apache.calcite.rel.mutable.MutableRels;
import org.apache.calcite.rel.mutable.MutableUnion;
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
          .add(UnionToUnionUnifyRule.INSTANCE)
          .add(JoinOnLeftProjectToJoinUnifyRule.INSTANCE)
          .add(JoinOnRightProjectToJoinUnifyRule.INSTANCE)
          .add(JoinOnProjectsToJoinUnifyRule.INSTANCE)
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
   * {@link MutableUnion} to a {@link MutableUnion} where the query and target
   * have the same inputs but might not have the same order.
   */
  private static class UnionToUnionUnifyRule extends AbstractUnifyRule {
    public static final UnionToUnionUnifyRule INSTANCE = new UnionToUnionUnifyRule();

    private UnionToUnionUnifyRule() {
      super(any(MutableUnion.class), any(MutableUnion.class), 0);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableUnion query = (MutableUnion) call.query;
      final MutableUnion target = (MutableUnion) call.target;
      List<MutableRel> queryInputs = query.getInputs();
      List<MutableRel> targetInputs = target.getInputs();
      if (queryInputs.size() == targetInputs.size()) {
        for (MutableRel rel: queryInputs) {
          int index = targetInputs.indexOf(rel);
          if (index == -1) {
            return null;
          } else {
            targetInputs.remove(index);
          }
        }
        return call.result(target);
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

  /**
   * Implementation of {@link SubstitutionVisitor.UnifyRule} that matches a
   * {@link MutableJoin} with {@link MutableProject} as left child to a
   * {@link MutableJoin}. Join type and condition should match exactly.
   */
  private static class JoinOnLeftProjectToJoinUnifyRule extends AbstractUnifyRule {

    public static final JoinOnLeftProjectToJoinUnifyRule INSTANCE =
        new JoinOnLeftProjectToJoinUnifyRule();

    private JoinOnLeftProjectToJoinUnifyRule() {
      super(
          operand(MutableJoin.class,
              operand(MutableProject.class, query(0)),
              query(1)),
          operand(MutableJoin.class,
              target(0),
              target(1)), 2);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableJoin query = (MutableJoin) call.query;
      final MutableProject queryLeft = (MutableProject) query.getInputs().get(0);
      final MutableRel queryRight = query.getInputs().get(1);
      final int qLeftFieldCount = queryLeft.rowType.getFieldCount();
      final int qLeftInputFieldCount = queryLeft.getInput().rowType.getFieldCount();
      final MutableJoin target = (MutableJoin) call.target;

      if (query.joinType == target.joinType) {
        RexNode newJoinCondition = new RexShuttle() {
          @Override public RexNode visitInputRef(RexInputRef inputRef) {
            if (inputRef.getIndex() < qLeftFieldCount) {
              return queryLeft.projects.get(inputRef.getIndex());
            } else {
              return new RexInputRef(inputRef.getIndex() - qLeftFieldCount
                  + qLeftInputFieldCount, inputRef.getType());
            }
          }
        }.apply(query.condition);

        if (newJoinCondition.equals(target.condition)) {
          final List<RexNode> newProjects = new ArrayList<>();
          for (int i = 0; i < query.rowType.getFieldCount(); i++) {
            if (i < qLeftFieldCount) {
              newProjects.add(queryLeft.projects.get(i));
            } else {
              newProjects.add(
                  new RexInputRef(i - qLeftFieldCount + qLeftInputFieldCount,
                      queryRight.rowType.getFieldList().get(i - qLeftFieldCount).getType()));
            }
          }
          return call.result(MutableProject.of(query.rowType, target, newProjects));
        }
      }
      return null;
    }
  }

  /**
   * Implementation of {@link SubstitutionVisitor.UnifyRule} that matches a
   * {@link MutableJoin} with {@link MutableProject} as right child to a
   * {@link MutableJoin}. Join type and condition should match exactly.
   */
  private static class JoinOnRightProjectToJoinUnifyRule extends AbstractUnifyRule {

    public static final JoinOnRightProjectToJoinUnifyRule INSTANCE =
        new JoinOnRightProjectToJoinUnifyRule();

    private JoinOnRightProjectToJoinUnifyRule() {
      super(
          operand(MutableJoin.class,
              query(0),
              operand(MutableProject.class, query(1))),
          operand(MutableJoin.class,
              target(0),
              target(1)), 2);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableJoin query = (MutableJoin) call.query;
      final MutableRel queryLeft = query.getInputs().get(0);
      final MutableProject queryRightInput = (MutableProject) query.getInputs().get(1);
      final int qLeftFieldCount = queryLeft.rowType.getFieldCount();
      final MutableJoin target = (MutableJoin) call.target;

      if (query.joinType == target.joinType) {
        RexNode newJoinCondition = new RexShuttle() {
          @Override public RexNode visitInputRef(RexInputRef inputRef) {
            if (inputRef.getIndex() < qLeftFieldCount) {
              return inputRef;
            } else {
              RexShuttle shuttle0 = new RexShuttle() {
                @Override public RexNode visitInputRef(RexInputRef inputRef) {
                  return new RexInputRef(
                      inputRef.getIndex() + qLeftFieldCount, inputRef.getType());
                }
              };
              return shuttle0.apply(
                  queryRightInput.projects.get(inputRef.getIndex() - qLeftFieldCount));
            }
          }
        }.apply(query.condition);

        if (newJoinCondition.equals(target.condition)) {
          final List<RexNode> newProjects = new ArrayList<>();
          for (int i = 0; i < query.rowType.getFieldCount(); i++) {
            if (i < queryLeft.rowType.getFieldCount()) {
              newProjects.add(
                  new RexInputRef(i, queryLeft.rowType.getFieldList().get(i).getType()));
            } else {
              RexShuttle shuttle0 = new RexShuttle() {
                @Override public RexNode visitInputRef(RexInputRef inputRef) {
                  return new RexInputRef(
                      inputRef.getIndex() + qLeftFieldCount, inputRef.getType());
                }
              };
              newProjects.add(
                  shuttle0.apply(queryRightInput.projects.get(i - qLeftFieldCount)));
            }
          }
          return call.result(MutableProject.of(query.rowType, target, newProjects));
        }
      }
      return null;
    }
  }

  /**
   * Implementation of {@link SubstitutionVisitor.UnifyRule} that matches a
   * {@link MutableJoin} with {@link MutableProject}s as inputs to a
   * {@link MutableJoin}. Join type and condition should match exactly.
   */
  private static class JoinOnProjectsToJoinUnifyRule extends AbstractUnifyRule {

    public static final JoinOnProjectsToJoinUnifyRule INSTANCE =
        new JoinOnProjectsToJoinUnifyRule();

    private JoinOnProjectsToJoinUnifyRule() {
      super(
          operand(MutableJoin.class,
              operand(MutableProject.class, query(0)),
              operand(MutableProject.class, query(1))),
          operand(MutableJoin.class,
              target(0),
              target(1)), 2);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableJoin query = (MutableJoin) call.query;
      final MutableProject queryLeft = (MutableProject) query.getInputs().get(0);
      final MutableProject queryRight = (MutableProject) query.getInputs().get(1);
      final int qLeftFieldCount = queryLeft.rowType.getFieldCount();
      final int qLeftInputFieldCount = queryLeft.getInput().rowType.getFieldCount();
      final MutableJoin target = (MutableJoin) call.target;
      if (query.joinType == target.joinType) {
        RexNode newJoinCondition = new RexShuttle() {
          @Override public RexNode visitInputRef(RexInputRef inputRef) {
            if (inputRef.getIndex() < qLeftFieldCount) {
              return queryLeft.projects.get(inputRef.getIndex());
            } else {
              RexShuttle shuttle0 = new RexShuttle() {
                @Override public RexNode visitInputRef(RexInputRef inputRef) {
                  return new RexInputRef(
                      inputRef.getIndex() + qLeftInputFieldCount, inputRef.getType());
                }
              };
              return shuttle0.apply(
                  queryRight.projects.get(inputRef.getIndex() - qLeftFieldCount));
            }
          }
        }.apply(query.condition);
        if (newJoinCondition.equals(target.condition)) {
          final List<RexNode> newProjects = new ArrayList<>();
          for (int i = 0; i < query.rowType.getFieldCount(); i++) {
            if (i < queryLeft.rowType.getFieldCount()) {
              newProjects.add(queryLeft.projects.get(i));
            } else {
              RexShuttle shuttle0 = new RexShuttle() {
                @Override public RexNode visitInputRef(RexInputRef inputRef) {
                  return new RexInputRef(
                    inputRef.getIndex() + qLeftInputFieldCount, inputRef.getType());
                }
              };
              newProjects.add(
                  shuttle0.apply(queryRight.projects.get(i - qLeftFieldCount)));
            }
          }
          return call.result(MutableProject.of(query.rowType, target, newProjects));
        }
      }
      return null;
    }
  }
}

// End MaterializedViewSubstitutionVisitor.java
