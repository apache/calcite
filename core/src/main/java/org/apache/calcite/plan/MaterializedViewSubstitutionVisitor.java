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
package plan;

import org.apache.calcite.plan.SubstitutionVisitor;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import com.google.common.collect.ImmutableList;

import java.util.List;
/**
 * Substitutes part of a tree of relational expressions with another tree.
 *
 * <p>The call {@code new SubstitutionVisitor(target, query).go(replacement))}
 * will return {@code query} with every occurrence of {@code target} replaced
 * by {@code replacement}.</p>
 *
 * <p>The following example shows how {@code SubstitutionVisitor} can be used
 * for materialized view recognition.</p>
 *
 * <ul>
 * <li>query = SELECT a, c FROM t WHERE x = 5 AND b = 4</li>
 * <li>target = SELECT a, b, c FROM t WHERE x = 5</li>
 * <li>replacement = SELECT * FROM mv</li>
 * <li>result = SELECT a, c FROM mv WHERE b = 4</li>
 * </ul>
 *
 * <p>Note that {@code result} uses the materialized view table {@code mv} and a
 * simplified condition {@code b = 4}.</p>
 *
 * <p>Uses a bottom-up matching algorithm. Nodes do not need to be identical.
 * At each level, returns the residue.</p>
 *
 * <p>The inputs must only include the core relational operators:
 * {@link org.apache.calcite.rel.logical.LogicalTableScan},
 * {@link LogicalFilter},
 * {@link LogicalProject},
 * {@link org.apache.calcite.rel.logical.LogicalJoin},
 * {@link LogicalUnion},
 * {@link LogicalAggregate}.</p>
 */
public class MaterializedViewSubstitutionVisitor extends SubstitutionVisitor {

  public MaterializedViewSubstitutionVisitor(RelNode target_, RelNode query_) {
    super(target_, query_);
    this.unifyRules = ImmutableList.<UnifyRule>of(ProjectToProjectUnifyRule1.INSTANCE);
  }

  public RelNode go(RelNode replacement_) {
    this.RULE_MAP.clear();
    return super.go(replacement_);
  }

  /**
   * Project to Project Unify rule.
   */

  private static class ProjectToProjectUnifyRule1 extends AbstractUnifyRule {
    public static final ProjectToProjectUnifyRule1 INSTANCE =
            new ProjectToProjectUnifyRule1();

    private ProjectToProjectUnifyRule1() {
      super(operand(MutableProject.class, query(0)),
              operand(MutableProject.class, target(0)), 1);
    }

    @Override
    protected UnifyResult apply(UnifyRuleCall call) {
      final MutableProject query = (MutableProject) call.query;

      final List<RelDataTypeField> oldFieldList = query.getInput().getRowType().getFieldList();
      final List<RelDataTypeField> newFieldList = call.target.getRowType().getFieldList();
      List<RexNode> newProjects;
      try {
        newProjects = transformRex(query.getProjects(), oldFieldList, newFieldList);
      } catch (MatchFailed e) {
        return null;
      }

      final MutableProject newProject =
              MutableProject.of(
                      query.getRowType(), call.target, newProjects);

      final MutableRel newProject2 = MutableRels.strip(newProject);
      return call.result(newProject2);
    }

    @Override
    protected UnifyRuleCall match(SubstitutionVisitor visitor, MutableRel query,
                        MutableRel target) {
      assert query instanceof MutableProject && target instanceof MutableProject;

      if (queryOperand.matches(visitor, query)) {
        if (targetOperand.matches(visitor, target)) {
          return null;
        } else if (targetOperand.isWeaker(visitor, target)) {

          final MutableProject queryProject = (MutableProject) query;
          if (queryProject.getInput() instanceof MutableFilter) {

            final MutableFilter innerFilter = (MutableFilter) (queryProject.getInput());
            RexNode newCondition;
            try {
              newCondition = transformRex(innerFilter.getCondition(),
                      innerFilter.getInput().getRowType().getFieldList(),
                      target.getRowType().getFieldList());
            } catch (MatchFailed e) {
              return null;
            }
            final MutableFilter newFilter = MutableFilter.of(target,
                    newCondition);

            return visitor.new UnifyRuleCall(this, query, newFilter,
                    copy(visitor.getSlots(), slotCount));
          }
        }
      }
      return null;
    }

    private RexNode transformRex(RexNode node, final List<RelDataTypeField> oldFields,
                                       final List<RelDataTypeField> newFields) {
      List<RexNode> nodes = transformRex(ImmutableList.of(node), oldFields, newFields);
      return nodes.get(0);
    }

    private List<RexNode> transformRex(List<RexNode> nodes, final List<RelDataTypeField> oldFields,
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
}

// End SubstitutionVisitor.java
