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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that matches a
 * {@link org.apache.calcite.rel.core.Join}
 * and expands OR clauses in join conditions.
 *
 * <p>this rule would expand the OR condition into
 * two separate join conditions, allowing the optimizer
 * to handle these conditions more effectively.
 */
@Value.Enclosing
public class JoinConditionOrExpansionRule
    extends RelRule<JoinConditionOrExpansionRule.Config>
    implements TransformationRule {

  /** Creates an JoinConditionExpansionOrRule. */
  protected JoinConditionOrExpansionRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    RelBuilder relBuilder = call.builder();
    List<RexNode> orConds = RelOptUtil.disjunctions(join.getCondition());

    if (orConds.size() <= 1) {
      return;
    }

    RelNode converted;
    switch (join.getJoinType()) {
    case INNER:
      converted = expandInnerJoin(join, orConds, relBuilder);
      break;
    case SEMI:
      converted =
          expandSemiOrAntiJoin(join, JoinRelType.SEMI, orConds, relBuilder);
      break;
    case ANTI:
      converted =
          expandSemiOrAntiJoin(join, JoinRelType.ANTI, orConds, relBuilder);
      break;
    case LEFT:
      converted = expandLeftJoin(join, orConds, relBuilder);
      break;
    case RIGHT:
      converted = expandRightJoin(join, orConds, relBuilder);
      break;
    case FULL:
      converted = expandFullJoin(join, orConds, relBuilder);
      break;
    default:
      return;
    }
    call.transformTo(converted);
  }

  /**
   * This method will make the following conversions.
   *
   * <p>Project[*]
   *    └── Join[OR(t1.id=t2.id, t1.age=t2.age), left]
   *        ├── TableScan[t1]
   *        └── TableScan[t2]
   *
   * <p>into
   *
   * <p>Project[*]
   *    └── UnionAll
   *        ├── Join[t1.id=t2.id, inner]
   *        │   ├── TableScan[t1]
   *        │   └── TableScan[t2]
   *        ├── Join[t1.age=t2.age AND t1.id≠t2.id, inner]
   *        │   ├── TableScan[t1]
   *        │   └── TableScan[t2]
   *        └── Project[t1-side cols + NULLs]
   *            └── Join[t1.id=t2.id, anti]
   *                ├── Join[t1.age=t2.age, anti]
   *                │   ├── TableScan[t1]
   *                │   └── TableScan[t2]
   *                └── TableScan[t2]
   */
  private RelNode expandLeftJoin(Join join, List<RexNode> orConds,
      RelBuilder relBuilder) {
    List<RelNode> joins = expandLeftJoinToRelNodes(join, orConds, relBuilder);
    return relBuilder.pushAll(joins)
        .union(true, joins.size())
        .build();
  }

  private List<RelNode> expandLeftJoinToRelNodes(Join join, List<RexNode> orConds,
      RelBuilder relBuilder) {
    List<RelNode> joins = new ArrayList<>();
    joins.addAll(expandInnerJoinToRelNodes(join, orConds, relBuilder));
    joins.add(expandSemiOrAntiJoin(join, JoinRelType.ANTI, orConds, relBuilder));
    return joins;
  }

  private RelNode expandRightJoin(Join join, List<RexNode> orConds,
      RelBuilder relBuilder) {
    List<RelNode> joins = expandRightJoinToRelNodes(join, orConds, relBuilder);
    return relBuilder.pushAll(joins)
        .union(true, joins.size())
        .build();
  }

  private List<RelNode> expandRightJoinToRelNodes(Join join, List<RexNode> orConds,
      RelBuilder relBuilder) {
    List<RelNode> joins = new ArrayList<>();
    joins.addAll(expandInnerJoinToRelNodes(join, orConds, relBuilder));
    joins.add(expandSemiOrAntiJoin(join, JoinRelType.SEMI, orConds, relBuilder));
    return joins;
  }

  /**
   * This method will make the following conversions.
   *
   * <p>Project[*]
   *    └── Join[OR(t1.id=t2.id, t1.age=t2.age), full]
   *        ├── TableScan[t1]
   *        └── TableScan[t2]
   *
   * <p>into
   *
   * <p>Project[*]
   *    └── UnionAll
   *        ├── Join[t1.id=t2.id, inner]
   *        │   ├── TableScan[t1]
   *        │   └── TableScan[t2]
   *        ├── Join[t1.age=t2.age AND t1.id≠t2.id, inner]
   *        │   ├── TableScan[t1]
   *        │   └── TableScan[t2]
   *        ├── Project[t1-side cols + NULLs]
   *        │   └── Join[t1.id=t2.id, anti]
   *        │       ├── Join[t1.age=t2.age, anti]
   *        │       │   ├── TableScan[t1]
   *        │       │   └── TableScan[t2]
   *        │       └── TableScan[t2]
   *        └── Project[t1-side cols + NULLs]
   *            └── Join[t1.id=t2.id, semi]
   *                ├── Join[t1.age=t2.age, semi]
   *                │   ├── TableScan[t1]
   *                │   └── TableScan[t2]
   *                └── TableScan[t2]
   */
  private RelNode expandFullJoin(Join join, List<RexNode> orConds,
      RelBuilder relBuilder) {
    List<RelNode> joins = new ArrayList<>();
    joins.addAll(expandLeftJoinToRelNodes(join, orConds, relBuilder));
    joins.add(expandSemiOrAntiJoin(join, JoinRelType.SEMI, orConds, relBuilder));
    relBuilder.pushAll(joins)
        .union(true, joins.size());

    final List<RexNode> projects = join.getRowType().getFieldList().stream()
        .map(field -> {
          RexNode rexNode = relBuilder.field(field.getIndex());
          return field.getType().equals(rexNode.getType())
              ? rexNode
              : relBuilder.getRexBuilder().makeCast(field.getType(), rexNode, true, false);
        }).collect(Collectors.toList());

    return relBuilder.project(projects)
        .build();
  }

  /**
   * This method will make the following conversions.
   *
   * <p>                         Project[*]
   *                                |
   *                 Join[OR(t1.id=t2.id, t1.age=t2.age), inner]
   *                             /     \
   *                  TableScan[t1]   TableScan[t2]
   *
   * <p>into
   *
   * <p>                         Project[*]
   *                                |
   *                             UnionAll
   *                             /     \
   *       Join[t1.id=t2.id, inner]    Join[t1.age=t2.age AND t1.id≠t2.id, inner]
   *           /        \                      /        \
   *  TableScan[t1]    TableScan[t2]    TableScan[t1]    TableScan[t2]
   */
  private RelNode expandInnerJoin(Join join, List<RexNode> orConds,
      RelBuilder relBuilder) {
    List<RelNode> joins = expandInnerJoinToRelNodes(join, orConds, relBuilder);
    return relBuilder.pushAll(joins)
        .union(true, joins.size())
        .build();
  }

  private List<RelNode> expandInnerJoinToRelNodes(Join join, List<RexNode> orConds,
      RelBuilder relBuilder) {
    List<RelNode> joins = new ArrayList<>();
    for (int i = 0; i < orConds.size(); i++) {
      RexNode orCond = orConds.get(i);
      for (int j = 0; j < i; j++) {
        orCond = relBuilder.and(orCond, relBuilder.not(orConds.get(j)));
      }
      joins.add(relBuilder.push(join.getLeft())
          .push(join.getRight())
          .join(JoinRelType.INNER, orCond)
          .build());
    }
    return joins;
  }

  /**
   * This method will make the following conversions.
   *
   * <p>Project[*]
   *    └── Join[OR(id=id0, age=age0), anti]
   *        ├── TableScan[tbl]
   *        └── TableScan[tbl]
   *
   * <p>into
   *
   * <p>HashJoin[id=id0, anti]
   *    ├── HashJoin[age=age0, anti]
   *    │   ├── TableScan[tbl]
   *    │   └── TableScan[tbl]
   *    └── TableScan[tbl]
   */
  private RelNode expandSemiOrAntiJoin(Join join, JoinRelType joinType,
      List<RexNode> orConds, RelBuilder relBuilder) {
    RelNode left = join.getLeft();
    for (int i = 0; i < orConds.size(); i++) {
      RexNode orCond = orConds.get(i);
      relBuilder.push(left)
          .push(join.getRight())
          .join(joinType, orCond);
      left = relBuilder.build();
    }

    relBuilder.push(left);
    List<RexNode> projects = new ArrayList<>(relBuilder.fields());
    int fieldCount = relBuilder.fields().size();
    while (fieldCount < join.getRowType().getFieldCount()) {
      projects.add(
          relBuilder.getRexBuilder().makeNullLiteral(
              join.getRowType().getFieldList().get(fieldCount).getType()));
      fieldCount++;
    }
    return relBuilder.project(projects)
        .build();
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableJoinConditionOrExpansionRule.Config.of()
        .withOperandFor(Join.class);

    @Override default JoinConditionOrExpansionRule toRule() {
      return new JoinConditionOrExpansionRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Join> joinClass) {
      return withOperandSupplier(b -> b.operand(joinClass).anyInputs())
          .as(Config.class);
    }
  }
}
