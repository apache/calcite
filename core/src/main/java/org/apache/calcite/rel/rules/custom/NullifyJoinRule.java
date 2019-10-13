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
package org.apache.calcite.rel.rules.custom;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * NullifyJoinRule nullifies a 1-sided outer join or inner join operator in the
 * following way:
 * 1) 1-sided outer join: nullify the null-producing side;
 * 2) inner join: nullify both sides;
 */
public class NullifyJoinRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the rule that nullifies inner, left outer or right outer join. */
  public static final NullifyJoinRule INSTANCE =
      new NullifyJoinRule(operand(Join.class, any()), null);

  /**
   * A map from table name to a set of predicates, which becomes the relation's
   * nullification set. This map shall only be filled once.
   */
  private static final Map<String, Set<RexNode>> NULLIFICATION_SET_MAP = new HashMap<>();

  /**
   * A boolean flag to indicate whether we have filled in the above map.
   */
  private static boolean hasFilledMap = false;

  //~ Constructors -----------------------------------------------------------

  public NullifyJoinRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public NullifyJoinRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    if (!hasFilledMap) {
      fillNullificationSetMap(call.getPlanner().getRoot());
      hasFilledMap = true;
    }

    RelBuilder builder = call.builder();

    // The join operator at the current node.
    final Join join = call.rel(0);
    final JoinRelType joinType = join.getJoinType();
    if (!joinType.canApplyNullify()) {
      LOGGER.debug("Invalid join relation type for nullify: " + joinType.toString());
      return;
    }

    // The new join operator (as outer-cartesian product).
    final RelNode outerCartesianJoin = join.copy(
        join.getTraitSet(),
        builder.literal(true),  // Uses a literal condition which is always true.
        join.getLeft(),
        join.getRight(),
        JoinRelType.OUTER_CARTESIAN,
        join.isSemiJoinDone());

    // Determines the nullification attribute list based on the join type.
    List<RelDataTypeField> joinFieldList = outerCartesianJoin.getRowType().getFieldList();
    List<RelDataTypeField> nullifyFieldList;
    int leftFieldCount = join.getLeft().getRowType().getFieldCount();
    switch (joinType) {
    case LEFT:
      nullifyFieldList = joinFieldList.subList(leftFieldCount, joinFieldList.size());
      break;
    case RIGHT:
      nullifyFieldList = joinFieldList.subList(0, leftFieldCount);
      break;
    case INNER:
      nullifyFieldList = joinFieldList;
      break;
    default:
      throw new AssertionError(joinType);
    }
    List<RexNode> nullificationList = nullifyFieldList.stream()
        .map(field -> new RexInputRef(field.getIndex(), field.getType()))
        .collect(Collectors.toList());

    // Determines the nullification condition.
    RexNode nullificationCondition = join.getCondition();

    // Builds the transformed relational tree.
    final RelNode transformedNode = builder.push(outerCartesianJoin)
        .nullify(nullificationCondition, nullificationList)
        .bestMatch()
        .build();
    call.transformTo(transformedNode);
  }

  /**
   * Fills the nullification set map of this query.
   *
   * @param root is the root node of the query.
   * @return a list consisting of the full names of all tables under this node.
   */
  private static List<String> fillNullificationSetMap(RelNode root) {
    if (root instanceof TableScan) {
      TableScan tableScan = (TableScan) root;
      String tableName = getTableFullName(tableScan);

      // Initializes the nullification set of the base relation to be an empty set.
      NULLIFICATION_SET_MAP.put(tableName, new HashSet<>());

      // Returns this base relation's full name.
      return ImmutableList.of(tableName);
    } else if (root instanceof RelSubset) {
      RelSubset relSubset = (RelSubset) root;
      return fillNullificationSetMap(relSubset.getOriginal());
    } else if (root instanceof Join) {
      Join join = (Join) root;

      // Traverses its left and right children first (the algorithm requires postfix order).
      List<String> leftNames = fillNullificationSetMap(join.getLeft());
      List<String> rightNames = fillNullificationSetMap(join.getRight());

      // The current join predicate will always be added.
      Set<RexNode> toAdd = new HashSet<>();
      toAdd.add(join.getCondition());

      // Populates the nullification set.
      switch (join.getJoinType()) {
      case LEFT:
        // Prepares all the predicates that need to be added.
        for (String name: leftNames) {
          toAdd.addAll(NULLIFICATION_SET_MAP.get(name));
        }

        // Updates the nullification set.
        for (String name: rightNames) {
          Set<RexNode> current = NULLIFICATION_SET_MAP.get(name);
          current.addAll(toAdd);
        }
        break;
      case RIGHT:
        // Prepares all the predicates that need to be added.
        for (String name: rightNames) {
          toAdd.addAll(NULLIFICATION_SET_MAP.get(name));
        }

        // Updates the nullification set.
        for (String name: leftNames) {
          Set<RexNode> current = NULLIFICATION_SET_MAP.get(name);
          current.addAll(toAdd);
        }
        break;
      case INNER:
        // Prepares all the predicates that need to be added.
        for (String name: leftNames) {
          toAdd.addAll(NULLIFICATION_SET_MAP.get(name));
        }
        Set<RexNode> toAdd2 = new HashSet<>();
        toAdd2.add(join.getCondition());
        for (String name: rightNames) {
          toAdd2.addAll(NULLIFICATION_SET_MAP.get(name));
        }

        // Updates the nullification set for both sides.
        for (String name: rightNames) {
          Set<RexNode> current = NULLIFICATION_SET_MAP.get(name);
          current.addAll(toAdd);
        }
        for (String name: leftNames) {
          Set<RexNode> current = NULLIFICATION_SET_MAP.get(name);
          current.addAll(toAdd2);
        }
        break;
      default:
        throw new AssertionError("Unsupported join type: " + join.getJoinType());
      }

      // Merges the table names from both sides together.
      return ImmutableList.copyOf(Iterables.concat(leftNames, rightNames));
    } else {
      return fillNullificationSetMap(root.getInput(0));
    }
  }

  private static String getTableFullName(TableScan tableScan) {
    StringJoiner joiner = new StringJoiner(".");
    for (String name: tableScan.getTable().getQualifiedName()) {
      joiner.add(name);
    }
    return joiner.toString();
  }
}

// End NullifyJoinRule.java
