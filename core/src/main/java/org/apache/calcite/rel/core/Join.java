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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexChecker;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Relational expression that combines two relational expressions according to
 * some condition.
 *
 * <p>Each output row has columns from the left and right inputs.
 * The set of output rows is a subset of the cartesian product of the two
 * inputs; precisely which subset depends on the join condition.
 */
public abstract class Join extends BiRel {
  //~ Instance fields --------------------------------------------------------

  protected final RexNode condition;
  protected final ImmutableSet<String> variablesStopped;

  /**
   * Values must be of enumeration {@link JoinRelType}, except that
   * {@link JoinRelType#RIGHT} is disallowed.
   */
  protected JoinRelType joinType;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a Join.
   *
   * @param cluster          Cluster
   * @param traits           Traits
   * @param left             Left input
   * @param right            Right input
   * @param condition        Join condition
   * @param joinType         Join type
   * @param variablesStopped Set of names of variables which are set by the
   *                         LHS and used by the RHS and are not available to
   *                         nodes above this LogicalJoin in the tree
   */
  protected Join(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode left,
      RelNode right,
      RexNode condition,
      JoinRelType joinType,
      Set<String> variablesStopped) {
    super(cluster, traits, left, right);
    this.condition = condition;
    this.variablesStopped = ImmutableSet.copyOf(variablesStopped);
    assert joinType != null;
    assert condition != null;
    this.joinType = joinType;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public List<RexNode> getChildExps() {
    return ImmutableList.of(condition);
  }

  @Override public RelNode accept(RexShuttle shuttle) {
    RexNode condition = shuttle.apply(this.condition);
    if (this.condition == condition) {
      return this;
    }
    return copy(traitSet, condition, left, right, joinType, isSemiJoinDone());
  }

  public RexNode getCondition() {
    return condition;
  }

  public JoinRelType getJoinType() {
    return joinType;
  }

  // TODO: enable
  public boolean isValid_(boolean fail) {
    if (!super.isValid(fail)) {
      return false;
    }
    if (getRowType().getFieldCount()
        != getSystemFieldList().size()
        + left.getRowType().getFieldCount()
        + right.getRowType().getFieldCount()) {
      assert !fail : "field count mismatch";
      return false;
    }
    if (condition != null) {
      if (condition.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
        assert !fail
            : "condition must be boolean: " + condition.getType();
        return false;
      }
      // The input to the condition is a row type consisting of system
      // fields, left fields, and right fields. Very similar to the
      // output row type, except that fields have not yet been made due
      // due to outer joins.
      RexChecker checker =
          new RexChecker(
              getCluster().getTypeFactory().builder()
                  .addAll(getSystemFieldList())
                  .addAll(getLeft().getRowType().getFieldList())
                  .addAll(getRight().getRowType().getFieldList())
                  .build(),
              fail);
      condition.accept(checker);
      if (checker.getFailureCount() > 0) {
        assert !fail
            : checker.getFailureCount() + " failures in condition "
            + condition;
        return false;
      }
    }
    return true;
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner) {
    // REVIEW jvs 9-Apr-2006:  Just for now...
    double rowCount = RelMetadataQuery.getRowCount(this);
    return planner.getCostFactory().makeCost(rowCount, 0, 0);
  }

  /** @deprecated Use {@link RelMdUtil#getJoinRowCount(Join, RexNode)}. */
  @Deprecated // to be removed before 2.0
  public static double estimateJoinedRows(
      Join joinRel,
      RexNode condition) {
    return Util.first(RelMdUtil.getJoinRowCount(joinRel, condition), 1D);
  }

  @Override public double getRows() {
    return Util.first(RelMdUtil.getJoinRowCount(this, condition), 1D);
  }

  @Override public Set<String> getVariablesStopped() {
    return variablesStopped;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("condition", condition)
        .item("joinType", joinType.name().toLowerCase())
        .itemIf(
            "systemFields",
            getSystemFieldList(),
            !getSystemFieldList().isEmpty());
  }

  @Override protected RelDataType deriveRowType() {
    return deriveJoinRowType(
        left.getRowType(),
        right.getRowType(),
        joinType,
        getCluster().getTypeFactory(),
        null,
        getSystemFieldList());
  }

  /**
   * Returns whether this LogicalJoin has already spawned a
   * {@link SemiJoin} via
   * {@link org.apache.calcite.rel.rules.JoinAddRedundantSemiJoinRule}.
   *
   * <p>The base implementation returns false.</p>
   *
   * @return whether this join has already spawned a semi join
   */
  public boolean isSemiJoinDone() {
    return false;
  }

  /**
   * Returns a list of system fields that will be prefixed to
   * output row type.
   *
   * @return list of system fields
   */
  public List<RelDataTypeField> getSystemFieldList() {
    return Collections.emptyList();
  }

  /**
   * Derives the type of a join relational expression.
   *
   * @param leftType        Row type of left input to join
   * @param rightType       Row type of right input to join
   * @param joinType        Type of join
   * @param typeFactory     Type factory
   * @param fieldNameList   List of names of fields; if null, field names are
   *                        inherited and made unique
   * @param systemFieldList List of system fields that will be prefixed to
   *                        output row type; typically empty but must not be
   *                        null
   * @return join type
   */
  public static RelDataType deriveJoinRowType(
      RelDataType leftType,
      RelDataType rightType,
      JoinRelType joinType,
      RelDataTypeFactory typeFactory,
      List<String> fieldNameList,
      List<RelDataTypeField> systemFieldList) {
    assert systemFieldList != null;
    switch (joinType) {
    case LEFT:
      rightType = typeFactory.createTypeWithNullability(rightType, true);
      break;
    case RIGHT:
      leftType = typeFactory.createTypeWithNullability(leftType, true);
      break;
    case FULL:
      leftType = typeFactory.createTypeWithNullability(leftType, true);
      rightType = typeFactory.createTypeWithNullability(rightType, true);
      break;
    default:
      break;
    }
    return createJoinType(
        typeFactory, leftType, rightType, fieldNameList, systemFieldList);
  }

  /**
   * Returns the type the row which results when two relations are joined.
   *
   * <p>The resulting row type consists of
   * the system fields (if any), followed by
   * the fields of the left type, followed by
   * the fields of the right type. The field name list, if present, overrides
   * the original names of the fields.
   *
   * @param typeFactory     Type factory
   * @param leftType        Type of left input to join
   * @param rightType       Type of right input to join
   * @param fieldNameList   If not null, overrides the original names of the
   *                        fields
   * @param systemFieldList List of system fields that will be prefixed to
   *                        output row type; typically empty but must not be
   *                        null
   * @return type of row which results when two relations are joined
   */
  public static RelDataType createJoinType(
      RelDataTypeFactory typeFactory,
      RelDataType leftType,
      RelDataType rightType,
      List<String> fieldNameList,
      List<RelDataTypeField> systemFieldList) {
    assert (fieldNameList == null)
        || (fieldNameList.size()
        == (systemFieldList.size()
        + leftType.getFieldCount()
        + rightType.getFieldCount()));
    List<String> nameList = new ArrayList<>();
    final List<RelDataType> typeList = new ArrayList<>();

    // use a hashset to keep track of the field names; this is needed
    // to ensure that the contains() call to check for name uniqueness
    // runs in constant time; otherwise, if the number of fields is large,
    // doing a contains() on a list can be expensive
    final HashSet<String> uniqueNameList = new HashSet<>();
    addFields(systemFieldList, typeList, nameList, uniqueNameList);
    addFields(leftType.getFieldList(), typeList, nameList, uniqueNameList);
    if (rightType != null) {
      addFields(
          rightType.getFieldList(), typeList, nameList, uniqueNameList);
    }
    if (fieldNameList != null) {
      assert fieldNameList.size() == nameList.size();
      nameList = fieldNameList;
    }
    return typeFactory.createStructType(typeList, nameList);
  }

  private static void addFields(
      List<RelDataTypeField> fieldList,
      List<RelDataType> typeList,
      List<String> nameList,
      HashSet<String> uniqueNameList) {
    for (RelDataTypeField field : fieldList) {
      String name = field.getName();

      // Ensure that name is unique from all previous field names
      if (uniqueNameList.contains(name)) {
        String nameBase = name;
        for (int j = 0;; j++) {
          name = nameBase + j;
          if (!uniqueNameList.contains(name)) {
            break;
          }
        }
      }
      nameList.add(name);
      uniqueNameList.add(name);
      typeList.add(field.getType());
    }
  }

  @Override public final Join copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 2;
    return copy(traitSet, getCondition(), inputs.get(0), inputs.get(1),
        joinType, isSemiJoinDone());
  }

  /**
   * Creates a copy of this join, overriding condition, system fields and
   * inputs.
   *
   * <p>General contract as {@link RelNode#copy}.
   *
   * @param traitSet      Traits
   * @param conditionExpr Condition
   * @param left          Left input
   * @param right         Right input
   * @param joinType      Join type
   * @param semiJoinDone  Whether this join has been translated to a
   *                      semi-join
   * @return Copy of this join
   */
  public abstract Join copy(RelTraitSet traitSet, RexNode conditionExpr,
      RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone);

  /**
   * Analyzes the join condition.
   *
   * @return Analyzed join condition
   */
  public JoinInfo analyzeCondition() {
    return JoinInfo.of(left, right, condition);
  }
}

// End Join.java
