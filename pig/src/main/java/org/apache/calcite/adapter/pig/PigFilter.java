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
package org.apache.calcite.adapter.pig;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.SqlKind.INPUT_REF;
import static org.apache.calcite.sql.SqlKind.LITERAL;

/** Implementation of {@link org.apache.calcite.rel.core.Filter} in
 * {@link PigRel#CONVENTION Pig calling convention}. */
public class PigFilter extends Filter implements PigRel {

  /** Creates a PigFilter. */
  public PigFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode condition) {
    super(cluster, traitSet, input, condition);
    assert getConvention() == PigRel.CONVENTION;
  }

  @Override public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new PigFilter(getCluster(), traitSet, input, condition);
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    implementor.addStatement(getPigFilterStatement(implementor));
  }

  /**
   * Override this method so it looks down the tree to find the table this node
   * is acting on.
   */
  @Override public RelOptTable getTable() {
    return getInput().getTable();
  }

  /**
   * Generates Pig Latin filtering statements, for example
   *
   * <blockquote>
   *   <pre>table = FILTER table BY score &gt; 2.0;</pre>
   * </blockquote>
   */
  private String getPigFilterStatement(Implementor implementor) {
    Preconditions.checkState(containsOnlyConjunctions(condition));
    String relationAlias = implementor.getPigRelationAlias(this);
    List<String> filterConditionsConjunction = new ArrayList<>();
    for (RexNode node : RelOptUtil.conjunctions(condition)) {
      filterConditionsConjunction.add(getSingleFilterCondition(implementor, node));
    }
    String allFilterConditions =
        String.join(" AND ", filterConditionsConjunction);
    return relationAlias + " = FILTER " + relationAlias + " BY " + allFilterConditions + ';';
  }

  private String getSingleFilterCondition(Implementor implementor, RexNode node) {
    switch (node.getKind()) {
    case EQUALS:
      return getSingleFilterCondition(implementor, "==", (RexCall) node);
    case LESS_THAN:
      return getSingleFilterCondition(implementor, "<", (RexCall) node);
    case LESS_THAN_OR_EQUAL:
      return getSingleFilterCondition(implementor, "<=", (RexCall) node);
    case GREATER_THAN:
      return getSingleFilterCondition(implementor, ">", (RexCall) node);
    case GREATER_THAN_OR_EQUAL:
      return getSingleFilterCondition(implementor, ">=", (RexCall) node);
    default:
      throw new IllegalArgumentException("Cannot translate node " + node);
    }
  }

  private String getSingleFilterCondition(Implementor implementor, String op, RexCall call) {
    final String fieldName;
    final String literal;
    final RexNode left = call.operands.get(0);
    final RexNode right = call.operands.get(1);
    if (left.getKind() == LITERAL) {
      if (right.getKind() != INPUT_REF) {
        throw new IllegalArgumentException(
            "Expected a RexCall with a single field and single literal");
      } else {
        fieldName = implementor.getFieldName(this, ((RexInputRef) right).getIndex());
        literal = getLiteralAsString((RexLiteral) left);
      }
    } else if (right.getKind() == LITERAL) {
      if (left.getKind() != INPUT_REF) {
        throw new IllegalArgumentException(
            "Expected a RexCall with a single field and single literal");
      } else {
        fieldName = implementor.getFieldName(this, ((RexInputRef) left).getIndex());
        literal = getLiteralAsString((RexLiteral) right);
      }
    } else {
      throw new IllegalArgumentException(
          "Expected a RexCall with a single field and single literal");
    }

    return '(' + fieldName + ' ' + op + ' ' + literal + ')';
  }

  private boolean containsOnlyConjunctions(RexNode condition) {
    return RelOptUtil.disjunctions(condition).size() == 1;
  }

  /**
   * TODO: do proper literal to string conversion + escaping
   */
  private String getLiteralAsString(RexLiteral literal) {
    return '\'' + RexLiteral.stringValue(literal) + '\'';
  }
}

// End PigFilter.java
