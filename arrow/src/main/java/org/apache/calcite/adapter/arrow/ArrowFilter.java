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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter}
 * relational expression in Arrow.
 */
public class ArrowFilter extends Filter implements ArrowRel {

  Integer field;
  Object value;
  String operator;

  public ArrowFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition) {
    super(cluster, traitSet, child, condition);

    Translator translator = new Translator(getRowType());
    final SqlKind operator = condition.getKind();

    final RexCall rexCall = (RexCall) condition;
    RexNode left = rexCall.getOperands().get(0);
    RexNode right = rexCall.getOperands().get(1);
    this.field = ((RexInputRef) left).getIndex();
    this.value = ((RexLiteral) right).getValue2();
    this.operator = translator.matchCondition(operator);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  public ArrowFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new ArrowFilter(getCluster(), traitSet, input, condition);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    implementor.add(null, operator, field, value);
  }

  /**
   * Translates SQL condition to string.
   */
  static class Translator {
    private final RelDataType rowType;
    private final List<String> fieldNames;

    Translator(RelDataType rowType) {
      this.rowType = rowType;
      this.fieldNames = ArrowRules.arrowFieldNames(rowType);
    }

    String matchCondition(SqlKind condition) {
      switch (condition) {
      case EQUALS:
        return "equal";
      case LESS_THAN:
        return "less_than";
      case GREATER_THAN:
        return "greater_than";
      case LESS_THAN_OR_EQUAL:
        return "less_than_or_equal_to";
      case GREATER_THAN_OR_EQUAL:
        return "greater_than_or_equal_to";
      default:
        throw new AssertionError("Operator not supported");
      }
    }
  }
}
