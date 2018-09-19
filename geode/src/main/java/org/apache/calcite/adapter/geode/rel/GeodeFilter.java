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
package org.apache.calcite.adapter.geode.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR;
import static org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES;

/**
 * Implementation of
 * {@link Filter} relational expression in Geode.
 */
public class GeodeFilter extends Filter implements GeodeRel {

  private final String match;

  public GeodeFilter(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RexNode condition) {

    super(cluster, traitSet, input, condition);

    Translator translator = new Translator(getRowType());
    this.match = translator.translateMatch(condition);

    assert getConvention() == GeodeRel.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override public GeodeFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new GeodeFilter(getCluster(), traitSet, input, condition);
  }

  @Override public void implement(GeodeImplementContext geodeImplementContext) {
    // first call the input down the tree.
    ((GeodeRel) getInput()).implement(geodeImplementContext);
    geodeImplementContext.addPredicates(Collections.singletonList(match));
  }

  /**
   * Translates {@link RexNode} expressions into Geode expression strings.
   */
  static class Translator {
    private final RelDataType rowType;

    private final List<String> fieldNames;

    Translator(RelDataType rowType) {
      this.rowType = rowType;
      this.fieldNames = GeodeRules.geodeFieldNames(rowType);
    }

    /**
     * Converts the value of a literal to a string.
     *
     * @param literal Literal to translate
     * @return String representation of the literal
     */
    private static String literalValue(RexLiteral literal) {
      Object value = literal.getValue2();
      StringBuilder buf = new StringBuilder();
      buf.append(value);
      return buf.toString();
    }

    /**
     * Produce the OQL predicate string for the given condition.
     *
     * @param condition Condition to translate
     * @return OQL predicate string
     */
    private String translateMatch(RexNode condition) {
      // Returns condition decomposed by OR
      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
      if (disjunctions.size() == 1) {
        return translateAnd(disjunctions.get(0));
      } else {
        return translateOr(disjunctions);
      }
    }

    /**
     * Translate a conjunctive predicate to a OQL string.
     *
     * @param condition A conjunctive predicate
     * @return OQL string for the predicate
     */
    private String translateAnd(RexNode condition) {
      List<String> predicates = new ArrayList<>();
      for (RexNode node : RelOptUtil.conjunctions(condition)) {
        predicates.add(translateMatch2(node));
      }

      return Util.toString(predicates, "", " AND ", "");
    }

    private String translateOr(List<RexNode> disjunctions) {
      List<String> predicates = new ArrayList<>();
      for (RexNode node : disjunctions) {
        if (RelOptUtil.conjunctions(node).size() > 1) {
          predicates.add("(" + translateMatch(node) + ")");
        } else {
          predicates.add(translateMatch2(node));
        }
      }

      return Util.toString(predicates, "", " OR ", "");
    }

    /**
     * Translate a binary relation.
     */
    private String translateMatch2(RexNode node) {
      // We currently only use equality, but inequalities on clustering keys
      // should be possible in the future
      switch (node.getKind()) {
      case EQUALS:
        return translateBinary("=", "=", (RexCall) node);
      case LESS_THAN:
        return translateBinary("<", ">", (RexCall) node);
      case LESS_THAN_OR_EQUAL:
        return translateBinary("<=", ">=", (RexCall) node);
      case GREATER_THAN:
        return translateBinary(">", "<", (RexCall) node);
      case GREATER_THAN_OR_EQUAL:
        return translateBinary(">=", "<=", (RexCall) node);
      default:
        throw new AssertionError("cannot translate " + node);
      }
    }

    /**
     * Translates a call to a binary operator, reversing arguments if
     * necessary.
     */
    private String translateBinary(String op, String rop, RexCall call) {
      final RexNode left = call.operands.get(0);
      final RexNode right = call.operands.get(1);
      String expression = translateBinary2(op, left, right);
      if (expression != null) {
        return expression;
      }
      expression = translateBinary2(rop, right, left);
      if (expression != null) {
        return expression;
      }
      throw new AssertionError("cannot translate op " + op + " call " + call);
    }

    /**
     * Translates a call to a binary operator. Returns null on failure.
     */
    private String translateBinary2(String op, RexNode left, RexNode right) {
      switch (right.getKind()) {
      case LITERAL:
        break;
      default:
        return null;
      }

      final RexLiteral rightLiteral = (RexLiteral) right;
      switch (left.getKind()) {
      case INPUT_REF:
        final RexInputRef left1 = (RexInputRef) left;
        String name = fieldNames.get(left1.getIndex());
        return translateOp2(op, name, rightLiteral);
      case CAST:
        // FIXME This will not work in all cases (for example, we ignore string encoding)
        return translateBinary2(op, ((RexCall) left).operands.get(0), right);
      case OTHER_FUNCTION:
        String item = left.accept(new GeodeRules.RexToGeodeTranslator(this.fieldNames));
        return (item == null) ? null : item + " " + op + " " + quoteCharLiteral(rightLiteral);
      default:
        return null;
      }
    }

    private String quoteCharLiteral(RexLiteral literal) {
      String value = literalValue(literal);
      if (literal.getTypeName() == CHAR) {
        value = "'" + value + "'";
      }
      return value;
    }

    /**
     * Combines a field name, operator, and literal to produce a predicate string.
     */
    private String translateOp2(String op, String name, RexLiteral right) {
      String valueString = literalValue(right);
      SqlTypeName typeName = rowType.getField(name, true, false).getType().getSqlTypeName();
      if (NUMERIC_TYPES.contains(typeName) || BOOLEAN_TYPES.contains(typeName)) {
        // leave the value as it is
      } else if (typeName != SqlTypeName.CHAR) {
        valueString = "'" + valueString + "'";
      }
      return name + " " + op + " " + valueString;
    }
  }
}

// End GeodeFilter.java
