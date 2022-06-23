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

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.math.BigDecimal;

import static org.apache.calcite.util.DateTimeStringUtils.ISO_DATETIME_FRACTIONAL_SECOND_FORMAT;
import static org.apache.calcite.util.DateTimeStringUtils.getDateFormatter;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter}
 * relational expression in Arrow.
 */
public class ArrowFilter extends Filter implements ArrowRel {
  private List<String> match;

  public ArrowFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition) {
    super(cluster, traitSet, child, condition);

    Translator translator = new Translator(getRowType());
    this.match = translator.translateMatch(condition);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  public ArrowFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new ArrowFilter(getCluster(), traitSet, input, condition);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    implementor.add(null, match);
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

    private List<String> translateMatch(RexNode condition) {
      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
      if (disjunctions.size() == 1) {
        return translateAnd(disjunctions.get(0));
      } else {
        throw new AssertionError("cannot translate " + condition);
      }
    }

    /** Returns the value of the literal.
     *
     * @param literal Literal to translate
     * @return The value of the literal in the form of the actual type.
     */
    private static Object literalValue(RexLiteral literal) {
      Comparable value = RexLiteral.value(literal);
      switch (literal.getTypeName()) {
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        assert value instanceof TimestampString;
        final SimpleDateFormat dateFormatter =
            getDateFormatter(ISO_DATETIME_FRACTIONAL_SECOND_FORMAT);
        return dateFormatter.format(literal.getValue2());
      case DATE:
        assert value instanceof DateString;
        return value.toString();
      default:
        return literal.getValue3();
      }
    }

    /** Translate a conjunctive predicate to a SQL string.
     *
     * @param condition A conjunctive predicate
     * @return SQL string for the predicate
     */
    private List<String> translateAnd(RexNode condition) {
      List<String> predicates = new ArrayList<>();
      for (RexNode node : RelOptUtil.conjunctions(condition)) {
        predicates.add(translateMatch2(node));
      }
      return predicates;
    }

    /** Translate a binary relation. */
    private String translateMatch2(RexNode node) {
      switch (node.getKind()) {
      case EQUALS:
        return translateBinary("equal", "=", (RexCall) node);
      case LESS_THAN:
        return translateBinary("less_than", ">", (RexCall) node);
      case LESS_THAN_OR_EQUAL:
        return translateBinary("less_than_or_equal_to", ">=", (RexCall) node);
      case GREATER_THAN:
        return translateBinary("greater_than", "<", (RexCall) node);
      case GREATER_THAN_OR_EQUAL:
        return translateBinary("greater_than_or_equal_to", "<=", (RexCall) node);
      default:
        throw new AssertionError("cannot translate " + node);
      }
    }

    /** Translates a call to a binary operator, reversing arguments if
     * necessary. */
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

    /** Translates a call to a binary operator. Returns null on failure. */
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
      default:
        return null;
      }
    }

    /** Combines a field name, operator, and literal to produce a predicate string. */
    private String translateOp2(String op, String name, RexLiteral right) {
      // In case this is a key, record that it is now restricted

      Object value = literalValue(right);
      String valueString = value.toString();
      String valueType = getLiteralType(value);

      if (value instanceof String) {
        SqlTypeName typeName = rowType.getField(name, true, false).getType().getSqlTypeName();
        if (typeName != SqlTypeName.CHAR) {
          valueString = "'" + valueString + "'";
        }
      }
      return name + " " + op + " " + valueString + " " + valueType;
    }

    private String getLiteralType(Object literal) {
      if (literal instanceof BigDecimal) {
        BigDecimal bigDecimalLiteral = (BigDecimal) literal;
        int scale = bigDecimalLiteral.scale();
        if (scale == 0) return "integer";
        else if (scale > 0) return "float";
      }
      else if (String.class.equals(literal.getClass())) return "string";
      throw new AssertionError("Invalid literal");
    }
  }
}
