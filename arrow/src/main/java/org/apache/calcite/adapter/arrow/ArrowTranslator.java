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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.util.DateTimeStringUtils.ISO_DATETIME_FRACTIONAL_SECOND_FORMAT;
import static org.apache.calcite.util.DateTimeStringUtils.getDateFormatter;

import static java.util.Objects.requireNonNull;

/**
 * Translates a {@link RexNode} expression to a Gandiva string.
 */
class ArrowTranslator {
  final RexBuilder rexBuilder;
  final RelDataType rowType;
  final List<String> fieldNames;

  /** Private constructor. */
  ArrowTranslator(RexBuilder rexBuilder, RelDataType rowType) {
    this.rexBuilder = rexBuilder;
    this.rowType = rowType;
    this.fieldNames = ArrowRules.arrowFieldNames(rowType);
  }

  /** Creates an ArrowTranslator. */
  public static ArrowTranslator create(RexBuilder rexBuilder,
      RelDataType rowType) {
    return new ArrowTranslator(rexBuilder, rowType);
  }

  List<String> translateMatch(RexNode condition) {
    List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
    if (disjunctions.size() == 1) {
      return translateAnd(disjunctions.get(0));
    } else {
      throw new AssertionError("cannot translate " + condition);
    }
  }

  /**
   * Returns the value of the literal.
   *
   * @param literal Literal to translate
   *
   * @return The value of the literal in the form of the actual type
   */
  private static Object literalValue(RexLiteral literal) {
    switch (literal.getTypeName()) {
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      final SimpleDateFormat dateFormatter =
          getDateFormatter(ISO_DATETIME_FRACTIONAL_SECOND_FORMAT);
      Long millis = literal.getValueAs(Long.class);
      return dateFormatter.format(requireNonNull(millis, "millis"));
    case DATE:
      final DateString dateString = literal.getValueAs(DateString.class);
      return requireNonNull(dateString, "dateString").toString();
    default:
      return requireNonNull(literal.getValue3());
    }
  }

  /**
   * Translate a conjunctive predicate to a SQL string.
   *
   * @param condition A conjunctive predicate
   *
   * @return SQL string for the predicate
   */
  private List<String> translateAnd(RexNode condition) {
    List<String> predicates = new ArrayList<>();
    for (RexNode node : RelOptUtil.conjunctions(condition)) {
      switch (node.getKind()) {
      case SEARCH:
        final RexNode node2 = RexUtil.expandSearch(rexBuilder, null, node);
        predicates.addAll(translateMatch(node2));
        break;
      default:
        predicates.add(translateMatch2(node));
      }
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

  /**
   * Translates a call to a binary operator, reversing arguments if
   * necessary.
   */
  private String translateBinary(String op, String rop, RexCall call) {
    final RexNode left = call.operands.get(0);
    final RexNode right = call.operands.get(1);
    @Nullable String expression = translateBinary2(op, left, right);
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
  private @Nullable String translateBinary2(String op, RexNode left, RexNode right) {
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
      final RelDataTypeField field = requireNonNull(rowType.getField(name, true, false), "field");
      SqlTypeName typeName = field.getType().getSqlTypeName();
      if (typeName != SqlTypeName.CHAR) {
        valueString = "'" + valueString + "'";
      }
    }
    return name + " " + op + " " + valueString + " " + valueType;
  }

  private static String getLiteralType(Object literal) {
    if (literal instanceof BigDecimal) {
      BigDecimal bigDecimalLiteral = (BigDecimal) literal;
      int scale = bigDecimalLiteral.scale();
      if (scale == 0) {
        return "integer";
      } else if (scale > 0) {
        return "float";
      }
    } else if (String.class.equals(literal.getClass())) {
      return "string";
    }
    throw new AssertionError("Invalid literal");
  }
}
