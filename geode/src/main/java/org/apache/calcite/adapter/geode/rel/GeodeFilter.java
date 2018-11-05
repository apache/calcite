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

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.util.*;

import static org.apache.calcite.sql.type.SqlTypeName.*;

/**
 * Implementation of
 * {@link Filter} relational expression in Geode.
 */
public class GeodeFilter extends Filter implements GeodeRel {

  private final String match;

  GeodeFilter(RelOptCluster cluster, RelTraitSet traitSet,
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
    geodeImplementContext.visitChild(getInput());
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
      } else if (useInSetQueryClause(disjunctions)) {
        return translateInSet(disjunctions);
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

    /**
     * return the geode fieldName of the left RexNode for in set query
     *
     * @param left
     * @return left RexNode fieldName
     */
    private String getLeftNodeFieldNameForInSetPredicate(RexNode left) {
      switch (left.getKind()) {
        case INPUT_REF:
          final RexInputRef left1 = (RexInputRef) left;
          return fieldNames.get(left1.getIndex());
        case CAST:
          // FIXME This will not work in all cases (for example, we ignore string encoding)
          return getLeftNodeFieldNameForInSetPredicate(((RexCall) left).operands.get(0));
        default:
          return null;
      }
    }

    /**
     * Checks if we should use IN SET query clause instead of Multiple OR clauses, to improve
     * performance
     *
     * @param disjunctions
     * @return boolean
     */
    private boolean useInSetQueryClause(List<RexNode> disjunctions) {
      Set<String> leftFieldNameSet = new HashSet<>();

      return disjunctions.stream().allMatch(node -> {
        // In case if SqlKind is not same as SqlKind.EQUALS we should not use IN SET query
        SqlKind sqlKind = node.getKind();
        if (!sqlKind.equals(SqlKind.EQUALS))
          return false;

        RexCall call = (RexCall) node;

        final RexNode left = call.operands.get(0);
        final RexNode right = call.operands.get(1);

        // The right node should always be literal
        if (!right.getKind().equals(SqlKind.LITERAL))
          return false;

        String name = getLeftNodeFieldNameForInSetPredicate(left);
        if (name == null)
          return false;

        leftFieldNameSet.add(name);

        // Ensuring that left node field name is same for all RexNode in disjunctions
        if (leftFieldNameSet.size() != 1)
          return false;

        return true;
      });
    }

    /**
     * return in set oql oql string predicate
     *
     * @param disjunctions
     * @return in set oql string predicate
     */
    private String translateInSet(List<RexNode> disjunctions) {
      RexNode firstNode = disjunctions.get(0);
      RexCall firstCall = (RexCall) firstNode;

      final RexNode left = firstCall.operands.get(0);
      String name = getLeftNodeFieldNameForInSetPredicate(left);

      if (name == null)
        return null;

      RelDataTypeField relDataTypeField = rowType.getField(name, true, false);
      if (relDataTypeField == null)
        return null;

      SqlTypeName typeName = relDataTypeField.getType().getSqlTypeName();

      Set<String> rightLiteralValueList = new HashSet<>();

      disjunctions.forEach(node -> {
        RexCall call = (RexCall) node;
        RexLiteral rightLiteral = (RexLiteral) call.operands.get(1);

        rightLiteralValueList.add(literalValue(rightLiteral));
      });

      if (NUMERIC_TYPES.contains(typeName) || BOOLEAN_TYPES.contains(typeName)) {
        return String.format("%s IN SET (%s)", name, String.join(", ", rightLiteralValueList));
      } else {
        return String.format("%s IN SET ('%s')", name, String.join("', '", rightLiteralValueList));
      }
    }
  }
}

// End GeodeFilter.java
