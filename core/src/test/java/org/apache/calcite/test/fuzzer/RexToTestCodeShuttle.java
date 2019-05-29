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
package org.apache.calcite.test.fuzzer;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * Converts {@link RexNode} into a string form usable for inclusion into
 * {@link RexProgramFuzzyTest}.
 * For instance, it converts {@code AND(=(?0.bool0, true), =(?0.bool1, true))} to
 * {@code isTrue(and(eq(vBool(0), trueLiteral), eq(vBool(1), trueLiteral)))}.
 */
public class RexToTestCodeShuttle extends RexVisitorImpl<String> {
  private static final Map<SqlOperator, String> OP_METHODS =
      ImmutableMap.<SqlOperator, String>builder()
          .put(SqlStdOperatorTable.AND, "and")
          .put(SqlStdOperatorTable.OR, "or")
          .put(SqlStdOperatorTable.CASE, "case_")
          .put(SqlStdOperatorTable.COALESCE, "coalesce")
          .put(SqlStdOperatorTable.IS_NULL, "isNull")
          .put(SqlStdOperatorTable.IS_NOT_NULL, "isNotNull")
          .put(SqlStdOperatorTable.IS_UNKNOWN, "isUnknown")
          .put(SqlStdOperatorTable.IS_TRUE, "isTrue")
          .put(SqlStdOperatorTable.IS_NOT_TRUE, "isNotTrue")
          .put(SqlStdOperatorTable.IS_FALSE, "isFalse")
          .put(SqlStdOperatorTable.IS_NOT_FALSE, "isNotFalse")
          .put(SqlStdOperatorTable.IS_DISTINCT_FROM, "isDistinctFrom")
          .put(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, "isNotDistinctFrom")
          .put(SqlStdOperatorTable.NULLIF, "nullIf")
          .put(SqlStdOperatorTable.NOT, "not")
          .put(SqlStdOperatorTable.GREATER_THAN, "gt")
          .put(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, "ge")
          .put(SqlStdOperatorTable.LESS_THAN, "lt")
          .put(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "le")
          .put(SqlStdOperatorTable.EQUALS, "eq")
          .put(SqlStdOperatorTable.NOT_EQUALS, "ne")
          .put(SqlStdOperatorTable.PLUS, "plus")
          .put(SqlStdOperatorTable.UNARY_PLUS, "unaryPlus")
          .put(SqlStdOperatorTable.MINUS, "sub")
          .put(SqlStdOperatorTable.UNARY_MINUS, "unaryMinus")
          .put(SqlStdOperatorTable.MULTIPLY, "mul")
          .build();


  protected RexToTestCodeShuttle() {
    super(true);
  }

  @Override public String visitCall(RexCall call) {
    SqlOperator operator = call.getOperator();
    String method = OP_METHODS.get(operator);

    StringBuilder sb = new StringBuilder();
    if (method != null) {
      sb.append(method);
      sb.append('(');
    } else {
      sb.append("rexBuilder.makeCall(");
      sb.append("SqlStdOperatorTable.");
      sb.append(operator.getName().replace(' ', '_'));
      sb.append(", ");
    }
    List<RexNode> operands = call.getOperands();
    for (int i = 0; i < operands.size(); i++) {
      RexNode operand = operands.get(i);
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(operand.accept(this));
    }
    sb.append(')');
    return sb.toString();
  }

  @Override public String visitLiteral(RexLiteral literal) {
    RelDataType type = literal.getType();

    if (type.getSqlTypeName() == SqlTypeName.BOOLEAN) {
      if (literal.isNull()) {
        return "nullBool";
      }
      return literal.toString() + "Literal";
    }
    if (type.getSqlTypeName() == SqlTypeName.INTEGER) {
      if (literal.isNull()) {
        return "nullInt";
      }
      return "literal(" + literal.getValue() + ")";
    }
    if (type.getSqlTypeName() == SqlTypeName.VARCHAR) {
      if (literal.isNull()) {
        return "nullVarchar";
      }
    }
    return "/*" + literal.getTypeName().getName() + "*/" + literal.toString();
  }

  @Override public String visitFieldAccess(RexFieldAccess fieldAccess) {
    StringBuilder sb = new StringBuilder();
    sb.append("v");
    RelDataType type = fieldAccess.getType();
    switch (type.getSqlTypeName()) {
    case BOOLEAN:
      sb.append("Bool");
      break;
    case INTEGER:
      sb.append("Int");
      break;
    case VARCHAR:
      sb.append("Varchar");
      break;
    }
    if (!type.isNullable()) {
      sb.append("NotNull");
    }
    sb.append("(");
    sb.append(fieldAccess.getField().getIndex() % 10);
    sb.append(")");
    return sb.toString();
  }
}

// End RexToTestCodeShuttle.java
