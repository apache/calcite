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
package org.apache.calcite.sql2rel;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Converts a RelNode tree such that numeric arithmetic operations
 * use checked arithmetic.  Most SQL dialects should use checked arithmetic,
 * i.e., they should produce a runtime error on overflow.
 */
public class ConvertToChecked extends RelHomogeneousShuttle {
  final ConvertRexToChecked converter;

  public ConvertToChecked(RexBuilder builder) {
    this.converter = new ConvertRexToChecked(builder);
  }

  @Override public RelNode visit(RelNode other) {
    RelNode mNode = super.visitChildren(other);
    return mNode.accept(converter);
  }

  /**
   * Visitor which rewrites an expression tree such that all
   * arithmetic operations that produce numeric values use checked arithmetic.
   */
  class ConvertRexToChecked extends RexShuttle {
    private final RexBuilder builder;

    ConvertRexToChecked(RexBuilder builder) {
      this.builder = builder;
    }

    @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
      RelNode result = subQuery.rel.accept(ConvertToChecked.this);
      if (result != subQuery.rel) {
        return subQuery.clone(result);
      } else {
        return subQuery;
      }
    }

    @Override public RexNode visitCall(final RexCall call) {
      boolean[] update = {false};
      List<RexNode> clonedOperands = visitList(call.operands, update);
      SqlKind kind = call.getKind();
      SqlOperator operator = call.getOperator();
      switch (kind) {
      case PLUS:
        operator = SqlStdOperatorTable.CHECKED_PLUS;
        break;
      case MINUS:
        operator = SqlStdOperatorTable.CHECKED_MINUS;
        break;
      case TIMES:
        operator = SqlStdOperatorTable.CHECKED_MULTIPLY;
        break;
      case MINUS_PREFIX:
        operator = SqlStdOperatorTable.CHECKED_UNARY_MINUS;
        break;
      case DIVIDE:
        operator = SqlStdOperatorTable.CHECKED_DIVIDE;
        break;
      default:
        break;
      }
      SqlTypeName resultType = call.getType().getSqlTypeName();
      if (resultType == SqlTypeName.DECIMAL) {
        // Checked decimal arithmetic is implemented using unchecked
        // arithmetic followed by a CAST, which is always checked
        RexCall result;
        if (operator == call.getOperator()) {
          result = call.clone(call.getType(), clonedOperands);
        } else {
          result = call;
        }
        return builder.makeCast(call.getParserPosition(), call.getType(), result);
      } else if (!SqlTypeName.EXACT_TYPES.contains(resultType)) {
        // Do not rewrite operator if the type is e.g., DOUBLE or DATE
        operator = call.getOperator();
      }
      update[0] = update[0] || operator != call.getOperator();
      if (update[0]) {
        if (operator == call.getOperator()) {
          return call.clone(call.getType(), clonedOperands);
        } else {
          return builder.makeCall(
              call.getParserPosition(), call.getType(), operator, clonedOperands);
        }
      } else {
        return call;
      }
    }
  }
}
