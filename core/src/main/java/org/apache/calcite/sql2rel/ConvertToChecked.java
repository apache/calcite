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

  public ConvertToChecked(RexBuilder builder, boolean allArithmetic) {
    this.converter = new ConvertRexToChecked(builder, allArithmetic);
  }

  @Override public RelNode visit(RelNode other) {
    RelNode mNode = super.visitChildren(other);
    return mNode.accept(converter);
  }

  /**
   * Visitor which rewrites an expression tree such that arithmetic operations
   * use checked arithmetic.
   */
  class ConvertRexToChecked extends RexShuttle {
    private final RexBuilder builder;
    // If true all arithmetic operations are converted.
    // Otherwise, only arithmetic operations on INTERVAL values is checked.
    private final boolean allArithmetic;

    /**
     * Create a visitor which converts all arithmetic operations to checked.
     *
     * @deprecated Use #ConvertRexToChecked(RexBuilder, boolean).
     */
    @Deprecated
    ConvertRexToChecked(RexBuilder builder) {
      this(builder, true);
    }

    /**
     * Create a converter that replaces arithmetic with checked arithmetic.
     *
     * @param builder         RexBuilder to use.
     * @param allArithmetic   If true all exact arithmetic operations are converted to checked.
     *                        If false, only operations that produce INTERVAL-typed results
     *                        are converted to checked.
     */
    ConvertRexToChecked(RexBuilder builder, boolean allArithmetic) {
      this.builder = builder;
      this.allArithmetic = allArithmetic;
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
      SqlTypeName resultType = call.getType().getSqlTypeName();
      boolean anyOperandIsInterval = false;
      for (RexNode op : call.getOperands()) {
        if (SqlTypeName.INTERVAL_TYPES.contains(op.getType().getSqlTypeName())) {
          anyOperandIsInterval = true;
          break;
        }
      }
      boolean resultIsInterval = SqlTypeName.INTERVAL_TYPES.contains(resultType);
      boolean rewrite =
          // Do not rewrite operator if the type is e.g., DOUBLE or DATE
          (this.allArithmetic && SqlTypeName.EXACT_TYPES.contains(resultType))
          // But always rewrite if the type is an INTERVAL and any operand is INTERVAL
          // This will not rewrite date subtraction, for example
          || (resultIsInterval && anyOperandIsInterval);

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
      if (resultType == SqlTypeName.DECIMAL && this.allArithmetic) {
        // Checked decimal arithmetic is implemented using unchecked
        // arithmetic followed by a CAST, which is always checked
        RexCall result;
        if (operator == call.getOperator()) {
          result = call.clone(call.getType(), clonedOperands);
        } else {
          result = call;
        }
        return builder.makeCast(call.getParserPosition(), call.getType(), result);
      }

      if (!rewrite) {
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
