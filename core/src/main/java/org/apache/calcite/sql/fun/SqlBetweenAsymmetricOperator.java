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
package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Util;

/**
 * Defines the BETWEEN operator.
 *
 * <p>Syntax:
 *
 * <blockquote><code>X [NOT] BETWEEN  Y AND
 * Z</code></blockquote>
 *
 *
 * <p>This operator is always expanded (into something like <code>
 * BETWEEN(X,Y,Z)
 * </code>) before being converted into Rex nodes.
 */
public class SqlBetweenAsymmetricOperator extends SqlFunction {

  private static final String BETWEEN = "BETWEEN";
  private static final String NOT_BETWEEN = "NOT BETWEEN";

  SqlBetweenAsymmetricOperator(boolean negated) {
    super(negated ? NOT_BETWEEN : BETWEEN,
        SqlKind.BETWEEN,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.COMPARABLE_COMPARABLE_COMPARABLE_ORDERED,
        SqlFunctionCategory.SYSTEM);
  }

  /**
   * Ordinal of the 'value' operand.
   */
  public static final int VALUE_OPERAND = 0;

  /**
   * Ordinal of the 'lower' operand.
   */
  public static final int LOWER_OPERAND = 1;

  /**
   * Ordinal of the 'upper' operand.
   */
  public static final int UPPER_OPERAND = 2;

  private static final SqlWriter.FrameType FRAME_TYPE =
      SqlWriter.FrameTypeEnum.create("BETWEEN");

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(FRAME_TYPE, "", "");
    call.operand(VALUE_OPERAND).unparse(writer, getLeftPrec(), 0);
    writer.sep(super.getName());

    // If the expression for the lower bound contains a call to an AND
    // operator, we need to wrap the expression in parentheses to prevent
    // the AND from associating with BETWEEN. For example, we should
    // unparse
    //    a BETWEEN b OR (c AND d) OR e AND f
    // as
    //    a BETWEEN (b OR c AND d) OR e) AND f
    // If it were unparsed as
    //    a BETWEEN b OR c AND d OR e AND f
    // then it would be interpreted as
    //    (a BETWEEN (b OR c) AND d) OR (e AND f)
    // which would be wrong.
    final SqlNode lower = call.operand(LOWER_OPERAND);
    final SqlNode upper = call.operand(UPPER_OPERAND);
    int lowerPrec = new AndFinder().containsAnd(lower) ? 100 : 0;
    lower.unparse(writer, lowerPrec, lowerPrec);
    writer.sep("AND");
    upper.unparse(writer, 0, getRightPrec());
    writer.endList(frame);
  }

  /**
   * Finds an AND operator in an expression.
   */
  private static class AndFinder extends SqlBasicVisitor<Void> {
    @Override public Void visit(SqlCall call) {
      final SqlOperator operator = call.getOperator();
      if (operator == SqlStdOperatorTable.AND) {
        throw Util.FoundOne.NULL;
      }
      return super.visit(call);
    }

    boolean containsAnd(SqlNode node) {
      try {
        node.accept(this);
        return false;
      } catch (Util.FoundOne e) {
        return true;
      }
    }
  }
}
