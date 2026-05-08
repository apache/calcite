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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * The colon operator {@code :}, used for variant path access.
 *
 * <p>Operands are {@code (base, path)}, where {@code path} is a
 * {@link SqlNodeList} of segments.
 */
public class SqlColonOperator extends SqlSpecialOperator {
  SqlColonOperator() {
    super("COLON", SqlKind.COLON, 100, true, null, null, OperandTypes.COLON);
  }

  // Path segments are structural literals/identifiers, never column refs, so
  // they must not be routed through expression visitors such as AggChecker.
  @Override public <R> void acceptCall(SqlVisitor<R> visitor, SqlCall call,
      boolean onlyExpressions, SqlBasicVisitor.ArgHandler<R> argHandler) {
    if (onlyExpressions) {
      argHandler.visitChild(visitor, call, 0, call.operand(0));
    } else {
      super.acceptCall(visitor, call, onlyExpressions, argHandler);
    }
  }

  @Override public ReduceResult reduceExpr(int ordinal, TokenSequence list) {
    final SqlNode left = list.node(ordinal - 1);
    final SqlNode right = list.node(ordinal + 1);
    return new ReduceResult(ordinal - 1, ordinal + 2,
        createCall(
            SqlParserPos.sum(
                Arrays.asList(
                    requireNonNull(left, "left").getParserPosition(),
                    requireNonNull(right, "right").getParserPosition(),
                    list.pos(ordinal))),
            left,
            right));
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.IDENTIFIER);
    call.operand(0).unparse(writer, leftPrec, 0);
    writer.setNeedWhitespace(false);
    writer.keyword(":");
    writer.setNeedWhitespace(false);
    boolean first = true;
    for (SqlNode segment : (SqlNodeList) call.operand(1)) {
      if (segment instanceof SqlIdentifier) {
        if (!first) {
          writer.sep(".");
        }
        segment.unparse(writer, 0, 0);
      } else {
        final SqlWriter.Frame b = writer.startList("[", "]");
        segment.unparse(writer, 0, 0);
        writer.endList(b);
      }
      first = false;
    }
    writer.endList(frame);
  }

  @Override public RelDataType deriveType(SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    // Do not derive type of operand 1; it is a path, not an expression.
    final RelDataType baseType =
        requireNonNull(validator.deriveType(scope, call.operand(0)));
    final RelDataType type =
        validator.getTypeFactory().createTypeWithNullability(baseType, true);
    validator.setValidatedNodeType(call, type);
    return type;
  }

  @Override public void validateCall(SqlCall call, SqlValidator validator,
      SqlValidatorScope scope, SqlValidatorScope operandScope) {
    assert call.getOperator() == this;
    // Do not validate operand 1; it is a path, not an expression.
    call.operand(0).validateExpr(validator, operandScope);
    checkOperandTypes(new SqlCallBinding(validator, scope, call), true);
  }

  @Override public boolean validRexOperands(final int count, final Litmus litmus) {
    return litmus.fail("COLON is valid only for SqlCall not for RexCall");
  }
}
