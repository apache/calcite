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
package org.apache.calcite.sql.util;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import org.junit.jupiter.api.Test;

import static java.util.Objects.requireNonNull;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Unit tests for {@link SqlShuttle}. */
class SqlShuttleTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7809">[CALCITE-7809]
  * Reduce temporary object allocation in expression shuttles</a>. */
  @Test void testUnchangedCallIsReused() {
    final SqlLiteral[] operands = createLiterals();
    final SqlCall call = createCall(operands);
    final SqlLiteral absent = SqlLiteral.createExactNumeric("4", SqlParserPos.ZERO);

    final SqlNode result =
        requireNonNull(call.accept(new ReplacingSqlShuttle(absent, absent)));

    assertSame(call, result);
  }

  @Test void testChangedOperandCopiesCall() {
    final SqlLiteral[] operands = createLiterals();
    final SqlCall call = createCall(operands);
    final SqlLiteral replacement =
        SqlLiteral.createExactNumeric("4", SqlParserPos.ZERO);

    final SqlCall result =
        (SqlCall) requireNonNull(call.accept(
            new ReplacingSqlShuttle(operands[1], replacement)));

    assertNotSame(call, result);
    assertEquals(operands.length, result.operandCount());
    assertSame(operands[0], result.operand(0));
    assertSame(replacement, result.operand(1));
    assertSame(operands[2], result.operand(2));
  }

  @Test void testAlwaysCopyCopiesUnchangedCall() {
    final SqlLiteral[] operands = createLiterals();
    final SqlCall call = createCall(operands);
    final SqlShuttle shuttle = new SqlShuttle();
    final SqlShuttle.CallCopyingArgHandler argHandler =
        shuttle.new CallCopyingArgHandler(call, true);

    call.getOperator().acceptCall(shuttle, call, false, argHandler);
    final SqlCall result = (SqlCall) argHandler.result();

    assertNotSame(call, result);
    assertEquals(operands.length, result.operandCount());
    assertSame(operands[0], result.operand(0));
    assertSame(operands[1], result.operand(1));
    assertSame(operands[2], result.operand(2));
  }

  private static SqlLiteral[] createLiterals() {
    return new SqlLiteral[] {
        SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO),
        SqlLiteral.createExactNumeric("2", SqlParserPos.ZERO),
        SqlLiteral.createExactNumeric("3", SqlParserPos.ZERO)
    };
  }

  private static SqlCall createCall(SqlNode... operands) {
    return SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR.createCall(
        SqlParserPos.ZERO, operands);
  }

  /** Shuttle that replaces one target node. */
  private static class ReplacingSqlShuttle extends SqlShuttle {
    private final SqlNode target;
    private final SqlNode replacement;

    ReplacingSqlShuttle(SqlNode target, SqlNode replacement) {
      this.target = target;
      this.replacement = replacement;
    }

    @Override public SqlNode visit(SqlLiteral literal) {
      return literal == target ? replacement : literal;
    }
  }
}
