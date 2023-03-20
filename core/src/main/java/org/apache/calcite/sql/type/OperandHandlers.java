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
package org.apache.calcite.sql.type;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.TimeFrame;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Strategies for handling operands.
 *
 * <p>This class defines singleton instances of strategy objects for operand
 * expansion. {@link OperandTypes}, {@link ReturnTypes} and {@link InferTypes}
 * provide similar strategies for operand type checking and inference,
 * and operator return type inference.
 *
 * @see SqlOperandTypeChecker
 * @see ReturnTypes
 * @see InferTypes
 */
public abstract class OperandHandlers {
  /** The default operand handler visits all operands. */
  public static final SqlOperandHandler DEFAULT =
      new SqlOperandHandler() {
      };

  /** An operand handler that tries to convert operand #1 (0-based) into a time
   * frame.
   *
   * <p>For example, the {@code DATE_TRUNC} function uses this; the calls
   * {@code DATE_TRUNC('month', orders.order_date)},
   * {@code DATE_TRUNC(orders.order_date, MONTH)},
   * {@code DATE_TRUNC(orders.order_date, MINUTE15)}
   * are all valid. The last uses a user-defined time frame, which appears
   * to the validator as a {@link SqlIdentifier} and is then converted to a
   * {@link SqlIntervalQualifier} when it matches a defined time frame. */
  public static final SqlOperandHandler OPERAND_1_MIGHT_BE_TIME_FRAME =
      new TimeFrameOperandHandler(1);

  /** Creates an operand handler that applies a function to a call. */
  public static SqlOperandHandler of(BiFunction<SqlValidator, SqlCall, SqlCall> fn) {
    return new SqlOperandHandler() {
      @Override public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
        return fn.apply(validator, call);
      }
    };
  }

  /** Operand handler for a function whose {@code timeFrameOperand} operand
   * (0-based) may be a time frame. If the operand is of type
   * {@link SqlIdentifier}, looks up the custom time frame and converts it to a
   * {@link SqlIntervalQualifier}. */
  private static class TimeFrameOperandHandler implements SqlOperandHandler {
    private final int timeFrameOperand;

    TimeFrameOperandHandler(int timeFrameOperand) {
      this.timeFrameOperand = timeFrameOperand;
    }

    private SqlNode getOperand(int i, SqlNode operand,
        Function<String, @Nullable TimeFrame> timeFrameResolver) {
      if (i == timeFrameOperand
          && operand instanceof SqlIdentifier
          && ((SqlIdentifier) operand).isSimple()) {
        final String name = ((SqlIdentifier) operand).getSimple();
        final TimeFrame timeFrame = timeFrameResolver.apply(name);
        if (timeFrame != null) {
          return new SqlIntervalQualifier(name, operand.getParserPosition());
        }
      }
      return operand;
    }

    @Override public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
      final List<SqlNode> newOperandList = new ArrayList<>();
      Ord.forEach(call.getOperandList(), (operand, i) ->
          newOperandList.add(
              getOperand(i, operand, name ->
                  validator.getTimeFrameSet().getOpt(name))));
      if (newOperandList.equals(call.getOperandList())) {
        return call;
      }
      return call.getOperator().createCall(call.getFunctionQuantifier(),
          call.getParserPosition(), newOperandList);
    }
  }
}
