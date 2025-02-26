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

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlStaticAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The {@code LITERAL_AGG} aggregate function.
 *
 * <p>It accepts zero regular aggregate arguments (the kind that reference
 * columns of the input group) but one argument of type
 * {@link org.apache.calcite.rex.RexLiteral}, and its return is that literal.
 */
public class SqlLiteralAggFunction {
  /** This is utility class. It is never instantiated. */
  private SqlLiteralAggFunction() {
  }

  public static final SqlBasicAggFunction INSTANCE =
      SqlBasicAggFunction.create(SqlKind.LITERAL_AGG,
              SqlLiteralAggFunction::inferReturnType, OperandTypes.NILADIC)
          .withStatic(SqlLiteralAggFunction::constant);

  /** Implements {@link org.apache.calcite.sql.type.SqlReturnTypeInference}. */
  private static RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    // LITERAL_AGG takes one pre-operand and zero (post-)operands.
    if (opBinding.getPreOperandCount() != 1
        || opBinding.getOperandCount() != 1) {
      throw new AssertionError();
    }
    return opBinding.getOperandType(0);
  }

  /** Implements {@link SqlStaticAggFunction}. */
  private static @Nullable RexNode constant(RexBuilder rexBuilder,
      ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets,
      AggregateCall aggregateCall) {
    // LITERAL_AGG[literal]() evaluates to "literal".
    return Iterables.getOnlyElement(aggregateCall.rexList);
  }
}
