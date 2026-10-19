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
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Aggregate function that returns a constant result for sufficiently many
 * identical non-null arguments, and NULL for fewer arguments.
 *
 * <p>For example, STDDEV_POP and VAR_POP return 0 for one or more identical
 * non-null values. STDDEV_SAMP and VAR_SAMP require at least two such values.
 * NULL arguments are ignored; DISTINCT, if present, is applied before counting.
 *
 * <p>This interface allows optimization rules to identify and reduce such
 * aggregate functions without hard-coded checks for specific function types.
 */
public interface SqlConstantValueAggFunction {
  /**
   * Generates the constant result expression when this aggregate function is
   * applied to arguments that are constant within each group (GROUP BY keys,
   * deterministic expressions of those keys, or constants), with at least
   * {@link #getMinCountForConstantResult()} non-null values.
   *
   * <p>For example:
   * <ul>
   *   <li>{@code STDDEV_POP(constant)} returns {@code 0}
   *   <li>{@code VAR_SAMP(constant)} returns {@code 0} for at least two values
   *   <li>{@code STDDEV_SAMP(constant)} returns {@code 0} for at least two values
   * </ul>
   *
   * @param rexBuilder Rex builder for creating the result expression
   * @param returnType The return type of the aggregate function
   * @return An expression representing the constant result, or null if this function
   *     does not return a constant value for constant arguments
   */
  @Nullable RexNode getConstantResult(RexBuilder rexBuilder, RelDataType returnType);

  /**
   * Returns the minimum number of identical non-null values required for
   * {@link #getConstantResult} to apply. Must be positive.
   * Below this count, including empty input, the aggregate returns NULL.
   * This contract applies when {@code getConstantResult} returns an expression.
   */
  default int getMinCountForConstantResult() {
    return 1;
  }
}
