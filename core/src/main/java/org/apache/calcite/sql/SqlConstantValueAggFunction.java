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
 * Aggregate function that returns a constant value when applied to constant
 * (GROUP BY key) arguments.
 *
 * <p>For example, statistical functions like STDDEV_POP, STDDEV_SAMP, VAR_POP,
 * VAR_SAMP always return 0 when applied to a constant value, since there is
 * no variation in a set of identical values.
 *
 * <p>This interface allows optimization rules to identify and reduce such
 * aggregate functions without hard-coded checks for specific function types.
 */
public interface SqlConstantValueAggFunction {
  /**
   * Generates the constant result expression when this aggregate function is
   * applied to arguments that are constant within each group (i.e., GROUP BY keys
   * or expressions derived only from GROUP BY keys).
   *
   * <p>For example:
   * <ul>
   *   <li>{@code STDDEV_POP(constant)} returns {@code 0}
   *   <li>{@code VAR_SAMP(constant)} returns {@code 0}
   *   <li>{@code STDDEV_SAMP(constant)} returns {@code 0}
   * </ul>
   *
   * @param rexBuilder Rex builder for creating the result expression
   * @param returnType The return type of the aggregate function
   * @return An expression representing the constant result, or null if this function
   *     does not return a constant value for constant arguments
   */
  @Nullable RexNode getConstantResult(RexBuilder rexBuilder, RelDataType returnType);
}
