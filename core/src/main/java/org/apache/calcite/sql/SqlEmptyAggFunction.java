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

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Aggregate function that can describe its result on empty input.
 *
 * <p>This contract is independent of {@link SqlConstantValueAggFunction},
 * which describes the result for sufficiently many identical non-null values.
 */
public interface SqlEmptyAggFunction {
  /** Returns the result expression for a call whose input is known to be empty.
   * The expression must not reference input rows, but may depend on the call's
   * type and literal arguments, as for LITERAL_AGG.
   *
   * <p>A SQL NULL result is represented by a
   * {@link org.apache.calcite.rex.RexLiteral}. Java {@code null} means that the
   * empty-input result is unknown. This method does not establish that the
   * input is empty, or that a group exists; callers must check those conditions
   * or use the result in the empty branch of a conditional expression.
   *
   * @param rexBuilder Rex builder
   * @param aggregateCall Aggregate call
   * @return Result expression, or null if the empty-input result is unknown
   */
  @Nullable RexNode getEmptyResult(RexBuilder rexBuilder, AggregateCall aggregateCall);
}
