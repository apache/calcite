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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.tree.Expression;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Implements an aggregate function by generating expressions to
 * initialize, add to, and get a result from, an accumulator.
 *
 * @see org.apache.calcite.adapter.enumerable.StrictAggImplementor
 * @see org.apache.calcite.adapter.enumerable.StrictWinAggImplementor
 * @see org.apache.calcite.adapter.enumerable.RexImpTable.CountImplementor
 * @see org.apache.calcite.adapter.enumerable.RexImpTable.SumImplementor
 */
public interface AggImplementor {
  /**
   * Returns the types of the intermediate variables used by the aggregate
   * implementation.
   *
   * <p>For instance, for "concatenate to string" this can be
   * {@link java.lang.StringBuilder}.
   * Calcite calls this method before all other {@code implement*} methods.
   *
   * @param info Aggregate context
   * @return Types of the intermediate variables used by the aggregate
   *   implementation
   */
  List<Type> getStateType(AggContext info);

  /**
   * Implements reset of the intermediate variables to the initial state.
   * {@link AggResetContext#accumulator()} should be used to reference
   * the state variables.
   * For instance, to zero the count, use the following code:
   *
   * <blockquote><code>reset.currentBlock().add(<br>
   *   Expressions.statement(<br>
   *     Expressions.assign(reset.accumulator().get(0),<br>
   *       Expressions.constant(0)));</code></blockquote>
   *
   * @param info Aggregate context
   * @param reset Reset context
   */
  void implementReset(AggContext info, AggResetContext reset);

  /**
   * Updates intermediate values to account for the newly added value.
   * {@link AggResetContext#accumulator()} should be used to reference
   * the state variables.
   *
   * @param info Aggregate context
   * @param add Add context
   */
  void implementAdd(AggContext info, AggAddContext add);

  /**
   * Calculates the resulting value based on the intermediate variables.
   * Note: this method must NOT destroy the intermediate variables as
   * calcite might reuse the state when calculating sliding aggregates.
   * {@link AggResetContext#accumulator()} should be used to reference
   * the state variables.
   *
   * @param info Aggregate context
   * @param result Result context
   * @return Expression that is a result of calculating final value of
   *   the aggregate being implemented
   */
  Expression implementResult(AggContext info, AggResultContext result);
}

// End AggImplementor.java
