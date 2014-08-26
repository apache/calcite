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
package net.hydromatic.optiq.rules.java;

import net.hydromatic.linq4j.expressions.Expression;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Implements an aggregate function by generating expressions to
 * initialize, add to, and get a result from, an accumulator.
 *
 * @see net.hydromatic.optiq.rules.java.StrictAggImplementor
 * @see net.hydromatic.optiq.rules.java.StrictWinAggImplementor
 * @see net.hydromatic.optiq.rules.java.RexImpTable.CountImplementor
 * @see net.hydromatic.optiq.rules.java.RexImpTable.SumImplementor
 */
public interface AggImplementor {
  /**
   * Returns the types of the intermediate variables used by the aggregate
   * implementation.
   * For instance, for "concatenate to string" this can be {@link java.lang.StringBuilder}.
   * Optiq calls this method before all other {@code implement*} methods.
   * @param info aggregate context
   * @return types of the intermediate variables used by the aggregate
   *   implementation
   */
  List<Type> getStateType(AggContext info);

  /**
   * Implements reset of the intermediate variables to the initial state.
   * {@link AggResetContext#accumulator()} should be used to reference
   * the state variables.
   * For instance, to zero the count use the following code:
   * {@code reset.currentBlock().add(Expressions.statement(
   * Expressions.assign(reset.accumulator().get(0), Expressions.constant(0)));}
   * @param info aggregate context
   * @param reset reset context
   */
  void implementReset(AggContext info, AggResetContext reset);

  /**
   * Updates intermediate values to account for the newly added value.
   * {@link AggResetContext#accumulator()} should be used to reference
   * the state variables.
   * @param info aggregate context
   * @param add add context
   */
  void implementAdd(AggContext info, AggAddContext add);

  /**
   * Calculates the resulting value based on the intermediate variables.
   * Note: this method must NOT destroy the intermediate variables as
   * optiq might reuse the state when calculating sliding aggregates.
   * {@link AggResetContext#accumulator()} should be used to reference
   * the state variables.
   * @param info aggregate context
   * @param result result context
   */
  Expression implementResult(AggContext info, AggResultContext result);
}

// End AggImplementor.java
