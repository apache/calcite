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
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Aggregate function whose value may be a constant expression, based on
 * only the contents of the GROUP BY clause. */
public interface SqlStaticAggFunction {
  /** Generates an expression for the aggregate function; or null if the value
   * is not constant.
   *
   * <p>For example:
   * <ul>
   *   <li>{@code GROUPING(deptno)} expands to literal {@code 0}
   *       if the aggregate has {@code GROUP BY deptno}
   * </ul>
   *
   * @param rexBuilder Rex builder
   * @param groupSet Group set
   * @param groupSets Group sets
   * @param aggregateCall Aggregate call
   *
   * @return Expression for single row
   */
  @Nullable RexNode constant(RexBuilder rexBuilder, ImmutableBitSet groupSet,
      ImmutableList<ImmutableBitSet> groupSets, AggregateCall aggregateCall);
}
