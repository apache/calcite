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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

/** Aggregate function that knows how to convert itself to a scalar value
 * when applied to a single row. */
public interface SqlSingletonAggFunction {
  /** Generates an expression for the value of the aggregate function when
   * applied to a single row.
   *
   * <p>For example, if there is one row:
   * <ul>
   *   <li>{@code SUM(x)} is {@code x}
   *   <li>{@code MIN(x)} is {@code x}
   *   <li>{@code MAX(x)} is {@code x}
   *   <li>{@code COUNT(x)} is {@code CASE WHEN x IS NOT NULL THEN 1 ELSE 0 END 1}
   *   which can be simplified to {@code 1} if {@code x} is never null
   *   <li>{@code COUNT(*)} is 1
   *   <li>{@code GROUPING(deptno)} if 0 if {@code deptno} is being grouped,
   *       1 otherwise
   * </ul>
   *
   * @param rexBuilder Rex builder
   * @param inputRowType Input row type
   * @param aggregateCall Aggregate call
   *
   * @return Expression for single row
   */
  RexNode singleton(RexBuilder rexBuilder, RelDataType inputRowType,
      AggregateCall aggregateCall);
}
