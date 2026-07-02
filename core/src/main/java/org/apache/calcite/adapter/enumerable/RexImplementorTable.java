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

import org.apache.calcite.adapter.enumerable.RexImpTable.RexCallImplementor;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlMatchFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWindowTableFunction;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Provides the implementor that generates code for calls to an operator.
 *
 * <p>Enumerable code generation translates each operator call into
 * {@linkplain org.apache.calcite.linq4j.tree.Expression linq4j code} using an
 * implementor. This table looks up that implementor for a scalar operator, an
 * aggregate function, a {@code MATCH_RECOGNIZE} function, or a windowed table
 * function.
 *
 * <p>A lookup returns {@code null} if this table has no implementor for the
 * given operator.
 */
public interface RexImplementorTable {
  /** Returns the implementor of a scalar operator, or null if this table has
   * none. */
  @Nullable RexCallImplementor get(SqlOperator operator);

  /** Returns the implementor of an aggregate function (in window context when
   * {@code forWindowAggregate} is true), or null if this table has none. */
  @Nullable AggImplementor get(SqlAggFunction aggregation,
      boolean forWindowAggregate);

  /** Returns the implementor of a {@code MATCH_RECOGNIZE} function, or null if
   * this table has none. */
  @Nullable MatchImplementor get(SqlMatchFunction function);

  /** Returns the implementor of a windowed table function, or null if this
   * table has none. */
  @Nullable TableFunctionCallImplementor get(SqlWindowTableFunction operator);
}
