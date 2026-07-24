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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlMatchFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWindowTableFunction;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for {@link RexImplementorTable}.
 */
public abstract class RexImplementorTables {
  private RexImplementorTables() {
  }

  /** Returns the implementor table registered on the {@code cluster}'s planner
   * {@link org.apache.calcite.plan.Context}, or the built-in
   * {@link RexImpTable#instance()} when none is registered. */
  public static RexImplementorTable of(RelOptCluster cluster) {
    return cluster.getPlanner().getContext()
        .maybeUnwrap(RexImplementorTable.class)
        .orElse(RexImpTable.instance());
  }

  /** Creates a table that consults each of the given tables in turn, returning
   * the first non-null implementor.
   *
   * <p>Earlier tables take precedence: when more than one table has an
   * implementor for the same operator, the one earliest in the list is
   * returned. Listing {@link RexImpTable#instance()} last makes the built-in
   * implementors the fallback. */
  public static RexImplementorTable chain(RexImplementorTable... tables) {
    return chain(ImmutableList.copyOf(tables));
  }

  /** Creates a table that consults each of the given tables in turn.
   *
   * @see #chain(RexImplementorTable...) */
  public static RexImplementorTable chain(
      Iterable<? extends RexImplementorTable> tables) {
    final List<RexImplementorTable> list = new ArrayList<>();
    for (RexImplementorTable table : tables) {
      addFlattened(list, table);
    }
    if (list.size() == 1) {
      return list.get(0);
    }
    return new Chain(ImmutableList.copyOf(list));
  }

  private static void addFlattened(List<RexImplementorTable> list,
      RexImplementorTable table) {
    if (table instanceof Chain) {
      list.addAll(((Chain) table).tables);
    } else {
      list.add(table);
    }
  }

  /** Implementor table that consults a list of tables in order, returning the
   * first non-null implementor. */
  private static class Chain implements RexImplementorTable {
    final ImmutableList<RexImplementorTable> tables;

    Chain(ImmutableList<RexImplementorTable> tables) {
      this.tables = tables;
    }

    @Override public @Nullable RexCallImplementor get(SqlOperator operator) {
      for (RexImplementorTable table : tables) {
        final RexCallImplementor implementor = table.get(operator);
        if (implementor != null) {
          return implementor;
        }
      }
      return null;
    }

    @Override public @Nullable AggImplementor get(SqlAggFunction aggregation,
        boolean forWindowAggregate) {
      for (RexImplementorTable table : tables) {
        final AggImplementor implementor =
            table.get(aggregation, forWindowAggregate);
        if (implementor != null) {
          return implementor;
        }
      }
      return null;
    }

    @Override public @Nullable MatchImplementor get(SqlMatchFunction function) {
      for (RexImplementorTable table : tables) {
        final MatchImplementor implementor = table.get(function);
        if (implementor != null) {
          return implementor;
        }
      }
      return null;
    }

    @Override public @Nullable TableFunctionCallImplementor get(
        SqlWindowTableFunction operator) {
      for (RexImplementorTable table : tables) {
        final TableFunctionCallImplementor implementor = table.get(operator);
        if (implementor != null) {
          return implementor;
        }
      }
      return null;
    }
  }
}
