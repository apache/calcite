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
package org.apache.calcite.statistic;

import org.apache.calcite.materialize.SqlStatisticProvider;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Implementation of {@link SqlStatisticProvider} that reads and writes a
 * cache.
 */
public class CachingSqlStatisticProvider implements SqlStatisticProvider {
  private final SqlStatisticProvider provider;
  private final Cache<List, Object> cache;

  public CachingSqlStatisticProvider(SqlStatisticProvider provider,
      Cache<List, Object> cache) {
    super();
    this.provider = provider;
    this.cache = cache;
  }

  public double tableCardinality(RelOptTable table) {
    try {
      final ImmutableList<Object> key =
          ImmutableList.of("tableCardinality",
              table.getQualifiedName());
      return (Double) cache.get(key,
          () -> provider.tableCardinality(table));
    } catch (UncheckedExecutionException | ExecutionException e) {
      Util.throwIfUnchecked(e.getCause());
      throw new RuntimeException(e.getCause());
    }
  }

  public boolean isForeignKey(RelOptTable fromTable, List<Integer> fromColumns,
      RelOptTable toTable, List<Integer> toColumns) {
    try {
      final ImmutableList<Object> key =
          ImmutableList.of("isForeignKey",
              fromTable.getQualifiedName(),
              ImmutableIntList.copyOf(fromColumns),
              toTable.getQualifiedName(),
              ImmutableIntList.copyOf(toColumns));
      return (Boolean) cache.get(key,
          () -> provider.isForeignKey(fromTable, fromColumns, toTable,
              toColumns));
    } catch (UncheckedExecutionException | ExecutionException e) {
      Util.throwIfUnchecked(e.getCause());
      throw new RuntimeException(e.getCause());
    }
  }

  public boolean isKey(RelOptTable table, List<Integer> columns) {
    try {
      final ImmutableList<Object> key =
          ImmutableList.of("isKey", table.getQualifiedName(),
              ImmutableIntList.copyOf(columns));
      return (Boolean) cache.get(key, () -> provider.isKey(table, columns));
    } catch (UncheckedExecutionException | ExecutionException e) {
      Util.throwIfUnchecked(e.getCause());
      throw new RuntimeException(e.getCause());
    }
  }
}

// End CachingSqlStatisticProvider.java
