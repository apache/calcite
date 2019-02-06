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
package org.apache.calcite.materialize;

import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Implementation of {@link LatticeStatisticProvider} that gets statistics by
 * executing "SELECT COUNT(DISTINCT ...) ..." SQL queries.
 */
class SqlLatticeStatisticProvider implements LatticeStatisticProvider {
  static final Factory FACTORY = SqlLatticeStatisticProvider::new;

  static final Factory CACHED_FACTORY = lattice -> {
    LatticeStatisticProvider provider = FACTORY.apply(lattice);
    return new CachingLatticeStatisticProvider(lattice, provider);
  };

  private final Lattice lattice;

  /** Creates a SqlLatticeStatisticProvider. */
  private SqlLatticeStatisticProvider(Lattice lattice) {
    this.lattice = Objects.requireNonNull(lattice);
  }

  public double cardinality(List<Lattice.Column> columns) {
    final List<Double> counts = new ArrayList<>();
    for (Lattice.Column column : columns) {
      counts.add(cardinality(lattice, column));
    }
    return (int) Lattice.getRowCount(lattice.getFactRowCount(), counts);
  }

  private double cardinality(Lattice lattice, Lattice.Column column) {
    final String sql = lattice.countSql(ImmutableBitSet.of(column.ordinal));
    final Table table =
        new MaterializationService.DefaultTableFactory()
            .createTable(lattice.rootSchema, sql, ImmutableList.of());
    final Object[] values =
        Iterables.getOnlyElement(((ScannableTable) table).scan(null));
    return ((Number) values[0]).doubleValue();
  }
}

// End SqlLatticeStatisticProvider.java
