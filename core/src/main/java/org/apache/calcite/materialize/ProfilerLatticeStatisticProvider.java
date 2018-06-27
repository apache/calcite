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

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.profile.Profiler;
import org.apache.calcite.profile.ProfilerImpl;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Implementation of {@link LatticeStatisticProvider} that uses a
 * {@link org.apache.calcite.profile.Profiler}.
 */
class ProfilerLatticeStatisticProvider implements LatticeStatisticProvider {
  static final Factory FACTORY =
      new Factory() {
        public LatticeStatisticProvider apply(Lattice lattice) {
          return new ProfilerLatticeStatisticProvider(lattice);
        }
      };

  /** Converts an array of values to a list of {@link Comparable} values,
   * converting null values to sentinels. */
  private static final Function1<Object[], List<Comparable>> TO_LIST =
      new Function1<Object[], List<Comparable>>() {
        public List<Comparable> apply(Object[] values) {
          for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
              values[i] = NullSentinel.INSTANCE;
            }
          }
          //noinspection unchecked
          return (List) Arrays.asList(values);
        }
      };

  private final Lattice lattice;
  private final Supplier<Profiler.Profile> profile =
      Suppliers.memoize(new Supplier<Profiler.Profile>() {
        public Profiler.Profile get() {
          final ProfilerImpl profiler =
              ProfilerImpl.builder()
                  .withPassSize(200)
                  .withMinimumSurprise(0.3D)
                  .build();
          final List<Profiler.Column> columns = new ArrayList<>();
          for (Lattice.Column column : lattice.columns) {
            columns.add(new Profiler.Column(column.ordinal, column.alias));
          }
          final String sql =
              lattice.sql(ImmutableBitSet.range(lattice.columns.size()),
                  false, ImmutableList.<Lattice.Measure>of());
          final Table table =
              new MaterializationService.DefaultTableFactory()
                  .createTable(lattice.rootSchema, sql,
                      ImmutableList.<String>of());
          final ImmutableList<ImmutableBitSet> initialGroups =
              ImmutableList.of();
          final Enumerable<List<Comparable>> rows =
              ((ScannableTable) table).scan(null).select(TO_LIST);
          return profiler.profile(rows, columns, initialGroups);
        }
      });

  /** Creates a ProfilerLatticeStatisticProvider. */
  private ProfilerLatticeStatisticProvider(Lattice lattice) {
    this.lattice = Preconditions.checkNotNull(lattice);
  }

  public double cardinality(List<Lattice.Column> columns) {
    final ImmutableBitSet build = Lattice.Column.toBitSet(columns);
    final double cardinality = profile.get().cardinality(build);
//    System.out.println(columns + ": " + cardinality);
    return cardinality;
  }
}

// End ProfilerLatticeStatisticProvider.java
