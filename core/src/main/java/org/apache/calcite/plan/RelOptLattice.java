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
package org.apache.calcite.plan;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.materialize.TileKey;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * Use of a lattice by the query optimizer.
 */
public class RelOptLattice {
  public final Lattice lattice;
  public final RelOptTable starRelOptTable;

  public RelOptLattice(Lattice lattice, RelOptTable starRelOptTable) {
    this.lattice = lattice;
    this.starRelOptTable = starRelOptTable;
  }

  public RelOptTable rootTable() {
    return lattice.rootNode.relOptTable();
  }

  /** Rewrites a relational expression to use a lattice.
   *
   * <p>Returns null if a rewrite is not possible.
   *
   * @param node Relational expression
   * @return Rewritten query
   */
  public RelNode rewrite(RelNode node) {
    return RelOptMaterialization.tryUseStar(node, starRelOptTable);
  }

  /** Retrieves a materialized table that will satisfy an aggregate query on
   * the star table.
   *
   * <p>The current implementation creates a materialization and populates it,
   * provided that {@link Lattice#auto} is true.
   *
   * <p>Future implementations might return materializations at a different
   * level of aggregation, from which the desired result can be obtained by
   * rolling up.
   *
   * @param planner Current planner
   * @param groupSet Grouping key
   * @param measureList Calls to aggregate functions
   * @return Materialized table
   */
  public Pair<CalciteSchema.TableEntry, TileKey> getAggregate(
      RelOptPlanner planner, ImmutableBitSet groupSet,
      List<Lattice.Measure> measureList) {
    final CalciteConnectionConfig config =
        planner.getContext().unwrap(CalciteConnectionConfig.class);
    if (config == null) {
      return null;
    }
    final MaterializationService service = MaterializationService.instance();
    boolean create = lattice.auto && config.createMaterializations();
    final CalciteSchema schema = starRelOptTable.unwrap(CalciteSchema.class);
    return service.defineTile(lattice, groupSet, measureList, schema, create,
        false);
  }
}

// End RelOptLattice.java
