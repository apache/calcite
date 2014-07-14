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
package org.eigenbase.relopt;

import java.util.List;

import org.eigenbase.rel.RelNode;

import net.hydromatic.optiq.materialize.Lattice;

import com.google.common.collect.Lists;

/**
 * Use of a lattice by the query optimizer.
 */
public class RelOptLattice {
  private final Lattice lattice;
  private final RelOptTable starRelOptTable;
  private final List<RelOptMaterialization> materializations =
      Lists.newArrayList();

  public RelOptLattice(Lattice lattice, RelOptTable starRelOptTable) {
    this.lattice = lattice;
    this.starRelOptTable = starRelOptTable;
  }

  public RelOptTable rootTable() {
    return lattice.nodes.get(0).scan.getTable();
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
}

// End RelOptLattice.java
