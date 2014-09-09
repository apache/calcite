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

import java.util.BitSet;
import java.util.List;

import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.sql.SqlDialect;
import org.eigenbase.util.Util;
import org.eigenbase.util.mapping.IntPair;

import net.hydromatic.optiq.config.OptiqConnectionConfig;
import net.hydromatic.optiq.jdbc.OptiqSchema;
import net.hydromatic.optiq.materialize.Lattice;
import net.hydromatic.optiq.materialize.MaterializationKey;
import net.hydromatic.optiq.materialize.MaterializationService;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.util.BitSets;

import com.google.common.collect.Lists;

/**
 * Use of a lattice by the query optimizer.
 */
public class RelOptLattice {
  private final Lattice lattice;
  public final RelOptTable starRelOptTable;

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

  /** Retrieves a materialized table that will satisfy an aggregate query on
   * the star table.
   *
   * <p>The current implementation creates a materialization and populates it.
   *
   * <p>Future implementations might return materializations at a different
   * level of aggregation, from which the desired result can be obtained by
   * rolling up.
   *
   * @param planner Current planner
   * @param groupSet Grouping key
   * @param aggCallList Aggregate functions
   * @return Materialized table
   */
  public OptiqSchema.TableEntry getAggregate(RelOptPlanner planner,
      BitSet groupSet, List<AggregateCall> aggCallList) {
    final OptiqConnectionConfig config =
        planner.getContext().unwrap(OptiqConnectionConfig.class);
    if (config == null || !config.createMaterializations()) {
      return null;
    }
    String sql = sql(starRelOptTable, groupSet, aggCallList);
    final MaterializationService service = MaterializationService.instance();
    final OptiqSchema schema = starRelOptTable.unwrap(OptiqSchema.class);
    final MaterializationKey materializationKey = service
        .defineMaterialization(schema, sql, schema.path(null), "m" + groupSet);
    return service.checkValid(materializationKey);
  }

  private String sql(RelOptTable starRelOptTable, BitSet groupSet,
      List<AggregateCall> aggCallList) {
    BitSet columns = (BitSet) groupSet.clone();
    for (AggregateCall call : aggCallList) {
      for (int arg : call.getArgList()) {
        columns.set(arg);
      }
    }
    // Figure out which nodes are needed. Use a node if its columns are used
    // or if has a child whose columns are used.
    List<Lattice.Node> usedNodes = Lists.newArrayList();
    for (Lattice.Node node : lattice.nodes) {
      if (BitSets.range(node.startCol, node.endCol).intersects(columns)) {
        use(usedNodes, node);
      }
    }
    final SqlDialect dialect = SqlDialect.DatabaseProduct.OPTIQ.getDialect();
    final StringBuilder buf = new StringBuilder();
    buf.append("SELECT DISTINCT ");
    int k = 0;
    for (int i : BitSets.toIter(groupSet)) {
      final RelDataTypeField field =
          starRelOptTable.getRowType().getFieldList().get(i);
      if (k++ > 0) {
        buf.append(", ");
      }
      final List<String> identifiers = lattice.columns.get(i);
      dialect.quoteIdentifier(buf, identifiers);
      if (!Util.last(identifiers).equals(field.getName())) {
        buf.append(" AS ");
        dialect.quoteIdentifier(buf, field.getName());
      }
    }
    buf.append("\nFROM ");
    for (Lattice.Node node : usedNodes) {
      if (node.parent != null) {
        buf.append("\nJOIN ");
      }
      dialect.quoteIdentifier(buf, node.scan.getTable().getQualifiedName());
      buf.append(" AS ");
      dialect.quoteIdentifier(buf, node.alias);
      if (node.parent != null) {
        buf.append(" ON ");
        k = 0;
        for (IntPair pair : node.link) {
          if (k++ > 0) {
            buf.append(" AND ");
          }
          dialect.quoteIdentifier(buf,
              lattice.getColumn(node.parent.startCol + pair.source));
          buf.append(" = ");
          dialect.quoteIdentifier(buf,
              lattice.getColumn(node.startCol + pair.target));
        }
      }
    }
    if (OptiqPrepareImpl.DEBUG) {
      System.out.println("Lattice SQL:\n" + buf);
    }
    return buf.toString();
  }

  private void use(List<Lattice.Node> usedNodes, Lattice.Node node) {
    if (!usedNodes.contains(node)) {
      if (node.parent != null) {
        use(usedNodes, node.parent);
      }
      usedNodes.add(node);
    }
  }
}

// End RelOptLattice.java
