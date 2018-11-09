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

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/** Source relation of a lattice.
 *
 * <p>Relations form a tree; all relations except the root relation
 * (the fact table) have precisely one parent and an equi-join
 * condition on one or more pairs of columns linking to it. */
public abstract class LatticeNode {
  public final LatticeTable table;
  final int startCol;
  final int endCol;
  public final String alias;
  private final ImmutableList<LatticeChildNode> children;
  public final String digest;

  /** Creates a LatticeNode.
   *
   * <p>The {@code parent} and {@code mutableNode} arguments are used only
   * during construction. */
  LatticeNode(LatticeSpace space, LatticeNode parent, MutableNode mutableNode) {
    this.table = Objects.requireNonNull(mutableNode.table);
    this.startCol = mutableNode.startCol;
    this.endCol = mutableNode.endCol;
    this.alias = mutableNode.alias;
    Preconditions.checkArgument(startCol >= 0);
    Preconditions.checkArgument(endCol > startCol);

    final StringBuilder sb = new StringBuilder()
        .append(space.simpleName(table));
    if (parent != null) {
      sb.append(':');
      int i = 0;
      for (IntPair p : mutableNode.step.keys) {
        if (i++ > 0) {
          sb.append(",");
        }
        sb.append(parent.table.field(p.source).getName());
      }
    }
    if (mutableNode.children.isEmpty()) {
      this.children = ImmutableList.of();
    } else {
      sb.append(" (");
      final ImmutableList.Builder<LatticeChildNode> b = ImmutableList.builder();
      int i = 0;
      for (MutableNode mutableChild : mutableNode.children) {
        if (i++ > 0) {
          sb.append(' ');
        }
        final LatticeChildNode node =
            new LatticeChildNode(space, this, mutableChild);
        sb.append(node.digest);
        b.add(node);
      }
      this.children = b.build();
      sb.append(")");
    }
    this.digest = sb.toString();

  }

  @Override public String toString() {
    return digest;
  }

  public RelOptTable relOptTable() {
    return table.t;
  }

  abstract void use(List<LatticeNode> usedNodes);

  void flattenTo(ImmutableList.Builder<LatticeNode> builder) {
    builder.add(this);
    for (LatticeChildNode child : children) {
      child.flattenTo(builder);
    }
  }

  void createPathsRecurse(LatticeSpace space, List<Step> steps,
      List<Path> paths) {
    paths.add(space.addPath(steps));
    for (LatticeChildNode child : children) {
      steps.add(space.addEdge(table, child.table, child.link));
      child.createPathsRecurse(space, steps, paths);
      steps.remove(steps.size() - 1);
    }
  }

}

// End LatticeNode.java
