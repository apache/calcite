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

import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.Ordering;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Mutable version of {@link LatticeNode}, used while a graph is being
 * built. */
class MutableNode {
  final LatticeTable table;
  final MutableNode parent;
  final Step step;
  int startCol;
  int endCol;
  String alias;
  final List<MutableNode> children = new ArrayList<>();

  /** Comparator for sorting children within a parent. */
  static final Ordering<MutableNode> ORDERING =
      Ordering.from(
          new Comparator<MutableNode>() {
            public int compare(MutableNode o1, MutableNode o2) {
              int c = Ordering.<String>natural().lexicographical().compare(
                  o1.table.t.getQualifiedName(), o2.table.t.getQualifiedName());
              if (c == 0) {
                // The nodes have the same table. Now compare them based on the
                // columns they use as foreign key.
                c = Ordering.<Integer>natural().lexicographical().compare(
                    IntPair.left(o1.step.keys), IntPair.left(o2.step.keys));
              }
              return c;
            }
          });

  /** Creates a root node. */
  MutableNode(LatticeTable table) {
    this(table, null, null);
  }

  /** Creates a non-root node. */
  MutableNode(LatticeTable table, MutableNode parent, Step step) {
    this.table = Objects.requireNonNull(table);
    this.parent = parent;
    this.step = step;
    if (parent != null) {
      parent.children.add(this);
      Collections.sort(parent.children, ORDERING);
    }
  }

  /** Populates a flattened list of mutable nodes. */
  void flatten(List<MutableNode> flatNodes) {
    flatNodes.add(this);
    for (MutableNode child : children) {
      child.flatten(flatNodes);
    }
  }

  /** Returns whether this node is cylic, in an undirected sense; that is,
   * whether the same descendant can be reached by more than one route. */
  boolean isCyclic() {
    final Set<MutableNode> descendants = new HashSet<>();
    return isCyclicRecurse(descendants);
  }

  private boolean isCyclicRecurse(Set<MutableNode> descendants) {
    if (!descendants.add(this)) {
      return true;
    }
    for (MutableNode child : children) {
      if (child.isCyclicRecurse(descendants)) {
        return true;
      }
    }
    return false;
  }

  void addPath(Path path, String alias) {
    MutableNode n = this;
    for (Step step1 : path.steps) {
      MutableNode n2 = n.findChild(step1);
      if (n2 == null) {
        n2 = new MutableNode(step1.target(), n, step1);
        if (alias != null) {
          n2.alias = alias;
        }
      }
      n = n2;
    }
  }

  private MutableNode findChild(Step step) {
    for (MutableNode child : children) {
      if (child.table.equals(step.target())
          && child.step.equals(step)) {
        return child;
      }
    }
    return null;
  }
}

// End MutableNode.java
