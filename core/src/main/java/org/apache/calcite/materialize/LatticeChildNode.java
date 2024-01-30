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

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/** Non-root node in a {@link Lattice}. */
public class LatticeChildNode extends LatticeNode {
  public final LatticeNode parent;
  public final ImmutableList<IntPair> link;

  LatticeChildNode(LatticeSpace space, LatticeNode parent,
      MutableNode mutableNode) {
    super(space, parent, mutableNode);
    this.parent = requireNonNull(parent, "parent");
    this.link = ImmutableList.copyOf(requireNonNull(mutableNode.step, "step").keys);
  }

  @Override void use(List<LatticeNode> usedNodes) {
    if (!usedNodes.contains(this)) {
      parent.use(usedNodes);
      usedNodes.add(this);
    }
  }
}
