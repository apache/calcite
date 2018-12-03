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

import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Allows to build nested code blocks with tracking of current context and the
 * nullability of particular {@link org.apache.calcite.rex.RexNode} expressions.
 *
 * @see org.apache.calcite.adapter.enumerable.StrictAggImplementor#implementAdd(AggContext, AggAddContext)
 */
public class NestedBlockBuilderImpl implements NestedBlockBuilder {
  private final List<BlockBuilder> blocks = new ArrayList<>();
  private final List<Map<RexNode, Boolean>> nullables =
      new ArrayList<>();

  /**
   * Constructs nested block builders starting of a given code block.
   * @param block root code block
   */
  public NestedBlockBuilderImpl(BlockBuilder block) {
    nestBlock(block);
  }

  /**
   * Starts nested code block. The resulting block can optimize expressions
   * and reuse already calculated values from the parent blocks.
   * @return new code block that can optimize expressions and reuse already
   * calculated values from the parent blocks.
   */
  public final BlockBuilder nestBlock() {
    BlockBuilder block = new BlockBuilder(true, currentBlock());
    nestBlock(block, Collections.emptyMap());
    return block;
  }

  /**
   * Uses given block as the new code context.
   * The current block will be restored after {@link #exitBlock()} call.
   * @param block new code block
   * @see #exitBlock()
   */
  public final void nestBlock(BlockBuilder block) {
    nestBlock(block, Collections.emptyMap());
  }

  /**
   * Uses given block as the new code context and the map of nullability.
   * The current block will be restored after {@link #exitBlock()} call.
   * @param block new code block
   * @param nullables map of expression to its nullability state
   * @see #exitBlock()
   */
  public final void nestBlock(BlockBuilder block,
      Map<RexNode, Boolean> nullables) {
    blocks.add(block);
    Map<RexNode, Boolean> prev = this.nullables.isEmpty()
        ? Collections.emptyMap()
        : this.nullables.get(this.nullables.size() - 1);
    Map<RexNode, Boolean> next;
    if (nullables == null || nullables.isEmpty()) {
      next = prev;
    } else {
      next = new HashMap<>(nullables);
      next.putAll(prev);
      next = Collections.unmodifiableMap(next);
    }
    this.nullables.add(next);
  }

  /**
   * Returns the current code block
   * @return current code block
   */
  public final BlockBuilder currentBlock() {
    return blocks.get(blocks.size() - 1);
  }

  /**
   * Returns the current nullability state of rex nodes.
   * The resulting value is the summary of all the maps in the block hierarchy.
   * @return current nullability state of rex nodes
   */
  public final Map<RexNode, Boolean> currentNullables() {
    return nullables.get(nullables.size() - 1);
  }

  /**
   * Leaves the current code block.
   * @see #nestBlock()
   */
  public final void exitBlock() {
    blocks.remove(blocks.size() - 1);
    nullables.remove(nullables.size() - 1);
  }
}

// End NestedBlockBuilderImpl.java
