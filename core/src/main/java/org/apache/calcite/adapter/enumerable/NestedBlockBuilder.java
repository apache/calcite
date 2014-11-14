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

import java.util.Map;

/**
 * Allows to build nested code blocks with tracking of current context and the
 * nullability of particular {@link org.apache.calcite.rex.RexNode} expressions.
 *
 * @see org.apache.calcite.adapter.enumerable.StrictAggImplementor#implementAdd(AggContext, AggAddContext)
 */
public interface NestedBlockBuilder {
  /**
   * Starts nested code block. The resulting block can optimize expressions
   * and reuse already calculated values from the parent blocks.
   * @return new code block that can optimize expressions and reuse already
   * calculated values from the parent blocks.
   */
  BlockBuilder nestBlock();

  /**
   * Uses given block as the new code context.
   * The current block will be restored after {@link #exitBlock()} call.
   * @param block new code block
   * @see #exitBlock()
   */
  void nestBlock(BlockBuilder block);

  /**
   * Uses given block as the new code context and the map of nullability.
   * The current block will be restored after {@link #exitBlock()} call.
   * @param block new code block
   * @param nullables map of expression to its nullability state
   * @see #exitBlock()
   */
  void nestBlock(BlockBuilder block,
      Map<RexNode, Boolean> nullables);

  /**
   * Returns the current code block
   * @return current code block
   */
  BlockBuilder currentBlock();

  /**
   * Returns the current nullability state of rex nodes.
   * The resulting value is the summary of all the maps in the block hierarchy.
   * @return current nullability state of rex nodes
   */
  Map<RexNode, Boolean> currentNullables();

  /**
   * Leaves the current code block.
   * @see #nestBlock()
   */
  void exitBlock();
}

// End NestedBlockBuilder.java
