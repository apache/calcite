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
package org.apache.calcite.rel;

import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.util.Litmus;

import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;

/**
 * Visitor that checks that every {@link RelNode} in a tree is valid.
 *
 * @see RelNode#isValid(Litmus, RelNode.Context)
 */
public class RelValidityChecker extends RelVisitor
    implements RelNode.Context {
  private int invalidCount;
  private final Deque<RelNode> stack = new ArrayDeque<>();

  @Override public Set<CorrelationId> correlationIds() {
    final ImmutableSet.Builder<CorrelationId> builder =
        ImmutableSet.builder();
    for (RelNode r : stack) {
      builder.addAll(r.getVariablesSet());
    }
    return builder.build();
  }

  @Override public void visit(RelNode node, int ordinal,
      @Nullable RelNode parent) {
    try {
      stack.push(node);
      if (!node.isValid(Litmus.THROW, this)) {
        ++invalidCount;
      }
      super.visit(node, ordinal, parent);
    } finally {
      stack.pop();
    }
  }

  public int invalidCount() {
    return invalidCount;
  }
}
