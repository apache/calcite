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

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Implementation of {@link RelShuttle} that calls
 * {@link RelNode#accept(RelShuttle)} on each child, and
 * {@link RelNode#copy(org.apache.calcite.plan.RelTraitSet, java.util.List)} if
 * any children change. It also keeps track of parents by adding them to a
 * protected stack before visiting the child node. When the stack is not required,
 * user should consider using {@link RelBasicShuttle} instead.
 */
public class RelShuttleImpl extends RelBasicShuttle {
  protected final Deque<RelNode> stack = new ArrayDeque<>();

  @Override protected RelNode visitChild(RelNode parent, int i, RelNode child) {
    stack.push(parent);
    try {
      return super.visitChild(parent, i, child);
    } finally {
      stack.pop();
    }
  }
}

// End RelShuttleImpl.java
