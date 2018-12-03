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
package org.apache.calcite.test.fuzzer;

import org.apache.calcite.rex.RexNode;

/**
 * Tracks rex nodes used in {@link RexProgramFuzzyTest} to identify the ones
 * which take most time to simplify.
 */
class SimplifyTask implements Comparable<SimplifyTask> {
  public final RexNode node;
  public final long seed;
  public final RexNode result;
  public final long duration;

  SimplifyTask(RexNode node, long seed, RexNode result, long duration) {
    this.node = node;
    this.seed = seed;
    this.result = result;
    this.duration = duration;
  }

  @Override public int compareTo(SimplifyTask o) {
    if (duration != o.duration) {
      return Long.compare(duration, o.duration);
    }
    return Integer.compare(node.toString().length(), o.node.toString().length());
  }
}

// End SimplifyTask.java
