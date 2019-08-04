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

package org.apache.calcite.rel.hint;

/**
 * A collection of hint strategies.
 */
public abstract class HintStrategies {
  /** A hint strategy that indicates a hint can only be used to
   * the whole query(no specific nodes). */
  public static final HintStrategy SET_VAR =
      new NodeTypeHintStrategy(NodeTypeHintStrategy.NodeType.SET_VAR);

  /** A hint strategy that indicates a hint can only be used to
   * {@link org.apache.calcite.rel.core.Join} nodes. */
  public static final HintStrategy JOIN =
      new NodeTypeHintStrategy(NodeTypeHintStrategy.NodeType.JOIN);

  /** A hint strategy that indicates a hint can only be used to
   * {@link org.apache.calcite.rel.core.TableScan} nodes. */
  public static final HintStrategy TABLE_SCAN =
      new NodeTypeHintStrategy(NodeTypeHintStrategy.NodeType.TABLE_SCAN);

  /** A hint strategy that indicates a hint can only be used to
   * {@link org.apache.calcite.rel.core.Project} nodes. */
  public static final HintStrategy PROJECT =
      new NodeTypeHintStrategy(NodeTypeHintStrategy.NodeType.PROJECT);

  /**
   * Create a hint strategy from a specific matcher whose rules are totally customized.
   *
   * @param matcher The strategy matcher
   * @return A ExplicitHintStrategy instance.
   */
  public static HintStrategy explicit(ExplicitHintMatcher matcher) {
    return new ExplicitHintStrategy(matcher);
  }

  /**
   * Creates a HintStrategyCascade instance whose strategy rules are satisfied only if
   * all the {@code hintStrategies} are satisfied.
   */
  public static HintStrategy cascade(HintStrategy... hintStrategies) {
    return new HintStrategyCascade(hintStrategies);
  }
}

// End HintStrategies.java
