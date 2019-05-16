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

import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * Callback for an expression to dump itself to.
 *
 * <p>It is used for generating EXPLAIN PLAN output, and also for serializing
 * a tree of relational expressions to JSON.</p>
 */
public interface RelWriter {
  /**
   * Prints an explanation of a node, with a list of (term, value) pairs.
   *
   * <p>The term-value pairs are generally gathered by calling
   * {@link org.apache.calcite.rel.RelNode#explain(RelWriter)}.
   * Each sub-class of {@link org.apache.calcite.rel.RelNode}
   * calls {@link #input(String, org.apache.calcite.rel.RelNode)}
   * and {@link #item(String, Object)} to declare term-value pairs.</p>
   *
   * @param rel       Relational expression
   * @param valueList List of term-value pairs
   */
  void explain(RelNode rel, List<Pair<String, Object>> valueList);

  /**
   * @return detail level at which plan should be generated
   */
  SqlExplainLevel getDetailLevel();

  /**
   * Adds an input to the explanation of the current node.
   *
   * @param term  Term for input, e.g. "left" or "input #1".
   * @param input Input relational expression
   */
  default RelWriter input(String term, RelNode input) {
    return item(term, input);
  }

  /**
   * Adds an attribute to the explanation of the current node.
   *
   * @param term  Term for attribute, e.g. "joinType"
   * @param value Attribute value
   */
  RelWriter item(String term, Object value);

  /**
   * Adds an input to the explanation of the current node, if a condition
   * holds.
   */
  default RelWriter itemIf(String term, Object value, boolean condition) {
    return condition ? item(term, value) : this;
  }

  /**
   * Writes the completed explanation.
   */
  RelWriter done(RelNode node);

  /**
   * Returns whether the writer prefers nested values. Traditional explain
   * writers prefer flattened values.
   */
  default boolean nest() {
    return false;
  }
}

// End RelWriter.java
