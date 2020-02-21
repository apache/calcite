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

import org.apache.calcite.rel.RelNode;

/**
 * A function to customize whether a relational expression should match a hint.
 *
 * <p>Usually you may not need to implement this function,
 * {@link NodeTypeHintPredicate} is enough for most of the {@link RelHint}s.
 *
 * <p>Some of the hints can only be matched to the relational
 * expression with special match conditions(not only the relational expression type).
 * i.e. "hash_join(r, st)", this hint can only be applied to JOIN expression that
 * has "r" and "st" as the input table names. To implement this, you can customize an
 * {@link ExplicitHintPredicate} with the {@link ExplicitHintMatcher}.
 *
 * @see ExplicitHintPredicate
 * @see HintPredicates
 */
@FunctionalInterface
public interface ExplicitHintMatcher {

  /**
   * Returns whether the given hint can attach to the relational expression.
   *
   * @param hint Hints
   * @param node Relational expression to test if the hint matches
   * @return true if the {@code hint} can attach to the {@code node}
   */
  boolean matches(RelHint hint, RelNode node);
}
