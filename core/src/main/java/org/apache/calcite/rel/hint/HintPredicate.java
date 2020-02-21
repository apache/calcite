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
 * A {@code HintPredicate} indicates whether a {@link org.apache.calcite.rel.RelNode}
 * can apply the specified hint.
 *
 * <p>Every supported hint should register a {@code HintPredicate}
 * into the {@link HintStrategyTable}. For example, {@link HintPredicates#JOIN} implies
 * that this hint would be propagated and applied to the {@link org.apache.calcite.rel.core.Join}
 * relational expressions.
 *
 * <p>A {@code HintPredicate} can be used independently or cascaded with other strategies
 * with method {@link HintPredicates#and}.
 *
 * <p>In {@link HintStrategyTable} the predicate is used for
 * hints registration.
 *
 * @see HintStrategyTable
 */
public interface HintPredicate {

  /**
   * Decides if the given {@code hint} can be applied to
   * the relational expression {@code rel}.
   *
   * @param hint The hint
   * @param rel  The relational expression
   * @return True if the {@code hint} can be applied to the {@code rel}
   */
  boolean apply(RelHint hint, RelNode rel);
}
